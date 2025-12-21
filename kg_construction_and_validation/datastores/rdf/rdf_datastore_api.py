import logging
import os
import sys
import uuid
from enum import Enum
from typing import List, Tuple, Dict
from dotenv import load_dotenv

import uvicorn
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel

from datastores.rdf.qlever_datastore import QleverRDFDatastore
from datastores.rdf.rdf_datastore import UpdateType, RDFDatastore, MAIN_GRAPH_IRI
from datastores.rdf.virtuoso_datastore import VirtuosoRDFDatastore

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

module_dir = os.path.dirname(__file__)
load_dotenv(os.path.join(module_dir, '../../.env'))

RDF_DATASTORE_API_HOST = os.environ.get("RDF_DATASTORE_API_HOST")
RDF_DATASTORE_API_PORT = os.environ.get("RDF_DATASTORE_API_PORT")

class DatastoreType(Enum):
    VIRTUOSO = "virtuoso"
    QLEVER = "qlever"


rdf_store: RDFDatastore = VirtuosoRDFDatastore()
rdf_store_type: DatastoreType = DatastoreType.VIRTUOSO
app = FastAPI()


class QueryRequest(BaseModel):
    query: str


class UpdateAction(BaseModel):
    action: str
    update_type: str


class UpdatesRequest(BaseModel):
    actions: List[Tuple[str, UpdateType, str | None]]
    graph_iri: str = ""


class FileUploadRequest(BaseModel):
    file_as_str: str
    file_extension: str
    graph_iri: str = MAIN_GRAPH_IRI


class BulkFileUploadRequest(BaseModel):
    files_as_str: List[Tuple[str, str]]
    graph_iri: str = MAIN_GRAPH_IRI
    use_lock: bool = True


class DumpRequest(BaseModel):
    output_file: str = "datastore_dump.nt"


def is_in_docker_deployment():
    return os.environ.get('IN_DOCKER_DEPLOYMENT', False)


def get_random_file_name(file_extension: str):
    return f"{uuid.uuid4()}.{file_extension}"

@app.post("/launch_query")
async def rpc_launch_query(payload: QueryRequest):
    """
    Executes a SPARQL query and returns the JSON response from the endpoint
    """
    try:
        result = await rdf_store.launch_query(payload.query)

        return {"status": result.status_code, "data": result.json()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/launch_updates")
async def rpc_launch_updates(payload: UpdatesRequest):
    """
    Launches a set of update queries with an exclusive lock. Note that this is not a transaction, i.e. there is no rollback
    mechanism if any of the updates fails
    """
    try:
        actions = []
        for query_or_file_str, update_type, file_extension_or_none in payload.actions:
            if update_type == UpdateType.query:
                actions.append((query_or_file_str, update_type))
            else:
                random_filename = get_random_file_name(file_extension_or_none)
                open(f"{random_filename}", "w").write(query_or_file_str)
                actions.append((random_filename, update_type))

        await rdf_store.launch_updates(
            actions,
            graph_iri=payload.graph_iri,
            delete_files_after_upload=True # We write a tempfile at the virtuoso endpoint
        )

        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload_file")
async def rpc_upload_file(payload: FileUploadRequest):
    """
    Uploads a local RDF file to the SPARQL endpoint.

    If no graph IRI is specified, it will be stored in the CRC 1625 graph.

    TODO: This is all local and assumes that the server has access to the file path, no files
          are uploaded for now
    """
    try:
        random_filename = get_random_file_name(payload.file_extension)
        open(f"{random_filename}", "w").write(payload.file_as_str)

        await rdf_store.upload_file(
            file=random_filename,
            graph_iri=payload.graph_iri,
            delete_file_after_upload=True # We write a tempfile at the virtuoso endpoint
        )

        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/bulk_file_load")
async def rpc_bulk_file_load(payload: BulkFileUploadRequest):
    """
    Uploads a collection of local RDF files to the SPARQL endpoint.

    If no graph IRI is specified, it will be stored in the CRC 1625 graph.
    """
    try:
        file_paths = []
        for file_as_str, extension in payload.files_as_str:
            random_filename = get_random_file_name(extension)
            open(f"{random_filename}", "w").write(file_as_str)
            file_paths.append(random_filename)

        await rdf_store.bulk_file_load(
            file_paths = file_paths,
            graph_iri = payload.graph_iri,
            delete_files_after_upload = True, # We write a tempfile at the virtuoso endpoint
            use_lock=payload.use_lock
        )

        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/dump_triples")
async def rpc_dump_triples(payload: DumpRequest):
    """
    Output all triples to the designated file, in turtle (.ttl) format
    """
    try:
        await rdf_store.dump_triples(output_file=payload.output_file)

        return {"status": "success", "file": payload.output_file}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/clear_triples")
async def rpc_clear_triples(graph_iri: str = Body(embed=True)):
    """
    Clears all triples from the graph
    """
    try:
        await rdf_store.clear_triples(graph_iri=graph_iri)

        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/run_isql")
async def rpc_run_isql(isql: str = Body(embed=True)):
    """
    Run an ISQL command on the endpoint.
    This is only applicable if the KG is running under Virtuoso, and will fail otherwise
    """
    if isinstance(rdf_store, VirtuosoRDFDatastore):
        rdf_store._run_isql(isql)
        return {"status": "success"}
    else:
        raise HTTPException(status_code=500, detail="ISQL commands are only possible when running Virtuoso.")

@app.get("/start_datastore")
async def rpc_start_datastore() -> Dict[str, str]:
    """
    Starts the underlying RDF datastore
    """
    if is_in_docker_deployment():
        return {"status": "success"}

    try:
        await rdf_store.start_datastore()

        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stop_datastore")
async def rpc_stop_datastore() -> Dict[str, str]:
    """
    Stops the underlying RDF datastore. Does not affect the remote API endpoint itself
    """
    if is_in_docker_deployment():
        return {"status": "success"}

    try:
        await rdf_store.stop_datastore()

        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/restart_datastore")
async def rpc_restart_datastore() -> Dict[str, str]:
    """
    Restarts the underlying RDF datastore. Does not affect the remote API endpoint itself
    """
    if is_in_docker_deployment():
        return {"status": "success"}

    try:
        await rdf_store.restart_datastore()

        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_datastore_type")
async def rpc_get_datastore_type() -> Dict[str, str]:
    """
    Returns the name of the underlying RDF datastore
    """
    return {"status": "success", "data": rdf_store_type.value}


def run(rdf_store_to_serve: DatastoreType,
        debug: bool = False):
    global rdf_store
    global rdf_store_type

    rdf_store_type = rdf_store_to_serve

    if rdf_store_to_serve == DatastoreType.VIRTUOSO:
        rdf_store = VirtuosoRDFDatastore()
    elif rdf_store_to_serve == DatastoreType.QLEVER:
        rdf_store = QleverRDFDatastore()
    else:
        raise ValueError("Unknown RDF datastore type selected")

    if not is_in_docker_deployment() and not rdf_store.is_datastore_running():
        logging.info("The datastore is not running. Starting it...")
        rdf_store.start_datastore()

    log_level = 'warning'
    access_log = False
    if debug:
        log_level = 'debug'
        access_log = True

    uvicorn.run(app,
                host="0.0.0.0",
                port=int(RDF_DATASTORE_API_PORT),
                # The RDF store is already async, so we don't
                # want to conflict with multiple threadpools
                workers=1,
                log_level=log_level,
                access_log=access_log)
