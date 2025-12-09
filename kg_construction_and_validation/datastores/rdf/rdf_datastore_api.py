import os
from enum import Enum
from typing import List, Optional, Tuple, Any, Dict
from dotenv import load_dotenv

import uvicorn
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel

from datastores.rdf.rdf_datastore_client import UpdateType
from datastores.rdf.virtuoso_datastore import VirtuosoRDFDatastore

module_dir = os.path.dirname(__file__)
load_dotenv(os.path.join(module_dir, '../../.env'))

RDF_DATASTORE_API_HOST = os.environ.get("RDF_DATASTORE_API_HOST")
RDF_DATASTORE_API_PORT = os.environ.get("RDF_DATASTORE_API_PORT")

class DatastoreType(Enum):
    VIRTUOSO = "virtuoso"
    QLEVER = "qlever"


rdf_store: Optional[Any] = None
rdf_store_type: DatastoreType = None
app = FastAPI()


class QueryRequest(BaseModel):
    query: str


class UpdateAction(BaseModel):
    action: str
    update_type: str


class UpdatesRequest(BaseModel):
    actions: List[Tuple[str, UpdateType]]
    graph_iri: str = ""
    delete_files_after_upload: bool = False


class FileUploadRequest(BaseModel):
    file_path: str
    graph_iri: str = "https://crc1625.mdi.ruhr-uni-bochum.de/graph"
    delete_files_after_upload: bool = False


class BulkFileUploadRequest(BaseModel):
    file_paths: List[str]
    graph_iri: str = "https://crc1625.mdi.ruhr-uni-bochum.de/graph"
    delete_files_after_upload: bool = False
    use_lock: bool = True


class DumpRequest(BaseModel):
    output_file: str = "datastore_dump.nt"


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
        await rdf_store.launch_updates(
            payload.actions,
            graph_iri=payload.graph_iri,
            delete_files_after_upload=payload.delete_files_after_upload
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
        await rdf_store.upload_file(
            file=payload.file_path,
            graph_iri=payload.graph_iri,
            delete_file_after_upload=payload.delete_files_after_upload
        )

        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/bulk_file_load")
async def rpc_bulk_file_load(payload: BulkFileUploadRequest):
    """
    Uploads a collection of local RDF files to the SPARQL endpoint.

    If no graph IRI is specified, it will be stored in the CRC 1625 graph.

    TODO: This is all local and assumes that the server has access to the file path, no files
          are uploaded for now
    """
    try:
        await rdf_store.bulk_file_load(
            file_paths = payload.file_paths,
            graph_iri = payload.graph_iri,
            delete_files_after_upload = payload.delete_files_after_upload
        )

        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/dump_triples")
async def rpc_dump_triples(payload: DumpRequest): # TODO this is local for now
    """
    Output all triples to the designated file, in Ntriples format
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


def run(rdf_store_to_serve: DatastoreType, debug: bool = False):
    global rdf_store
    global rdf_store_type

    rdf_store_type = rdf_store_to_serve

    if rdf_store_to_serve == DatastoreType.VIRTUOSO:
        rdf_store = VirtuosoRDFDatastore()
    else:
        raise ValueError("No other datastores are supported yet")

    log_level = 'warning'
    access_log = False
    if debug:
        log_level = 'debug'
        access_log = True

    uvicorn.run(app,
                host=RDF_DATASTORE_API_HOST,
                port=int(RDF_DATASTORE_API_PORT),
                # The RDF store is already async, so we don't
                # want to conflict with multiple threadpools
                workers=1,
                log_level=log_level,
                access_log=access_log)
