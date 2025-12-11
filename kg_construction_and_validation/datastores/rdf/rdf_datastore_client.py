import asyncio
import os
from pathlib import Path
from typing import List, Tuple, Coroutine
from dotenv import load_dotenv

import httpx

from datastores.rdf.rdf_datastore import UpdateType, MAIN_GRAPH_IRI

module_dir = os.path.dirname(__file__)
load_dotenv(os.path.join(module_dir, '../../.env'))

RDF_DATASTORE_API_HOST = os.environ.get("RDF_DATASTORE_API_HOST")
RDF_DATASTORE_API_PORT = os.environ.get("RDF_DATASTORE_API_PORT")
RDF_DATASTORE_API_ENDPOINT = os.environ.get("RDF_DATASTORE_API_ENDPOINT")


"""
RDF datastore client functions that interact with a (possibly remote) RDF datastore API

All methods are fully async. The run_sync() method can be used to execute any of the calls synchronously
"""
async def _post(endpoint: str, payload: dict, return_full_response: bool = False):
    url = f"{RDF_DATASTORE_API_ENDPOINT}/{endpoint}"
    try:
        async with httpx.AsyncClient(timeout=None) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            if return_full_response:
                return response.json()
            else:
                return response

    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"Remote call failed: {e.response.text}") from e

    except httpx.RequestError as e:
        raise RuntimeError(f"Connection error: {e}") from e


async def _get(endpoint: str, return_full_response: bool = False):
    url = f"{RDF_DATASTORE_API_ENDPOINT}/{endpoint}"
    try:
        async with httpx.AsyncClient(timeout=None) as client:
            response = await client.get(url)
            response.raise_for_status()
            if return_full_response:
                return response.json()
            else:
                return response

    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"Remote call failed: {e.response.text}") from e

    except httpx.RequestError as e:
        raise RuntimeError(f"Connection error: {e}") from e


async def launch_query(query: str,
                       return_full_response: bool = False):
    """
    Executes a SPARQL query and returns the JSON response from the endpoint
    """
    if return_full_response:
        return await _post("launch_query", {"query": query}, return_full_response=return_full_response)
    else:
        return (await _post("launch_query", {"query": query})).json()['data']


async def launch_update(query: str,
                        graph_iri: str = ""):
    """
    Launches an update query with an exclusive writer lock
    """
    payload = {
        "actions": [(query, UpdateType.query, None)],
        "graph_iri": graph_iri
    }
    return await _post("launch_updates", payload)


async def launch_updates(actions: List[Tuple[str, UpdateType]],
                         graph_iri: str = "",
                         delete_files_after_upload: bool = False):
    """
    Launches a set of update queries with an exclusive writer lock. Note that this is not a transaction, i.e. there is no rollback
    mechanism if any of the updates fails.

    Any file upload update will its file read and uploaded to the API transparently
    """
    actions_to_send = []
    for query_or_file_path, update_type in actions:
        if update_type == UpdateType.query:
            actions_to_send.append((query_or_file_path, update_type, None))
        else:
            actions_to_send.append((open(query_or_file_path, 'r').read(), update_type, Path(query_or_file_path).suffix[1:]))

    payload = {
        "actions": actions_to_send,
        "graph_iri": graph_iri
    }
    response = await _post("launch_updates", payload)

    if response.is_success and delete_files_after_upload:
        for query_or_file_path, update_type in actions:
            if update_type == UpdateType.file_upload:
                os.remove(query_or_file_path)

    return response

async def upload_file(file_path: str,
                      graph_iri: str = MAIN_GRAPH_IRI,
                      delete_file_after_upload: bool = False):
    """
    Uploads a local RDF file to the SPARQL endpoint. The file must be in turtle (.ttl) format.

    If no graph IRI is specified, it will be stored in the CRC 1625 graph.

    The file is read and uploaded to the API transparently.
    """
    payload = {
        "file_as_str": open(file_path, 'r').read(),
        "file_extension": Path(file_path).suffix[1:],
        "graph_iri": graph_iri
    }
    response = await _post("upload_file", payload)

    if response.is_success and delete_file_after_upload:
        os.remove(file_path)

    return response


async def bulk_file_load(file_paths: list[str],
                         delete_files_after_upload: bool = False,
                         use_lock: bool = True,
                         graph_iri: str = MAIN_GRAPH_IRI):
    """
    Uploads a collection of local RDF files to the SPARQL endpoint. The files must be in turtle (.ttl) format.

    If no graph IRI is specified, it will be stored in the CRC 1625 graph.

    The files are read and uploaded to the API transparently.
    """
    payload = {
        "files_as_str" : [(open(file_path, 'r').read(), Path(file_path).suffix[1:]) for file_path in file_paths],
        "graph_iri" : graph_iri,
        "delete_files_after_upload" : True, # We write a tempfile at the virtuoso endpoint
        "use_lock": use_lock
    }
    response = await _post("bulk_file_load", payload)

    if response.is_success and delete_files_after_upload:
        for file_path in file_paths:
            os.remove(file_path)

    return response

async def dump_triples(output_file: str = "datastore_dump.ttl"):
    """
    Output all triples to the designated file, in turtle (.ttl) format

    WARNING: This is a debugging, local-only function (intended to be run as part of the testing in the same host as the RDF store)
    """
    return await _post("dump_triples", {"output_file": output_file})

async def clear_triples(graph_iri: str = MAIN_GRAPH_IRI):
    """
    Clears all triples from the graph
    """
    return await _post("clear_triples", {"graph_iri": graph_iri})

async def run_isql(isql: str):
    """
    Run an ISQL command on the endpoint.
    This is only applicable if the KG is running under Virtuoso, and will fail otherwise
    """
    return await _post("run_isql", {"isql": isql})

async def start_datastore():
    """
    Returns the name of the underlying RDF datastore
    """
    return (await _get("start_datastore")).json()['data']

async def stop_datastore():
    """
    Returns the name of the underlying RDF datastore
    """
    return (await _get("stop_datastore")).json()['data']

async def restart_datastore():
    """
    Returns the name of the underlying RDF datastore
    """
    return (await _get("restart_datastore")).json()['data']

async def get_datastore_type():
    """
    Returns the name of the underlying RDF datastore
    """
    return (await _get("get_datastore_type")).json()['data']

def run_sync(coroutine : Coroutine):
    """
    Runs any of the above methods synchronously. You can also simply use asyncio.run() directly.
    """
    return asyncio.run(coroutine)
