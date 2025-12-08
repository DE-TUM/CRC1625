import asyncio
import os
from enum import Enum
from typing import List, Tuple
from dotenv import load_dotenv

import httpx


class UpdateType(str, Enum):
    query = "query"
    file_upload = "file_upload"


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
                        graph_iri: str = "",
                        delete_files_after_upload: bool = False):
    """
    Launches an update query with an exclusive writer lock
    """
    payload = {
        "actions": [(query, UpdateType.query)],
        "graph_iri": graph_iri,
        "delete_files_after_upload": delete_files_after_upload
    }
    return await _post("launch_updates", payload)


async def launch_updates(actions: List[Tuple[str, UpdateType]],
                         graph_iri: str = "",
                         delete_files_after_upload: bool = False):
    """
    Launches a set of update queries with an exclusive writer lock. Note that this is not a transaction, i.e. there is no rollback
    mechanism if any of the updates fails
    """
    payload = {
        "actions": actions,
        "graph_iri": graph_iri,
        "delete_files_after_upload": delete_files_after_upload
    }
    return await _post("launch_updates", payload)

async def upload_file(file_path: str,
                      graph_iri: str = "https://crc1625.mdi.ruhr-uni-bochum.de/graph",
                      delete_file_after_upload: bool = False):
    """
    Uploads a local RDF file to the SPARQL endpoint.

    If no graph IRI is specified, it will be stored in the CRC 1625 graph.

    TODO: This is all local and assumes that the server has access to the file path, no files
          are uploaded for now
    """
    payload = {
        "file_path": file_path,
        "graph_iri": graph_iri,
        "delete_file_after_upload": delete_file_after_upload
    }
    return await _post("upload_file", payload)


async def bulk_file_load(file_paths: list[str],
                         graph_iri="https://crc1625.mdi.ruhr-uni-bochum.de/graph",
                         delete_files_after_upload=False,
                         use_lock=True):
    """
    Uploads a collection of local RDF files to the SPARQL endpoint.

    If no graph IRI is specified, it will be stored in the CRC 1625 graph.

    TODO: This is all local and assumes that the server has access to the file path, no files
          are uploaded for now
    """
    payload = {
        "file_paths" : file_paths,
        "graph_iri" : graph_iri,
        "delete_files_after_upload" : delete_files_after_upload
    }
    return await _post("bulk_file_load", payload)

async def dump_triples(output_file: str = "datastore_dump.nt"): # TODO this is local for now
    """
    Output all triples to the designated file, in Ntriples format
    """
    return await _post("dump_triples", {"output_file": output_file})

async def clear_triples(graph_iri: str = "https://crc1625.mdi.ruhr-uni-bochum.de/graph"):
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

async def get_datastore_type():
    """
    Returns the name of the underlying RDF datastore
    """
    return (await _get("get_datastore_type")).json()['data']

def run_sync(coroutine):
    """
    Runs any of the above methods synchronously
    """
    return asyncio.run(coroutine)
