import asyncio
import logging
import os
import subprocess
import sys
from contextlib import nullcontext

import rdflib
from dotenv import load_dotenv

import aiorwlock
import httpx

from datastores.rdf.rdf_datastore import RDFDatastore, MAIN_GRAPH_IRI, UpdateType

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

module_dir = os.path.dirname(__file__)
load_dotenv(os.path.join(module_dir, '../../.env'))

QLEVER_PORT = os.environ.get("QLEVER_PORT")
QLEVER_ENDPOINT = os.environ.get("QLEVER_ADDRESS")

QLEVER_DIR = os.path.join(module_dir, "../../../qlever")
QLEVER_CONFIG_FILE_NAME = "Qleverfile_templated.CRC1625"
QLEVER_CONFIG_FILE_PATH = os.path.join(module_dir, "../../../qlever", QLEVER_CONFIG_FILE_NAME)
COMPLETE_QLEVER_CONFIG_FILE_NAME = "Qleverfile.CRC1625"
COMPLETE_QLEVER_CONFIG_FILE_PATH = os.path.join(module_dir, "../../../qlever", COMPLETE_QLEVER_CONFIG_FILE_NAME)

QLEVER_ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")

DOCKER_CONTAINER_NAME = "qlever.server.CRC1625"

class QleverRDFDatastore(RDFDatastore):
    """
    Wrapper for a Qlever instance deployed as a local docker container. In comparison to the Virtuoso wrapper, we purely use
    here SPARQL 1.1 and 1.2 alongside some custom auth headers, so it can be used as a template for other fully-spec compliant
    datastores

    All methods are fully async
    """

    def write_complete_qlever_file(self):
        with open(COMPLETE_QLEVER_CONFIG_FILE_PATH, 'w') as f:
            f.write(open(QLEVER_CONFIG_FILE_PATH, 'r').read()
                    .replace('{ACCESS_TOKEN}', QLEVER_ACCESS_TOKEN)
                    .replace('{QLEVER_PORT}', QLEVER_PORT))

    def __init__(self, *args, **kwargs):
        super().__init__()

        self.write_complete_qlever_file()

        # We lock everything with a read-write mutex to prevent deadlocks when using the web apps
        self.rwlock = aiorwlock.RWLock()

    async def launch_query(self, query: str):
        """
        Executes a SPARQL query and returns the HTTP response from the endpoint
        """
        async with self.rwlock.reader_lock:
            result = await httpx.AsyncClient(timeout=None).get(
                QLEVER_ENDPOINT,
                params={"query": query},
                headers={
                    "Authorization": f"Bearer {QLEVER_ACCESS_TOKEN}"
                }
            )

            if result.is_error:
                raise RuntimeError(f"Error occurred on query {query}: {result.status_code}, {result.text}")

        return result

    async def launch_updates(self,
                             actions: list[tuple[str, UpdateType]],
                             graph_iri: str = MAIN_GRAPH_IRI,
                             delete_files_after_upload: bool = False):
        """
        Launches a set of update queries with an exclusive lock
        """
        async with self.rwlock.writer_lock:
            for (action, update_type) in actions:
                if update_type == UpdateType.query:
                    await self.launch_update(action,
                                             use_lock=False)
                elif update_type == UpdateType.file_upload:
                    await self.upload_file(action,
                                           graph_iri=graph_iri,
                                           delete_file_after_upload=delete_files_after_upload,
                                           use_lock=False)

    async def launch_update(self, query: str, use_lock=True):
        """
        Launches a single update query
        """
        context_manager = self.rwlock.writer_lock if use_lock else nullcontext()
        async with context_manager:
            result = await httpx.AsyncClient(timeout=None).post(
                QLEVER_ENDPOINT,
                content = query,
                headers = {
                    "Authorization": f"Bearer {QLEVER_ACCESS_TOKEN}",
                    "Content-Type": "application/sparql-update"
                }
            )

            if result.is_error:
                raise RuntimeError(f"Error occurred on update {query}: {result.status_code}, {result.text}")

    async def _upload_file(self, file_path: str, graph_iri: str = MAIN_GRAPH_IRI):
        """
        Uploads a file using the SPARQL 1.2 Graph Store Protocol. The file must be in turtle (.ttl) format.
        """
        file_contents = open(file_path, 'r').read()

        # This shoud be illegal
        g = rdflib.Graph()
        g.parse(data=file_contents, format='ttl')
        nt_string = g.serialize(format='nt')

        query = f"""INSERT DATA {{ GRAPH <{graph_iri}> {{ {nt_string} }} }}"""


        response = await httpx.AsyncClient(timeout=None).post(
            QLEVER_ENDPOINT,
            content = query,
            headers = {
                "Authorization": f"Bearer {QLEVER_ACCESS_TOKEN}",
                "Content-Type": "application/sparql-update"
            }
        )

        if response.is_error:
            logging.error(f"Error when uploading file: {response.status_code}, {response.text}")


    async def bulk_file_load(self,
                             file_paths: list[str],
                             graph_iri:str = MAIN_GRAPH_IRI,
                             delete_files_after_upload=False,
                             use_lock=True):
        """
        Uploads RDF files to the SPARQL endpoint, optimized for speed by
        parallelizing requests if possible. The files must be in turtle (.ttl) format.

        If no graph IRI is specified, it will be stored in the CRC 1625 graph.
        """
        context_manager = self.rwlock.writer_lock if use_lock else nullcontext()
        async with context_manager:
            upload_tasks = [self._upload_file(file_path, graph_iri) for file_path in file_paths]

            await asyncio.gather(*upload_tasks)

            if delete_files_after_upload:
                for file in file_paths:
                    os.remove(file)

    async def upload_file(self,
                          file: str,
                          graph_iri: str = MAIN_GRAPH_IRI,
                          delete_file_after_upload=False,
                          use_lock=True):
        """
        Uploads an RDF file to the SPARQL endpoint.

        content_type is ignored for Virtuoso. File formats are handled internally

        If no graph IRI is specified, it will be stored in the CRC 1625 graph.
        """
        await self.bulk_file_load([file], graph_iri, delete_file_after_upload, use_lock)

    async def dump_triples(self, output_file: str | None = "datastore_dump.ttl"):
        """
        Output all triples to the designated file, in turtle (.ttl) format
        content_type is ignored for Virtuoso. File formats are handled internally
        """
        # In the case of Qlever, literals are untyped when written, no matter what...
        async with self.rwlock.reader_lock:
            query = """
            CONSTRUCT {
                ?s ?p ?o
            }
            WHERE {
                GRAPH <https://crc1625.mdi.ruhr-uni-bochum.de/graph> {
                    ?s ?p ?o
                }
            }
            """

            response = await httpx.AsyncClient(timeout=None).get(
                QLEVER_ENDPOINT,
                params={"query": query},
                headers={
                    "Authorization": f"Bearer {QLEVER_ACCESS_TOKEN}",
                    "Accept": "application/n-triples"
                }
            )

            if response.is_success:
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(response.text)
            else:
                logging.error(f"Error when dumping triples: {response.status_code}, {response.text}")

    async def clear_triples(self, graph_iri: str = MAIN_GRAPH_IRI):
        """
        Clear all CRC1625 KG triples from the graph, including its ontologies. The graph IRI can be changed
        to, e.g., clear the workflows graph
        """
        async with self.rwlock.writer_lock:
            # Doesn't seem to be supported...
            #query = f"""
            #CLEAR GRAPH <{graph_iri}>
            #"""

            query = f"""
            DELETE {{ GRAPH <{graph_iri}> {{ ?s ?p ?o }} }} WHERE {{ GRAPH <{graph_iri}> {{ ?s ?p ?o }} }}
            """

            response = await httpx.AsyncClient(timeout=None).post(
                QLEVER_ENDPOINT,
                content = query,
                headers = {
                    "Authorization": f"Bearer {QLEVER_ACCESS_TOKEN}",
                    "Content-Type": "application/sparql-update"
                }
            )
            if response.is_error:
                logging.error(f"Error when clearing triples: {response.status_code}, {response.text}")


    def stop_datastore(self, timeout: int = 60 * 5):
        """
        Stops the virtuoso container. The output is not checked, as sometimes virtuoso takes slightly more time to stop
        than docker is willing to wait, which we set to 60 seconds (up from default 10 seconds).

        The container should already exist.
        """
        cmd = [
            "qlever",
            "--qleverfile",
            COMPLETE_QLEVER_CONFIG_FILE_NAME,
            "stop"
        ]

        subprocess.run(cmd,
                       stdout=subprocess.DEVNULL,
                       #stderr=subprocess.DEVNULL, # For some reason, they print debug messages in stderr...
                       check=False,
                       cwd=QLEVER_DIR)


    def start_datastore(self, timeout: int = 60 * 5):
        """
        Starts the virtuoso container, and waits 5 minutes for it to allocate all its memory and be fully operational.
        This timeout can be controlled by the timeout parameter.

        The output of the command is not checked, and the container should already exist.
        """
        cmd = [
            "qlever",
            "--qleverfile",
            COMPLETE_QLEVER_CONFIG_FILE_NAME,
            "start",
            "--access-token",
            QLEVER_ACCESS_TOKEN,
        ]

        subprocess.run(cmd,
                       stdout=subprocess.DEVNULL,
                       #stderr=subprocess.DEVNULL,  # For some reason, they print debug messages in stderr...
                       check=False,
                       cwd=QLEVER_DIR)


    def restart_datastore(self, timeout: int = 60 * 5):
        """
        Restarts the virtuoso container.
        This timeout can be controlled by the timeout parameter, and is applied to the stopping and starting phases separately.

        The output of the command is not checked, and the container should already exist.
        """
        self.stop_datastore(timeout)
        self.start_datastore(timeout)

    def is_datastore_running(self) -> bool:
        """
        Returns True if the datastore's docker container exists and is running, False otherwise.
        """
        return super()._is_datastore_docker_container_running(DOCKER_CONTAINER_NAME)
