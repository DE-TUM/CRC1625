import glob
import logging
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import as_completed, ThreadPoolExecutor
from contextlib import nullcontext
from dotenv import load_dotenv

import aiorwlock
import httpx

from kg_construction_and_validation.datastores.rdf.rdf_datastore import RDFDatastore, GRAPH_IRI, UpdateType

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

module_dir = os.path.dirname(__file__)
load_dotenv(os.path.join(module_dir, '../../.env'))

VIRTUOSO_ADDRESS = os.environ.get("VIRTUOSO_ADDRESS")
QUERY_ENDPOINT = f"{VIRTUOSO_ADDRESS}/sparql"
UPDATE_ENDPOINT = f"{VIRTUOSO_ADDRESS}/sparql"
VIRTUOSO_USER = os.environ.get("VIRTUOSO_USER")
VIRTUOSO_PASS = os.environ.get("VIRTUOSO_PASS")
ODBC_PORT = os.environ.get("VIRTUOSO_ODBC_PORT")


HOST_DATA_DIR = os.path.join(module_dir, "../../../virtuoso/data")
CONTAINER_DATA_DIR = "/data"

class VirtuosoRDFDatastore(RDFDatastore):
    """
    Wrapper for a Virtuoso instance deployed as a local docker container

    All methods are fully async
    """

    def __init__(self, *args, **kwargs):
        super().__init__()

        # We lock everything with a read-write mutex to prevent deadlocks when using the web apps
        self.rwlock = aiorwlock.RWLock()

    async def launch_query(self, query: str):
        """
        Executes a SPARQL query and returns the HTTP response from the endpoint
        """
        async with self.rwlock.reader_lock:
            result = await httpx.AsyncClient(timeout=None).post(
                QUERY_ENDPOINT,
                # https://github.com/openlink/virtuoso-opensource/issues/950
                params={"query": "DEFINE sql:signal-void-variables 0\n" + query},
                # params={
                #    "query": query,
                #    "signal_void": "off",
                #    "signal_unconnected": "off"
                # },
                headers={"Accept": "application/sparql-results+json"},
                auth=(VIRTUOSO_USER, VIRTUOSO_PASS)
            )

        if result.status_code != 200:
            raise RuntimeError(f"Error occurred on query {query}: {result.text}")

        return result

    async def launch_updates(self,
                             actions: list[tuple[str, UpdateType]],
                             graph_iri: str = GRAPH_IRI,
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
                UPDATE_ENDPOINT,
                # https://github.com/openlink/virtuoso-opensource/issues/950
                data=("DEFINE sql:signal-void-variables 0\n" + query).encode("utf-8"),
                # data=query.encode("utf-8"),
                # params={
                #    "signal_void": "off",
                #    "signal_unconnected": "off"
                # },
                headers={"Content-Type": "application/sparql-update"},
                auth=(VIRTUOSO_USER, VIRTUOSO_PASS)
            )
            if result.status_code not in [200, 204]:
                raise RuntimeError(f"Error occurred on query {query}: {result.text}")

    def _run_isql(self, command: str):
        """
        Run a Virtuoso command over isql
        """
        cmd = [
            "docker",
            "exec",
            "-i",
            "virtuoso_CRC_1625",
            "isql",
            ODBC_PORT,
            VIRTUOSO_USER,
            VIRTUOSO_PASS,
            f"exec={command}"
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)

    def _register_file(self, file_path: str):
        """
        Copies the .ttl file to the Virtuoso data folder, for later processing, and returns its file path

        The actual registration into Virtuoso's bulk loader is done over the entire
        folder after registering all files
        """
        filename = os.path.basename(file_path)
        target_path = os.path.join(HOST_DATA_DIR, filename)
        shutil.copy(file_path, target_path)

        return target_path

    async def bulk_file_load(self,
                             file_paths: list[str],
                             graph_iri:str = GRAPH_IRI,
                             delete_files_after_upload=False,
                             use_lock=True):
        """
        Uploads RDF files to the SPARQL endpoint, optimized for speed by
        parallelizing requests if possible

        If no graph IRI is specified, it will be stored in the CRC 1625 graph.
        """
        context_manager = self.rwlock.writer_lock if use_lock else nullcontext()
        async with context_manager:
            # Clear the existing files. For example, we may not want to upload
            # leftover ontology files when validating the mappings output
            for file_path in glob.glob(os.path.join(HOST_DATA_DIR, "*")):
                if os.path.isfile(file_path):
                    os.remove(file_path)

            registered_file_paths = []
            for file in file_paths:
                registered_file_paths.append(self._register_file(file))

            # Write a file called global.graph in CONTAINER_DATA_DIR containing only GRAPH_IRI as its contents
            with open(os.path.join(HOST_DATA_DIR, "global.graph"), "w") as f:
                f.write(graph_iri)

            self._run_isql(f"DELETE FROM DB.DBA.load_list;")  # This took a while to discover...
            self._run_isql(f"ld_dir('{CONTAINER_DATA_DIR}', '*.ttl', '{graph_iri}');")

            with ThreadPoolExecutor(max_workers=16) as executor:
                futures = [executor.submit(self._run_isql, "rdf_loader_run();") for _ in range(0, 16)]
                for future in as_completed(futures):
                    future.result()

            self._run_isql("checkpoint;")

            if delete_files_after_upload:
                for file in file_paths:
                    os.remove(file)

                for file in registered_file_paths:
                    os.remove(file)

    async def upload_file(self,
                          file: str,
                          graph_iri: str = GRAPH_IRI,
                          delete_file_after_upload=False,
                          use_lock=True):
        """
        Uploads an RDF file to the SPARQL endpoint.

        content_type is ignored for Virtuoso. File formats are handled internally

        If no graph IRI is specified, it will be stored in the CRC 1625 graph.
        """
        await self.bulk_file_load([file], graph_iri, delete_file_after_upload, use_lock)

    async def dump_triples(self, output_file: str | None = "datastore_dump.nt"):
        """
        Output all triples to the designated file, in Ntriples format
        content_type is ignored for Virtuoso. File formats are handled internally
        """
        async with self.rwlock.reader_lock:
            query = """
            DEFINE output:format "NT"
    
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
                QUERY_ENDPOINT,
                params={"query": query},
                auth=(VIRTUOSO_USER, VIRTUOSO_PASS),
                headers={"Accept": "text/ntriples"}
            )

            if response.status_code == 200:
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(response.text)
            else:
                logging.error(f"Error when dumping triples: {response.status_code}, {response.text}")

    async def clear_triples(self, graph_iri: str = GRAPH_IRI):
        """
        Clear all CRC1625 KG triples from the graph, including its ontologies. The graph IRI can be changed
        to, e.g., clear the workflows graph
        """
        async with self.rwlock.writer_lock:
            self._run_isql("log_enable(3,1);")  # Autocommit mode, write transactions to log. Avoids running out of memory on large graphs
            # self.run_isql("SPARQL CLEAR GRAPH  <https://crc1625.mdi.ruhr-uni-bochum.de/graph>;")
            self._run_isql(f"DELETE FROM rdf_quad WHERE g = iri_to_id ('{graph_iri}');")
            self._run_isql("checkpoint;")


    def stop_datastore(self, timeout: int = 60 * 5):
        """
        Stops the virtuoso container. The output is not checked, as sometimes virtuoso takes slightly more time to stop
        than docker is willing to wait, which we set to 60 seconds (up from default 10 seconds).

        The container should already exist.
        """
        cmd = [
            "docker",
            "stop",
            "-t",
            str(timeout),
            "virtuoso_CRC_1625"
        ]
        subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL)


    def start_datastore(self, timeout: int = 60 * 5):
        """
        Starts the virtuoso container, and waits 5 minutes for it to allocate all its memory and be fully operational.
        This timeout can be controlled by the timeout parameter.

        The output of the command is not checked, and the container should already exist.
        """
        cmd = [
            "docker",
            "start",
            "virtuoso_CRC_1625"
        ]
        subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL)

        time.sleep(timeout)


    def restart_datastore(self, timeout: int = 60 * 5):
        """
        Restarts the virtuoso container.
        This timeout can be controlled by the timeout parameter, and is applied to the stopping and starting phases separately.

        The output of the command is not checked, and the container should already exist.
        """
        self.stop_datastore(timeout)
        self.start_datastore(timeout)
