from abc import ABC, abstractmethod
from enum import Enum

from requests import Response

class UpdateType(str, Enum):
    query = "query"
    file_upload = "file_upload"

GRAPH_IRI = "https://crc1625.mdi.ruhr-uni-bochum.de/graph"

class RDFDatastore(ABC):
    """
    Abstract class for operating with an RDF store
    """
    @abstractmethod
    async def launch_query(self, query: str) -> Response:
        """
        Executes a SPARQL query and returns the HTTP response from the endpoint
        """
        pass

    @abstractmethod
    async def launch_updates(self,
                             actions: list[tuple[str, UpdateType]],
                             graph_iri: str = GRAPH_IRI,
                             delete_files_after_upload: bool = False):
        """
        Launches a set of update queries. They must be run with an exclusive lock (~ a transaction without rollbacks)
        """
        pass

    async def launch_update(self, query: str, use_lock=True):
        """
        Launches a single update query
        """
        pass

    async def bulk_file_load(self,
                             file_paths: list[str],
                             graph_iri=GRAPH_IRI,
                             delete_files_after_upload=False,
                             use_lock=True):
        """
        Uploads RDF files to the SPARQL endpoint, optimized for speed by
        parallelizing requests if possible

        If no graph IRI is specified, it will be stored in the CRC 1625 graph.
        """
        pass

    @abstractmethod
    async def upload_file(self,
                          file: str,
                          graph_iri=GRAPH_IRI,
                          delete_file_after_upload=False,
                          use_lock=True):
        """
        Uploads an RDF file to the SPARQL endpoint.

        content_type is ignored for Virtuoso. File formats are handled internally

        If no graph IRI is specified, it will be stored in the CRC 1625 graph.
        """
        pass

    @abstractmethod
    def dump_triples(self, output_file: str | None ="datastore_dump.nt"):
        """
        Output all triples to the designated file, in Ntriples format
        """
        pass

    @abstractmethod
    async def clear_triples(self, graph_iri: str = GRAPH_IRI):
        """
        Clear all CRC1625 KG triples from the graph, including its ontologies. The graph IRI can be changed
        to, e.g., clear the workflows graph
        """
        pass

    @abstractmethod
    def stop_datastore(self, timeout: int = 60 * 5):
        """
        Stops the RDF store. An optional timeout can be used to safely wait for its completion.
        """
        pass

    @abstractmethod
    def start_datastore(self, timeout: int = 60 * 5):
        """
        Starts the RDF store. An optional timeout can be used to safely wait for its completion.
        """
        pass

    @abstractmethod
    def restart_datastore(self, timeout: int = 60 * 5):
        """
        Restarts the RDF store. An optional timeout can be used to safely wait for its completion.
        """
        pass