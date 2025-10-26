from abc import ABC, abstractmethod

from requests import Response


class RDFDatastore(ABC):
    """
    Abstract class that allows future RDF stores to be employed. Right now, OxigraphRDFDatastore and VirtuosoRDFDatastore
    implement this class.
    """
    @abstractmethod
    def launch_query(self, query: str) -> Response:
        """
        Executes a SPARQL query and returns the HTTP response from the endpoint
        """
        pass

    @abstractmethod
    def launch_update(self, query: str):
        """
        Executes a SPARQL update
        """
        pass

    @abstractmethod
    def upload_file(self, file_path: str, content_type: str | None="text/turtle", graph_iri: str = ""):
        """
        Uploads an RDF file to the SPARQL endpoint
        """
        pass

    @abstractmethod
    def bulk_file_load(self, file_paths: list[str]):
        """
        Uploads RDF files to the SPARQL endpoint, optimized for speed by
        parallelizing requests if possible. All files should be in .ttl format
        """
        pass

    @abstractmethod
    def dump_triples(self, output_file: str | None ="datastore_dump.nt"):
        """
        Output all triples to the designated file, in Ntriples format
        (for compatibility reasons wrt/ Virtuoso)
        """
        pass

    @abstractmethod
    def clear_triples(self, graph_iri="https://crc1625.mdi.ruhr-uni-bochum.de/graph"):
        """
        Clear all CRC1625 KG triples from the graph, including its ontologies
        """
        pass