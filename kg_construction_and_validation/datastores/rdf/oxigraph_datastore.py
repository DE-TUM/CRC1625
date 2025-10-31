import logging
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from rdflib import Graph, Namespace
from requests import Response

from .rdf_datastore import RDFDatastore

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

GRAPH_IRI = "https://crc1625.mdi.ruhr-uni-bochum.de/graph"

class OxigraphRDFDatastore(RDFDatastore):
    DOCKER_CONTAINER_NAME = "oxigraph_crc1625"
    MAX_SPARQL_WORKERS = 16

    def launch_query(self, query: str) -> Response:
        """
        Executes a SPARQL query and returns the HTTP response from the endpoint
        """
        endpoint = "http://0.0.0.0:7878/query"
        result = requests.post(endpoint,
                               data={"query": query},
                               headers={"Accept": "application/sparql-results+json"})

        if result.status_code != 200:
            raise RuntimeError(f"Error occurred on query {query}: {result.text}")

        return result


    def launch_update(self, query: str):
        """
        Executes a SPARQL update
        """
        endpoint = "http://0.0.0.0:7878/update"
        headers = {"Content-Type": "application/sparql-update"}
        result = requests.post(endpoint, data=query, headers=headers)

        if result.status_code not in [200, 204]:
            raise RuntimeError(f"Error occurred on query {query}: {result.text}")


    def upload_file(self, file_path, content_type: str | None="text/turtle", graph_iri=GRAPH_IRI):
        """
        Uploads an RDF file to the SPARQL endpoint.

        If no graph IRI is specified, it will be stored in the CRC 1625 graph.
        """
        with open(file_path, 'rb') as f:
            response = requests.post(
                f"http://0.0.0.0:7878/store?graph={graph_iri}",
                headers={"Content-Type": content_type},
                data=f
            )

            if response.status_code not in [200, 201, 204]:
                raise RuntimeError(f"Error occurred when loading {file_path} (Status code: {response.status_code})")
            else:
                logging.info(f"Loaded {file_path} (Status code: {response.status_code})")


    def bulk_file_load(self, file_paths: str | None, graph_iri=GRAPH_IRI):
        """
        Uploads RDF files to the SPARQL endpoint, optimized for speed by
        parallelizing requests. All files should be in .ttl format.

        If no graph IRI is specified, it will be stored in the CRC 1625 graph.
        """
        with ThreadPoolExecutor(max_workers=self.MAX_SPARQL_WORKERS) as executor:
            futures = [executor.submit(self.upload_file, file_path, content_type="text/turtle", graph_iri=graph_iri) for file_path in file_paths]
            for f in futures:
                f.result()


    def dump_triples(self, output_file: str | None ="datastore_dump.nt"):
        """
        Output all triples to the designated file, in Ntriples format
        """
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

        response = requests.get(
            "http://0.0.0.0:7878/query",
            params={"query": query},
            headers={"Accept": "text/ntriples"}
        )

        if response.status_code == 200:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(response.text)
        else:
            logging.error("Error:", response.status_code, response.text)


    def dump_triples_debug(self, output_file: str | None ="datastore_dump.nt"):
        """
        Output all triples to the designated file, in turtle format and employing prefixes for readability. Only used for
        debugging
        """
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

        response = requests.get(
            "http://0.0.0.0:7878/query",
            params={"query": query},
            headers={"Accept": "text/ntriples"}
        )

        if response.status_code == 200:
            g = Graph()
            g.parse(data=response.text, format="turtle")

            namespaces = {
                "chebi": "http://purl.obolibrary.org/obo/chebi/",
                "pmdco": "https://w3id.org/pmd/co/",
                "pmd": "https://w3id.org/pmd/co/",
                "owl": "http://www.w3.org/2002/07/owl#",
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "xml": "http://www.w3.org/XML/1998/namespace",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "prov": "http://www.w3.org/ns/prov#",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "": "https://crc1625.mdi.ruhr-uni-bochum.de/",
                "user": "https://crc1625.mdi.ruhr-uni-bochum.de/user/",
                "project": "https://crc1625.mdi.ruhr-uni-bochum.de/project/",
                "materials_library": "https://crc1625.mdi.ruhr-uni-bochum.de/object/",
                "substrate": "https://crc1625.mdi.ruhr-uni-bochum.de/substrate/",
                "measurement": "https://crc1625.mdi.ruhr-uni-bochum.de/measurement/",
                "measurement_type": "https://crc1625.mdi.ruhr-uni-bochum.de/measurement_type/",
                "measurement_area": "https://crc1625.mdi.ruhr-uni-bochum.de/measurement_area/",
                "bulk_composition": "https://crc1625.mdi.ruhr-uni-bochum.de/bulk_composition/",
                "EDX_composition": "https://crc1625.mdi.ruhr-uni-bochum.de/EDX_composition/",
                "idea_or_experiment_plan": "https://crc1625.mdi.ruhr-uni-bochum.de/idea_or_experiment_plan/",
                "request_for_synthesis": "https://crc1625.mdi.ruhr-uni-bochum.de/request_for_synthesis/",
                "workflow_instance": "https://crc1625.mdi.ruhr-uni-bochum.de/workflow_instance/",
                "workflow_model": "https://crc1625.mdi.ruhr-uni-bochum.de/workflow_model/",
                "handover": "https://crc1625.mdi.ruhr-uni-bochum.de/handover/",
                "activity": "https://crc1625.mdi.ruhr-uni-bochum.de/activity/",
            }

            for prefix, uri in namespaces.items():
                g.bind(prefix, Namespace(uri))

            with open(output_file+"_debug.ttl", "w", encoding="utf-8") as f:
                f.write(g.serialize(format="turtle"))
        else:
            logging.error("Error:", response.status_code, response.text)


    def clear_triples(self, graph_iri="https://crc1625.mdi.ruhr-uni-bochum.de/graph"):
        """
        Clear all CRC1625 KG triples from the graph, including its ontologies
        """
        endpoint = "http://0.0.0.0:7878/update"
        headers = {"Content-Type": "application/sparql-update"}
        clear_triples_query = f"""
        DELETE {{
            GRAPH <{graph_iri}> {{
                ?s ?p ?o 
            }}
        }} WHERE {{
            GRAPH <{graph_iri}> {{
                ?s ?p ?o 
            }}
        }}"""
        result = requests.post(endpoint, data=clear_triples_query, headers=headers)
        if result.status_code not in [200, 204]:
            raise RuntimeError(f"Error occurred  when clearing triples: {result.text}")

    def start_oxigraph(self):
        """
        Starts a docker container with the latest Oxigraph image
        It will be bound to the 7878 port, and thus locally accessible at http://0.0.0.0:7878/
        """
        logging.info("Starting Oxigraph container...")
        subprocess.check_output([
            "docker", "run", "-d", "--rm",
            # "-v", f"{os.getcwd()}/oxigraph:/oxigraph:z", # TODO for persistence
            "-p", "7878:7878",
            "--name", self.DOCKER_CONTAINER_NAME,
            "ghcr.io/oxigraph/oxigraph", "serve",
            "--location", "/oxigraph", "--bind", "0.0.0.0:7878"
        ]).decode().strip()
        logging.info("Sleeping 10 seconds to wait for Oxigraph to be ready...")
        time.sleep(10)

    def stop_oxigraph(self):
        """
        Stops and removes the oxigraph container. It will not fail if it is not found
        """
        logging.info("Stopping Oxigraph container...")
        subprocess.check_output([
            "docker", "rm", "-f", self.DOCKER_CONTAINER_NAME
        ]).decode().strip()