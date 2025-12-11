"""
Validates the results of the mappings by executing the KG creation and postprocessing pipelines on manually created
DBs with different edge cases. For a test to pass, the generated KG must match 1:1 to its equivalent, manually created
KG stored in .ttl files.

This ensures that any changes to the mappings of postprocessing do not produce different results.

The module is callable as a CLI application, allowing to execute all or one of the tests stored in the
test_files and test_names variables

This test can be run on either an Oxigraph or Virtuoso instance. *All* datastore contents will be cleared

How to add a test:
    1. Store the synthetic DB .bak file (you can use the existing ones as the base) in datastores/sql/db_dumps/
    2. Add an entry to the args for datastores/sql/sql_db.py
    3. Store the corresponding KG dump as a .ttl file under mappings_output_test/
    4. Include references to both in this file under test_files, test_names and the main function's args
"""

import argparse
import asyncio
import logging
import os
import sys

from datastores.rdf import rdf_datastore_client
from main import serve_KG

from rdflib import Graph, Literal, XSD
from rdflib.compare import isomorphic, graph_diff

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

module_dir = os.path.dirname(__file__)

prefixes = open(os.path.join(module_dir, 'mappings_output_test/queries/prefixes_validation.sparql')).read()
validate_compositions_query = prefixes + open(os.path.join(module_dir, 'mappings_output_test/queries/validate_compositions.sparql')).read()
validate_compositions_query_virtuoso = prefixes + open(os.path.join(module_dir, 'mappings_output_test/queries/validate_compositions_virtuoso.sparql')).read()

ML_1 = "https://crc1625.mdi.ruhr-uni-bochum.de/object/1"
ML_2 = "https://crc1625.mdi.ruhr-uni-bochum.de/object/2"

test_files = {
    'v': os.path.join(module_dir, 'mappings_output_test/graph_dump_validation_main_test.ttl'),
    'v_1': os.path.join(module_dir, 'mappings_output_test/graph_dump_validation_v_1.ttl'),
    'v_2': os.path.join(module_dir, 'mappings_output_test/graph_dump_validation_v_2.ttl'),
    'v_3': os.path.join(module_dir, 'mappings_output_test/graph_dump_validation_v_3.ttl'),
    'v_4': os.path.join(module_dir, 'mappings_output_test/graph_dump_validation_v_4.ttl'),
    'v_5': os.path.join(module_dir, 'mappings_output_test/graph_dump_validation_v_5.ttl'),
}

test_names = {
    'v': 'Main validation',
    'v_1': 'Handover chain test: Group consisting of initial work handover together with other handovers',
    'v_2': 'Handover chain test: User belonging multiple projects',
    'v_3': 'Handover chain test: Isolated handover surrounded by two handover groups of size 2, belonging to the same project',
    'v_4': 'Handover chain test: Isolated handover surrounded by two handover groups of size 4, belonging to the same project',
    'v_5': 'Handover chain test: Manually linked measurements to handovers'

}

def validate_compositions():
    """
    Aside of comparing the compositions in the .ttl files, this test ensures that they are also sound by querying for
    them via SPARQL
    """
    # Each store has some quirks when querying for decimal values
    if asyncio.run(rdf_datastore_client.get_datastore_type()) == "virtuoso":
        response = asyncio.run(rdf_datastore_client.launch_query(validate_compositions_query_virtuoso))
    else:
        response = asyncio.run(rdf_datastore_client.launch_query(validate_compositions_query))

    bindings = response["results"]["bindings"]

    MLs = dict()
    for binding in bindings:
        if binding["ML"]["value"] not in MLs:
            MLs[binding["ML"]["value"]] = 0

        if int(binding["comp_elements"]["value"]) == 2:
            MLs[binding["ML"]["value"]] += 1

    # We only need to check that they have 342 MAs with 2 composition elements,
    # the query validates their uniqueness and their values
    return MLs[ML_1] == 342 and MLs[ML_2] == 342

def load_ttl_graph(file_path: str, delete_original: bool = False) -> Graph:
    g = Graph()

    g.parse(file_path, format="turtle")

    if delete_original:
        os.remove(file_path)

    return g

def load_endpoint_graph() -> Graph:
    asyncio.run(rdf_datastore_client.dump_triples(os.path.join(module_dir, "datastore_dump.nt")))
    return load_ttl_graph(os.path.join(module_dir, "datastore_dump.nt"), delete_original=True)


def clear_datastores():
    asyncio.run(rdf_datastore_client.clear_triples())


def run_validation_test(test_key: str):
    logging.info(f"Running test: {test_names[test_key]}")

    serve_KG(skip_ontologies_upload=True,
             db_option=test_key,
             skip_materialization=False)

    ttl_graph = load_ttl_graph(test_files[test_key])
    endpoint_graph = load_endpoint_graph()

    logging.info("Comparing the generated KG against the reference...")
    # Use isomorphic function to check equivalence
    if isomorphic(ttl_graph, endpoint_graph):
        logging.info("The validation graph and the materialized graph are identical!")

        if test_key == 'v': # The subtests don't contain measurement data
            logging.info("Validating the compositions...")
            if validate_compositions():
                logging.info("Test successful! ＼(＾O＾)／")

                clear_datastores()
                return True
            else:
                logging.error("The validation check for the compositions was unsuccessful, please check the output of the validation query")
                return False
        else:
            logging.info("Test successful! ＼(＾O＾)／")

            clear_datastores()
            return True
    else:
        logging.info("The validation graph and the materialized graph are different, differences were written to files")
        in_both, in_first, in_second = graph_diff(ttl_graph, endpoint_graph)

        with open("in_both.txt", "w") as f1:
            for triple in in_both:
                f1.write(f"{triple}\n")

        # Triples in validation graph but not in materialized graph
        with open("in_validation_not_materialized.txt", "w") as f1:
            for triple in in_first:
                f1.write(f"{triple}\n")

        # Triples in materialized graph but not in validation graph
        with open("in_materialized_not_validation.txt", "w") as f2:
            for triple in in_second:
                f2.write(f"{triple}\n")

        clear_datastores()
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--test",
        choices=["all", "v", "v_1", "v_2", "v_3", "v_4", "v_5"],
        required=True,
        help="Test to run. Possible values: 'all', 'v', 'v_1', 'v_2', 'v_3', 'v_4', 'v_5'"
    )

    args = parser.parse_args()

    if asyncio.run(rdf_datastore_client.get_datastore_type()) == "qlever":
        logging.warning("WARNING: Running the mappings output test under Qlever is unsupported. "
                        "Until Qlever correctly serializes datatypes, it will produce mismatches on composition values and thus incorrectly fail the tests.")

    if args.test == "all":
        results = {}

        for key in test_names.keys():
            results[key] = run_validation_test(key)

        logging.info("Mappings output validation results:")
        for key, res in results.items():
            logging.info(f"{test_names[key]}. Passed: {res}")
    else:
        run_validation_test(args.test)

