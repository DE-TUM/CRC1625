"""
Main script used to execute the complete KG generation pipeline. It can be called via CLI to control its different behaviors
(e.g., avoid performing postprocessing..., etc.)
"""
import argparse
import logging
import sys
import time

import datastores.sql.sql_db as sql_db
import materialization.materialization as materialization
import postprocessing.postprocessing as postprocessing
from datastores.rdf import rdf_datastore_client

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# List of ontologies to upload to the KG
ontology_files = [
    {
        "name": "PMD-core",
        "file": "../ontologies/pmd_core.ttl",
        "content_type": "text/turtle"
    },
    {
        "name": "CRC",
        "file": "../ontologies/crc.ttl",
        "content_type": "text/turtle"
    },
    {
        "name": "OCE",
        "file": "../ontologies/oce.ttl",
        "content_type": "text/turtle"
    }
]

def upload_materialized_triples(file_names_to_add_to_rdf_store: list[str], delete_materialized_triples_files: bool = True):
    rdf_datastore_client.run_sync(rdf_datastore_client.bulk_file_load(file_names_to_add_to_rdf_store, delete_materialized_triples_files))


def upload_ontology_files(ontology_files: list[dict[str, str]]):
    rdf_datastore_client.run_sync(rdf_datastore_client.bulk_file_load([f["file"] for f in ontology_files], delete_files_after_upload=False))


def serve_KG(skip_ontologies_upload: bool = True,
             db_option: str = None,
             skip_db_setup: bool = False,
             skip_materialization: bool = False,
             skip_postprocessing: bool = False,
             delete_materialized_triples_files: bool = True,
             use_rmlstreamer: bool = False):
    performance_log_postprocessing = dict()

    db = sql_db.MSSQLDB()
    if not skip_db_setup:
        db.select_and_start_db(db_option)

    materialized_files, performance_log_mappings, resource_usage_mappings = materialization.run_mappings(db,
                                                                                                         skip_materialization=skip_materialization,
                                                                                                         use_rmlstreamer=use_rmlstreamer)

    logging.info("Materialization of the KG finished!")
    rdf_datastore_client.run_sync(rdf_datastore_client.clear_triples())

    file_upload_start = time.perf_counter()

    upload_materialized_triples(materialized_files, delete_materialized_triples_files)

    if not skip_ontologies_upload:
        upload_ontology_files(ontology_files)

    file_upload_end = time.perf_counter() - file_upload_start


    logging.info("Triples loaded! running postprocessing...")
    resource_usage_postprocessing = []
    if not skip_postprocessing:
        performance_log_postprocessing, resource_usage_postprocessing = postprocessing.run_postprocessing()

    logging.info("Postprocessing finished!")

    if not skip_db_setup and not db.is_remote:
        db.stop_DB()

    return performance_log_mappings, resource_usage_mappings, performance_log_postprocessing, resource_usage_postprocessing, file_upload_end


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--db_option",
        type=str,
        default=None,
        required=False,
        help="""Database to use. If none provided, it will be asked via CLI. Possible values:
            Production DB endpoints:
            - 'p': Remote production endpoint (only available within the CRC, will not create a local Docker container)

            Production DB dumps:
            - 'm': Main CRC1625 DB dump (only available within the CRC)

            Validation DB dumps, containing a manually created instance for testing the mappings:
            - 'v': Validation DB dump, main test
            - 'v_1': Validation DB dump, subtest 1
            - 'v_2': Validation DB dump, subtest 2
            - 'v_3': Validation DB dump, subtest 3
            - 'v_4': Validation DB dump, subtest 3

            Other DB dumps:
            - 'c': Clear DB dump, containing no data. Used for the performance tests"""
    )

    parser.add_argument(
        "--skip_ontologies_upload",
        action="store_true",
        default=False,
        help="Skip uploading ontologies to the knowledge graph"
    )

    parser.add_argument(
        "--skip_db_setup",
        action="store_true",
        default=False,
        help="Skip the database setup, assuming it is already running instead"
    )

    parser.add_argument(
        "--skip_materialization",
        action="store_true",
        default=False,
        help="Skip the materialization step, assuming that the materialized .ttl files are already present"
    )

    parser.add_argument(
        "--skip_postprocessing",
        action="store_true",
        default=False,
        help="Skip the postprocessing step"
    )

    parser.add_argument(
        "--do_not_delete_materialized_triples_files",
        action="store_false",
        default=True,
        help="Do not delete the files containing the materialized triples after uploading them to the RDF datastore"
    )

    parser.add_argument(
        "--use_rmlstreamer",
        action="store_true",
        default=False,
        help="Use RMLStreamer instead of RMLMapper. Only recommended for very large databases due to its overhead"
    )



    args = parser.parse_args()

    serve_KG(skip_ontologies_upload=args.skip_ontologies_upload,
             db_option=args.db_option,
             skip_db_setup=args.skip_db_setup,
             skip_materialization=args.skip_materialization,
             skip_postprocessing=args.skip_postprocessing,
             delete_materialized_triples_files=not args.do_not_delete_materialized_triples_files,
             use_rmlstreamer=args.use_rmlstreamer)