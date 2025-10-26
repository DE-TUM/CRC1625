"""
Main script used to execute the complete KG generation pipeline. It can be called via CLI to control its different behaviors
(e.g., avoid performing postprocessing..., etc.)
"""
import argparse
import logging
import sys
import time

import datastores.sql.sql_db as sql_db
import datastores.rdf.virtuoso_datastore as virtuoso_datastore
import datastores.rdf.oxigraph_datastore as oxigraph_datastore
import materialization.materialization as materialization
import postprocessing.postprocessing as postprocessing
from datastores.rdf.rdf_datastore import RDFDatastore

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

def upload_materialized_triples(datastore: RDFDatastore,
                                file_names_to_add_to_rdf_store: list[str]):
    datastore.bulk_file_load(file_names_to_add_to_rdf_store)


def upload_ontology_files(datastore: RDFDatastore,
                          ontology_files: list[dict[str]]):
    datastore.bulk_file_load(f["file"] for f in ontology_files)


def serve_KG(skip_oxigraph_initialization: bool = True,
             skip_ontologies_upload: bool = True,
             db_option: str = None,
             skip_db_setup: bool = False,
             skip_materialization: bool = False,
             skip_postprocessing: bool = False,
             run_only_sql_queries: bool = False,
             store: str = "virtuoso"):
    performance_log_postprocessing = dict()
    resource_usage_postprocessing = dict()
    file_upload_end = 0

    if store == "virtuoso":
        datastore = virtuoso_datastore.VirtuosoRDFDatastore()
    elif store == "oxigraph":
        datastore = oxigraph_datastore.OxigraphRDFDatastore()
    else:
        raise ValueError(f"Incorrect store name '{store}'")

    if not skip_db_setup:
        db = sql_db.MSSQLDB()
        db.select_and_start_db(db_option)

    materialized_files, performance_log_mappings, resource_usage_mappings = materialization.run_mappings(skip_materialization=skip_materialization,
                                                                                                         run_only_sql_queries=run_only_sql_queries)

    logging.info("Materialization of the KG finished!")
    if not run_only_sql_queries:
        if not skip_oxigraph_initialization and store == "oxigraph":
            datastore.start_oxigraph()

        datastore.clear_triples()

        file_upload_start = time.perf_counter()

        upload_materialized_triples(datastore, materialized_files)

        if not skip_ontologies_upload:
            upload_ontology_files(datastore, ontology_files)

        file_upload_end = time.perf_counter() - file_upload_start


        logging.info("Triples loaded! running postprocessing...")
        resource_usage_postprocessing = []
        if not skip_postprocessing:
            performance_log_postprocessing, resource_usage_postprocessing = postprocessing.run_postprocessing(datastore)

        if not skip_oxigraph_initialization and store == "oxigraph":
            logging.info(f"""
                Postprocessing finished! The endpoint is available at http://0.0.0.0:7878
                To stop Oxigraph, execute `docker rm -f {oxigraph_datastore.OxigraphRDFDatastore().DOCKER_CONTAINER_NAME}`
                """)
        elif store == "oxigraph":
            logging.info("Postprocessing finished! The endpoint is available at http://0.0.0.0:7878")
        else:
            logging.info("Postprocessing finished! The endpoint is available at http://localhost:8891/sparql/")

    if not skip_db_setup:
        db.stop_DB()

    return performance_log_mappings, resource_usage_mappings, performance_log_postprocessing, resource_usage_postprocessing, file_upload_end


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--store",
        choices=["oxigraph", "virtuoso"],
        required=True,
        help="RDF store to use. Possible values: 'oxigraph' or 'virtuoso'"
    )

    parser.add_argument(
        "--skip_oxigraph_initialization",
        action="store_true",
        default=False,
        help="Avoid starting an empty Oxigraph endpoint, assuming it is already running instead"
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

    args = parser.parse_args()

    serve_KG(skip_oxigraph_initialization=args.skip_oxigraph_initialization,
             skip_ontologies_upload=args.skip_ontologies_upload,
             skip_db_setup=args.skip_db_setup,
             skip_materialization=args.skip_materialization,
             skip_postprocessing=args.skip_postprocessing,
             store=args.store)