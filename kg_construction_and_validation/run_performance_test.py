"""
Module to run a series of performance tests, via the following procedure:
    1. Instantiate a production RDMS instance, and query for statistic such as the probabilities of different object types
    2. Create a set of run configurations using these statistics for different numbers of samples. This test will be repeated
       by increasing the probabilities, thus simulating increasingly active RDMS instances
    3. For each run, generate a synthetic DB following its probabilities, materialize the KG and perform all postprocessing
       steps on it. Performance logs will be saved to .json files

All configurations and results are saved to .json files, and indicated by calling this script as a CLI application

Assumes that virtuoso is already started. It will be stopped and started between runs. *All* datastore contents will be cleared
"""

import argparse
import json
import logging
import math
import sys
import time
import traceback
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import pymssql

from create_synthetic_records import create_synthetic_records
from datastores.rdf.rdf_datastore_api import RDFDatastore
from datastores.rdf.virtuoso_datastore import VirtuosoRDFDatastore
from datastores.rdf.oxigraph_datastore import OxigraphRDFDatastore
from datastores.sql.sql_db import MSSQLDB
from main import serve_KG

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

module_dir = os.path.dirname(__file__)

DEFAULT_RUNS_CONFIG_FILE = os.path.join(module_dir, "./performance_test/runs_configuration.json")

prefixes = open(os.path.join(module_dir, './mappings_output_test/queries/prefixes_validation.sparql')).read()

n_users_query = prefixes + open(os.path.join(module_dir, './performance_test/queries/n_users.sparql')).read()
n_projects_query = prefixes + open(os.path.join(module_dir, './performance_test/queries/n_projects.sparql')).read()
n_samples_query = prefixes + open(os.path.join(module_dir, './performance_test/queries/n_samples.sparql')).read()

chance_to_have_piece_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/chance_to_have_piece.sparql')).read()
max_piece_depth_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/max_piece_depth.sparql')).read()

n_substrates_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/n_substrates.sparql')).read()

chance_to_have_idea_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/chance_to_have_idea.sparql')).read()

chance_to_have_request_for_synthesis_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/chance_to_have_request_for_synthesis.sparql')).read()

chance_to_have_handover_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/chance_to_have_handover.sparql')).read()
max_handovers_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/max_handovers.sparql')).read()

chance_to_have_measurement_in_main_sample_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/chance_to_have_measurement_in_main_sample.sparql')).read()
max_measurements_in_main_samples_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/max_measurements_in_main_samples.sparql')).read()
chance_to_have_measurement_in_sample_piece_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/chance_to_have_measurement_in_sample_piece.sparql')).read()
max_measurements_in_sample_pieces_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/max_measurements_in_sample_pieces.sparql')).read()

chance_for_EDX_measurement_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/chance_for_EDX_measurement.sparql')).read()

avg_handovers_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/avg_handovers.sparql')).read()
avg_piece_depth_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/avg_piece_depth.sparql')).read()

count_triples_query = prefixes + open(
    os.path.join(module_dir, './performance_test/queries/count_triples.sparql')).read()


def run_querying_benchmark(datastore: RDFDatastore) -> dict[str, tuple[float, float]]:
    """
    Executes all query files contained in the ./performance_test/queries folder. Each query file must have a corresponding
    counterpart in SQL/SPARQL using the .sql or .sparql extension, respectively, with the same base name.

    :return: A dictionary of base query names corresponding to tuples of (sql_query_time, sparql_query_time)
    """

    benchmark_results: dict[str, tuple[float, float]] = dict()

    sparql_query_files = list(Path(os.path.join(module_dir, './performance_test/queries/sql_sparql_benchmark/')).rglob("*.sparql"))

    conn = pymssql.connect(
        server='localhost',
        port='1433',
        user='sa',
        password='DebugPassword123@',
        database='RUB_INF'
    )
    cursor = conn.cursor()

    for sparql_file in sparql_query_files:
        sparql_query = prefixes + open(sparql_file).read()

        start = time.perf_counter()
        try:
            datastore.launch_query(sparql_query) # We do not care about the result
        except Exception as e:
            print(f"Error on SPARQL query '{sparql_file}': {e}")
            raise
        time_sparql = time.perf_counter() - start

        sql_file = sparql_file.with_suffix(".sql")
        sql_query = open(sql_file).read()

        start = time.perf_counter()
        try:
            cursor.execute(sql_query)  # Same here
        except Exception as e:
            print(f"Error on SQL query '{sql_file}': {e}")
            raise

        time_sql = time.perf_counter() - start

        query_name, _ = os.path.splitext(os.path.basename(sparql_file))
        benchmark_results[query_name] = (time_sql, time_sparql)

    return benchmark_results



def soft_scale_root(x, k):
    """
    Scales a probability with a root function.
    Used to avoid saturating/overflowing probabilities
    """
    return x ** (1.0 / k)


def get_value_from_query(q: str,
                         value: str,
                         datatype: type,
                         store: str) -> Any:
    """
    Convenience function to get a single value of a SPARQL query in the desired type
    """
    if store == "oxigraph":
        return datatype(OxigraphRDFDatastore().launch_query(q).json()["results"]["bindings"][0][value]["value"])
    else:
        return datatype(VirtuosoRDFDatastore().launch_query(q).json()["results"]["bindings"][0][value]["value"])


def get_runs_configuration_from_datastore(store: str, n_repetitions: int=1):
    """
    Generate a runs_configuration.json file containing the statistics and probabilities of each required parameter
    for the creation of a synthetic DB. This will be obtaining by launching SPARQL queries to the specified store type
    ("virtuoso" or "oxigraph")
    """
    sql_db.select_and_start_db(db_option='m')

    serve_KG(skip_oxigraph_initialization=False,
                 skip_ontologies_upload=False,
                 skip_db_setup=True,
                 skip_materialization=False,
                 store=store)

    n_users = get_value_from_query(n_users_query, "n_users", int, store)
    n_projects = get_value_from_query(n_projects_query, "n_projects", int, store)
    n_samples = get_value_from_query(n_samples_query, "n_samples", int, store)

    n_substrates = get_value_from_query(n_substrates_query, "n_substrates", int, store)
    chance_to_have_idea = get_value_from_query(chance_to_have_idea_query, "chance_to_have_idea", float, store)
    chance_to_have_request_for_synthesis = get_value_from_query(chance_to_have_request_for_synthesis_query, "chance_to_have_request_for_synthesis", float, store)

    chance_to_have_piece = get_value_from_query(chance_to_have_piece_query, "chance_to_have_piece", float, store)
    max_piece_depth = math.ceil(get_value_from_query(max_piece_depth_query, "max_piece_depth", float, store))

    chance_to_have_handover = get_value_from_query(chance_to_have_handover_query, "chance_to_have_handover", float, store)
    max_handovers = math.ceil(get_value_from_query(max_handovers_query, "max_handovers", float, store))

    chance_to_have_measurement_in_main_sample = get_value_from_query(chance_to_have_measurement_in_main_sample_query,
                                                                  "chance_to_have_measurement_in_main_sample", float, store)
    max_measurements_in_main_samples = math.ceil(get_value_from_query(max_measurements_in_main_samples_query,
                                                                     "max_measurements_in_main_samples", float,
                                                                     store))
    chance_to_have_measurement_in_sample_piece = get_value_from_query(chance_to_have_measurement_in_sample_piece_query,
                                                         "chance_to_have_measurement_in_sample_piece", float, store)
    max_measurements_in_sample_pieces = math.ceil(get_value_from_query(max_measurements_in_sample_pieces_query,
                                                            "max_measurements_in_sample_pieces", float,
                                                            store))

    chance_for_EDX_measurement = get_value_from_query(chance_for_EDX_measurement_query,
                                                            "chance_for_EDX_measurement", float,
                                                            store)

    logging.info("Completed! Resetting datastores...")
    stop_datastores(args, sql_db)

    runs_configuration_w_o_repetitions = [
        {
            "num_users": n_users,
            "num_areas": 3,
            "num_projects": n_projects,

            "num_main_samples": [n_samples, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10_000],
            "chance_to_have_piece": chance_to_have_piece,
            "max_piece_depth": max_piece_depth,

            "num_substrates": n_substrates,
            "chance_to_have_idea": chance_to_have_idea,
            "chance_to_have_request_for_synthesis": chance_to_have_request_for_synthesis,

            "chance_to_have_handover": chance_to_have_handover,
            "max_handovers_per_sample": max_handovers,

            "chance_to_have_measurement_in_main_sample": chance_to_have_measurement_in_main_sample,
            "max_measurements_in_main_samples": max_measurements_in_main_samples,
            "chance_to_have_measurement_in_sample_piece": chance_to_have_measurement_in_sample_piece,
            "max_measurements_in_sample_pieces": max_measurements_in_sample_pieces,

            "chance_for_EDX_measurement": chance_for_EDX_measurement
        },

        # 1.25x the number of users, projects and overall activity
        {
            "num_users": int(n_users * 1.25),
            "num_areas": int(3 * 1.25),
            "num_projects": int(n_projects * 1.25),

            "num_main_samples": [n_samples, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10_000],
            "chance_to_have_piece": soft_scale_root(chance_to_have_piece, 1.25),
            "max_piece_depth": int(max_piece_depth * 1.25),

            "num_substrates": int(n_substrates * 1.25),
            "chance_to_have_idea": soft_scale_root(chance_to_have_idea, 1.25),
            "chance_to_have_request_for_synthesis": soft_scale_root(chance_to_have_request_for_synthesis, 1.25),

            "chance_to_have_handover": soft_scale_root(chance_to_have_handover, 1.25),
            "max_handovers_per_sample": int(max_handovers * 1.25),

            "chance_to_have_measurement_in_main_sample": soft_scale_root(chance_to_have_measurement_in_main_sample,
                                                                         1.25),
            "max_measurements_in_main_samples": int(max_measurements_in_main_samples * 1.25),
            "chance_to_have_measurement_in_sample_piece": soft_scale_root(chance_to_have_measurement_in_sample_piece,
                                                                          1.25),
            "max_measurements_in_sample_pieces": int(max_measurements_in_sample_pieces * 1.25),

            "chance_for_EDX_measurement": soft_scale_root(chance_for_EDX_measurement, 1.25)
        },

        # 1.5x the number of users, projects and overall activity
        {
            "num_users": int(n_users * 1.5),
            "num_areas": int(3 * 1.5),
            "num_projects": int(n_projects * 1.5),

            "num_main_samples": [n_samples, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10_000],
            "chance_to_have_piece": soft_scale_root(chance_to_have_piece, 1.5),
            "max_piece_depth": int(max_piece_depth * 1.5),

            "num_substrates": int(n_substrates * 1.5),
            "chance_to_have_idea": soft_scale_root(chance_to_have_idea, 1.5),
            "chance_to_have_request_for_synthesis": soft_scale_root(chance_to_have_request_for_synthesis, 1.5),

            "chance_to_have_handover": soft_scale_root(chance_to_have_handover, 1.5),
            "max_handovers_per_sample": int(max_handovers * 1.5),

            "chance_to_have_measurement_in_main_sample": soft_scale_root(chance_to_have_measurement_in_main_sample, 1.5),
            "max_measurements_in_main_samples": int(max_measurements_in_main_samples * 1.5),
            "chance_to_have_measurement_in_sample_piece": soft_scale_root(chance_to_have_measurement_in_sample_piece,
                                                                          1.5),
            "max_measurements_in_sample_pieces": int(max_measurements_in_sample_pieces * 1.5),

            "chance_for_EDX_measurement": soft_scale_root(chance_for_EDX_measurement, 1.5)
        },

        # 1.75x the number of users, projects and overall activity
        {
            "num_users": int(n_users * 1.75),
            "num_areas": int(3 * 1.75),
            "num_projects": int(n_projects * 1.75),

            "num_main_samples": [n_samples, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10_000],
            "chance_to_have_piece": soft_scale_root(chance_to_have_piece, 1.75),
            "max_piece_depth": int(max_piece_depth * 1.75),

            "num_substrates": int(n_substrates * 1.75),
            "chance_to_have_idea": soft_scale_root(chance_to_have_idea, 1.75),
            "chance_to_have_request_for_synthesis": soft_scale_root(chance_to_have_request_for_synthesis, 1.75),

            "chance_to_have_handover": soft_scale_root(chance_to_have_handover, 1.75),
            "max_handovers_per_sample": int(max_handovers * 1.75),

            "chance_to_have_measurement_in_main_sample": soft_scale_root(chance_to_have_measurement_in_main_sample,
                                                                         1.75),
            "max_measurements_in_main_samples": int(max_measurements_in_main_samples * 1.75),
            "chance_to_have_measurement_in_sample_piece": soft_scale_root(chance_to_have_measurement_in_sample_piece,
                                                                          1.75),
            "max_measurements_in_sample_pieces": int(max_measurements_in_sample_pieces * 1.75),

            "chance_for_EDX_measurement": soft_scale_root(chance_for_EDX_measurement, 1.75)
        },

        # 2x the number of users, projects and overall activity
        {
            "num_users": n_users * 2,
            "num_areas": 3 * 2,
            "num_projects": n_projects * 2,

            "num_main_samples": [n_samples, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10_000],
            "chance_to_have_piece": soft_scale_root(chance_to_have_piece, 2),
            "max_piece_depth": int(max_piece_depth * 2),

            "num_substrates": n_substrates * 2,
            "chance_to_have_idea": soft_scale_root(chance_to_have_idea, 2),
            "chance_to_have_request_for_synthesis": soft_scale_root(chance_to_have_request_for_synthesis, 2),

            "chance_to_have_handover": soft_scale_root(chance_to_have_handover, 2),
            "max_handovers_per_sample": max_handovers * 2,

            "chance_to_have_measurement_in_main_sample": soft_scale_root(chance_to_have_measurement_in_main_sample, 2),
            "max_measurements_in_main_samples": max_measurements_in_main_samples * 2,
            "chance_to_have_measurement_in_sample_piece": soft_scale_root(chance_to_have_measurement_in_sample_piece, 2),
            "max_measurements_in_sample_pieces": max_measurements_in_sample_pieces * 2,

            "chance_for_EDX_measurement": soft_scale_root(chance_for_EDX_measurement, 2)
        }
    ]

    runs_configuration_w_repetitions = []

    for run_config in runs_configuration_w_o_repetitions:
        for n_run in range(0, n_repetitions):
            run_config_clone = run_config.copy()
            run_config_clone["n_run"] = n_run

            runs_configuration_w_repetitions.append(run_config_clone)

    logging.info("Configurations to run:")
    logging.info(json.dumps(runs_configuration_w_repetitions, indent=4))

    with open(DEFAULT_RUNS_CONFIG_FILE, "w") as f:
        json.dump(runs_configuration_w_repetitions, f, indent=4)
    logging.info(f"Configurations file saved to: {DEFAULT_RUNS_CONFIG_FILE}")


    return runs_configuration_w_repetitions


def get_runs_configuration_from_file(f):
    with open(f, 'r') as f:
        return json.load(f)


def stop_datastores(args, sql_db: MSSQLDB):
    if args.store == "oxigraph":
        logging.info("Stopping and removing Oxigraph container...")
        OxigraphRDFDatastore().stop_oxigraph()
    elif args.store == "virtuoso":
        logging.info("Clearing Virtuoso...")
        VirtuosoRDFDatastore().clear_triples()
        logging.info("Stopping Virtuoso...")
        VirtuosoRDFDatastore().stop_virtuoso()
        logging.info("Starting Virtuoso...")
        VirtuosoRDFDatastore().start_virtuoso()

    sql_db.stop_DB()


def get_log_file(log_file):
    if not os.path.exists(log_file):
        logging.info("No log file found. A log file will be created after the first run.")
        return []
    with open(log_file, 'r') as f:
        return json.load(f)


def is_run_completed(completed_runs,
                     num_main_samples,
                     run_config):
    run_config_for_log = run_config.copy()
    run_config_for_log["num_main_samples"] = num_main_samples

    for completed_run in completed_runs:
        if completed_run["config"] == run_config_for_log:
            return True

    return False


def save_run(completed_runs,
             num_main_samples,
             run_config: dict,
             mappings_performance_log,
             resource_usage_mappings,
             performance_log_postprocessing,
             resource_usage_postprocessing,
             file_upload_time,
             n_triples,
             query_benchmark_results,
             log_file):
    run_config_for_log = run_config.copy()
    run_config_for_log["num_main_samples"] = num_main_samples

    completed_runs.append(
        {
            "config": run_config_for_log,

            "mappings_performance_log": mappings_performance_log,
            "resource_usage_mappings": resource_usage_mappings,
            "file_upload_time": file_upload_time,
            "postprocessing_performance_log": performance_log_postprocessing,
            "resource_usage_postprocessing": resource_usage_postprocessing,

            "n_triples_generated": n_triples,
            "query_benchmark_results": query_benchmark_results
        }
    )

    with open(log_file, "w") as f:
        json.dump(completed_runs, f, indent=4)

    return completed_runs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--store",
        choices=["oxigraph", "virtuoso"],
        required=True,
        help="RDF store to use. Possible values: 'oxigraph' or 'virtuoso'"
    )

    parser.add_argument(
        "--log_file",
        required=True,
        help="Path of the log file to resume experiments from and store results"
    )

    parser.add_argument(
        "--runs_configuration_file",
        required=False,
        help="""Path of the configuration file to be used across multiple executions. 
        If not indicated, a new configuration file will be generated by querying the CRC1625 instance. 
        Note that there is already a default file located at ./performance_test/runs_configuration"""
    )

    parser.add_argument(
        "--evaluate_only_sql_queries",
        required=False,
        action="store_true",
        help="""Run the performance test without materializing or doing anything RDF-related.
        The output will be the same, but will only reflect SQL-to-csv times."""
    )

    args = parser.parse_args()

    sql_db = MSSQLDB()

    if args.runs_configuration_file is None:
        logging.info("""No runs configuration file indicated.
        Materializing CRC1625 KG to obtain statistics for the synthetic data generation...""")

        runs_config = get_runs_configuration_from_datastore(args.store, 3)
    else:
        runs_config = get_runs_configuration_from_file(args.runs_configuration_file)

    completed_runs = get_log_file(args.log_file)
    backup_identifier = None

    try:
        for i, run_config in enumerate(runs_config):
            for num_main_samples in run_config["num_main_samples"]:
                if is_run_completed(completed_runs,
                                    num_main_samples,
                                    run_config):
                    logging.info("Run already completed, skipping...")
                    continue

                # TODO this is DB-backup-specific
                max_measurements_in_main_samples = run_config["max_measurements_in_main_samples"]
                if max_measurements_in_main_samples == 4:
                    multiplier = "No multiplier"
                elif max_measurements_in_main_samples == 5:
                    multiplier = "1.25"
                elif max_measurements_in_main_samples == 6:
                    multiplier = "1.5"
                elif max_measurements_in_main_samples == 7:
                    multiplier = "1.75"
                elif max_measurements_in_main_samples == 8:
                    multiplier = "2"
                else:
                    continue

                logging.info(f"Executing test for multiplier '{multiplier}', n_samples {num_main_samples}, n_run: {run_config['n_run']}")


                backup_identifier = f"performance_test_db_dump_{num_main_samples}_main_samples_multiplier_{multiplier}_run_{run_config['n_run']}"

                sql_db.select_and_start_db(db_option='c')

                if sql_db.database_backup_exists(backup_identifier):
                    logging.info("Backup already exists for configuration, restoring...")
                    sql_db.restore_database(backup_identifier)
                else:
                    create_synthetic_records(
                        run_config["num_users"],
                        run_config["num_areas"],
                        run_config["num_projects"],

                        num_main_samples,
                        run_config["chance_to_have_piece"],
                        run_config["max_piece_depth"],

                        run_config["num_substrates"],
                        run_config["chance_to_have_idea"],
                        run_config["chance_to_have_request_for_synthesis"],

                        run_config["chance_to_have_handover"],
                        run_config["max_handovers_per_sample"],

                        run_config["chance_to_have_measurement_in_main_sample"],
                        run_config["max_measurements_in_main_samples"],
                        run_config["chance_to_have_measurement_in_sample_piece"],
                        run_config["max_measurements_in_sample_pieces"],

                        run_config["chance_for_EDX_measurement"],
                    )

                mappings_performance_log, resource_usage_mappings, performance_log_postprocessing, resource_usage_postprocessing,  file_upload_time = (
                    serve_KG(skip_oxigraph_initialization=False,
                             skip_ontologies_upload=False,
                             skip_db_setup=True,
                             skip_materialization=False,
                             run_only_sql_queries=args.evaluate_only_sql_queries,
                             store=args.store))

                if not args.evaluate_only_sql_queries:
                    n_triples = get_value_from_query(count_triples_query, "n_triples", int, args.store)

                    postprocessing_time = sum([time for time in performance_log_postprocessing.values()])

                    logging.info(f"File upload time: {timedelta(seconds=file_upload_time)}")
                    logging.info(f"Postprocessing time: {timedelta(seconds=postprocessing_time)}")

                    logging.info(f"Triples generated: {n_triples}")

                    logging.info(f"Benchmarking query times...")

                    if args.store == "virtuoso":
                        datastore = VirtuosoRDFDatastore()
                    else:
                        datastore = OxigraphRDFDatastore()
                    query_benchmark_results = run_querying_benchmark(datastore)
                else:
                    n_triples = 0
                    query_benchmark_results = {}

                completed_runs = save_run(completed_runs,
                                          num_main_samples,
                                          run_config,
                                          mappings_performance_log,
                                          resource_usage_mappings,
                                          performance_log_postprocessing,
                                          resource_usage_postprocessing,
                                          file_upload_time,
                                          n_triples,
                                          query_benchmark_results,
                                          args.log_file)

                if not sql_db.database_backup_exists(backup_identifier):
                    logging.info("Backing up the SQL DB...")
                    sql_db.dump_database(backup_identifier)

                if not args.evaluate_only_sql_queries:
                    stop_datastores(args, sql_db)

    except KeyboardInterrupt:
        logging.info("Ctrl-c detected, stopping datastores...")
        logging.info("Backing up the SQL DB...")
        if backup_identifier is not None:
            sql_db.dump_database(backup_identifier+"_stopped")
        stop_datastores(args, sql_db)

    except Exception as e:
        logging.error(f"\nCaught unexpected exception: {e}")
        logging.error(traceback.format_exc())

        logging.info("Backing up the SQL DB...")
        if backup_identifier is not None:
            sql_db.dump_database(backup_identifier + "_error")

        logging.error("The datastores have not been stopped for debugging...")
        #stop_datastores(args, sql_db)
