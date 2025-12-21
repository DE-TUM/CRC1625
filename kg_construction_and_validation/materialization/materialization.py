"""
Module used that handles:
    - Parsing of templated YARRRML files and their conversion to valid (untemplated) YARRRML files
    - Conversion of the resulting YARRRML files to RML
    - Parallel execution of RMLMapper over each RML file to generate the KG triples, saved in .ttl files

How to add a new mapping:
    - Create a {mapping_name}_templated.yml file containing templated YARRRML mappings, and a {mapping_name}.sql
      file containing the SQL query from which the mappings file will fill values from. The files may not be templated,
      in which case the replacement step will simply output the same, unmodified files. The files may be saved in any
      folder within materialization/mappings. Right now, they are ordered according to a set of categories roughly 
      matching RDMS types. Both files should be in the *same* folder.

    - Add the path to {mapping_name}_templated.yml to templated_file_names list in this file. Be mindful of also
      employing os.path.join(module_dir, {file_path}) for the route!
      The .sql file path doesn't need to be indicated.

How to run:
    - Call the run_mappings function
"""
import logging
import multiprocessing
import sys
import time
from concurrent.futures import as_completed, ThreadPoolExecutor
from pathlib import Path
from typing import final

import psutil
from tqdm import tqdm

import os
import subprocess

from datastores.sql.sql_db import MSSQLDB
from .fill_template_values import fill_template_values

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

module_dir = os.path.dirname(__file__)


ontology_files = [
    {
        "name": "PMD-core",
        "file": os.path.join(module_dir, "./ontologies/pmd_core.ttl"),
        "content_type": "text/turtle"
    },
    {
        "name": "CRC",
        "file": os.path.join(module_dir, "./ontologies/crc.ttl"),
        "content_type": "text/turtle"
    },
    {
        "name": "OCE",
        "file": os.path.join(module_dir, "./ontologies/oce.owl"),
        "content_type": "application/rdf+xml"
    }
]




# List of mappings to execute
# Each entry consists of either of the two:
#   - A simple mapping, indicated as a tuple of (path_to_untemplated_file, use_rmlstreamer)
#   - A "typed" mapping, indicated as a tuple of (path_to_untemplated_file, SQL_query_replacements, YARRML_file_replacements, use_rmlstreamer)
#   
# In the second case, SQL_query_replacements and YARRML_file_replacements correspond to a dict of str_to_replace -> replacements 
# in the SQL query and YARRRML files, respectively. All entries in both dictionaries should have the same number of values.
# This is currently used to avoid declaring individual mappings for every single measurement type in the RDMS
#
# The use_rmlstreamer flag is recommended for mappings for which we expect very large results. Right now, they are used for
# composition-related mappings
templated_file_names : list[tuple[str, bool]] | list[tuple[str, dict[str, str], dict[str, str], bool]]= [
                            (os.path.join(module_dir, "mappings/users/user_ids_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/users/user_given_names_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/users/user_names_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/users/user_surnames_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/users/projects_templated.yml"), False),

                            (os.path.join(module_dir, "mappings/samples/samples_workflow_instances_and_initial_work_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/samples/sample_elements_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/samples/substrates_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/samples/physical_sample_types_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/samples/computational_sample_types_templated.yml"), False),

                            (os.path.join(module_dir, "mappings/requests_for_synthesis/requests_for_synthesis_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/requests_for_synthesis/bulk_compositions_of_requests_for_synthesis_templated.yml"), False),

                            (os.path.join(module_dir, "mappings/ideas/ideas_templated.yml"), False),

                            (os.path.join(module_dir, "mappings/publications_and_lit_references/publications_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/publications_and_lit_references/literature_references_templated.yml"), False),

                            (os.path.join(module_dir, "mappings/handovers/handover_metadata_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/handovers/handover_chains_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/handovers/initial_work_handover_to_first_handover_templated.yml"), False),

                            (os.path.join(module_dir, "mappings/handovers/activities/activities_templated.yml"),

                            # SQL templates
                            {
                                "{measurement_ids}": [
                                    "12",
                                    "13, 15, 19, 53, 78, 79",
                                    "17, 31, 44, 55, 56, 97",
                                    "30",
                                    "18",
                                    "48",
                                    "27, 38, 39, 40",
                                    "24",
                                    "14, 16, 33",
                                    "41, 80, 81, 82",
                                    "20",
                                    "26",
                                    "57, 58, 85",
                                    "50, 59, 60, 86, 87",
                                    "47",
                                    "147",
                                    "96, 98, 107, 139"
                                ]
                            },

                            # Additional YARRRML templates
                            {
                                "{measurement_name}": [
                                    "Photo",
                                    "EDX",
                                    "XRD",
                                    "XPS",
                                    "Annealing",
                                    "LEIS",
                                    "Thickness",
                                    "SEM",
                                    "Resistance",
                                    "Bandgap",
                                    "APT",
                                    "TEM",
                                    "SDC",
                                    "SECCM",
                                    "FIM",
                                    "PSM",
                                    "Report"
                                ],
                                "{measurement_class_name}": [
                                    "PhotoProcess",
                                    "EDXMicroscopyProcess",
                                    "XRDProcess",
                                    "XPSProcess",
                                    "AnnealingProcess",
                                    "LEISProcess",
                                    "ThicknessProcess",
                                    "SEMProcess",
                                    "ResistanceProcess",
                                    "BandgapProcess",
                                    "APTProcess",
                                    "TEMProcess",
                                    "SDCProcess",
                                    "SECCMProcess",
                                    "FIMProcess",
                                    "PSMProcess",
                                    "ReportProcess"
                                ]
                            }, False),
                            (os.path.join(module_dir, "mappings/handovers/activities/activities_prior_to_first_handover_templated.yml"),
                            # SQL templates
                            {
                                "{measurement_ids}": [
                                    "12",
                                    "13, 15, 19, 53, 78, 79",
                                    "17, 31, 44, 55, 56, 97",
                                    "30",
                                    "18",
                                    "48",
                                    "27, 38, 39, 40",
                                    "24",
                                    "14, 16, 33",
                                    "41, 80, 81, 82",
                                    "20",
                                    "26",
                                    "57, 58, 85",
                                    "50, 59, 60, 86, 87",
                                    "47",
                                    "147",
                                    "96, 98, 107, 139"
                                ]
                            },

                            # Additional YARRRML templates
                            {
                                "{measurement_name}": [
                                    "Photo",
                                    "EDX",
                                    "XRD",
                                    "XPS",
                                    "Annealing",
                                    "LEIS",
                                    "Thickness",
                                    "SEM",
                                    "Resistance",
                                    "Bandgap",
                                    "APT",
                                    "TEM",
                                    "SDC",
                                    "SECCM",
                                    "FIM",
                                    "PSM",
                                    "Report"
                                ],
                                "{measurement_class_name}": [
                                    "PhotoProcess",
                                    "EDXMicroscopyProcess",
                                    "XRDProcess",
                                    "XPSProcess",
                                    "AnnealingProcess",
                                    "LEISProcess",
                                    "ThicknessProcess",
                                    "SEMProcess",
                                    "ResistanceProcess",
                                    "BandgapProcess",
                                    "APTProcess",
                                    "TEMProcess",
                                    "SDCProcess",
                                    "SECCMProcess",
                                    "FIMProcess",
                                    "PSMProcess",
                                    "ReportProcess"
                                ]
                            }, False),
                            (os.path.join(module_dir, "mappings/handovers/activities/activities_with_no_handovers_templated.yml"),
                            # SQL templates
                            {
                                "{measurement_ids}": [
                                    "12",
                                    "13, 15, 19, 53, 78, 79",
                                    "17, 31, 44, 55, 56, 97",
                                    "30",
                                    "18",
                                    "48",
                                    "27, 38, 39, 40",
                                    "24",
                                    "14, 16, 33",
                                    "41, 80, 81, 82",
                                    "20",
                                    "26",
                                    "57, 58, 85",
                                    "50, 59, 60, 86, 87",
                                    "47",
                                    "147",
                                    "96, 98, 107, 139"
                                ]
                            },

                            # Additional YARRRML templates
                            {
                                "{measurement_name}": [
                                    "Photo",
                                    "EDX",
                                    "XRD",
                                    "XPS",
                                    "Annealing",
                                    "LEIS",
                                    "Thickness",
                                    "SEM",
                                    "Resistance",
                                    "Bandgap",
                                    "APT",
                                    "TEM",
                                    "SDC",
                                    "SECCM",
                                    "FIM",
                                    "PSM",
                                    "Report"
                                ],
                                "{measurement_class_name}": [
                                    "PhotoProcess",
                                    "EDXMicroscopyProcess",
                                    "XRDProcess",
                                    "XPSProcess",
                                    "AnnealingProcess",
                                    "LEISProcess",
                                    "ThicknessProcess",
                                    "SEMProcess",
                                    "ResistanceProcess",
                                    "BandgapProcess",
                                    "APTProcess",
                                    "TEMProcess",
                                    "SDCProcess",
                                    "SECCMProcess",
                                    "FIMProcess",
                                    "PSMProcess",
                                    "ReportProcess"
                                ]
                            }, False),

                            (os.path.join(module_dir, "mappings/handovers/activities/measurements_with_explicit_links_to_handovers_templated.yml"),
                            # SQL templates
                            {
                                "{measurement_ids}": [
                                    "12",
                                    "13, 15, 19, 53, 78, 79",
                                    "17, 31, 44, 55, 56, 97",
                                    "30",
                                    "18",
                                    "48",
                                    "27, 38, 39, 40",
                                    "24",
                                    "14, 16, 33",
                                    "41, 80, 81, 82",
                                    "20",
                                    "26",
                                    "57, 58, 85",
                                    "50, 59, 60, 86, 87",
                                    "47",
                                    "147",
                                    "96, 98, 107, 139"
                                ]
                            },

                            # Additional YARRRML templates
                            {
                                "{measurement_name}": [
                                    "Photo",
                                    "EDX",
                                    "XRD",
                                    "XPS",
                                    "Annealing",
                                    "LEIS",
                                    "Thickness",
                                    "SEM",
                                    "Resistance",
                                    "Bandgap",
                                    "APT",
                                    "TEM",
                                    "SDC",
                                    "SECCM",
                                    "FIM",
                                    "PSM",
                                    "Report"
                                ],
                                "{measurement_class_name}": [
                                    "PhotoProcess",
                                    "EDXMicroscopyProcess",
                                    "XRDProcess",
                                    "XPSProcess",
                                    "AnnealingProcess",
                                    "LEISProcess",
                                    "ThicknessProcess",
                                    "SEMProcess",
                                    "ResistanceProcess",
                                    "BandgapProcess",
                                    "APTProcess",
                                    "TEMProcess",
                                    "SDCProcess",
                                    "SECCMProcess",
                                    "FIMProcess",
                                    "PSMProcess",
                                    "ReportProcess"
                                ]
                            }, False),

                            (os.path.join(module_dir, "mappings/handovers/activities/activities_for_other_types_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/handovers/activities/activities_for_other_types_prior_to_first_handover_templated.yml"), False),
                            (os.path.join(module_dir, "mappings/handovers/activities/activities_for_other_types_with_no_handovers_templated.yml"), False),

                            (os.path.join(module_dir, "mappings/measurements/measurements_templated.yml"), False),

                            (os.path.join(module_dir, "mappings/generic_relations/object_to_object_templated.yml"), False),

                            (os.path.join(module_dir, "mappings/compositions/compositions_metadata_templated.yml"), True),
                            (os.path.join(module_dir, "mappings/compositions/activities_for_compositions_templated.yml"), True),
                            (os.path.join(module_dir, "mappings/compositions/activities_for_compositions_prior_to_first_handover_templated.yml"), True),
                            (os.path.join(module_dir, "mappings/compositions/activities_for_compositions_with_no_handovers_templated.yml"), True),
                            (os.path.join(module_dir, "mappings/compositions/properties_of_compositions_templated.yml"), True)
                       ]

# RML file all the YARRRML mappings must coalesce to
final_RML_file_path = os.path.join(module_dir, 'mappings/final_RML_file.rml')
materialized_triples_file_path = os.path.join(module_dir, 'materialized_triples/materialized_triples.ttl')
rmlstreamer_path = os.path.join(module_dir, 'materialized_triples')


def resource_usage_job(stop_event, resource_usage):
    while not stop_event.is_set():
        cpu = psutil.cpu_percent(interval=1)
        psutil.cpu_stats()

        used_mem = {
            "used_mem_per_file": {},
            "sql_server": 0
        }
        for proc in psutil.process_iter(['cmdline', 'memory_info']):
            try:
                cmd = proc.info['cmdline']
                if not cmd:
                    continue
                elif "sqlservr" in " ".join(cmd):
                    used_mem["sql_server"] = proc.info['memory_info'].rss

                elif "rmlmapper.jar" in " ".join(cmd):
                    out_idx = cmd.index("-o") + 1
                    output_file = cmd[out_idx]
                    used_mem["used_mem_per_file"][Path(output_file).stem] = proc.info['memory_info'].rss

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        resource_usage.append((cpu, used_mem))


def prepare_YARRRML_files() -> list[tuple[str, str, str]]:
    """
    Fills and writes all untemplated YARRRML files

    Returns a list of (untemplated YARRRML file path, SQL query to execute, CSV file path to store the SQL query results in)
    """
    untemplated_yarrrml_file_names_and_jobs = []

    for i, mapping in enumerate(templated_file_names):
        if len(mapping) == 2:
            templated_yarrrml_file, use_rmlstreamer = mapping
            custom_sql_template = None
            custom_yml_template = None
        else:
            (templated_yarrrml_file, custom_sql_template, custom_yml_template, use_rmlstreamer) = mapping

        untemplated_base_yarrrml_file = templated_yarrrml_file.replace("_templated", "")
        untemplated_yarrrml_file_names_and_jobs += fill_template_values(templated_yml=templated_yarrrml_file,
                                                                        output_file_name=untemplated_base_yarrrml_file,
                                                                        custom_sql_template=custom_sql_template,
                                                                        custom_yml_template=custom_yml_template,
                                                                        convert_to_csv=True,
                                                                        # We only add prefixes to the first mapping
                                                                        add_prefixes= i == 0)

    return untemplated_yarrrml_file_names_and_jobs


def create_rml_file(untemplated_yarrrml_file_paths: list[str]):
    """
    Coalesces a list of valid YARRRML file paths into a single RML file

    The result will be written to `final_RML_file_path`
    """

    if os.environ.get('IN_DOCKER_DEPLOYMENT', False):
        # If we are in a docker deployment, assume we have it installed via npm (npm i -g @rmlio/yarrrml-parser)
        yarrrml_to_rml_cmd = [
            "yarrrml-parser"
        ]

        for f in untemplated_yarrrml_file_paths:
            yarrrml_to_rml_cmd += ["-i", f]
        yarrrml_to_rml_cmd += ["-o", final_RML_file_path]
    else:
        yarrrml_to_rml_cmd = [
            "docker", "run", "--rm",
            "--user", f"{os.getuid()}:{os.getgid()}",
            "-v", f"{os.path.join(module_dir, 'mappings/')}:/data:z",
            "rmlio/yarrrml-parser:latest",
        ]

        for f in untemplated_yarrrml_file_paths:
            yarrrml_to_rml_cmd += ["-i", f.replace(os.path.join(module_dir, 'mappings/'), "/data/")]
        yarrrml_to_rml_cmd += ["-o", final_RML_file_path.replace(os.path.join(module_dir, 'mappings/'), "/data/")]

    try:
        subprocess.run(yarrrml_to_rml_cmd,
                       check=True,
                       capture_output=False,
                       text=True).check_returncode()
    except subprocess.CalledProcessError as e:
        logging.error(f"YARRRML-parser process failed with return code {e.returncode}")
        logging.error(f"Error output:\n{e.stderr}")
        raise e


def run_mappings_queries(db: MSSQLDB,
                         untemplated_yarrrml_file_names_and_jobs: list[tuple[str, str, str]]):
    """
    Runs the SQL queries corresponding to each mapping in parallel, and saves the results to CSV files

    :param db: DB instance to run the queries against
    :param untemplated_yarrrml_file_names_and_jobs: List of (_, SQL query to execute, CSV file path to store the SQL query results in), obtained via `prepare_YARRRML_files()`.
    """
    sql_query_jobs = [(query, output_csv_file_path) for (_, query, output_csv_file_path) in untemplated_yarrrml_file_names_and_jobs]

    # To keep track of which query was being executed by the (unordered) finishing jobs
    sql_query_to_yarrrml_file_path = {query: f.replace(os.path.join(module_dir, 'mappings/'), "/data/") for (f, query, _) in untemplated_yarrrml_file_names_and_jobs}

    yarrrml_files_to_convert = []

    with ThreadPoolExecutor(os.cpu_count()) as executor:
        futures = []

        for (query, output_csv_file_path) in sql_query_jobs:
            futures.append(executor.submit(db.query_to_csv, query, output_csv_file_path))

        with tqdm(total=len(futures), desc="SQL queries executed", leave=True) as pbar:
            for future in as_completed(futures):
                (yielded_results, query) = future.result()
                if yielded_results:
                    yarrrml_files_to_convert.append(sql_query_to_yarrrml_file_path[query])
                # Else: we don't do anything for that mapping

                pbar.update(1)

    return yarrrml_files_to_convert


def execute_mappings(use_rmlstreamer: bool = False):
    """
    Runs RMLMapper or RMLStreamer over `final_RML_file_path`, writing the results to `materialized_triples_file_path`
    """
    # Let's be generous and offer it half of the system's RAM
    max_heap = int(psutil.virtual_memory().total * 0.5 / (1024 ** 3))

    if not use_rmlstreamer:
        cmd = [
            "java",
            f"-Xmx{max_heap}g",
            "-cp", f"{os.path.join(module_dir, 'rmlmapper.jar:sqljdbc')}", "be.ugent.rml.cli.Main",
            "-m", final_RML_file_path,
            "-o", materialized_triples_file_path
        ]

    else:
        cmd = [
            "java", "-jar", os.path.join(module_dir, 'RMLStreamer.jar'), "toFile",
            "-m", final_RML_file_path,
            "-o", rmlstreamer_path
        ]

        with open(materialized_triples_file_path, 'wb') as outfile:
            for filename in sorted(os.listdir(rmlstreamer_path)):
                file_path = os.path.join(rmlstreamer_path, filename)

                if os.path.isfile(file_path) and file_path != os.path.abspath(materialized_triples_file_path):
                    with open(file_path, 'rb') as infile:
                        outfile.write(infile.read())
                    os.remove(file_path)


    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, text=True).check_returncode()
    except subprocess.CalledProcessError as e:
        logging.error(f"RMLMapper materialization process failed with return code {e.returncode}")
        logging.error(f"Error output:\n{e.stderr}")
        raise


def run_mappings(db: MSSQLDB,
                 skip_materialization: bool = False,
                 use_rmlstreamer: bool = False) -> (list[str],
                                                    dict[str, dict[str, float]],
                                                    list[(float, float)]):
    """
    Executes the complete pipeline of templated YARRRML file parsing -> YARRRML to RML files conversion -> mappings execution
    (See the module's documentation for how to include/extend the mappings)

    :param db: DB instance to run the queries against
    :param skip_materialization: Avoids running the entire pipeline (query execution, preparation and conversion of YARRRML files and execution of mappings).
                                 It will return a list of materialized file paths, but assuming they are already present

    :return: Tuple containing:
                - A list of file paths containing the materialized triples in turtle format (For now, it's only one)
                - A dict of file path -> time_measurement_identifier -> time measurement (in s.), containing execution time
                  logs for the different phases of the pipeline
                - A list of pairs of (cpu_percent_usage, bytes_of_memory_used) taken every second for the duration of this function
    """
    manager = multiprocessing.Manager()
    resource_usage = manager.list()
    stop_event = multiprocessing.Event()

    resource_usage_tracker = multiprocessing.Process(
        target=resource_usage_job,
        args=(stop_event, resource_usage)
    )
    resource_usage_tracker.start()

    performance_log: dict[str: float | dict[str: float]] = dict()
    performance_log["per_mapping_times"] = {}

    if skip_materialization:
        logging.info("Skipping materialization. Note that the system will assume the materialized files already exist.")
        return [materialized_triples_file_path], performance_log, list(resource_usage)

    logging.info("Filling the templated YARRRML mappings...")
    untemplated_yarrrml_file_names_and_jobs = prepare_YARRRML_files()
    logging.info(f"YARRRML files created: {len(untemplated_yarrrml_file_names_and_jobs)}")

    logging.info("Executing SQL queries...")
    time_query_execution_start = time.perf_counter()
    yarrrml_files_to_convert = run_mappings_queries(db, untemplated_yarrrml_file_names_and_jobs)
    performance_log["query_execution"] = time.perf_counter() - time_query_execution_start
    logging.info(f" YARRRML files whose queries yielded results: {len(yarrrml_files_to_convert)} / {len(untemplated_yarrrml_file_names_and_jobs)}")

    logging.info("Converting YARRRML mappings to RML...")
    time_yarrrml_to_rml_conversion_start = time.perf_counter()
    create_rml_file(yarrrml_files_to_convert)
    performance_log["yarrrml_to_rml_conversion_real_time"] = time.perf_counter() - time_yarrrml_to_rml_conversion_start


    logging.info(f"Materializing the triples via {"RMLStreamer" if use_rmlstreamer else "RMLMapper"}...")
    time_materialization_start = time.perf_counter()
    execute_mappings(use_rmlstreamer)
    performance_log["materialization_real_time"] = time.perf_counter() - time_materialization_start

    # Stop the resource logging
    stop_event.set()
    resource_usage_tracker.join()

    logging.info("Cleaning up temporary files...")
    for (yml_file, _, csv_file) in untemplated_yarrrml_file_names_and_jobs:
        if os.path.exists(yml_file):
            os.remove(yml_file)

        if os.path.exists(csv_file):
            os.remove(csv_file)

    return [materialized_triples_file_path], performance_log, list(resource_usage)