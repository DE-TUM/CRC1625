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
import shutil
import sys
import time
from concurrent.futures import as_completed, ThreadPoolExecutor
from pathlib import Path
from typing import Callable

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


def convert_yarrrml_to_rml_job(output_file_name, rml_file_name):
    start_yarrrml_to_rml_conversion = time.perf_counter()

    yarrrml_to_rml_cmd = [
        "docker", "run", "--rm",
        "--user", f"{os.getuid()}:{os.getgid()}",
        "-v", f"{os.path.join(module_dir, 'mappings/')}:/data:z",
        "rmlio/yarrrml-parser:latest",
        "-i", output_file_name.replace(os.path.join(module_dir, 'mappings/'), "/data/"),
        "-o", rml_file_name.replace(os.path.join(module_dir, 'mappings/'), "/data/")
    ]

    try:
        subprocess.run(yarrrml_to_rml_cmd, check=True, capture_output=False, text=True)
    except subprocess.CalledProcessError as e:
        logging.error(
            f"{output_file_name}: YARRRML to RML conversion process failed with return code {e.returncode}")
        logging.error(f"Error output:\n{e.stderr}")

    return time.perf_counter() - start_yarrrml_to_rml_conversion


def rmlmapper_materialization_job(yarrrml_file: str):
    rml_file_name = yarrrml_file.split('.')[0] + "_rml.ttl"
    materialized_file_name = rml_file_name.replace("/mappings/", "/materialized_triples/").split('.')[
                                 0] + "_materialized.ttl"

    max_heap = int(psutil.virtual_memory().total * 0.5 / (1024 ** 3))
    cmd = [
        "java",
        f"-Xmx{max_heap}g",
        "-cp", f"{os.path.join(module_dir, 'rmlmapper.jar:sqljdbc')}", "be.ugent.rml.cli.Main",
        "-m", rml_file_name,
        "-o", materialized_file_name
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=False, text=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"{yarrrml_file}: RML Materialization process failed with return code {e.returncode}")
        logging.error(f"Error output:\n{e.stderr}")
        raise

    os.remove(yarrrml_file)
    os.remove(rml_file_name)

    return yarrrml_file, materialized_file_name


def rmlstreamer_materialization_job(yarrrml_file: str, csv_job: tuple[str, str, bool]):
    (query, csv_file, run_only_sql_queries) = csv_job
    MSSQLDB().query_to_csv(query, csv_file)

    rml_file_name = yarrrml_file.split('.')[0] + "_rml.ttl"
    materialized_file_name = rml_file_name.replace("/mappings/", "/materialized_triples/").split('.')[
                                 0] + "_materialized.ttl"

    output_dir = materialized_file_name.replace(".ttl", "")
    cmd = [
        "java", "-jar", os.path.join(module_dir, 'RMLStreamer.jar'), "toFile",
        "-m", rml_file_name,
        "-o", output_dir
    ]

    if not run_only_sql_queries:
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, text=True)
        except subprocess.CalledProcessError as e:
            logging.error(f"RMLStreamer materialization process failed with return code {e.returncode}")
            logging.error(f"Error output:\n{e.stderr}")
            raise

        with open(materialized_file_name, "w") as outfile:
            for file_path in Path(output_dir).iterdir():
                if file_path.is_file():
                    with open(file_path, "r") as infile:
                        outfile.write(infile.read())

        # Clean up RMLStreamer's output dir
        shutil.rmtree(output_dir)
        # and the rest of the files
        os.remove(yarrrml_file)
        os.remove(rml_file_name)
        os.remove(csv_file)

    return yarrrml_file, materialized_file_name


def get_mapping_jobs(mapping: tuple[str, bool] | tuple[str, dict, dict, bool],
                     skip_materialization: bool = False,
                     run_only_sql_queries: bool = False) \
        -> tuple[str, list[tuple[tuple[Callable, str], tuple[Callable, str, bool] | None]]]:
    """
    :param run_only_sql_queries: Forces the job to be an RMLStreamer job that will only run the SQL query. This is only
                                  used to measure query times for debugging

    :returns: If use_rmlstreamer = False, a tuple of untemplated_base_yarrrml_file, [((rmlmapper_materialization_job, yarrrml_file_name), None)]
              If use_rmlstreamer = True, a tuple of untemplated_base_yarrrml_file, [((rmlstreamer_materialization_job, yarrrml_file_name), (query, csv_file, run_only_sql_queries))]

              In both cases, the returned tuple consists of the reference to the untemplated YARRRML file and a list of jobs to execute. If RMLStreamer is
              to be used, then the list will also contain the jobs to execute in order to generate the CSV files RMLStreamer requires as input
    """

    if len(mapping) == 2:
        templated_yarrrml_file, use_rmlstreamer = mapping
        custom_sql_template = None
        custom_yml_template = None
    else:
        (templated_yarrrml_file, custom_sql_template, custom_yml_template, use_rmlstreamer) = mapping

    if run_only_sql_queries:
        use_rmlstreamer = True # Force it to generate CSV files

    untemplated_base_yarrrml_file = templated_yarrrml_file.replace("_templated", "")
    untemplated_yarrrml_file_names_and_jobs = fill_template_values(templated_yarrrml_file,
                                                                   untemplated_base_yarrrml_file,
                                                                   custom_sql_template,
                                                                   custom_yml_template,
                                                                   use_rmlstreamer)

    jobs = []
    if not skip_materialization:
        if use_rmlstreamer:
            for (yarrrml_file_name, query, csv_file) in untemplated_yarrrml_file_names_and_jobs:
                jobs.append(((rmlstreamer_materialization_job, yarrrml_file_name), (query, csv_file, run_only_sql_queries)))
        else:
            for yarrrml_file_name in untemplated_yarrrml_file_names_and_jobs:
                jobs.append(((rmlmapper_materialization_job, yarrrml_file_name), None))

    return untemplated_base_yarrrml_file, jobs


def run_mappings(skip_materialization: bool =False, run_only_sql_queries: bool = False) -> (list[str],
                                                                                            dict[str, dict[str, float]],
                                                                                            list[(float, float)]):
    """
    Executes the complete pipeline of templated YARRRML file parsing -> YARRRML to RML files conversion -> mappings execution
    (See the module's documentation for how to include/extend the mappings)

    :param skip_materialization: Avoids materializing the YARRRML files. It will assume that they are present in the
                                 materialization/materialized_triples/ folder
    :param run_only_sql_queries: Runs the entire pipeline without executing RML->RDF mapping jobs. Instead, only run
                                 their SQL queries by saving them into .csv files. This is only used to measure
                                 query times for debugging


    :return: Tuple containing:
                - A list of file paths containing the materialized triples in turtle format
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

    results = []
    performance_log: dict[str: float | dict[str: float]] = dict()
    performance_log["per_mapping_times"] = {}

    yarrrml_file_correspondences = dict()

    # Create all untemplated YARRRML files, and save a reference to each one as an individual job
    mapping_jobs = []
    for m in templated_file_names:
        base_untemplated_yarrrml_file_name, jobs = get_mapping_jobs(m, skip_materialization, run_only_sql_queries)
        mapping_jobs.append((base_untemplated_yarrrml_file_name, jobs))

        performance_log["per_mapping_times"][base_untemplated_yarrrml_file_name] = dict()

        for (_, yarrrml_file), _ in jobs:
            yarrrml_file_correspondences[yarrrml_file] = base_untemplated_yarrrml_file_name


    # Convert all YARRRML files to RML
    time_yarrrml_to_rml_conversion_start = time.perf_counter()

    with ThreadPoolExecutor(os.cpu_count()) as executor:
        yarrrml_to_rml_jobs = []
        for _, jobs in mapping_jobs:
            for (_, yarrrml_file), _ in jobs:
                rml_file_name = yarrrml_file.split('.')[0] + "_rml.ttl"
                yarrrml_to_rml_jobs.append((convert_yarrrml_to_rml_job, yarrrml_file, rml_file_name))

        futures = {
            executor.submit(function, yarrrml_file, rml_file_name) for (function, yarrrml_file, rml_file_name) in yarrrml_to_rml_jobs
        }
        with tqdm(total=len(futures), desc="Mappings converted to RML", leave=True) as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)

    performance_log["yarrrml_to_rml_conversion_real_time"] = time.perf_counter() - time_yarrrml_to_rml_conversion_start

    # Execute all mapping jobs
    time_materialization_start = time.perf_counter()

    with ThreadPoolExecutor(os.cpu_count()) as executor:
        total_materialization_jobs = []
        for base_untemplated_yarrrml_file_name, jobs in mapping_jobs:
            total_materialization_jobs += jobs

        futures = []
        for ((function, arg), csv_job) in total_materialization_jobs:
            if csv_job is not None:
                futures.append(executor.submit(function, arg, csv_job))
            else:
                futures.append(executor.submit(function, arg))

        with tqdm(total=len(futures), desc="Mappings processed", leave=True) as pbar:
            for future in as_completed(futures):
                yarrrml_file, materialized_file_name = future.result()

                base_untemplated_yarrrml_file_name = yarrrml_file_correspondences[yarrrml_file]

                materialization_time_for_file = time.perf_counter() - time_materialization_start

                performance_log["per_mapping_times"][base_untemplated_yarrrml_file_name][yarrrml_file] = {
                    "rml_materialization": materialization_time_for_file,
                }

                tqdm.write(f"✔ Processing of {yarrrml_file} completed! "
                           f"Materialization time = {materialization_time_for_file:.2f}s")

                pbar.update(1)

                results.append(materialized_file_name)

    # Log the time for all the mappings to finish. This differs from the sum of all
    # mapping times if multithreading is used (by default)
    performance_log["materialization_real_time"] = time.perf_counter() - time_materialization_start

    stop_event.set()
    resource_usage_tracker.join()

    return results, performance_log, list(resource_usage)