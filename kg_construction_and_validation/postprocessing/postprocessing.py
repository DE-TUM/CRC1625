"""
Module that performs insertions and deletions in the graph after the materialization of the KG has finished and an
endpoint is available.
This is currently used to:
    - Perform modifications to the KG structure which would be costly or very hard to implement in
      SQL. Right now, it is used for the conversion of handover chains to a two-level hierarchy of handover groups and
      handovers)
    - Programmatically integrate the KG with other ontologies (right now, CheBI) via SPARQL queries, thus avoiding hardcoded
      mappings.
    - Remove temporary IRIs used in the mappings for the postprocessing itself, or replace entity IRIs with richer string
      identifiers

All necessary queries are to be contained in postprocessing/postprocessing_queries
"""

import time
from datetime import datetime
import json
import logging
import os
import sys
from concurrent.futures import as_completed, ProcessPoolExecutor

import psutil
import multiprocessing
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import split_uri, Namespace
from tqdm import tqdm

from datastores.rdf import rdf_datastore_client

HANDOVER_GROUP_CREATION_CHUNK_SIZE = 10_000
CROSS_GROUP_CHAIN_CREATION_CHUNK_SIZE = 10_000

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

module_dir = os.path.dirname(__file__)

# We use a shortened version of the prefixes, because otherwise Virtuoso will refuse to process the queries
# due to their size
prefixes = open(os.path.join(module_dir, './postprocessing_queries/prefixes_postprocessing.sparql')).read()

### Linking with CheBI

# Add CheBI reference for each element in measurement area compositions
add_chebi_elements = prefixes + open(
    os.path.join(module_dir, 'postprocessing_queries/chebi_elements/add_chebi_elements.sparql')).read()

# Delete temporary triples pointing to raw element strings
delete_temporary_triples = prefixes + open(
    os.path.join(module_dir, 'postprocessing_queries/chebi_elements/delete_temporary_triples.sparql')).read()

# Conversion of handover chains into a two-level hierarchy
# (interconnected groups of handovers that take place within the same CRC project)
#
# Obtain all pairs of handovers that take place in different groups
get_cross_project_handovers = prefixes + open(os.path.join(module_dir,
                                                           'postprocessing_queries/handover_chains/get_cross_project_handovers.sparql')).read()

# Obtain all complete handover chains, yielding the beginning and end of them
get_handover_chains = prefixes + open(os.path.join(module_dir,
                                                   'postprocessing_queries/handover_chains/get_handover_chains.sparql')).read()

# Create handover groups for samples that only contain a virtual handover (initial work)
create_groups_for_isolated_initial_work_handovers = prefixes + open(os.path.join(module_dir,
                                                                                 'postprocessing_queries/handover_chains/create_groups_for_isolated_initial_work_handovers.sparql')).read()


# Make the workflow instances point to their corresponding initial work handover group instead of the initial handover itself
redirect_workflow_instances = prefixes + open(
    os.path.join(module_dir, 'postprocessing_queries/handover_chains/redirect_workflow_instances.sparql')).read()

# Delete all pmdco:nextProcess relations between two handovers in different projects
delete_cross_group_links = prefixes + open(
    os.path.join(module_dir, 'postprocessing_queries/handover_chains/delete_cross_group_links.sparql')).read()

# Replace an entity's IRI with another
replace_entity_IRI = prefixes + open(os.path.join(module_dir, 'postprocessing_queries/replace_entity.sparql')).read()

# Map of entity IRIs to replace
entity_replacements: dict = json.load(
    open(os.path.join(module_dir, 'postprocessing_queries/entity_replacements.json'), 'r'))

crc_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/")
crc_hnd_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/handover/")
pmdco_prefix = Namespace("https://w3id.org/pmd/co/")
rdf_prefix = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
rdfs_prefix = Namespace("http://www.w3.org/2000/01/rdf-schema#")
prov_prefix = Namespace("http://www.w3.org/ns/prov#")


def is_rdf_store_virtuoso():
    return rdf_datastore_client.run_sync(rdf_datastore_client.get_datastore_type()) == "virtuoso"


def resource_usage_job(stop_event, resource_usage):
    while not stop_event.is_set():
        cpu = psutil.cpu_percent(interval=1)

        if is_rdf_store_virtuoso():
            process_name = "virtuoso-t"
        else:
            process_name = "oxigraph"

        used_mem = 0
        for proc in psutil.process_iter(['cmdline', 'memory_info']):
            try:
                if proc.info['cmdline'] and any(process_name in arg for arg in proc.info['cmdline']):
                    used_mem += proc.info['memory_info'].rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        resource_usage.append((cpu, used_mem))


def create_handover_group_triples(worker_id: int, chains_batch: list[(str, (str, str, str, str, str, str))]) -> str:
    """
    Creates handover group triples for a batch of (sample_id, (hnd_start, hnd_start_id, hnd_end, hnd_end_id, hnd_start_date, project_iri))
    The results are increasingly cached to a rdflib graph and serialized into a file identified by i

    :returns: The (unique) filename to which the triples for all handovers in the batch have been saved, in turtle format
    """
    g = Graph()

    for (sample_id, chains) in chains_batch:
        chains.sort(key=lambda x: x[4])  # Sort based on hnd_start_date

        for i, (hnd_start, hnd_start_id, hnd_end, hnd_end_id, hnd_start_date, project_iri) in enumerate(chains):
            _, project_name = split_uri(project_iri)

            if hnd_start_id == "initial_work" and hnd_end_id == "initial_work":
                hnd_group_iri = f"https://crc1625.mdi.ruhr-uni-bochum.de/handover/handovers_in_project_{project_name}_initial_work_for_ML_{sample_id}"

                replacements = {
                    "hnd_group_iri": hnd_group_iri,
                    "hnd_1_iri": hnd_start,
                    "project_iri": project_iri,
                    "label": f"Initial work handover group within the {project_name} project"
                }

            else:
                hnd_group_iri = f"https://crc1625.mdi.ruhr-uni-bochum.de/handover/handovers_in_project_{project_name}_{hnd_start_id}_{hnd_end_id}"

                replacements = {
                    "hnd_group_iri": hnd_group_iri,
                    "hnd_1_iri": hnd_start,
                    "project_iri": project_iri,
                    "label": f"Consecutive handovers within the {project_name} project"
                }

            g.add((URIRef(replacements["hnd_group_iri"]), rdf_prefix.type, crc_prefix.HandoverGroup))
            g.add((URIRef(replacements["hnd_group_iri"]), pmdco_prefix.subordinateProcess, URIRef(replacements["hnd_1_iri"])))
            g.add((URIRef(replacements["hnd_group_iri"]), rdfs_prefix.label, Literal(replacements["label"])))
            g.add((URIRef(replacements["hnd_group_iri"]), prov_prefix.wasAssociatedWith, URIRef(replacements["project_iri"])))

            if i + 1 < len(chains):  # Link it with the next group
                (_, hnd_start_id_next, _, hnd_end_id_next, _, project_iri_next) = chains[i + 1]
                _, project_name_next = split_uri(project_iri_next)

                hnd_group_iri_next = f"https://crc1625.mdi.ruhr-uni-bochum.de/handover/handovers_in_project_{project_name_next}_{hnd_start_id_next}_{hnd_end_id_next}"

                g.add((URIRef(hnd_group_iri), pmdco_prefix.nextProcess, URIRef(hnd_group_iri_next)))

    file_name = os.path.join(module_dir, f"handover_groups_{worker_id}.ttl")
    g.serialize(destination=os.path.join(module_dir, file_name), format='turtle')

    return file_name


def create_handover_group_chains():
    """
    Transforms the handover chains in the datastore to a two-level hierarchy of handover groups and virtual/non-virtual
    handovers, required for the validation of workflows

    The conversion is based on the following steps:
        1. Cache all handover pais that are located in different groups, alongside metadata about which sample they belong to, etc.
        2. Delete the pmdco:nextProcess links between these handovers
        3. Query for the resulting (isolated) handover chains
        4. Using the cached information from step 1., group the handover chains by the sample they belong to, sort them based
           on their creation dates, create a handover group for each chain and connect them accordingly
        5. Create handover groups for remaining cases, such as samples which only had one single virtual (initial work) handover
    """

    performance_log = dict()

    logging.info("Querying for cross-project handover pairs...")
    start = time.perf_counter()
    response_cross_project_handovers = rdf_datastore_client.run_sync(rdf_datastore_client.launch_query(get_cross_project_handovers))
    performance_log["query_cross_project_handovers"] = time.perf_counter() - start

    # Using the query, cache the correspondence of beginnings and ends of handover chains to their samples
    # This will be used later to identify to which sample the remaining handover chains belong to
    hnd_to_sample: dict[str, str] = dict()
    for binding in response_cross_project_handovers["results"]["bindings"]:
        hnd_start = binding["start"]["value"]
        hnd_end = binding["end"]["value"]

        sample_id = binding["sample_id"]["value"]

        hnd_to_sample[hnd_start] = sample_id
        hnd_to_sample[hnd_end] = sample_id

    logging.info("Deleting cross-project links between handovers...")
    query = delete_cross_group_links
    if is_rdf_store_virtuoso():
        # Prevents OOMs from writing transactions to memory if there are many affected triples
        query = "DEFINE sql:log-enable 3\n" + query
    start = time.perf_counter()
    rdf_datastore_client.run_sync(rdf_datastore_client.launch_update(query))
    performance_log["delete_cross_project_links"] = time.perf_counter() - start

    logging.info("Querying for the remaining handover chains...")
    start = time.perf_counter()
    response_get_handover_chains = rdf_datastore_client.run_sync(rdf_datastore_client.launch_query(get_handover_chains))
    performance_log["query_remaining_handover_chains"] = time.perf_counter() - start

    logging.info("Processing and sorting handover chains for each sample...")
    start = time.perf_counter()
    # Dict of sample_id -> [(hnd_start, hnd_start_id, hnd_end, hnd_end_id, hnd_start_date, group)], containing all
    # necessary information to instantiate the triples for each of the handover chains the sample contains
    handover_chains: dict[str, (str, str, str, str, str, str)] = dict()
    for binding in response_get_handover_chains["results"]["bindings"]:
        # For each handover chain, we identify the handover IDs and samples they belong to, distinguishing between virtual
        # and "normal" handovers
        sample_id = None

        hnd_start = binding["start"]["value"]

        if "start_id" in binding:
            # It's a normal handover, we have to get the sample it belongs to from the previously run query
            hnd_start_id = binding["start_id"]["value"]
        else:
            # It's an initial work handover, we can get the sample it belongs to right away
            hnd_start_id = "initial_work"
            sample_id = binding["sample_id"]["value"]

        hnd_start_date = datetime.fromisoformat(binding["start_date"]["value"])


        hnd_end = binding["end"]["value"]
        if "end_id" in binding:
            # It's a normal handover, we have to get the sample it belongs to from the previously run query
            hnd_end_id = binding["end_id"]["value"]
        else:
            # It's an initial work handover, we can get the sample it belongs to right away
            hnd_end_id = "initial_work"
            sample_id = binding["sample_id"]["value"]

        group = binding["group"]["value"]

        if sample_id is None:
            if hnd_end in hnd_to_sample:
                sample_id = hnd_to_sample[hnd_end]
            elif hnd_start in hnd_to_sample:
                sample_id = hnd_to_sample[hnd_start]

        if sample_id not in handover_chains:
            handover_chains[sample_id] = []

        handover_chains[sample_id].append((hnd_start, hnd_start_id, hnd_end, hnd_end_id, hnd_start_date, group))


    handover_chains_list = list(handover_chains.items())
    handover_chains_chunks = [handover_chains_list[i:i + HANDOVER_GROUP_CREATION_CHUNK_SIZE]
                              for i in range(0, len(handover_chains_list), HANDOVER_GROUP_CREATION_CHUNK_SIZE)]

    performance_log["process_remaining_handover_chains"] = time.perf_counter() - start

    # Create and load the handover groups. Since there are potentially thousands, we chunk them into jobs and do this in
    # parallel. Each worker will create a .ttl file containing the triples for its assigned chunk and yield its path. Once
    # all workers have finished, they will be uploaded in bulk to the datastore
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=len(handover_chains_chunks)) as executor:
        futures = [executor.submit(create_handover_group_triples, i, chunk) for (i ,chunk) in enumerate(handover_chains_chunks)]

        files_to_upload = []
        for future in tqdm(as_completed(futures), total=len(futures),
                           desc=f"Creating handover groups (batches of {HANDOVER_GROUP_CREATION_CHUNK_SIZE} handovers)..."):
            files_to_upload.append(future.result())

        logging.info("Bulk loading handover groups...")
        rdf_datastore_client.run_sync(rdf_datastore_client.bulk_file_load(files_to_upload, delete_files_after_upload=True))

    performance_log["create_and_load_handover_groups"] = time.perf_counter() - start

    logging.info("Redirecting workflow instances...")
    # Make the workflow instances point to the first group instead of the first handover
    query = redirect_workflow_instances
    if is_rdf_store_virtuoso():
        # Prevents OOMs from writing transactions to memory if there are many affected triples
        query = "DEFINE sql:log-enable 3\n" + query
    start = time.perf_counter()
    rdf_datastore_client.run_sync(rdf_datastore_client.launch_update(query))
    performance_log["redirect_workflow_instances"] = time.perf_counter() - start

    logging.info("Creating handover groups for remaining initial work handovers...")
    query = create_groups_for_isolated_initial_work_handovers
    if is_rdf_store_virtuoso():
        # Prevents OOMs from writing transactions to memory if there are many affected triples
        query = "DEFINE sql:log-enable 3\n" + query
    start = time.perf_counter()
    rdf_datastore_client.run_sync(rdf_datastore_client.launch_update(query))
    performance_log["create_handover_groups_for_initial_work_handovers"] = time.perf_counter() - start

    return performance_log

def replace_entity_iris():
    """
    Replace entity IRIs in the KG. Currently used to have clearer IRI names for measurement types
    """
    for old_iri, new_iri in entity_replacements.items():
        replacements = {
            "{old_iri}": old_iri,
            "{new_iri}": new_iri
        }
        query = replace_entity_IRI
        for k, v in replacements.items():
            query = query.replace(k, v)

        if is_rdf_store_virtuoso():
            # Prevents OOMs from writing transactions to memory if there are many affected triples
            query = "DEFINE sql:log-enable 3\n" + query
        rdf_datastore_client.run_sync(rdf_datastore_client.launch_update(query))


def integrate_with_CheBI():
    """
    Add references to CheBI elements to every composition present in the KG. This is done in a programmatic way by
    querying for the CheBI entities that contain the same string formulas that are for now present in the compositions
    """
    performance_log = dict()

    logging.info("Adding CheBI elements to EDX compositions and samples...")
    query = add_chebi_elements
    if is_rdf_store_virtuoso():
        # Prevents OOMs from writing transactions to memory if there are many affected triples
        query = "DEFINE sql:log-enable 3\n" + query
    start = time.perf_counter()
    rdf_datastore_client.run_sync(rdf_datastore_client.launch_update(query))
    performance_log["add_chebi_elements"] = time.perf_counter() - start

    logging.info("Deleting temporary triples...")
    start = time.perf_counter()
    # The amount of temporary triples can be quite high, as they are used for the compositions
    # If we are using virtuoso, we do it in SQL directly
    if is_rdf_store_virtuoso():
            # TODO: This is unsafe
            rdf_datastore_client.run_sync(rdf_datastore_client.run_isql("""DELETE FROM DB.DBA.RDF_QUAD WHERE
                                                                           P = iri_to_id('https://crc1625.mdi.ruhr-uni-bochum.de/temporaryDatatypeProperty')
                                                                           AND G = iri_to_id('https://crc1625.mdi.ruhr-uni-bochum.de/graph');"""))
            rdf_datastore_client.run_sync(rdf_datastore_client.run_isql('checkpoint;'))
    else:
        rdf_datastore_client.run_sync(rdf_datastore_client.launch_update(delete_temporary_triples))

    performance_log["delete_temporary_triples"] = time.perf_counter() - start

    return performance_log


def run_postprocessing() -> (dict[str, float], list[(float, float)]):
    """
    Performs insertions and deletions in the graph after the materialization of the KG has finished and an
    endpoint is available. The endpoint type is indicated by the datastore parameter.
    This is currently used to:
        - Perform modifications to the KG structure which would be costly or very hard to implement in
          SQL. Right now, it is used for the conversion of handover chains to a two-level hierarchy of handover groups and
          handovers)
        - Programmatically integrate the KG with other ontologies (right now, CheBI) via SPARQL queries, thus avoiding hardcoded
          mappings.
        - Remove temporary IRIs used in the mappings for the postprocessing itself, or replace entity IRIs with richer string
          identifiers

    :return: Tuple containing:
                - A dict of file path -> time_measurement_identifier -> time measurement (in s.), containing execution time
                  logs for the different phases of the postprocessing pipeline
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
    time.sleep(1.0) # Give enough time to catch a trace of the datastore *before* running any queries

    performance_log = dict()

    performance_log_handover_chains = create_handover_group_chains()

    performance_log_chebi_integration = integrate_with_CheBI()

    logging.info("Replacing temporary entity IRIs...")
    start = time.perf_counter()
    replace_entity_iris()
    performance_log["replace_temporary_entity_iris"] = time.perf_counter() - start

    # Combine them
    performance_log = performance_log | performance_log_handover_chains
    performance_log = performance_log | performance_log_chebi_integration

    time.sleep(1.0)  # Give enough time to catch a trace of the datastore *after* running all postprocessing queries
    stop_event.set()
    resource_usage_tracker.join()

    return performance_log, list(resource_usage)



