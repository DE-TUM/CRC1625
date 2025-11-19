"""
Validates the correct behavior of the Handover workflows validation API

To do so, it will load existing graph dumps and (workflow model, workflow instance) pairs from ./handover_workflows_validation/validation_test
Each pair contains a specific configuration for which we know the validation results of every step. These can be found in
the different run_* functions.

If there is a mismatch during execution, the process will stop and output the shapes and the (mismatching) validation results

This test can be run on either an Oxigraph or Virtuoso instance. *All* datastore contents will be cleared
"""

import argparse
import logging
import os
import sys
import traceback

from datastores.rdf.oxigraph_datastore import OxigraphRDFDatastore
from datastores.rdf.rdf_datastore import RDFDatastore
from datastores.rdf.virtuoso_datastore import VirtuosoRDFDatastore
from handover_workflows_validation.handover_workflows_validation import (generate_SHACL_shapes_for_workflow, validate_SHACL_rules, read_workflow_model, get_workflow_instances_of_model, WORKFLOWS_GRAPH_IRI)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

module_dir = os.path.dirname(__file__)

sample_name_to_id = {
    'Sample1': 1,
    'AnnealedSample1': 2,
    'AnnealedSample1Piece1': 3,
    'AnnealedSample1Piece2': 4,
}

def run_test(path: str,
             step_validities: dict[str, dict[int, bool]],
             store: RDFDatastore):
    """
    Runs a validation test.
    :param path Expects the base path containing *exclusively* the name of the workflow (e.g.,
    `handover_workflows_validation/validation_test/workflow_config_files/incorrect_activities/step_1`), to which
    `workflow_model.ttl` and `_workflow_instance.ttl` will be appended to load the respective files.

    :param step_validities Contains a dictionary of step name -> sample id -> validity, which indicates whether a given
    sample id should pass the SHACL validation for its parent step. If, when trying to replicate these results, there
    is a mismatch, will be stopped and a complete trace will be printed out.
    """
    store.clear_triples(WORKFLOWS_GRAPH_IRI)

    store.upload_file(path + "_workflow_model.ttl", graph_iri=WORKFLOWS_GRAPH_IRI)
    store.upload_file(path + "_workflow_instance.ttl", graph_iri=WORKFLOWS_GRAPH_IRI)

    workflow_model = read_workflow_model("example_workflow", 1, store)
    workflow_instance, _ = get_workflow_instances_of_model(workflow_model, 1, store)[0] # There's only one, no need to look it up

    steps_to_validate = generate_SHACL_shapes_for_workflow(workflow_model, workflow_instance, store)
    results = validate_SHACL_rules(steps_to_validate, store)

    for (workflow_step, workflow_step_name, sample_id, target_node, shacl_rules, conforms, results_text) in results:
        if conforms != step_validities[workflow_step_name][sample_id]:
            raise ValueError(f"""
            Mismatching validation for {path}. 
            Step: {workflow_step_name}
            Target node: {target_node}
            Sample: {sample_id}
            SHACL rules: {shacl_rules}
            Expected validation result: {step_validities[workflow_step_name][sample_id]}
            Validation result: {conforms}
            SHACL message: {results_text}
            """)
        else:
            # Clear the sample and the step if there are no samples remaining, to mark that they were
            # validated
            del step_validities[workflow_step_name][sample_id]
            if len(step_validities[workflow_step_name]) == 0:
                del step_validities[workflow_step_name]

    if len(step_validities) != 0:
        raise ValueError(f"""
        Some validation steps were not executed: {step_validities}
        """)

def run_incorrect_activities_tests(store: RDFDatastore):
    run_test(os.path.join(module_dir,'handover_workflows_validation/validation_test/workflow_config_files/incorrect_activities/step_1'),
             {
                 'step_1': {sample_name_to_id['Sample1']: False},
                 'step_2': {sample_name_to_id['Sample1']: True},
                 'step_3': {sample_name_to_id['Sample1']: True},
                 'step_4': {sample_name_to_id['AnnealedSample1']: True},
                 'step_5': {sample_name_to_id['AnnealedSample1']: True},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
             }, store)

    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_activities/step_2'),
             {
                 'step_1': {sample_name_to_id['Sample1']: True},
                 'step_2': {sample_name_to_id['Sample1']: False},
                 'step_3': {sample_name_to_id['Sample1']: True},
                 'step_4': {sample_name_to_id['AnnealedSample1']: True},
                 'step_5': {sample_name_to_id['AnnealedSample1']: True},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
             }, store)

    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_activities/step_3'),
             {
                 'step_1': {sample_name_to_id['Sample1']: True},
                 'step_2': {sample_name_to_id['Sample1']: True},
                 'step_3': {sample_name_to_id['Sample1']: False},
                 'step_4': {sample_name_to_id['AnnealedSample1']: True},
                 'step_5': {sample_name_to_id['AnnealedSample1']: True},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
             }, store)

    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_activities/step_4'),
             {
                 'step_1': {sample_name_to_id['Sample1']: True},
                 'step_2': {sample_name_to_id['Sample1']: True},
                 'step_3': {sample_name_to_id['Sample1']: True},
                 'step_4': {sample_name_to_id['AnnealedSample1']: False},
                 'step_5': {sample_name_to_id['AnnealedSample1']: True},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
             }, store)
    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_activities/step_5'),
             {
                 'step_1': {sample_name_to_id['Sample1']: True},
                 'step_2': {sample_name_to_id['Sample1']: True},
                 'step_3': {sample_name_to_id['Sample1']: True},
                 'step_4': {sample_name_to_id['AnnealedSample1']: True},
                 'step_5': {sample_name_to_id['AnnealedSample1']: False},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
             }, store)

    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_activities/step_6'),
             {
                 'step_1': {sample_name_to_id['Sample1']: True},
                 'step_2': {sample_name_to_id['Sample1']: True},
                 'step_3': {sample_name_to_id['Sample1']: True},
                 'step_4': {sample_name_to_id['AnnealedSample1']: True},
                 'step_5': {sample_name_to_id['AnnealedSample1']: True},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: False, sample_name_to_id['AnnealedSample1Piece2']: False}
             }, store)

def run_incorrect_projects_tests(store: RDFDatastore):
    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_projects/step_1'),
             {
                 'step_1': {sample_name_to_id['Sample1']: False},
                 'step_2': {sample_name_to_id['Sample1']: True},
                 'step_3': {sample_name_to_id['Sample1']: True},
                 'step_4': {sample_name_to_id['AnnealedSample1']: True},
                 'step_5': {sample_name_to_id['AnnealedSample1']: True},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
             }, store)

    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_projects/step_2'),
             {
                 'step_1': {sample_name_to_id['Sample1']: True},
                 'step_2': {sample_name_to_id['Sample1']: False},
                 'step_3': {sample_name_to_id['Sample1']: True},
                 'step_4': {sample_name_to_id['AnnealedSample1']: True},
                 'step_5': {sample_name_to_id['AnnealedSample1']: True},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
             }, store)

    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_projects/step_3'),
             {
                 'step_1': {sample_name_to_id['Sample1']: True},
                 'step_2': {sample_name_to_id['Sample1']: True},
                 'step_3': {sample_name_to_id['Sample1']: False},
                 'step_4': {sample_name_to_id['AnnealedSample1']: True},
                 'step_5': {sample_name_to_id['AnnealedSample1']: True},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
             }, store)

    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_projects/step_4'),
             {
                 'step_1': {sample_name_to_id['Sample1']: True},
                 'step_2': {sample_name_to_id['Sample1']: True},
                 'step_3': {sample_name_to_id['Sample1']: True},
                 'step_4': {sample_name_to_id['AnnealedSample1']: False},
                 'step_5': {sample_name_to_id['AnnealedSample1']: True},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
             }, store)
    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_projects/step_5'),
             {
                 'step_1': {sample_name_to_id['Sample1']: True},
                 'step_2': {sample_name_to_id['Sample1']: True},
                 'step_3': {sample_name_to_id['Sample1']: True},
                 'step_4': {sample_name_to_id['AnnealedSample1']: True},
                 'step_5': {sample_name_to_id['AnnealedSample1']: False},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
             }, store)

    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_projects/step_6'),
             {
                 'step_1': {sample_name_to_id['Sample1']: True},
                 'step_2': {sample_name_to_id['Sample1']: True},
                 'step_3': {sample_name_to_id['Sample1']: True},
                 'step_4': {sample_name_to_id['AnnealedSample1']: True},
                 'step_5': {sample_name_to_id['AnnealedSample1']: True},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: False, sample_name_to_id['AnnealedSample1Piece2']: False}
             }, store)

def run_incorrect_numbers_of_activities_tests(store: RDFDatastore):
    run_test(os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/incorrect_numbers_of_activities/step_5'),
             {
                 'step_1': {sample_name_to_id['Sample1']: True},
                 'step_2': {sample_name_to_id['Sample1']: True},
                 'step_3': {sample_name_to_id['Sample1']: True},
                 'step_4': {sample_name_to_id['AnnealedSample1']: True},
                 'step_5': {sample_name_to_id['AnnealedSample1']: False},
                 'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
             }, store)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--store",
        choices=["oxigraph", "virtuoso"],
        required=True,
        help="RDF store to use. Possible values: 'oxigraph' or 'virtuoso'"
    )

    args = parser.parse_args()

    store = None
    if args.store == "virtuoso":
        store = VirtuosoRDFDatastore()
    else:
        store = OxigraphRDFDatastore()
        store.start_oxigraph()

    store.clear_triples()
    store.upload_file(os.path.join(module_dir, "handover_workflows_validation/validation_test/validation_test_triples.ttl"))

    ontology_files: list[dict[str, str]] = [
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

    store.bulk_file_load(f["file"] for f in ontology_files)

    try:
        logging.info("Testing example correct workflow...")
        run_test(os.path.join(module_dir,
                              'handover_workflows_validation/validation_test/workflow_config_files/example_workflow_valid'),
                 {
                     'step_1': {sample_name_to_id['Sample1']: True},
                     'step_2': {sample_name_to_id['Sample1']: True},
                     'step_3': {sample_name_to_id['Sample1']: True},
                     'step_4': {sample_name_to_id['AnnealedSample1']: True},
                     'step_5': {sample_name_to_id['AnnealedSample1']: True},
                     'step_6': {sample_name_to_id['AnnealedSample1Piece1']: True, sample_name_to_id['AnnealedSample1Piece2']: True}
                 }, store)

        logging.info("Running incorrect activities tests...")
        run_incorrect_activities_tests(store)
        logging.info("Running incorrect projects tests...")
        run_incorrect_projects_tests(store)
        logging.info("Running incorrect numbers of activities tests...")
        run_incorrect_numbers_of_activities_tests(store)

        logging.info("Tests run successfully!")

        store.clear_triples()
        store.clear_triples(graph_iri=WORKFLOWS_GRAPH_IRI)
        if args.store == "oxigraph":
            store.stop_oxigraph()

    except Exception as e:
        logging.error(f"Exception while running the tests: {e}")
        logging.error(f"The stores have not been stopped: {e}")
        traceback.print_exc()
