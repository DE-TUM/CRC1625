"""
Main script used to execute the complete KG generation pipeline. It can be called via CLI to control its different behaviors
(e.g., avoid performing postprocessing..., etc.)
"""
import asyncio
import logging
import random
import sys
import uuid
from copy import deepcopy
from itertools import chain

from rdflib import URIRef, Graph

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore import WORKFLOWS_GRAPH_IRI, MAIN_GRAPH_IRI
from workflows_validation.CRC_1625_workflows_validator.CRC_1625_workflows_validator import CRC1625WorkflowModelStep, \
    activity_name_to_class_iri, project_name_to_iri
from workflows_validation.common import crc_prefix, rdf_prefix
from workflows_validation.workflow_instance import WorkflowInstance, StepAssignment, store_workflow_instance, get_workflow_instances_of_model
from workflows_validation.workflow_model import WorkflowModel, WorkflowModelStep, store_workflow_model, read_workflow_model
from workflows_validation.workflows_validator import is_workflow_instance_valid, ValidationStatus

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def generate_handover_group_definition(length: int) -> list[tuple[URIRef, list[tuple[str, URIRef]]]]:
    handover_group_definition: list[tuple[URIRef, list[tuple[str, URIRef]]]] = list()
    activity_names_and_class_iris = [[k, v] for k, v in activity_name_to_class_iri.items()]
    for i in range(length):
        project_name: URIRef = random.choice(list(project_name_to_iri.keys()))
        activity_choices = random.choices(activity_names_and_class_iris, k=random.choice(range(1, 5)))
        handover_group_definition.append((project_name, activity_choices))

    return handover_group_definition


def generate_handover_group_triples(handover_group_definition: list[tuple[URIRef, list[tuple[str, URIRef]]]]) -> tuple[Graph, URIRef]:
    g = Graph()

    workflow_instance_iri = crc_prefix.handover_workflow_instance
    g.add((workflow_instance_iri, rdf_prefix.type, crc_prefix.workflowModelInstance))

    first_handover_group_iri = None
    previous_handover_group_iri = None
    for i, (project_name, activity_class_names_and_iris) in enumerate(handover_group_definition):
        handover_group_iri = crc_prefix[f"handover_group_{i}"]
        if i == 0:
            g.add((workflow_instance_iri, crc_prefix.substep, handover_group_iri))
            first_handover_group_iri = handover_group_iri
        else:
            g.add((previous_handover_group_iri, crc_prefix.nextStep, handover_group_iri))

        g.add((handover_group_iri, crc_prefix.assignedTo, project_name_to_iri[project_name]))
        g.add((handover_group_iri, rdf_prefix.type, crc_prefix.HandoverGroup))

        for _, activity_class_iri in activity_class_names_and_iris:
            handover_iri = crc_prefix[uuid.uuid4().hex]
            activity_iri = crc_prefix[uuid.uuid4().hex]

            g.add((handover_group_iri, crc_prefix.substep, handover_iri))
            g.add((handover_iri, crc_prefix.substep, activity_iri))
            g.add((handover_iri, rdf_prefix.type, crc_prefix.Handover))

            g.add((activity_iri, rdf_prefix.type, crc_prefix.CharacterizationActivityInstance))
            g.add((activity_iri, rdf_prefix.type, activity_class_iri))


        previous_handover_group_iri = handover_group_iri

    return g, first_handover_group_iri


def generate_workflow_model_and_instance_for_handover_group_definition(handover_group_definition: list[tuple[URIRef, list[tuple[str, URIRef]]]],
                                                                       entity_IRI: URIRef) -> tuple[WorkflowModel, WorkflowInstance]:
    workflow_model = WorkflowModel()
    workflow_instance = WorkflowInstance()

    workflow_model.create_new_iri()
    workflow_instance.create_new_iri()
    workflow_instance.workflow_model_iri = workflow_model.iri

    previous_workflow_model_step = None
    for i, (project_name, activity_class_names_and_iris) in enumerate(handover_group_definition):
        workflow_model_step = CRC1625WorkflowModelStep()
        workflow_model_step.iri = crc_prefix[f"workflow_model_step_{i}"]

        workflow_model_step.set_allowed_project_names([project_name])
        workflow_model_step.set_allowed_activity_names([activity_name for activity_name, _ in activity_class_names_and_iris])
        workflow_model_step.set_other_activities_allowed_flag(False)

        workflow_model.workflow_model_steps[workflow_model_step.iri] = workflow_model_step.get_base_step()
        if previous_workflow_model_step is not None:
            workflow_model.workflow_model_steps[previous_workflow_model_step.iri].next_steps.append(workflow_model_step.iri)
        else:
            workflow_model.initial_step_iri = workflow_model_step.iri
        previous_workflow_model_step = workflow_model_step

        step_assignment = StepAssignment()
        step_assignment.create_new_iri()
        step_assignment.workflow_step_iri = workflow_model_step.iri
        step_assignment.assigned_entities.append(entity_IRI)
        step_assignment.property_to_follow = crc_prefix.nextStep

        workflow_instance.step_assignments[workflow_model_step.iri] = step_assignment

    return workflow_model, workflow_instance


def test_valid_workflows():
    for n_steps in [10, 20, 30, 40, 50]:
        asyncio.run(rdf_datastore_client.clear_triples())
        asyncio.run(rdf_datastore_client.clear_triples(WORKFLOWS_GRAPH_IRI))

        handover_group_definition = generate_handover_group_definition(n_steps)
        g, entity_IRI = generate_handover_group_triples(handover_group_definition)
        workflow_model, workflow_instance = generate_workflow_model_and_instance_for_handover_group_definition(handover_group_definition, entity_IRI)
        temporary_ttl_path = f"{uuid.uuid4().hex}.ttl"
        ttl_file_path = temporary_ttl_path
        g.serialize(destination=ttl_file_path, format='turtle')
        asyncio.run(rdf_datastore_client.upload_file(ttl_file_path, graph_iri=MAIN_GRAPH_IRI, delete_file_after_upload=True))

        asyncio.run(store_workflow_model(workflow_model, return_file=False))
        asyncio.run(store_workflow_instance(workflow_instance, return_file=False))

        validation_results, steps_with_no_target_node = asyncio.run(is_workflow_instance_valid(workflow_model, workflow_instance, return_individual_results=True))

        all_steps_conform = all(result.conforms for result in list(chain.from_iterable(chain.from_iterable(validation_results.values()))))
        if all_steps_conform and len(steps_with_no_target_node) == 0:
            logging.info(f"Valid workflow test of {n_steps} steps passed")
        else:
            raise ValueError(f"""
            The validation was not successful as expected. 
            Trace: {validation_results}
            Steps with no target node: {steps_with_no_target_node}
            """)

def test_missing_data_workflows():
    for n_steps in [10, 20, 30, 40, 50]:
        asyncio.run(rdf_datastore_client.clear_triples())
        asyncio.run(rdf_datastore_client.clear_triples(WORKFLOWS_GRAPH_IRI))

        handover_group_definition = generate_handover_group_definition(n_steps)

        cutoff_idx = random.choice([len(handover_group_definition) // 2, len(handover_group_definition) - 1])

        # Only instantiate handover group entities until a cutoff idx
        g, entity_IRI = generate_handover_group_triples(handover_group_definition[:cutoff_idx])

        # And instantiate all steps in the model
        workflow_model, workflow_instance = generate_workflow_model_and_instance_for_handover_group_definition(handover_group_definition, entity_IRI)

        # Get the workflow model step IRI that is going to trigger a missing data warning
        ordered_step_iris = []
        current_step = workflow_model.workflow_model_steps[workflow_model.initial_step_iri]
        while True:
            ordered_step_iris.append(current_step.iri)
            if not current_step.next_steps:
                break
            current_step = workflow_model.workflow_model_steps[current_step.next_steps[0]]

        workflow_model_step_iri_to_be_flagged = ordered_step_iris[cutoff_idx]

        temporary_ttl_path = f"{uuid.uuid4().hex}.ttl"
        ttl_file_path = temporary_ttl_path
        g.serialize(destination=ttl_file_path, format='turtle')
        asyncio.run(rdf_datastore_client.upload_file(ttl_file_path, graph_iri=MAIN_GRAPH_IRI, delete_file_after_upload=True))

        asyncio.run(store_workflow_model(workflow_model, return_file=False))
        asyncio.run(store_workflow_instance(workflow_instance, return_file=False))

        validation_results, steps_with_no_target_node = asyncio.run(is_workflow_instance_valid(workflow_model, workflow_instance, return_individual_results=True))

        if len(steps_with_no_target_node) != 1:
            return ValueError(f"""
            Incorrect number of workflow model steps to be marked as missing data
            Expected 1, got {len(steps_with_no_target_node)}: {steps_with_no_target_node}      
            Trace: {validation_results}
            """)
        elif steps_with_no_target_node[0].workflow_model_step.iri != workflow_model_step_iri_to_be_flagged:
            return ValueError(f"""
            Incorrect workflow model step to be marked as missing data
            Expected {workflow_model_step_iri_to_be_flagged}, got {steps_with_no_target_node[0].workflow_model_step.iri}      
            Trace: {validation_results}
            """)
        else:
            logging.info(f"Workflow test with missing data of {n_steps} steps passed")


def test_invalid_workflows():
    for n_steps in [10, 20, 30, 40, 50]:
        asyncio.run(rdf_datastore_client.clear_triples())
        asyncio.run(rdf_datastore_client.clear_triples(WORKFLOWS_GRAPH_IRI))

        handover_group_definition = generate_handover_group_definition(n_steps)
        handover_group_definition_invalidated = deepcopy(handover_group_definition)

        # Remove all activities from a random handover group, and use the valid definition
        # for the workflow model while instantiating the invalid definition handover groups
        handover_definition_idx_to_invalidate = random.choice([0, len(handover_group_definition_invalidated) - 1])
        handover_group_definition_invalidated[handover_definition_idx_to_invalidate][1].clear()

        g, entity_IRI = generate_handover_group_triples(handover_group_definition_invalidated)
        workflow_model, workflow_instance = generate_workflow_model_and_instance_for_handover_group_definition(handover_group_definition, entity_IRI)

        # Get the workflow model step IRI that is going to trigger a validation error
        ordered_step_iris = []
        current_step = workflow_model.workflow_model_steps[workflow_model.initial_step_iri]
        while True:
            ordered_step_iris.append(current_step.iri)
            if not current_step.next_steps:
                break
            current_step = workflow_model.workflow_model_steps[current_step.next_steps[0]]

        invalid_workflow_model_step_iri = ordered_step_iris[handover_definition_idx_to_invalidate]

        temporary_ttl_path = f"{uuid.uuid4().hex}.ttl"
        ttl_file_path = temporary_ttl_path
        g.serialize(destination=ttl_file_path, format='turtle')
        asyncio.run(rdf_datastore_client.upload_file(ttl_file_path, graph_iri=MAIN_GRAPH_IRI, delete_file_after_upload=True))

        asyncio.run(store_workflow_model(workflow_model, return_file=False))
        asyncio.run(store_workflow_instance(workflow_instance, return_file=False))

        validation_results, steps_with_no_target_node = asyncio.run(is_workflow_instance_valid(workflow_model, workflow_instance, return_individual_results=True))

        if len(steps_with_no_target_node) != 0:
            return ValueError(f"""
            Got an unexpected number of steps with missing data
            Expected 0, got {len(steps_with_no_target_node)}: {steps_with_no_target_node}      
            Trace: {validation_results}
            """)

        for entity_iri, validation_paths in validation_results.items():
            for validation_path in validation_paths:
                for validation_result in validation_path:
                    if validation_result.validation_job.paired_step.workflow_model_step.iri != invalid_workflow_model_step_iri:
                        if not validation_result.conforms:
                            return ValueError(f"""
                            A workflow model step expected to be valid did not conform
                            Workflow model step invalidated in the test: {handover_definition_idx_to_invalidate}
                            Step validation result: {validation_result}
                            Trace: {validation_results}
                            """)
                    else:
                        if validation_result.conforms:
                            return ValueError(f"""
                            A workflow model step expected to be invalid did conform
                            Workflow model step invalidated in the test: {handover_definition_idx_to_invalidate}
                            Step validation result: {validation_result}
                            Trace: {validation_results}
                            """)

        logging.info(f"Workflow test with invalid data of {n_steps} steps passed")

test_valid_workflows()
test_missing_data_workflows()
test_invalid_workflows()