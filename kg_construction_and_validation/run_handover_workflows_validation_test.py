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

from rdflib import URIRef, Graph

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore import WORKFLOWS_GRAPH_IRI, MAIN_GRAPH_IRI
from workflows_validation.CRC_1625_workflows_validator.CRC_1625_workflows_validator import CRC1625WorkflowModelStep, \
    activity_name_to_class_iri, project_name_to_iri
from workflows_validation.common import dw_prefix, rdf_prefix
from workflows_validation.workflow_instance import WorkflowInstance, StepAssignment
from workflows_validation.workflow_model import WorkflowModel, WorkflowModelStep
from workflows_validation.workflows_validator import is_workflow_instance_valid, ValidationStatus, ValidationResult

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def print_validation_results(validation_results: ValidationStatus | dict[URIRef, list[dict[URIRef, list[ValidationResult]]]]):
    for entity_iri, validation_paths in validation_results.items():
        for i, validation_path in enumerate(validation_paths):
            logging.error(f"Path {i}")
            for _, reses in validation_path.items():
                for r in reses:
                    logging.error(f"{r.validation_job.paired_step.workflow_model_step.name}, conforms: {r.conforms}, validation report: {r.validation_report}")


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

    workflow_instance_iri = dw_prefix.handover_workflow_instance
    g.add((workflow_instance_iri, rdf_prefix.type, dw_prefix.workflowModelInstance))

    first_handover_group_iri = None
    previous_handover_group_iri = None
    for i, (project_name, activity_class_names_and_iris) in enumerate(handover_group_definition):
        handover_group_iri = dw_prefix[f"handover_group_{i}"]
        if i == 0:
            g.add((workflow_instance_iri, dw_prefix.substep, handover_group_iri))
            first_handover_group_iri = handover_group_iri
        else:
            g.add((previous_handover_group_iri, dw_prefix.nextStep, handover_group_iri))

        g.add((handover_group_iri, dw_prefix.assignedTo, project_name_to_iri[project_name]))
        g.add((handover_group_iri, rdf_prefix.type, dw_prefix.HandoverGroup))

        for _, activity_class_iri in activity_class_names_and_iris:
            handover_iri = dw_prefix[uuid.uuid4().hex]
            activity_iri = dw_prefix[uuid.uuid4().hex]

            g.add((handover_group_iri, dw_prefix.substep, handover_iri))
            g.add((handover_iri, dw_prefix.substep, activity_iri))
            g.add((handover_iri, rdf_prefix.type, dw_prefix.Handover))

            g.add((activity_iri, rdf_prefix.type, dw_prefix.CharacterizationActivityInstance))
            g.add((activity_iri, rdf_prefix.type, activity_class_iri))


        previous_handover_group_iri = handover_group_iri

    return g, first_handover_group_iri


def generate_workflow_model_and_instance_for_handover_group_definition(handover_group_definition: list[tuple[URIRef, list[tuple[str, URIRef]]]],
                                                                       entity_IRI: URIRef,
                                                                       generate_redundant_branch: bool = False,
                                                                       break_in_half: bool = False) -> tuple[WorkflowModel, WorkflowInstance]:
    """
    If `generate_redundant_branch == True`, a random intermediate workflow model step will be chosen, and:
        - A new branch will be created by cloning its successor node, and adding the clone as another of its next steps
          The validation system must validate it as a different branch
        - Another branch will be created with an empty node with no assignments as its next step. The empty node will then have as its successor a clone of the initial step.
          The validation system must validate the successor of the empty node as a new branch, validating the entity from the beginning again
    """
    workflow_model = WorkflowModel()
    workflow_instance = WorkflowInstance()

    workflow_model.create_new_iri()
    workflow_instance.create_new_iri()
    workflow_instance.workflow_model_iri = workflow_model.iri

    previous_workflow_model_step = None
    for i, (project_name, activity_class_names_and_iris) in enumerate(handover_group_definition):
        workflow_model_step = CRC1625WorkflowModelStep()
        workflow_model_step.iri = dw_prefix[f"workflow_model_step_{i}"]
        workflow_model_step.name = f"workflow_model_step_{i}"

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
        step_assignment.property_to_follow = dw_prefix.nextStep

        workflow_instance.step_assignments[workflow_model_step.iri] = step_assignment

    # Choose a random intermediate step with successors
    random_step_iri = random.choice(list(workflow_model.workflow_model_steps.keys()))
    while random_step_iri != workflow_model.initial_step_iri and len(workflow_model.workflow_model_steps[random_step_iri].next_steps) == 0:
        random_step_iri = random.choice(list(workflow_model.workflow_model_steps.keys()))
    random_step = workflow_model.workflow_model_steps[random_step_iri]
    if generate_redundant_branch:
        # Get the successor of the random step, and copy it completely as another successor
        # We will then have two branches validating the same target node
        random_step_succesor = workflow_model.workflow_model_steps[random_step.next_steps[0]]
        random_step_succesor_copy = deepcopy(random_step_succesor)
        random_step_succesor_copy.create_new_iri()
        random_step_succesor_copy.next_steps = []
        random_step_succesor_copy.name = f"Cloned {random_step_succesor_copy.name}"

        step_assignment_copy = deepcopy(workflow_instance.step_assignments[random_step_succesor.iri])
        step_assignment_copy.create_new_iri()
        step_assignment_copy.workflow_step_iri = random_step_succesor_copy.iri

        workflow_instance.step_assignments[random_step_succesor_copy.iri] = step_assignment_copy
        workflow_model.workflow_model_steps[random_step_succesor_copy.iri] = random_step_succesor_copy
        random_step.next_steps.append(random_step_succesor_copy.iri)

    if generate_redundant_branch or break_in_half:
        # Add an empty step with no assignments as a successor to the random node,
        # and then a copy of the first step as the successor of the empty step
        # We will then have another branch that starts validating the entity again
        empty_workflow_model_step = WorkflowModelStep()
        empty_workflow_model_step.create_new_iri()

        first_step_copy = deepcopy(workflow_model.workflow_model_steps[workflow_model.initial_step_iri])
        first_step_copy.create_new_iri()
        first_step_copy.next_steps = []
        first_step_copy.name = "Cloned first step"

        first_step_assignment_copy = deepcopy(workflow_instance.step_assignments[workflow_model.initial_step_iri])
        first_step_assignment_copy.create_new_iri()
        first_step_assignment_copy.workflow_step_iri = first_step_copy.iri

        workflow_instance.step_assignments[first_step_copy.iri] = first_step_assignment_copy
        workflow_model.workflow_model_steps[empty_workflow_model_step.iri] = empty_workflow_model_step
        workflow_model.workflow_model_steps[first_step_copy.iri] = first_step_copy
        random_step.next_steps.append(empty_workflow_model_step.iri)
        empty_workflow_model_step.next_steps.append(first_step_copy.iri)

        if break_in_half:
            # Delete the original branch from the randomly chosen node, so that we only have a few
            # steps, a break with an empty node, and another node that starts validating the entity again
            # The validation system should create two branches (1st half of the original workflow, 2nd half with the single node)
            step_iris_to_delete = []
            visitor_stack = [random_step_iri]
            while len(visitor_stack) > 0:
                current_step = workflow_model.workflow_model_steps[visitor_stack.pop()]
                for next_step_iri in current_step.next_steps:
                    if next_step_iri != empty_workflow_model_step.iri:
                        step_iris_to_delete.append(next_step_iri)
                        visitor_stack.append(next_step_iri)

            for step_iri_to_delete in step_iris_to_delete:
                if step_iri_to_delete in workflow_instance.step_assignments:
                    del workflow_instance.step_assignments[step_iri_to_delete]
                del workflow_model.workflow_model_steps[step_iri_to_delete]

            random_step.next_steps = [empty_workflow_model_step.iri]

    return workflow_model, workflow_instance


def test_valid_workflows(generate_redundant_branch: bool = False,
                         break_in_half: bool = False):
    for n_steps in [3, 5, 10]:
        asyncio.run(rdf_datastore_client.clear_triples())
        asyncio.run(rdf_datastore_client.clear_triples(WORKFLOWS_GRAPH_IRI))

        handover_group_definition = generate_handover_group_definition(n_steps)
        g, entity_IRI = generate_handover_group_triples(handover_group_definition)
        workflow_model, workflow_instance = generate_workflow_model_and_instance_for_handover_group_definition(handover_group_definition,
                                                                                                               entity_IRI,
                                                                                                               generate_redundant_branch,
                                                                                                               break_in_half)
        temporary_ttl_path = f"{uuid.uuid4().hex}.ttl"
        ttl_file_path = temporary_ttl_path
        g.serialize(destination=ttl_file_path, format='turtle')
        asyncio.run(rdf_datastore_client.upload_file(ttl_file_path, graph_iri=MAIN_GRAPH_IRI, delete_file_after_upload=True))

        asyncio.run(rdf_datastore_client.launch_update(workflow_model.get_insert_query()))
        asyncio.run(rdf_datastore_client.launch_update(workflow_instance.get_insert_query()))

        validation_results = asyncio.run(is_workflow_instance_valid(workflow_model, workflow_instance, return_individual_results=True))

        all_validation_results: list[ValidationResult] = []
        for entity_iri, validation_paths in validation_results.items():
            for validation_path in validation_paths:
                for _, reses in validation_path.items():
                    for validation_result in reses:
                        all_validation_results.append(validation_result)

        if all(result.conforms for result in all_validation_results) and \
                all(not result.is_missing_data for result in all_validation_results) and \
                ((not generate_redundant_branch) or (len(list(validation_results.values())[0]) == 3)) and \
                ((not break_in_half) or (len(list(validation_results.values())[0]) == 2)):
            if generate_redundant_branch:
                logging.info(f"Valid workflow test of {n_steps} steps (w/ redundant branch) passed")
            elif break_in_half:
                logging.info(f"Valid workflow test of {n_steps} steps (broken in half) passed")
            else:
                logging.info(f"Valid workflow test of {n_steps} steps passed")
        else:
            print_validation_results(validation_results)
            raise ValueError(f"""The validation was not successful as expected. Please check the logging trace for more information""")

def test_missing_data_workflows():
    for n_steps in [3, 5, 10]:
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

        asyncio.run(rdf_datastore_client.launch_update(workflow_model.get_insert_query()))
        asyncio.run(rdf_datastore_client.launch_update(workflow_instance.get_insert_query()))

        validation_results = asyncio.run(is_workflow_instance_valid(workflow_model, workflow_instance, return_individual_results=True))

        all_validation_results: list[ValidationResult] = []
        for entity_iri, validation_paths in validation_results.items():
            for validation_path in validation_paths:
                for _, reses in validation_path.items():
                    for validation_result in reses:
                        all_validation_results.append(validation_result)

        steps_with_no_target_node = [result for result in all_validation_results if result.is_missing_data]
        if len(steps_with_no_target_node) != len(handover_group_definition[cutoff_idx:]):
            print_validation_results(validation_results)
            raise ValueError(f"""
            Incorrect number of workflow model steps to be marked as missing data
            Expected {len(handover_group_definition[cutoff_idx:])}, got {len(steps_with_no_target_node)}     
            Please check the logging trace above for more information
            """)
        elif steps_with_no_target_node[0].validation_job.paired_step.workflow_model_step.iri != workflow_model_step_iri_to_be_flagged:
            print_validation_results(validation_results)
            raise ValueError(f"""
            Incorrect workflow model step to be marked as missing data
            Expected {workflow_model_step_iri_to_be_flagged}, got {steps_with_no_target_node[0].validation_job.paired_step.workflow_model_step.iri}      
            Please check the logging trace above for more information
            """)
        else:
            logging.info(f"Workflow test with missing data of {n_steps} steps passed")


def test_invalid_workflows():
    for n_steps in [3, 5, 10]:
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

        asyncio.run(rdf_datastore_client.launch_update(workflow_model.get_insert_query()))
        asyncio.run(rdf_datastore_client.launch_update(workflow_instance.get_insert_query()))

        validation_results = asyncio.run(is_workflow_instance_valid(workflow_model, workflow_instance, return_individual_results=True))

        all_validation_results: list[ValidationResult] = []
        for entity_iri, validation_paths in validation_results.items():
            for validation_path in validation_paths:
                for _, reses in validation_path.items():
                    for validation_result in reses:
                        all_validation_results.append(validation_result)

        if any(result.is_missing_data for result in all_validation_results):
            print_validation_results(validation_results)
            raise ValueError(f"""
            Got an unexpected number of steps with missing data
            Expected 0, got {len([result for result in all_validation_results if result.is_missing_data])}      
            Please check the logging trace above for more information
            """)

        for entity_iri, validation_paths in validation_results.items():
            for validation_path in validation_paths:
                for _, reses in validation_path.items():
                    for validation_result in reses:
                        if validation_result.validation_job.paired_step.workflow_model_step.iri != invalid_workflow_model_step_iri:
                            if not validation_result.conforms:
                                print_validation_results(validation_results)
                                raise ValueError(f"""
                                A workflow model step expected to be valid did not conform
                                Workflow model step invalidated in the test: {handover_definition_idx_to_invalidate}
                                Please check the logging trace above for more information
                                """)
                        else:
                            if validation_result.conforms:
                                print_validation_results(validation_results)
                                raise ValueError(f"""
                                A workflow model step expected to be invalid did conform
                                Workflow model step invalidated in the test: {handover_definition_idx_to_invalidate}
                                Please check the logging trace above for more information
                                """)

        logging.info(f"Workflow test with invalid data of {n_steps} steps passed")


test_valid_workflows()
test_missing_data_workflows()
test_invalid_workflows()
test_valid_workflows(generate_redundant_branch=True)
test_valid_workflows(break_in_half=True)