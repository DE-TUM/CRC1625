"""
Workflows validation API. This is a fully functional implementation of all functions required functions to
perform workflows validation, which offers methods for:
- Reading, writing, modifying and deleting workflow models
- Reading, writing, modifying and deleting workflow model instances
- Performing validation given a workflow model and a workflow model instance,
  yielding either a simple or a full trace of validation results

Workflow models and their instances can be created via their respective objects, or
by writing them to RDF directly. They are intended to be stored in an RDF graph.

An example usage of this API can be found on `run_handover_workflows_validation_test.py`, which
performs a full test of its correctness using CRC 1625 workflows, or in the web UI.

All workflow classes are implemented as dataclasses, making it easy for them to be extended to, e.g.,
set up SHACL shapes and key-value replacements in a programmatic way. An example of this can be found
on `CRC_1625_workflows_validator.py`
"""

import asyncio
import os
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from enum import Enum

from jinja2 import Template
from pyshacl import validate
from rdflib import Graph, URIRef, Namespace, Dataset
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore_client import RDF_DATASTORE_API_ENDPOINT
from workflows_validation.common import prefixes
from workflows_validation.workflow_instance import WorkflowInstance
from workflows_validation.workflow_model import WorkflowModelStep, WorkflowModel

module_dir = os.path.dirname(__file__)

get_next_target_node_query = prefixes + open(os.path.join(module_dir, 'queries/get_next_target_node.sparql'), 'r').read()

@dataclass
class PairedStep:
    """
    Convenience class for a Workflow model step and
    the entity->target nodes for which it is to be validated
    """
    workflow_model_step: WorkflowModelStep = field(default_factory=WorkflowModelStep)
    entity_to_target_node_assignments: dict[URIRef, list[URIRef]] = field(default_factory=dict)


@dataclass
class EvaluationTrace:
    previous_paired_steps: list[PairedStep] = field(default_factory=list)


@dataclass
class StepToValidate:
    """
    Convenience class for a step and its
    generated SHACL shape
    """
    paired_step: PairedStep = field(default_factory=PairedStep)
    entity: URIRef = ""
    target_node: URIRef = ""
    shacl_shape: str = ""  # Syntactically valid SHACL shape, as a serialized string in any RDF format


@dataclass
class StepWithMissingData:
    """
    Convenience class for a step and
    the entity for which it could not
    fetch a target node for
    """
    workflow_model_step: WorkflowModelStep = field(default_factory=WorkflowModelStep)
    entity: URIRef = ""


async def get_next_target_nodes(current_target_node: URIRef,
                                successor_property: URIRef) -> list[URIRef]:
    next_target_nodes: list[URIRef] = list()

    substitutions = {
        "current_target_node": str(current_target_node),
        "successor_property": str(successor_property)
    }
    result = await rdf_datastore_client.launch_query(Template(get_next_target_node_query).render(substitutions))
    for binding in result["results"]["bindings"]:
        next_target_nodes.append(URIRef(binding["next_target_node"]["value"]))

    return next_target_nodes


def fill_SHACL_template(workflow_model_step: WorkflowModelStep,
                        target_node: URIRef) -> str:
    template = Template(workflow_model_step.SHACL_shape)
    substitutions = {"target_node": str(target_node)}
    substitutions.update(workflow_model_step.step_templates)
    return template.render(substitutions)


async def generate_SHACL_shapes_for_workflow(workflow_model: WorkflowModel,
                                             workflow_instance: WorkflowInstance) -> tuple[list[StepToValidate], list[StepWithMissingData]]:
    """
    Returns a list of steps to validate for the workflow model, following the target node assignments of its workflow instance,
    and a list of references to workflow model steps for which a target node did not have a corresponding successor for it
    (i.e., more steps in the workflow model than entities in the data workflow).

    It will iteratively follow the steps chain and generating as many shapes for a step as there are entities assigned to it,
    warning for loops.
    Each consecutive workflow model step assigned to the same entity will validate the successor of the previously validated
    entity, starting with the original entity itself (i.e., the entity assigned to a step serves as a marker for the starting
    point of the data workflow, and it appearing multiple times signals to continue validating on its successors). This removes
    the need to manually assign every individual target node to workflow model steps, and allows defining workflow model for
    possibly missing data.
    If a given workflow model step cannot be assigned a target node due to a successor being missing, the remaining SHACL shapes
    will not be generated and the validation on that branch will stop."""

    # Nodes to visit, iteratively expanded by the successors of the current step being checked
    visitor_stack: list[tuple[PairedStep, PairedStep | None]] = list()
    # Individual validation tasks to be run
    validation_queue: list[StepToValidate] = list()
    # References to the first step for which there was no target node from a given entity assigned to it
    steps_with_missing_data: list[StepWithMissingData] = list()

    # Start validating from the initial step, for every entity that is assigned to it
    initial_step = workflow_model.workflow_model_steps[workflow_model.initial_step_iri]
    initial_paired_step = PairedStep()
    initial_paired_step.workflow_model_step = initial_step

    entity_to_target_node_assignments: dict[URIRef, list[URIRef]] = {}
    for assigned_entity in workflow_instance.step_assignments[initial_step.iri].assigned_entities:
        # The assigned entity also acts as the target node in the first step
        entity_to_target_node_assignments[assigned_entity] = [assigned_entity]
    initial_paired_step.entity_to_target_node_assignments = entity_to_target_node_assignments

    visitor_stack.append((initial_paired_step, None))

    while len(visitor_stack) > 0:
        current_paired_step, previous_paired_step = visitor_stack.pop()
        if current_paired_step.workflow_model_step.iri in workflow_instance.step_assignments:  # Otherwise, don't do anything for this step
            for entity in workflow_instance.step_assignments[current_paired_step.workflow_model_step.iri].assigned_entities:
                if previous_paired_step is None or entity not in previous_paired_step.entity_to_target_node_assignments:
                    # It's the initial step or a new entity, so we must target the entity itself
                    step_to_validate = StepToValidate()
                    step_to_validate.entity = entity
                    step_to_validate.target_node = entity
                    step_to_validate.paired_step = current_paired_step
                    step_to_validate.shacl_shape = fill_SHACL_template(current_paired_step.workflow_model_step, entity)
                    validation_queue.append(step_to_validate)

                    # Overwrite the target nodes for the next iteration, if there are any steps after this one
                    current_paired_step.entity_to_target_node_assignments[entity] = [entity]
                else:
                    if len(previous_paired_step.entity_to_target_node_assignments[entity]) == 0:
                        # If the list is empty, it indicates that there were no target nodes to follow - we keep communicating this for the next steps
                        current_paired_step.entity_to_target_node_assignments[entity] = []
                    else:
                        for target_node in previous_paired_step.entity_to_target_node_assignments[entity]:
                            next_target_nodes = await get_next_target_nodes(target_node,
                                                                            workflow_instance.step_assignments[
                                                                                current_paired_step.workflow_model_step.iri].property_to_follow)
                            if len(next_target_nodes) == 0:
                                # There should be new target nodes, but we couldn't find any. We log it anyways as a step with missing data
                                step_with_missing_data = StepWithMissingData()
                                step_with_missing_data.entity = entity
                                step_with_missing_data.workflow_model_step = current_paired_step.workflow_model_step
                                steps_with_missing_data.append(step_with_missing_data)
                            else:
                                for next_target_node in next_target_nodes:
                                    step_to_validate = StepToValidate()
                                    step_to_validate.entity = entity
                                    step_to_validate.target_node = next_target_node
                                    step_to_validate.paired_step = current_paired_step
                                    step_to_validate.shacl_shape = fill_SHACL_template(current_paired_step.workflow_model_step, next_target_node)
                                    validation_queue.append(step_to_validate)

                            # Overwrite the target nodes for the next iteration, if there are any steps after this one
                            current_paired_step.entity_to_target_node_assignments[entity] = next_target_nodes

        # Expand the visitor stack
        for successor_model_step_iri in current_paired_step.workflow_model_step.next_steps:
            next_paired_step = PairedStep()
            next_paired_step.workflow_model_step = workflow_model.workflow_model_steps[successor_model_step_iri]
            visitor_stack.append((next_paired_step, current_paired_step))

    return validation_queue, steps_with_missing_data


@dataclass
class ValidationResult:
    step_to_validate: StepToValidate = field(default_factory=StepToValidate)
    conforms: bool = False
    pyshacl_output: str = ""


def validate_workflow_model_step(step_to_validate: StepToValidate, results: list[ValidationResult]):
    shacl_graph = Graph()
    shacl_graph.parse(data=step_to_validate.shacl_shape, format="turtle")

    store = SPARQLStore(
        query_endpoint=f"{RDF_DATASTORE_API_ENDPOINT}/launch_query_validation",
        method="POST"
    )

    intercepted_data_graph = Dataset(store, default_union=True)

    conforms, results_graph, pyshacl_output = validate(data_graph=intercepted_data_graph,
                                                       shacl_graph=shacl_graph,
                                                       inference=None,
                                                       abort_on_first=False,
                                                       allow_infos=False,
                                                       allow_warnings=False,
                                                       meta_shacl=False,
                                                       advanced=False,
                                                       js=False,
                                                       sparql_mode=True,
                                                       debug=False)
    validation_result = ValidationResult()
    validation_result.step_to_validate = step_to_validate
    validation_result.conforms = conforms
    validation_result.pyshacl_output = pyshacl_output
    results.append(validation_result)


def validation_task_wrapper(step_to_validate: StepToValidate) -> ValidationResult:
    local_results: list[ValidationResult] = []

    validate_workflow_model_step(step_to_validate,
                                 local_results)

    return local_results[0]


class ValidationStatus(Enum):
    Valid = 1
    Warning = 2
    Error = 3

    @property
    def description(self):
        descriptions = {
            ValidationStatus.Valid: "All steps were validated successfully.",
            ValidationStatus.Warning: "All steps were validated successfully, but some steps had no data to be validated against.",
            ValidationStatus.Error: "One or more steps failed validation."
        }
        return descriptions[self]


async def is_workflow_instance_valid(workflow_model, workflow_instance, return_individual_results=False) -> tuple[
    ValidationStatus | list[ValidationResult], list[StepWithMissingData]]:
    """
    Returns a ValidationStatus of for the provided workflow model against its instance's assignments

    `return_individual_results` can be set to `True` to instead return individual results for every workflow model step - entity - target node
    combination that has been validated

    The validation will be split into individual jobs run in a process pool
    """
    steps_to_validate, first_steps_with_no_target_node = await generate_SHACL_shapes_for_workflow(workflow_model, workflow_instance)

    with ProcessPoolExecutor() as executor:
        tasks = []

        for step_to_validate in steps_to_validate:
            task = asyncio.get_running_loop().run_in_executor(
                executor,
                validation_task_wrapper,
                step_to_validate
            )
            tasks.append(task)

        results: list[ValidationResult] = await asyncio.gather(*tasks)

    if return_individual_results:
        return results, first_steps_with_no_target_node

    all_steps_conform = all(result.conforms for result in results)
    if all_steps_conform and len(first_steps_with_no_target_node) == 0:
        return ValidationStatus.Valid, first_steps_with_no_target_node
    elif all_steps_conform:
        return ValidationStatus.Warning, first_steps_with_no_target_node
    else:
        return ValidationStatus.Error, first_steps_with_no_target_node
