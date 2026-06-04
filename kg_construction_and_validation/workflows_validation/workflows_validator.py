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

This module will use `logging` to log `debug` messages indicating validation paths, if set.
"""
import logging
import os
import urllib.parse
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, Future
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum

from jinja2 import Template
from pyshacl import validate
from rdflib import Graph, URIRef, Dataset
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore_client import RDF_DATASTORE_API_ENDPOINT
from workflows_validation.common import prefixes, dw_prefix
from workflows_validation.workflow_instance import WorkflowInstance
from workflows_validation.workflow_model import WorkflowModelStep, WorkflowModel

module_dir = os.path.dirname(__file__)

get_next_target_node_query = prefixes + open(os.path.join(module_dir, 'queries/get_next_target_node.sparql'), 'r').read()
parse_validation_report_query = prefixes + open(os.path.join(module_dir, 'queries/parse_validation_report.sparql'), 'r').read()
workflow_shape = open(os.path.join(module_dir, 'shapes/workflow_shape.shacl'), 'r').read()

@dataclass
class PairedStep:
    """
    Convenience class for a Workflow model step and the entity->target nodes for which it is to be validated
    """
    workflow_model_step: WorkflowModelStep = field(default_factory=WorkflowModelStep)
    entity_to_target_node_assignments: dict[URIRef, list[URIRef]] = field(default_factory=dict)


@dataclass
class ValidationJob:
    """
    Convenience class containing a validation task for a step, its assigned entity and specific target node,
    and the generated SHACL shape
    """
    paired_step: PairedStep = field(default_factory=PairedStep)
    entity: URIRef = ""
    target_node: URIRef | None = ""
    shacl_shape: str = ""  # Syntactically valid SHACL shape, as a serialized string in any RDF format

    def __hash__(self):
        return hash((self.paired_step.workflow_model_step.iri,
                     self.entity,
                     self.target_node,
                     self.shacl_shape))

@dataclass
class ValidationJobWithMissingData:
    """
    Convenience class for a validation job and the entity for which we could not fetch a target node for
    """
    workflow_model_step: WorkflowModelStep = field(default_factory=WorkflowModelStep)
    entity: URIRef = ""


async def get_next_target_nodes(current_target_node: URIRef,
                                successor_property: URIRef) -> list[URIRef]:
    """
    Query for the successor node(s) of a given target node, using the given property
    """
    next_target_nodes: list[URIRef] = list()

    substitutions = {
        "current_target_node": str(current_target_node),
        "successor_property": str(successor_property)
    }
    result = await rdf_datastore_client.launch_query(Template(get_next_target_node_query).render(substitutions))
    for binding in result["results"]["bindings"]:
        next_target_nodes.append(URIRef(binding["next_target_node"]["value"]))

    return next_target_nodes


class ValidationStatus(Enum):
    """
    Convenience class to indicate the validation status of a workflow as a whole
    """
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


def fill_SHACL_template(workflow_model_step: WorkflowModelStep,
                        target_node: URIRef) -> str:
    """
    Fills the SHACL template of the given workflow model step via Jinja,
    using its substitutions and details about its matched target node
    """
    template = Template(workflow_model_step.SHACL_shape)
    # We use the target node to specify the IRI of its corresponding node shape
    substitutions = {"node_shape_iri": dw_prefix[urllib.parse.quote(f"step_{workflow_model_step.name}_node_shape_for_{target_node}")]}
    substitutions.update(workflow_model_step.step_templates)

    return template.render(substitutions)


async def generate_SHACL_shapes_for_workflow(workflow_model: WorkflowModel,
                                             workflow_instance: WorkflowInstance) -> dict[URIRef, list[ValidationJob]]:
    """
    Returns a list of steps to validate for the workflow model, indexed by the workflow model steps, following the target node
    assignments of its workflow instance, and a list of references to workflow model steps for which a target node did not have
    a corresponding successor for it (i.e., more steps in the workflow model than entities in the data workflow).

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
    # Individual validation jobs to be run, indexed by workflow model step IRIs
    validation_queue: dict[URIRef, list[ValidationJob]] = dict()
    # References to the steps for which there was no target node from a given entity assigned to it
    #jobs_with_missing_data: list[ValidationJobWithMissingData] = list()

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
        if current_paired_step.workflow_model_step.iri in workflow_instance.step_assignments:  # Otherwise, there are no assignments for this workflow model step
            for entity in workflow_instance.step_assignments[current_paired_step.workflow_model_step.iri].assigned_entities:
                if previous_paired_step is None or entity not in previous_paired_step.entity_to_target_node_assignments:
                    # It's the initial step or a new entity, so we must target the entity itself
                    step_to_validate = ValidationJob()
                    step_to_validate.entity = entity
                    step_to_validate.target_node = entity
                    step_to_validate.paired_step = current_paired_step
                    step_to_validate.shacl_shape = fill_SHACL_template(current_paired_step.workflow_model_step, entity)
                    if step_to_validate.paired_step.workflow_model_step.iri not in validation_queue:
                        validation_queue[step_to_validate.paired_step.workflow_model_step.iri] = list()
                    validation_queue[step_to_validate.paired_step.workflow_model_step.iri].append(step_to_validate)

                    # Overwrite the target nodes for the next iteration, if there are any steps after this one
                    current_paired_step.entity_to_target_node_assignments[entity] = [entity]

                else:

                    if len(previous_paired_step.entity_to_target_node_assignments[entity]) == 0 or previous_paired_step.entity_to_target_node_assignments[entity] == [None]:
                        # If the list is empty, it indicates that there were no target nodes to follow. Since we want a full trace of both matched steps
                        # and steps with missing data, we keep passing it
                        current_paired_step.entity_to_target_node_assignments[entity] = [None]

                        # To indicate this, we insert a dummy job with None as target node and an empty string for the SHACL shape
                        step_to_validate = ValidationJob()
                        step_to_validate.entity = entity
                        step_to_validate.target_node = None
                        step_to_validate.paired_step = current_paired_step
                        step_to_validate.shacl_shape = ""
                        if step_to_validate.paired_step.workflow_model_step.iri not in validation_queue:
                            validation_queue[step_to_validate.paired_step.workflow_model_step.iri] = list()
                        validation_queue[step_to_validate.paired_step.workflow_model_step.iri].append(step_to_validate)

                    else:

                        next_target_nodes = []
                        for target_node in previous_paired_step.entity_to_target_node_assignments[entity]:
                            if target_node is not None:
                                current_next_nodes = await get_next_target_nodes(target_node,
                                                                                 workflow_instance.step_assignments[
                                                                                     current_paired_step.workflow_model_step.iri].property_to_follow)
                                next_target_nodes.extend(current_next_nodes)

                        if len(next_target_nodes) > 0:
                            for next_target_node in next_target_nodes:
                                step_to_validate = ValidationJob()
                                step_to_validate.entity = entity
                                step_to_validate.target_node = next_target_node
                                step_to_validate.paired_step = current_paired_step
                                step_to_validate.shacl_shape = fill_SHACL_template(current_paired_step.workflow_model_step, next_target_node)
                                if step_to_validate.paired_step.workflow_model_step.iri not in validation_queue:
                                    validation_queue[step_to_validate.paired_step.workflow_model_step.iri] = list()
                                validation_queue[step_to_validate.paired_step.workflow_model_step.iri].append(step_to_validate)
                        else:
                            # It's a workflow model step with no matching data. To indicate this, we insert a dummy job
                            # with None as the target node, an empty string for the SHACL shape, and indicate that
                            # the entity has [None] as its next target nodes
                            step_to_validate = ValidationJob()
                            step_to_validate.entity = entity
                            step_to_validate.target_node = None
                            step_to_validate.paired_step = current_paired_step
                            step_to_validate.shacl_shape = ""
                            if step_to_validate.paired_step.workflow_model_step.iri not in validation_queue:
                                validation_queue[step_to_validate.paired_step.workflow_model_step.iri] = list()
                            validation_queue[step_to_validate.paired_step.workflow_model_step.iri].append(step_to_validate)

                            next_target_nodes = [None]

                        # Overwrite the target nodes for the next iteration, if there are any steps after this one
                        current_paired_step.entity_to_target_node_assignments[entity] = next_target_nodes

        # Expand the visitor stack
        for successor_model_step_iri in current_paired_step.workflow_model_step.next_steps:
            next_paired_step = PairedStep()
            next_paired_step.workflow_model_step = workflow_model.workflow_model_steps[successor_model_step_iri]
            visitor_stack.append((next_paired_step, current_paired_step))

    logging.debug("Generated unsorted validation jobs:")
    for i, jobs in enumerate(list(validation_queue.values())):
        for job in jobs:
            logging.debug(f"Job {i}: {job.paired_step.workflow_model_step.name} for {job.target_node} (entity: {job.entity})")

    return validation_queue


@dataclass
class ValidationResult:
    """
    Complete trace of the details and execution of a validation job
    including the validation report and pySHACL's output

    A validation job can be either:
    - Conforming (`conforming` == True and `is_missing_data` == False),
    - Non-conforming due to an inconsistency reported by SHACL (`conforming` == False and `is_missing_data` == False)
    - Non-conforming due to missing data for its actual validation (`conforming` == False and `is_missing_data` == True)
    """
    validation_job: ValidationJob = None
    conforms: bool = False
    is_missing_data: bool = False
    validation_report: Graph | None = None
    pyshacl_output: str = ""


def parse_validation_report(workflow_model: WorkflowModel,
                            path: list[ValidationJob],
                            entity_to_validate: URIRef,
                            validation_report: Graph,
                            workflow_shape_graph: Graph) -> OrderedDict[URIRef, list[ValidationResult]]:
    """
    Breaks down the validation_report from running a workflow shape into individual validation results
    for every violation. The results are returned as a dict of workflow model step IRI -> Validation results.

    If a workflow model step was validated successfully, there will be a single conforming ValidationResult. If not,
    there will be one ValidationResult for every single violation found

    When traversed, the dictionary will be ordered exactly like the provided validation path
    """
    result = (validation_report + workflow_shape_graph).query(parse_validation_report_query)


    validation_results: dict[URIRef, list[ValidationResult]] = dict()
    for violation in result:
        validation_job = ValidationJob()
        paired_step = PairedStep()
        paired_step.workflow_model_step = workflow_model.workflow_model_steps[URIRef(violation.workflow_model_step)]
        validation_job.paired_step = paired_step
        validation_job.shacl_shape = workflow_shape_graph.serialize(format="turtle")
        validation_job.entity = entity_to_validate
        validation_job.target_node = URIRef(violation.target_node)
        validation_result = ValidationResult()
        validation_result.validation_job = validation_job
        validation_result.validation_report = validation_report
        validation_result.conforms = False
        validation_result.pyshacl_output = str(violation.message)

        if paired_step.workflow_model_step.iri not in validation_results:
            validation_results[paired_step.workflow_model_step.iri] = []

        validation_results[paired_step.workflow_model_step.iri].append(validation_result)

    # The rest of the workflow model steps *in the path* are valid
    step_iris_with_errors = set(validation_results.keys())
    for validation_job in path:
        step = validation_job.paired_step.workflow_model_step

        if step.iri not in step_iris_with_errors:
            validation_result = ValidationResult()
            validation_result.validation_job = validation_job
            validation_result.validation_report = Graph()
            validation_result.conforms = True
            validation_result.pyshacl_output = ""

            validation_results[step.iri] = [validation_result]

    # Yield the results in the same order as the jobs list (i.e., the validation path)
    ordered_validation_results: OrderedDict[URIRef, list[ValidationResult]] = OrderedDict()
    for job in path:
        step_iri = job.paired_step.workflow_model_step.iri
        ordered_validation_results[step_iri] = validation_results[step_iri]

    return ordered_validation_results


def validate_workflow_shape(workflow_shape: str) -> Graph:
    """
    Runs the given SHACL shape against pySHACL and returns the validation report
    """
    shacl_graph = Graph()
    shacl_graph.parse(data=workflow_shape, format="turtle")

    # We intercept and "fix" the queries launched by pySHACL to avoid making Virtuoso explode
    store = SPARQLStore(
        query_endpoint=f"{RDF_DATASTORE_API_ENDPOINT}/launch_query_validation",
        method="POST"
    )

    intercepted_data_graph = Dataset(store, default_union=True)

    _, validation_report, _ = validate(data_graph=intercepted_data_graph,
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
    return validation_report


def run_validation_task(workflow_shape_for_path: str,
                        workflow_model: WorkflowModel,
                        path: list[ValidationJob],
                        entity_iri: URIRef) -> OrderedDict[URIRef, list[ValidationResult]]:
    """
    Validation task wrapper for the concurrent process pool
    """
    return parse_validation_report(
        workflow_model,
        path,
        entity_iri,
        validate_workflow_shape(workflow_shape_for_path),
        Graph().parse(data=workflow_shape_for_path)
    )


def generate_validation_paths(workflow_model: WorkflowModel,
                              validation_jobs: dict[URIRef, list[ValidationJob]]) -> dict[URIRef, list[list[ValidationJob]]]:
    """
    Given the validation jobs generated by `generate_SHACL_shapes_for_workflow`, breaks them down across paths for each entity that is assigned to it.
    Each validation path corresponds to a sequence of consecutive workflow model steps paired against nodes in the same entity's data
    workflow, and contains one ValidationJob for every pairing in the path
    """
    validation_paths: dict[URIRef, list[list[ValidationJob]]] = dict()

    if workflow_model.initial_step_iri in validation_jobs:
        initial_step_iri_to_validate = workflow_model.initial_step_iri
    else:
        # Look for the first step to validate by doing a DFS traversal
        # (maybe we should forbid from having the initial step not assigned to any object)
        def get_initial_step_iri_to_validate(current_iri: URIRef) -> URIRef | None:
            if current_iri in validation_jobs:
                return current_iri

            step_data = workflow_model.workflow_model_steps.get(current_iri)
            if step_data:
                for next_step_iri in step_data.next_steps:
                    found_step = get_initial_step_iri_to_validate(next_step_iri)
                    if found_step:
                        return found_step
            return None

        initial_step_iri_to_validate = get_initial_step_iri_to_validate(workflow_model.initial_step_iri)

    if initial_step_iri_to_validate is not None:
        # List of (current_step_iri, active_paths_for_this_branch as a dict of Entity IRI -> list[(ValidationJob, is_active)]
        visitor_stack: list[tuple[URIRef, dict[URIRef, tuple[list[ValidationJob], bool]]]] = [(initial_step_iri_to_validate, dict())]

        # Completed unique paths, prevents duplicates from overlapping traversals
        seen_paths: dict[URIRef, set[tuple[ValidationJob, ...]]] = dict()

        def save_path(entity_iri: URIRef, completed_path: tuple[list[ValidationJob], bool]):
            if entity_iri not in validation_paths:
                validation_paths[entity_iri] = []
                seen_paths[entity_iri] = set()

            jobs = completed_path[0]
            path_tuple = tuple(jobs)
            if path_tuple not in seen_paths[entity_iri]:
                validation_paths[entity_iri].append(jobs)
                seen_paths[entity_iri].add(path_tuple)

        while len(visitor_stack) > 0:
            current_step_iri_to_validate, active_paths = visitor_stack.pop()

            # Deep copy active paths and reset their status
            next_active_paths = deepcopy(active_paths)
            for entity, path_info in list(next_active_paths.items()):
                next_active_paths[entity] = (path_info[0], False) # Reset their "is_active" status

            # Add validation jobs to those paths that are present in this node
            if current_step_iri_to_validate in validation_jobs:
                current_step_jobs = validation_jobs[current_step_iri_to_validate]

                for job in current_step_jobs:
                    if job.entity in next_active_paths: # Existing path
                        path, _ = next_active_paths[job.entity]
                        path.append(job)
                        next_active_paths[job.entity] = (path, True)
                    else: # New path (i.e., this entity was newly assigned to this step)
                        next_active_paths[job.entity] = ([job], True)

            # Check the next steps
            step_data = workflow_model.workflow_model_steps.get(current_step_iri_to_validate)
            next_steps = step_data.next_steps if step_data else []

            if not next_steps:
                # We hit a leaf node, so we save all remaining active paths for this branch
                #
                # Those that are still active will contain nodes up to this one, while
                # the rest will contain nodes up to the previous (parent) step
                for entity, path_info in next_active_paths.items():
                    save_path(entity, path_info)
            else:
                # Save all paths that *completely* stop here
                #
                # We know that a path stops by looking ahead and checking whether
                # the entity of each path is present in any of the next steps
                # If it is not present or it is not active in this node, then we
                # save it
                all_next_step_entities = set()
                for next_step_iri in next_steps:
                    if next_step_iri in validation_jobs:
                        all_next_step_entities.update(job.entity for job in validation_jobs[next_step_iri])

                for entity, path_info in list(next_active_paths.items()):
                    is_active = path_info[1]
                    if entity not in all_next_step_entities or not is_active:
                        save_path(entity, path_info)
                        del next_active_paths[entity] # And delete it to avoid propagating non-active paths

                # Propagate the remaining active paths to the next nodes
                for next_step_iri in next_steps:
                    if next_step_iri not in validation_jobs:
                        # Visit without any active paths, it resets everything
                        visitor_stack.append((next_step_iri, dict()))
                    else:
                        # We only forward paths that are still active and belong to an entity
                        # that will continue being validated in this specific next step
                        next_step_entities = {job.entity for job in validation_jobs[next_step_iri]}
                        branch_active_paths = dict()

                        for entity, path_info in next_active_paths.items():
                            if entity in next_step_entities:
                                branch_active_paths[entity] = path_info

                        visitor_stack.append((next_step_iri, branch_active_paths))

    if validation_paths:
        logging.debug("Generated validation paths:")
        for entity, paths in validation_paths.items():
            logging.debug(" Entity:", entity)
            for i, path in enumerate(paths):
                logging.debug(f"        Path {i}")
                logging.debug(f"        {' -> '.join([vr.paired_step.workflow_model_step.name for vr in path])}")

    return validation_paths

async def is_workflow_instance_valid(workflow_model: WorkflowModel,
                                     workflow_instance: WorkflowInstance,
                                     return_individual_results=False) -> ValidationStatus | dict[URIRef, list[OrderedDict[URIRef, list[ValidationResult]]]]:
    """
    Returns a ValidationStatus of for the provided workflow model against its instance's assignments

    `return_individual_results` can be set to `True` to instead return a full trace of the validation jobs
    executed, as a dict of entity -> list of validation path traces executed for the entity. Each validation
    path trace corresponds to a sequence of consecutive workflow model steps paired against target nodes in
    the same entity's data workflow, and contains a dict of workflow model step IRI -> ValidationResult. It
    will also contain pairings for which a target node was not found, which will be indicated within the
    ValidationResult

    When traversed, the dictionary will be ordered exactly like its corresponding validation path

    Note: Normally, only one validation path should appear, unless the entity has been assigned to non-consecutive
    workflow model steps for whichever reason

    The validation is split into individual jobs run in a process pool

    If the instance holds a valid (non-stale) cached validation result, the cached overall status is returned
    directly without running any validation jobs. Caching is only used for the overall status; a full trace
    (`return_individual_results=True`) is always recomputed
    """
    if (not return_individual_results
            and workflow_instance.has_valid_cache()
            and workflow_instance.cached_validation_status in ValidationStatus.__members__):
        return ValidationStatus[workflow_instance.cached_validation_status]

    validation_jobs = await generate_SHACL_shapes_for_workflow(workflow_model, workflow_instance)
    with ProcessPoolExecutor() as executor:
        # Get
        validation_paths: dict[URIRef, list[list[ValidationJob]]] = generate_validation_paths(workflow_model, validation_jobs)
        validation_results: dict[URIRef, list[tuple[OrderedDict[URIRef, list[ValidationResult]], int]]] = dict()

        futures_map: dict[Future, tuple[URIRef, int]] = {}

        for entity_iri, paths in validation_paths.items():
            validation_results[entity_iri] = []

            for i, path in enumerate(paths):
                # Every path is validated with a "workflow shape", containing at least one node shape
                # for the initial node in the data workflow, and 0 or more property shapes for the subsequent
                # nodes in the data workflow
                #
                # It is templated via Jinja as in the case of the individual workflow model steps

                # We only feed the steps that were correctly matched against a target node
                path_without_missing_data_jobs = [job for job in path if job.target_node is not None]
                substitutions = {
                    # Initial target node to which the workflow's SHACL shape will be assigned
                    # (i.e., the entity itself that marks the beginning of the data workflow)
                    #
                    # The rest of the nodes in the data workflow are validated via sh:property
                    # constraints
                    #
                    # Both the initial entity and all other nodes as validated as node shape
                    # constraints, filled by their corresponding workflow model steps
                    "entity_iri": entity_iri,
                    # Stored as metadata in thr SHACL shape via `crc:correspondingTargetNode`
                    # and `correspondingWorkflowModelStep` respectively, to be retrieved later
                    # when parsing the validation report
                    "target_node_iris": [j.target_node for j in path_without_missing_data_jobs],
                    "workflow_model_step_iris": [j.paired_step.workflow_model_step.iri for j in path_without_missing_data_jobs],
                    # Ordered list of paths to follow, indicated by the workflow model steps. They will be used to
                    # build the property shapes
                    "paths": [workflow_instance.step_assignments[j.paired_step.workflow_model_step.iri].property_to_follow for j in path_without_missing_data_jobs],
                    # Ordered list of (filled) workflow model step shapes and the IRIs to use in the workflow's shape
                    "node_shapes": [j.shacl_shape for j in path_without_missing_data_jobs],
                    "node_shape_iris" : [dw_prefix[urllib.parse.quote(f"step_{j.paired_step.workflow_model_step.name}_node_shape_for_{j.target_node}")] for j in path_without_missing_data_jobs],
                }
                future = executor.submit(
                    run_validation_task,
                    Template(workflow_shape).render(substitutions),
                    workflow_model,
                    path,
                    entity_iri
                )
                futures_map[future] = (entity_iri, i)

    for future in futures_map:
        entity_iri, path_idx = futures_map[future]
        if entity_iri not in validation_results:
            validation_results[entity_iri] = []

        validation_results[entity_iri].append((future.result(), path_idx))

    steps_with_missing_data_present = False
    for entity_iri, reses in validation_results.items():
        new_results_for_entity = []

        for (validation_results_for_path, path_idx) in reses:
            original_path = validation_paths[entity_iri][path_idx]

            results_with_missing_data_steps = OrderedDict()
            for original_job in original_path:
                step_iri = original_job.paired_step.workflow_model_step.iri
                if original_job.target_node is None:
                    steps_with_missing_data_present = True

                    validation_result = ValidationResult()
                    validation_result.validation_job = original_job
                    validation_result.validation_report = None
                    validation_result.conforms = False
                    validation_result.is_missing_data = True
                    validation_result.pyshacl_output = ""
                    results_with_missing_data_steps[step_iri] = [validation_result]
                else:
                    results_with_missing_data_steps[step_iri] = validation_results_for_path[step_iri]

            new_results_for_entity.append((results_with_missing_data_steps, path_idx))

        validation_results[entity_iri] = new_results_for_entity

    validation_results_to_return: dict[URIRef, list[OrderedDict[URIRef, list[ValidationResult]]]] = dict()
    for entity_iri, reses in validation_results.items():
        validation_results_to_return[entity_iri] = []
        for (validation_results_for_path, _) in reses:
            validation_results_to_return[entity_iri].append(validation_results_for_path)

    if return_individual_results:
        return validation_results_to_return
    else:
        all_validation_results: list[ValidationResult] = []
        for _, validation_paths_for_entity in validation_results.items():
            for (validation_path, _) in validation_paths_for_entity:
                for _, reses in validation_path.items():
                    for validation_result in reses:
                        all_validation_results.append(validation_result)

        if all(result.conforms for result in all_validation_results) and not steps_with_missing_data_present:
            return ValidationStatus.Valid
        elif all((result.conforms or result.is_missing_data) for result in all_validation_results):
            return ValidationStatus.Warning
        else:
            return ValidationStatus.Error
