import os

from rdflib import URIRef, Node, Literal
from rfc3987 import match

from workflows_validation.common import dw_prefix, prefixes
from workflows_validation.workflow_instance import WorkflowInstance, workflow_instance_iri_to_config_key, StepAssignment, \
    step_assignment_iri_to_config_key
from workflows_validation.workflow_model import WorkflowModel, workflow_model_iri_to_config_key, WorkflowModelStep, workflow_model_step_iri_to_config_key

module_dir = os.path.dirname(__file__)

workflow_model_details_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_model_details.sparql'), 'r').read()
workflow_model_step_details_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_model_step_details.sparql'), 'r').read()
workflow_model_step_templates_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_model_step_templates.sparql'), 'r').read()

workflow_instance_details_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_instance_details.sparql'), 'r').read()
workflow_instance_step_details_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_instance_step_details.sparql'), 'r').read()


def get_iri_or_literal(node_str: str) -> Node:
    if bool(match(node_str, rule='URI')):
        return URIRef(node_str)
    else:
        return Literal(node_str)


def get_iri_or_string(node_str: str) -> Node | str:
    if bool(match(node_str, rule='URI')):
        return URIRef(node_str)
    else:
        return node_str


async def get_workflow_instances_assigned_to_model(workflow_model: WorkflowModel,
                                                   query_fn,
                                                   *args,
                                                   **kwargs) -> dict[URIRef, WorkflowInstance]:
    """
    Returns a dict of workflow instances assigned to the provided model, indexed by IRI
    A `query_fn` callback must be provided, to which a valid query str will be provided as its first parameter,
    and the (optional) *args and **kwargs after it

    query_fn must yield results in sparql1*-results-json format, parsed as a python dict
    """

    # Caching dict to avoid constant lookups in the lists
    workflow_instances: dict[URIRef, WorkflowInstance] = dict()
    query = workflow_instance_details_query.replace("{workflow_model_iri}", workflow_model.iri)
    result = await query_fn(query, *args, **kwargs)
    data = result["results"]["bindings"]
    if not data:
        return {}

    # Fetch base details
    for binding in data:
        workflow_instance_iri = URIRef(binding["workflow_instance"]["value"])
        p: str = binding["p"]["value"]
        o: str = binding["o"]["value"]

        if workflow_instance_iri not in workflow_instances:
            workflow_instances[workflow_instance_iri] = WorkflowInstance()
            workflow_instances[workflow_instance_iri].iri = workflow_instance_iri

        workflow_instance = workflow_instances[workflow_instance_iri]

        if p != str(dw_prefix.hasAssignment):  # We query the templates separately
            if p in workflow_instance_iri_to_config_key:
                config_key = workflow_instance_iri_to_config_key.get(p)
                workflow_instance.set_option(config_key, get_iri_or_string(o))
            else:  # Add it to provenance records
                workflow_instance.set_option("provenance_records", (p, get_iri_or_literal(o)))

    # Fetch step assignments
    for workflow_instance in workflow_instances.values():
        query = workflow_instance_step_details_query.replace("{workflow_instance_iri}", workflow_instance.iri)
        result = await query_fn(query, *args, **kwargs)
        data = result["results"]["bindings"]

        # Caching dict to avoid constant lookups in the lists
        step_assignments: dict[URIRef, StepAssignment] = dict()

        for binding in data:
            step_assignment_iri = URIRef(binding["workflow_step_assignment_iri"]["value"])
            p: str = binding["p"]["value"]
            o: str = binding["o"]["value"]

            if step_assignment_iri not in step_assignments:
                step_assignments[step_assignment_iri] = StepAssignment()
                step_assignments[step_assignment_iri].iri = step_assignment_iri

            step_assignment = step_assignments[step_assignment_iri]

            if p in step_assignment_iri_to_config_key:
                config_key = step_assignment_iri_to_config_key.get(p)
                step_assignment.set_option(config_key, get_iri_or_string(o))
            else:  # Add it to provenance records
                step_assignment.set_option("provenance_records", (p, get_iri_or_literal(o)))

        # Reindex them by the workflow model step they refer to
        step_assignments = {step_assignment.workflow_step_iri: step_assignment for step_assignment in step_assignments.values()}
        workflow_instance.step_assignments = step_assignments

    return workflow_instances


async def _read_workflow_model_base_details(workflow_model_iri: URIRef,
                                            query_fn,
                                            *args,
                                            **kwargs) -> None | WorkflowModel:
    """
    Given the IRI of a workflow model, retrieves its base details from the KG and returns a WorkflowModel instance.
    Important: To retrieve its collection of `WorkflowModelStep` entities, `read_workflow_model_step_details` must be run after
    this.
    """
    workflow_model = WorkflowModel()
    workflow_model.iri = workflow_model_iri

    result = await query_fn(workflow_model_details_query.replace("{entity_iri}", workflow_model_iri), *args, **kwargs)
    for binding in result["results"]["bindings"]:
        p = binding["p"]["value"]
        o = binding["o"]["value"]

        if p in workflow_model_iri_to_config_key:
            config_key = workflow_model_iri_to_config_key.get(p)
            workflow_model.set_option(config_key, get_iri_or_string(o))
        else:  # Add it to provenance records
            workflow_model.set_option("provenance_records", (URIRef(p), get_iri_or_literal(o)))

    return workflow_model


async def _read_workflow_model_step_details(workflow_model: WorkflowModel,
                                            query_fn,
                                            *args,
                                            **kwargs) -> None | WorkflowModel:
    """
    Given a workflow model, retrieves its collection of `WorkflowModelStep` entities
    """

    # Caching dict to avoid constant lookups in the lists
    workflow_model_steps: dict[URIRef, WorkflowModelStep] = dict()

    # Fetch steps and their successor lists
    result = await query_fn(workflow_model_step_details_query.replace("{entity_iri}", workflow_model.iri), *args, **kwargs)

    for binding in result["results"]["bindings"]:
        workflow_model_step_iri = URIRef(binding["s"]["value"])
        p = binding["p"]["value"]
        o = binding["o"]["value"]

        if workflow_model_step_iri not in workflow_model_steps:
            workflow_model_steps[workflow_model_step_iri] = WorkflowModelStep()
            workflow_model_steps[workflow_model_step_iri].iri = workflow_model_step_iri
        workflow_model_step = workflow_model_steps[workflow_model_step_iri]

        if p != str(dw_prefix.hasTemplate):  # We query the templates separately
            if p in workflow_model_step_iri_to_config_key:
                config_key = workflow_model_step_iri_to_config_key.get(p)
                workflow_model_step.set_option(config_key, get_iri_or_string(o))
            else:  # Add it to provenance records
                workflow_model_step.set_option("provenance_records", (p, get_iri_or_literal(o)))

    workflow_model.workflow_model_steps = workflow_model_steps

    # Fetch templates for each step
    for workflow_model_step in workflow_model.workflow_model_steps.values():
        result = await query_fn(workflow_model_step_templates_query.replace("{entity_iri}", workflow_model_step.iri), *args, **kwargs)
        for binding in result["results"]["bindings"]:
            if binding["key"]["value"] not in workflow_model_step.step_templates:
                workflow_model_step.step_templates[binding["key"]["value"]] = list()
            workflow_model_step.step_templates[binding["key"]["value"]].append(binding["value"]["value"])

    # Coalesce single-value replacements from being lists to just a str
    for workflow_model_step in workflow_model.workflow_model_steps.values():
        for k, v in workflow_model_step.step_templates.items():
            if len(v) == 1:
                workflow_model_step.step_templates[k] = v[0]

    return workflow_model


async def read_workflow_model(workflow_model_iri: URIRef,
                              query_fn,
                              *args,
                              **kwargs) -> None | WorkflowModel:
    """
    Given a workflow model's IRI, retrieves it from the KG and returns a `WorkflowModel`

    A `query_fn` callback must be provided, to which a valid query str will be provided as its first parameter,
    and the (optional) *args and **kwargs after it

    query_fn must yield results in sparql1*-results-json format, parsed as a python dict
    """
    workflow_model = await _read_workflow_model_base_details(workflow_model_iri, query_fn, *args, **kwargs)
    if workflow_model is not None:
        return await _read_workflow_model_step_details(workflow_model, query_fn, *args, **kwargs)
    else:
        return None