import os
import uuid
from copy import deepcopy
from dataclasses import dataclass, field

from rdflib import URIRef, Graph, Literal, XSD

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore import WORKFLOWS_GRAPH_IRI, UpdateType
from workflows_validation.common import BaseWorkflowElement, base_workflow_element_iri_to_config_key, crc_prefix, prefixes, rdf_prefix, getURIOrString, \
    getURIOrLiteral, generate_unique_identifier
from workflows_validation.validation_cache import cache

module_dir = os.path.dirname(__file__)

workflow_model_details_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_model_details.sparql'), 'r').read()
workflow_model_step_details_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_model_step_details.sparql'), 'r').read()
workflow_model_step_templates_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_model_step_templates.sparql'), 'r').read()
delete_workflow_model_query = prefixes + open(os.path.join(module_dir, 'queries/delete_workflow_model.sparql'), 'r').read()
redirect_workflow_instances_query = prefixes + open(os.path.join(module_dir, 'queries/redirect_workflow_instances.sparql'), 'r').read()
redirect_workflow_instance_steps_query = prefixes + open(os.path.join(module_dir, 'queries/redirect_workflow_instance_steps.sparql'), 'r').read()
delete_workflow_instance_assignments_related_to_step_query = prefixes + open(
    os.path.join(module_dir, 'queries/delete_workflow_instance_assignments_related_to_step.sparql'), 'r').read()


@dataclass
class WorkflowModelStep(BaseWorkflowElement):
    """
    A step of a workflow model, containing restrictions for a tree of consecutive
    entities in the same order. The restrictions are formulated via a SHACL shape
    and, optionally, a collection of key->value replacements to apply on the SHACL
    shape, allowing the former to be reused.
    """

    """
    List of workflow model step IRIs that follow this one. Note that the system does not check for loops
    """
    next_steps: list[URIRef] = field(default_factory=list)

    """
    Key->value dict to replace in the step's SHACL shape, if any. The values can be either a list
    (multiple values being assigned to the same key), or a single str. This is treated as multiple 
    template triples when serializing the workflow model, and inferred when reading it
    """
    step_templates: dict[str, list[str] | str] = field(default_factory=dict)

    """
    SHACL shape serialized string as a Jinja template, to which the step templates will be applied.
    
    Important: At the very least, the template must contain a `target_node` replacement entry. When validated
    against a workflow instance, this entry will be replaced at validation time with the corresponding 
    target node from the assigned entities to this step.
    """
    SHACL_shape: str = ""


@dataclass
class WorkflowModel(BaseWorkflowElement):
    """
    A workflow model, consisting of a list of steps and a pointer to the initial step
    """

    """
    Initial step's IRI
    """
    initial_step_iri: URIRef = ""

    """
    Steps of the workflow, indexed by their IRI. The steps themselves indicate their successors, if any
    """
    workflow_model_steps: dict[URIRef, WorkflowModelStep] = field(default_factory=dict)

    def create_copy(self) -> "WorkflowModel":
        """
        Creates a copy of this workflow model ready to be serialized,
        ensuring that all of its entities have different URIs
        """
        workflow_model_copy = deepcopy(self)
        workflow_model_copy.create_new_iri()
        workflow_model_copy.name = "Copy of " + self.name

        for workflow_model_step in workflow_model_copy.workflow_model_steps.values():
            workflow_model_step.create_new_iri()

        return workflow_model_copy


workflow_model_iri_to_config_key = {
    **base_workflow_element_iri_to_config_key,
    str(crc_prefix.substep): "initial_step_iri",
}
workflow_model_config_key_to_iri = {v: k for k, v in workflow_model_iri_to_config_key.items()}

workflow_model_step_iri_to_config_key = {
    **base_workflow_element_iri_to_config_key,
    str(crc_prefix.nextStep): "next_steps",
    str(crc_prefix.hasTemplate): "step_templates",
    str(crc_prefix.assignedShape): "SHACL_shape",
}
workflow_model_step_config_key_to_iri = {v: k for k, v in workflow_model_step_iri_to_config_key.items()}

# The templates don't have a dataclass nor provenance metadata,
# they could be modeled as blank nodes or triple terms
workflow_model_step_template_iri_to_config_key = {
    # **base_workflow_element_iri_to_config_key,
    str(crc_prefix.templateKey): "key",
    str(crc_prefix.templateValue): "value",
}
workflow_model_step_template_config_key_to_iri = {v: k for k, v in workflow_model_step_template_iri_to_config_key.items()}


async def __read_workflow_model_base_details(workflow_model_iri: URIRef) -> None | WorkflowModel:
    """
    Given the IRI of a workflow model, retrieves its base details from the KG and returns a WorkflowModel instance.
    Important: To retrieve its collection of `WorkflowModelStep` entities, `read_workflow_model_step_details` must be run after
    this.
    """
    workflow_model = WorkflowModel()
    workflow_model.iri = workflow_model_iri

    result = await rdf_datastore_client.launch_query(workflow_model_details_query.replace("{entity_iri}", workflow_model_iri))
    for binding in result["results"]["bindings"]:
        p = binding["p"]["value"]
        o = binding["o"]["value"]

        if p in workflow_model_iri_to_config_key:
            config_key = workflow_model_iri_to_config_key.get(p)
            workflow_model.set_option(config_key, getURIOrString(o))
        else:  # Add it to provenance records
            workflow_model.set_option("provenance_records", (URIRef(p), getURIOrLiteral(o)))

    return workflow_model


async def __read_workflow_model_step_details(workflow_model: WorkflowModel) -> None | WorkflowModel:
    """
    Given a workflow model, retrieves its collection of `WorkflowModelStep` entities
    """

    # Caching dict to avoid constant lookups in the lists
    workflow_model_steps: dict[URIRef, WorkflowModelStep] = dict()

    # Fetch steps and their successor lists
    result = await rdf_datastore_client.launch_query(workflow_model_step_details_query.replace("{entity_iri}", workflow_model.iri))

    for binding in result["results"]["bindings"]:
        workflow_model_step_iri = URIRef(binding["s"]["value"])
        p = binding["p"]["value"]
        o = binding["o"]["value"]

        if workflow_model_step_iri not in workflow_model_steps:
            workflow_model_steps[workflow_model_step_iri] = WorkflowModelStep()
            workflow_model_steps[workflow_model_step_iri].iri = workflow_model_step_iri
        workflow_model_step = workflow_model_steps[workflow_model_step_iri]

        if p != str(crc_prefix.hasTemplate):  # We query the templates separately
            if p in workflow_model_step_iri_to_config_key:
                config_key = workflow_model_step_iri_to_config_key.get(p)
                workflow_model_step.set_option(config_key, getURIOrString(o))
            else:  # Add it to provenance records
                workflow_model_step.set_option("provenance_records", (p, getURIOrLiteral(o)))

    workflow_model.workflow_model_steps = workflow_model_steps

    # Fetch templates for each step
    for workflow_model_step in workflow_model.workflow_model_steps.values():
        result = await rdf_datastore_client.launch_query(workflow_model_step_templates_query.replace("{entity_iri}", workflow_model_step.iri))
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


async def read_workflow_model(workflow_model_iri: URIRef) -> None | WorkflowModel:
    """
    Given a workflow model's IRI, retrieves it from the KG and returns a `WorkflowModel`.
    """

    workflow_model = await __read_workflow_model_base_details(workflow_model_iri)
    if workflow_model is not None:
        return await __read_workflow_model_step_details(workflow_model)
    else:
        return None


async def store_workflow_model(workflow_model: WorkflowModel,
                               return_file: bool = False) -> str | None:
    """
    Serializes the provided workflow model into RDF, storing it in the KG.
    If `return_file` == True, it will write its RDF triples into a random-named .ttl file in this module's path.
    """
    g = Graph()

    # Type
    g.add((workflow_model.iri, rdf_prefix.type, crc_prefix.WorkflowModel))

    # Label
    g.add((workflow_model.iri, URIRef(workflow_model_config_key_to_iri["name"]), Literal(workflow_model.name, datatype=XSD.string)))

    # Comment
    g.add((workflow_model.iri, URIRef(workflow_model_config_key_to_iri["description"]), Literal(workflow_model.description, datatype=XSD.string)))

    # First step
    g.add((workflow_model.iri, URIRef(workflow_model_config_key_to_iri["initial_step_iri"]), workflow_model.initial_step_iri))

    for step in workflow_model.workflow_model_steps.values():
        # Type
        g.add((step.iri, rdf_prefix.type, crc_prefix.WorkflowModelStep))

        # Label
        g.add((step.iri, URIRef(workflow_model_step_config_key_to_iri["name"]), Literal(step.name, datatype=XSD.string)))

        # Comment
        g.add((step.iri, URIRef(workflow_model_step_config_key_to_iri["description"]), Literal(step.description, datatype=XSD.string)))

        # Next steps
        for next_step_iri in step.next_steps:
            g.add((step.iri, URIRef(workflow_model_step_config_key_to_iri["next_steps"]), next_step_iri))

        # SHACL shape
        g.add((step.iri, URIRef(workflow_model_step_config_key_to_iri["SHACL_shape"]), Literal(step.SHACL_shape, datatype=XSD.string)))

        # Templates
        for key, replacement in step.step_templates.items():
            template_iri = crc_prefix[generate_unique_identifier()]
            g.add((step.iri, URIRef(workflow_model_step_config_key_to_iri["step_templates"]), template_iri))
            g.add((template_iri, URIRef(workflow_model_step_template_config_key_to_iri["key"]), Literal(key)))

            if isinstance(replacement, str):
                g.add((template_iri, URIRef(workflow_model_step_template_config_key_to_iri["value"]), Literal(replacement)))
            elif isinstance(replacement, list):
                for value in replacement:
                    g.add((template_iri, URIRef(workflow_model_step_template_config_key_to_iri["value"]), Literal(value)))

        # User-defined metadata
        for (p, objs) in step.provenance_records.items():
            for o in objs:
                g.add((step.iri, URIRef(p), o))

    # User-defined metadata
    for (p, objs) in workflow_model.provenance_records.items():
        for o in objs:
            g.add((workflow_model.iri, URIRef(p), o))

    temporary_ttl_path = f"{uuid.uuid4().hex}.ttl"
    ttl_file_path = os.path.join(module_dir, temporary_ttl_path)
    g.serialize(destination=ttl_file_path, format='turtle')

    if return_file:
        return ttl_file_path
    else:
        await rdf_datastore_client.upload_file(ttl_file_path, graph_iri=WORKFLOWS_GRAPH_IRI, delete_file_after_upload=True)
        return None


async def delete_workflow_model(workflow_model: WorkflowModel,
                                return_query: bool = False) -> str | None:
    """
    Deletes the provided workflow model from the KG
    """
    query = delete_workflow_model_query.replace("{workflow_model_iri}", workflow_model.iri)
    if return_query:
        return query
    else:
        updates = [(query, UpdateType.query)]
        await rdf_datastore_client.launch_updates(updates, graph_iri=WORKFLOWS_GRAPH_IRI)
        cache.invalidate_model(workflow_model.iri)
        return None


async def overwrite_workflow_model(workflow_model: WorkflowModel,
                                   original_workflow_model: WorkflowModel):
    """
    Given an (updated) workflow model and its original version, deletes the existing, corresponding workflow model, and stores it again

    If the workflow model itself has been renamed or one of its steps deleted, the original workflow model will be
    used to redirect or remove its workflow instance assignments accordingly. If any steps have been renamed,
    a dictionary of renamed_step_name -> original_step_name must also be provided
    """
    actions = [(await (delete_workflow_model(original_workflow_model, return_query=True)), UpdateType.query),
               (await (store_workflow_model(workflow_model, return_file=True)), UpdateType.file_upload)]
    actions += [(query, UpdateType.query) for query in (await redirect_workflow_instance_steps(workflow_model,
                                                                                               original_workflow_model,
                                                                                               return_queries=True))]

    print(f"Stored: {workflow_model}")

    await rdf_datastore_client.launch_updates(actions, graph_iri=WORKFLOWS_GRAPH_IRI, delete_files_after_upload=True)
    cache.invalidate_model(original_workflow_model.iri)
    if workflow_model.iri != original_workflow_model.iri:
        cache.invalidate_model(workflow_model.iri)


async def redirect_workflow_instance_steps(new_workflow_model: WorkflowModel,
                                           old_workflow_model: WorkflowModel,
                                           return_queries: bool = False) -> list[str] | None:
    """
    Adapts all workflow instance step assignments of the old workflow model to the new one, removing those that don't have a corresponding
    workflow model step IRI in the new model
    """
    queries = []

    # Delete assignments referencing workflow model steps that don't exist anymore
    for old_step_iri, old_step in old_workflow_model.workflow_model_steps.items():
        if old_step_iri not in new_workflow_model.workflow_model_steps:
            queries.append((delete_workflow_instance_assignments_related_to_step_query
                            .replace("{workflow_model_step_iri}", old_step_iri)))

    # Redirect the instance as a whole, if applicable
    if old_workflow_model.iri != new_workflow_model.iri:
        queries.append((redirect_workflow_instances_query
                        .replace("{old_workflow_model_iri}", old_workflow_model.iri)
                        .replace("{new_workflow_model_iri}", new_workflow_model.iri)))

    if return_queries:
        return queries
    else:
        updates = [(query, UpdateType.query) for query in queries]
        await rdf_datastore_client.launch_updates(updates, graph_iri=WORKFLOWS_GRAPH_IRI)
        return None
