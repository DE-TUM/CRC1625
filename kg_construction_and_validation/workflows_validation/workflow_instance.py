import os
import uuid
from dataclasses import dataclass, field

from rdflib import URIRef, Graph, Literal, XSD

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore import WORKFLOWS_GRAPH_IRI, UpdateType
from workflows_validation.common import BaseWorkflowElement, crc_prefix, base_workflow_element_iri_to_config_key, prefixes, getURIOrString, getURIOrLiteral, \
    rdf_prefix
from workflows_validation.workflow_model import WorkflowModel

module_dir = os.path.dirname(__file__)

workflow_instance_details_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_instance_details.sparql'), 'r').read()
workflow_instance_step_details_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_instance_step_details.sparql'), 'r').read()
delete_workflow_instance_query = prefixes + open(os.path.join(module_dir, 'queries/delete_workflow_instance.sparql'), 'r').read()


@dataclass
class StepAssignment(BaseWorkflowElement):
    """
    An assignment of entities for a given step
    """

    """
    Workflow model step it refers to
    """
    workflow_step_iri: URIRef = ""

    """
    Entity IRIs that are to be validated by the step this assignment refers to
    """
    assigned_entities: list[URIRef] = field(default_factory=list)

    """
    Property through which we will get the next focus node corresponding to the previous
    one, or the entity itself from this step assignment if it wasn't validated in the 
    previous step
    """
    property_to_follow: URIRef = ""


@dataclass
class WorkflowInstance(BaseWorkflowElement):
    """
    Workflow model it refers to
    """
    workflow_model_iri: URIRef = ""

    """
    Step assignments, indexed by the workflow model step IRI they are assigned to
    """
    step_assignments: dict[URIRef, StepAssignment] = field(default_factory=dict)


workflow_instance_iri_to_config_key = {
    **base_workflow_element_iri_to_config_key,
    str(crc_prefix.workflowModelInstanceOf): "workflow_model_iri",
}
workflow_instance_config_key_to_iri = {v: k for k, v in workflow_instance_iri_to_config_key.items()}

step_assignment_iri_to_config_key = {
    **base_workflow_element_iri_to_config_key,
    # step_templates actually belongs to the workflow instance, but we handle their creation outside the automated config methods
    str(crc_prefix.hasAssignment): "step_assignments",
    str(crc_prefix.assignedWorkflowModelStep): "workflow_step_iri",
    str(crc_prefix.assignedEntity): "assigned_entities",
    str(crc_prefix.propertyToFollow): "property_to_follow",
}
step_assignment_config_key_to_iri = {v: k for k, v in step_assignment_iri_to_config_key.items()}


async def get_workflow_instances_of_model(workflow_model: WorkflowModel) -> dict[URIRef, WorkflowInstance]:
    """
    Returns a dict of workflow instances assigned to the provided model, indexed by IRI
    """

    # Caching dict to avoid constant lookups in the lists
    workflow_instances: dict[URIRef, WorkflowInstance] = dict()
    query = workflow_instance_details_query.replace("{workflow_model_iri}", workflow_model.iri)
    result = await rdf_datastore_client.launch_query(query)
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

        if p != str(crc_prefix.hasAssignment):  # We query the templates separately
            if p in workflow_instance_iri_to_config_key:
                config_key = workflow_instance_iri_to_config_key.get(p)
                workflow_instance.set_option(config_key, getURIOrString(o))
            else:  # Add it to provenance records
                workflow_instance.set_option("provenance_records", (p, getURIOrLiteral(o)))

    # Fetch step assignments
    for workflow_instance in workflow_instances.values():
        query = workflow_instance_step_details_query.replace("{workflow_instance_iri}", workflow_instance.iri)
        result = await rdf_datastore_client.launch_query(query)
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
                step_assignment.set_option(config_key, getURIOrString(o))
            else:  # Add it to provenance records
                step_assignment.set_option("provenance_records", (p, getURIOrLiteral(o)))

        # Reindex them by the workflow model step they refer to
        step_assignments = {step_assignment.workflow_step_iri: step_assignment for step_assignment in step_assignments.values()}
        workflow_instance.step_assignments = step_assignments

    return workflow_instances


async def store_workflow_instance(workflow_instance: WorkflowInstance,
                                  return_file: bool = False) -> str | None:
    """
    Serializes the workflow instance into RDF and stores it
    """
    g = Graph()

    if not workflow_instance.iri:  # The instance is new
        workflow_instance.create_new_iri()

    # Type
    g.add((workflow_instance.iri, rdf_prefix.type, crc_prefix.workflowModelInstance))

    # Label
    g.add((workflow_instance.iri, URIRef(workflow_instance_config_key_to_iri["name"]), Literal(workflow_instance.name, datatype=XSD.string)))

    # Description
    g.add((workflow_instance.iri, URIRef(workflow_instance_config_key_to_iri["description"]), Literal(workflow_instance.description, datatype=XSD.string)))

    # Link to workflow model
    g.add((workflow_instance.iri, URIRef(workflow_instance_config_key_to_iri["workflow_model_iri"]), workflow_instance.workflow_model_iri))

    for step_assignment in workflow_instance.step_assignments.values():
        # Type
        g.add((step_assignment.iri, rdf_prefix.type, crc_prefix.WorkflowModelStepAssignment))

        # Label
        g.add((step_assignment.iri, URIRef(step_assignment_config_key_to_iri["name"]), Literal(step_assignment.name, datatype=XSD.string)))

        # Description
        g.add((step_assignment.iri, URIRef(step_assignment_config_key_to_iri["description"]), Literal(step_assignment.description, datatype=XSD.string)))

        # Link to assignment
        g.add((workflow_instance.iri, URIRef(step_assignment_config_key_to_iri["step_assignments"]), step_assignment.iri))

        # Link to step
        g.add((step_assignment.iri, URIRef(step_assignment_config_key_to_iri["workflow_step_iri"]), step_assignment.workflow_step_iri))

        # Assigned entities
        for assigned_entity_iri in step_assignment.assigned_entities:
            g.add((step_assignment.iri, URIRef(step_assignment_config_key_to_iri["assigned_entities"]), assigned_entity_iri))

        # Property to follow
        g.add((step_assignment.iri, URIRef(step_assignment_config_key_to_iri["property_to_follow"]), step_assignment.property_to_follow))

        # User-defined metadata
        for (p, objs) in step_assignment.provenance_records.items():
            for o in objs:
                g.add((step_assignment.iri, URIRef(p), o))

    # User-defined metadata
    for (p, objs) in workflow_instance.provenance_records.items():
        for o in objs:
            g.add((workflow_instance.iri, URIRef(p), o))

    temporary_ttl_path = f"{uuid.uuid4().hex}.ttl"
    ttl_file_path = os.path.join(module_dir, temporary_ttl_path)
    g.serialize(destination=ttl_file_path, format='turtle')

    if return_file:
        return ttl_file_path
    else:
        await rdf_datastore_client.upload_file(ttl_file_path, graph_iri=WORKFLOWS_GRAPH_IRI, delete_file_after_upload=True)
        return None


async def delete_workflow_instance(workflow_instance: WorkflowInstance,
                                   return_query: bool = False):
    """
    Deletes the workflow instance corresponding to the provided one, and stores it again
    """
    query = delete_workflow_instance_query.replace("{workflow_instance_iri}", workflow_instance.iri)
    if return_query:
        return query
    else:
        updates = [(query, UpdateType.query)]
        await rdf_datastore_client.launch_updates(updates, workflow_instance.iri)


async def overwrite_workflow_instance(workflow_instance: WorkflowInstance):
    """
    Given an (updated) workflow instance, deletes its and stores it again
    """
    actions = []
    actions.append((await delete_workflow_instance(workflow_instance, return_query=True), UpdateType.query))
    actions.append((await store_workflow_instance(workflow_instance, return_file=True), UpdateType.file_upload))

    await rdf_datastore_client.launch_updates(actions, graph_iri=WORKFLOWS_GRAPH_IRI, delete_files_after_upload=True)
