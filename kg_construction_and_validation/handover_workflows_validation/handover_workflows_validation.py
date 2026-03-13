"""
Handover workflows validation API. This is a fully functional implementation of all functions required functions to
perform handover workflows validation, which offers methods for:
- Reading, writing, modifying and deleting workflow models
- Reading, writing, modifying and deleting workflow model instances
- Performing validation given a workflow model and a workflow model instance,
  yielding a full trace of validation results

Workflow models and their instances can be created via their respective objects, or
by writing them to RDF directly. They are intended to be stored in an RDF graph.

An example usage of this API can be found on run_workflow_validation_test.py, which performs
a full test of its correctness. RDF representations off the tested workflows can be found
on ./validation_test, alongside user-friendly .yml representations that were used to
bootstrap them. Moreover, the WebUI uses it extensively for reading and writing workflow models
and instances.
"""

import asyncio
import json
import os
import urllib
import uuid
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum

from pyshacl import validate
from rdflib import Graph, URIRef, Literal, Namespace, XSD

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore import UpdateType, WORKFLOWS_GRAPH_IRI

module_dir = os.path.dirname(__file__)

prefixes = open(os.path.join(module_dir, 'queries/prefixes.sparql')).read()
handovers_group_shape_templated = open(os.path.join(module_dir, 'shacl_shapes/handover_group_shape_templated.shacl'), 'r').read()
shape_require_activity_templated = open(os.path.join(module_dir, 'shacl_shapes/property_shape_require_activity.shacl'), 'r').read()
shape_restrict_number_of_activities_templated = open(os.path.join(module_dir, 'shacl_shapes/property_shape_restrict_number_of_activities.shacl'), 'r').read()

get_first_handover_group_query = prefixes + open(os.path.join(module_dir, 'queries/get_first_handover_group.sparql'), 'r').read()
get_handover_group_pairs_query = prefixes + open(os.path.join(module_dir, 'queries/get_handover_group_pairs.sparql'), 'r').read()
get_handovers_and_activities_for_sample_query = prefixes + open(os.path.join(module_dir, 'queries/get_handovers_and_activities_for_sample.sparql'), 'r').read()
delete_handover_workflow_model_query = prefixes + open(os.path.join(module_dir, 'queries/delete_handover_workflow_model.sparql'), 'r').read()
delete_handover_workflow_instance_query = prefixes + open(os.path.join(module_dir, 'queries/delete_handover_workflow_instance.sparql'), 'r').read()
redirect_workflow_instances_query = prefixes + open(os.path.join(module_dir, 'queries/redirect_workflow_instances.sparql'), 'r').read()
redirect_workflow_instance_steps_query = prefixes + open(os.path.join(module_dir, 'queries/redirect_workflow_instance_steps.sparql'), 'r').read()
delete_workflow_instance_steps_query = prefixes + open(os.path.join(module_dir, 'queries/delete_workflow_instance_steps.sparql'), 'r').read()
clean_handover_workflow_instance_steps_query = prefixes + open(os.path.join(module_dir, 'queries/clean_handover_workflow_instance_steps.sparql'), 'r').read()
workflow_model_details_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_model_details.sparql'), 'r').read()
workflow_instance_details_query = prefixes + open(os.path.join(module_dir, 'queries/workflow_instance_details.sparql'), 'r').read()
get_activity_type_query = prefixes + open(os.path.join(module_dir, 'queries/get_activity_type.sparql'), 'r').read()
get_workflow_model_names_and_creators_query = prefixes + open(os.path.join(module_dir, 'queries/get_workflow_model_names_and_creators.sparql'), 'r').read()
get_workflow_model_names_from_user_query = prefixes + open(os.path.join(module_dir, 'queries/get_workflow_model_names_from_user.sparql'), 'r').read()

crc_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/")
crc_workflow_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/workflow/")
crc_project_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/project/")
crc_sample_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/object/")
crc_user_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/user/")
pmdco_prefix = Namespace("https://w3id.org/pmd/co/")
rdf_prefix = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
rdfs_prefix = Namespace("http://www.w3.org/2000/01/rdf-schema#")
prov_prefix = Namespace("http://www.w3.org/ns/prov#")

ont_graph = Graph()
ont_graph.parse(os.path.join(module_dir, "../../ontologies/crc.ttl"), format="turtle")
ont_graph.parse(os.path.join(module_dir, "../../ontologies/pmd_core.ttl"), format="turtle")
ont_graph.parse(os.path.join(module_dir, "../../ontologies/oce.owl"), format="xml")


# We use the same measurement type ID -> activity correspondences declared for the materialization
measurement_type_ids_to_activities: list[dict[str, list[str] | str]] = json.load(open(os.path.join(module_dir, "../materialization/mappings/measurement_type_ids_to_activities.json")))

activity_name_to_iri = {
    m["measurement_name"]: str(crc_prefix[m["measurement_class_name"]])
    for m in measurement_type_ids_to_activities
}

iri_to_activity_name = {v: k for k, v in activity_name_to_iri.items()}


@dataclass
class WorkflowModelOptions:
    """
    General options for a workflow model that apply to all steps
    """

    """
    Indicates if the validation of the workflow model allows intermediate, non-compliant handover
    groups on an otherwise valid handover group chain while validating. If true, the validation system
    will attempt to keep validating the current workflow model step on the next handover group if the
    current handover group was non-compliant.
    """
    allow_intermediate_handover_groups: bool = True

    """
    Step name from which we will begin validating the workflow
    """
    initial_step_name: str = ""

    def set_option(self, k, v):
        if hasattr(self, k):
            setattr(self, k, v)
        else:
            raise AttributeError(f"Option '{k}' with value '{v}' is not a valid option for the workflow model")


@dataclass
class WorkflowModelStep:
    """
    A step of a workflow model, containing restrictions for a chain of consecutive
    handover groups in the same order
    """

    """
    Indicates if the current workflow model step is to be validated by the system.
    If false, the system will keep checking the current handover group from the next workflow model step.
    """
    enabled: bool = True

    """
    List of step names that follow this one. Note that the system does not check for loops
    """
    next_steps: list[str] = field(default_factory=list)

    step_name: str = "Unnamed step"
    step_description: str = "No description"

    """
    Indicates that the handover group that the workflow model step is checking is allowed to take 
    place in the given group(s)
    
    They should be in the form of 'A01', 'B03', etc.
    """
    projects: list[str] = field(default_factory=list)

    """
    Indicates that the handover group that the workflow model step is checking must contain a given 
    activity within its handovers. The type of measurement is indicated by the targeted project's entity's 
    class, by also belonging to one of the *Process classes (e.g. :FIMProcess)
    """
    required_activities: list[str] = field(default_factory=list)

    """
    Indicates that the handover group that the workflow model step is validating is allowed to have 
    activities other than the ones indicated by requiresActivity. If false, the presence of other 
    activities will cause the validation to fail in this step.
    """
    allow_other_activities: bool = True

    def set_option(self, k, v):
        if hasattr(self, k):
            setattr(self, k, v)
        else:
            raise AttributeError(f"Option '{k}' with value '{v}' is not a valid option for the workflow step")


@dataclass
class WorkflowModel:
    """
    Note that the workflow models are, for now, uniquely identified by their name. Thus, two workflow models
    cannot have the same name even though they are created by / belong to different users

    Once this is integrated into a UI, they can be uniquely identified by a combination of name, username
    and/or creation date
    """
    workflow_model_name: str = field(default_factory=str)

    workflow_model_options: WorkflowModelOptions = field(default_factory=WorkflowModelOptions)

    """
    Steps of the workflow, indexed by name for faster lookups and to enforce unique IDs. The steps
    themselves indicate their successors, if any
    """
    workflow_model_steps: dict[str, WorkflowModelStep] = field(default_factory=dict)

    """
    User ID of the creator and owner of this WorkflowInstance
    """
    creator_user_id: int = -1

    def __hash__(self):
        return hash(self.workflow_model_name+str(self.creator_user_id))


@dataclass
class WorkflowInstance:
    workflow_instance_name: str = ""

    """
    Workflow model name it refers to. Note that this will change when workflow model names stop 
    being unique identifiers
    """
    workflow_model_name: str = ""

    """
    Dict of Step name -> List of sample IDs (i.e. external IDs)
    Throughout the code, sample IDs will be referred as object IDs to simplify terminology. If we were to switch to check
    actual object IDs (i.e. internal IDs) instead, we just nee to change the predicates in the SPARQL queries accordingly
    
    The step names must refer to the step names contained in the workflow model it is specifying
    """
    step_assignments: dict[str, list[int]] = field(default_factory=dict)

    """
    User ID of the creator and owner of this WorkflowInstance
    """
    creator_user_id: int = -1

    def __hash__(self):
        return hash(self.workflow_instance_name+str(self.creator_user_id))


workflow_model_iri_to_config = {
    str(crc_prefix.objectName): "workflow_model_name",
    str(crc_prefix.substep): "initial_step",
    str(crc_prefix.allowIntermediateHandoverGroups): "allow_intermediate_handover_groups"
}

workflow_model_step_iri_to_config = {
    str(crc_prefix.isHandoverWorkflowStepEnabled): "enabled",
    str(crc_prefix.nextStep): "next_steps",
    str(crc_prefix.objectDescription): "step_description",
    str(crc_prefix.allowedProject): "projects",
    str(crc_prefix.requiresActivity): "required_activities",
    str(crc_prefix.allowsOtherActivities): "allow_other_activities"
}


def uuid_for_name(name: str, user_id: int):
    """
    Generates a UUID5 for the given name and creator in the DNS namespace. Used to uniquely identify workflow
    models and instances
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, name + (str(user_id))))


async def get_activity_type(entity_iri: str):
    """
    Returns the activity type IRI for an entity (a type that is pmdco_prefix.AnalysingProcess or a subclass of it)
    
    This is used to identify to which type of measurement the activity belongs to
    """
    query = get_activity_type_query.replace("{entity_iri}", entity_iri)
    result = await rdf_datastore_client.launch_query(query)
    result = result["results"]["bindings"]
    if len(result) > 0:
        return result[0]["type"]["value"]
    else:
        return str(pmdco_prefix.AnalysingProcess)  # It's an "Others" activity


async def get_workflow_model_names_and_creator_user_ids() -> list[tuple[str, int]]:
    workflow_models_list: list[tuple[str, int]] = []

    query = get_workflow_model_names_and_creators_query
    result = await rdf_datastore_client.launch_query(query)
    results = result["results"]["bindings"]
    for result in results:
        workflow_model_name = result["workflow_model_name"]["value"]
        workflow_model_creator = result["user_id"]["value"]

        workflow_models_list.append((workflow_model_name, workflow_model_creator))

    return workflow_models_list


async def get_workflow_model_names_from_user(user_id: int) -> list[str]:
    workflow_models_list: list[str] = []

    query = get_workflow_model_names_from_user_query.replace("{user_id}", str(user_id))
    result = await rdf_datastore_client.launch_query(query)
    results = result["results"]["bindings"]
    for result in results:
        workflow_model_name = result["workflow_model_name"]["value"]

        workflow_models_list.append(workflow_model_name)

    return workflow_models_list


async def read_workflow_model(workflow_model_name: str, workflow_model_creator_user_id: int) -> None | WorkflowModel:
    """
    Returns the WorkflowModel identified by the provided name and its creator's user ID
    """

    workflow_model = WorkflowModel()
    workflow_model.creator_user_id = workflow_model_creator_user_id

    workflow_model_id = uuid_for_name(workflow_model_name, workflow_model_creator_user_id)
    workflow_model_iri = crc_workflow_prefix["workflow_model_" + workflow_model_id]

    result = await rdf_datastore_client.launch_query(workflow_model_details_query.replace("{entity_iri}", workflow_model_iri))
    data = result["results"]["bindings"]
    if not data:
        return None

    labels_dict = dict()
    # Get the labels of everything first
    for binding in data:
        s = binding["s"]["value"]
        p = binding["p"]["value"]
        o = Literal(binding["o"]["value"], datatype=binding["o"].get("datatype")).toPython()

        if p == str(crc_prefix.objectName):
            # We should separately check the type of each entity, but this
            # way we avoid launching tons of individual queries
            if "step" not in s:
                workflow_model.workflow_model_name = o # Set it directly
            else:
                labels_dict[s] = o

    for binding in data:
        s = binding["s"]["value"]
        p = binding["p"]["value"]
        o = Literal(binding["o"]["value"], datatype=binding["o"].get("datatype")).toPython()

        if "step" not in s:
            if p in workflow_model_iri_to_config:
                if "initial_step" in workflow_model_iri_to_config[p]:
                    workflow_model.workflow_model_options.initial_step_name = labels_dict[o]
                elif p != str(crc_prefix.objectName): # We already set it, and the label is not an option
                    workflow_model.workflow_model_options.set_option(workflow_model_iri_to_config[p], o)

        else:  # It's a step
            step_name = labels_dict[s]
            if step_name not in workflow_model.workflow_model_steps:
                workflow_model.workflow_model_steps[step_name] = WorkflowModelStep()
                workflow_model.workflow_model_steps[step_name].step_name = step_name

            workflow_step = workflow_model.workflow_model_steps[step_name]
            if p in workflow_model_step_iri_to_config:
                match workflow_model_step_iri_to_config[p]:
                    case "next_steps":
                        workflow_step.next_steps.append(labels_dict[o])
                    case "projects":
                        workflow_step.projects.append(o.rsplit("/", 1)[-1])
                    case "required_activities":
                        workflow_step.required_activities.append(iri_to_activity_name[await get_activity_type(o)])
                    case _:
                        workflow_step.set_option(workflow_model_step_iri_to_config[p], o)

    return workflow_model


async def store_workflow_model(workflow_model: WorkflowModel,
                               return_file: bool = False) -> str | None:
    """
    Serializes the workflow model into RDF and stores it
    """
    g = Graph()
    name_to_uid = dict()

    workflow_model_id = uuid_for_name(workflow_model.workflow_model_name, workflow_model.creator_user_id)
    workflow_model_iri = crc_workflow_prefix["workflow_model_" + workflow_model_id]

    # Type
    g.add((workflow_model_iri, rdf_prefix.type, crc_prefix.HandoverWorkflowModel))

    # Label
    g.add((workflow_model_iri, crc_prefix.objectName, Literal(workflow_model.workflow_model_name, datatype=XSD.string)))

    # Attribution
    g.add((workflow_model_iri, crc_prefix.creator, crc_user_prefix[str(workflow_model.creator_user_id)]))

    # Settings
    g.add((workflow_model_iri, crc_prefix.allowIntermediateHandoverGroups,
           Literal(workflow_model.workflow_model_options.allow_intermediate_handover_groups, datatype=XSD.boolean)))

    for step_name, step in workflow_model.workflow_model_steps.items():
        if step_name not in name_to_uid:
            name_to_uid[step_name] = uuid_for_name(step_name, workflow_model.creator_user_id)

        step_iri = crc_workflow_prefix[f"workflow_step_{name_to_uid[step_name]}_for_workflow_model_{workflow_model_id}"]

        if step_name == workflow_model.workflow_model_options.initial_step_name:
            # Link to first step
            g.add((workflow_model_iri, crc_prefix.substep, step_iri))

        # Type
        g.add((step_iri, rdf_prefix.type, crc_prefix.HandoverWorkflowModelStep))

        # Label
        g.add((step_iri, crc_prefix.objectName, Literal(step_name, datatype=XSD.string)))

        # Attribution
        g.add((step_iri, crc_prefix.creator, crc_user_prefix[str(workflow_model.creator_user_id)]))

        # Comment
        g.add((step_iri, crc_prefix.objectDescription, Literal(step.step_description, datatype=XSD.string)))

        # Enabled
        g.add((step_iri, crc_prefix.isHandoverWorkflowStepEnabled, Literal(step.enabled, datatype=XSD.boolean)))

        # Projects
        for project_name in step.projects:
            g.add((step_iri, crc_prefix.allowedProject, crc_project_prefix[project_name]))

        # Next steps
        for next_step_name in step.next_steps:
            if next_step_name not in name_to_uid:
                name_to_uid[next_step_name] = uuid_for_name(next_step_name, workflow_model.creator_user_id)

            next_step_iri = crc_workflow_prefix[f"workflow_step_{name_to_uid[next_step_name]}_for_workflow_model_{workflow_model_id}"]
            g.add((step_iri, crc_prefix.nextStep, next_step_iri))

        # Required activities
        for required_activity in step.required_activities:
            activity_iri = crc_prefix[f"{urllib.parse.quote(required_activity)}_activity_for_workflow_step_{name_to_uid[step_name]}"]

            # Requirement
            g.add((step_iri, crc_prefix.requiresActivity, activity_iri))

            # Type(s) of the activity
            activity_type_iri = activity_name_to_iri[required_activity]

            g.add((activity_iri, rdf_prefix.type, URIRef(activity_type_iri)))
            g.add((activity_iri, rdf_prefix.type, crc_prefix.CharacterizationActivityModel))

        # Allow other activities
        g.add((step_iri, crc_prefix.allowsOtherActivities, Literal(step.allow_other_activities, datatype=XSD.boolean)))

    temporary_ttl_path = f"{uuid.uuid4().hex}.ttl"
    ttl_file_path = os.path.join(module_dir, temporary_ttl_path)
    g.serialize(destination=ttl_file_path, format='turtle')

    if return_file:
        return ttl_file_path
    else:
        await rdf_datastore_client.upload_file(ttl_file_path, graph_iri=WORKFLOWS_GRAPH_IRI, delete_file_after_upload=True)
        return None


async def redirect_workflow_instances(old_workflow_model: WorkflowModel,
                                      new_workflow_model: WorkflowModel,
                                      return_query: bool = False) -> str | None:
    """
    Redirects all workflow instances of the old workflow model to the new one

    Note that `redirect_workflow_instance_steps` must also be run to completely redirect the instances
    """
    old_workflow_model_id = uuid_for_name(old_workflow_model.workflow_model_name, old_workflow_model.creator_user_id)
    old_workflow_model_iri = crc_workflow_prefix["workflow_model_" + old_workflow_model_id]

    new_workflow_model_id = uuid_for_name(new_workflow_model.workflow_model_name, new_workflow_model.creator_user_id)
    new_workflow_model_iri = crc_workflow_prefix["workflow_model_" + new_workflow_model_id]

    query = (redirect_workflow_instances_query
             .replace("{old_workflow_model_iri}", old_workflow_model_iri)
             .replace("{new_workflow_model_iri}", new_workflow_model_iri))
    if return_query:
        return query
    else:
        updates = [(query, UpdateType.query)]
        await rdf_datastore_client.launch_updates(updates, graph_iri=WORKFLOWS_GRAPH_IRI)
        return None


async def redirect_workflow_instance_steps(new_workflow_model: WorkflowModel,
                                           old_workflow_model: WorkflowModel,
                                           step_renamings: dict[str, str] | None = None,
                                           return_queries: bool = False) -> list[str] | None:
    """
    Adapts all workflow instance steps of the old workflow model to the new one, renaming or removing them accordingly
    """
    # Switch the renamed steps dict around
    if step_renamings is not None:
        step_renamings = {v : k for k, v in step_renamings.items()}

    queries = []

    old_workflow_model_id = uuid_for_name(old_workflow_model.workflow_model_name, old_workflow_model.creator_user_id)
    old_workflow_model_iri = crc_workflow_prefix["workflow_model_" + old_workflow_model_id]

    new_workflow_model_id = uuid_for_name(new_workflow_model.workflow_model_name, new_workflow_model.creator_user_id)
    new_workflow_model_iri = crc_workflow_prefix["workflow_model_" + new_workflow_model_id]

    for old_step_name in old_workflow_model.workflow_model_steps:
        old_workflow_model_step_iri = crc_workflow_prefix[f"workflow_step_{uuid_for_name(old_step_name, old_workflow_model.creator_user_id)}_for_workflow_model_{old_workflow_model_id}"]
        if step_renamings is not None and old_step_name in step_renamings: # It has been renamed
            new_workflow_model_step_iri = crc_workflow_prefix[f"workflow_step_{uuid_for_name(step_renamings[old_step_name], new_workflow_model.creator_user_id)}_for_workflow_model_{new_workflow_model_id}"]

            queries.append((redirect_workflow_instance_steps_query
                            .replace("{old_workflow_model_step_iri}", old_workflow_model_step_iri)
                            .replace("{new_workflow_model_step_iri}", new_workflow_model_step_iri)))

        elif old_step_name not in new_workflow_model.workflow_model_steps: # It has been deleted
            queries.append((delete_workflow_instance_steps_query
                            .replace("{workflow_model_step_iri}", old_workflow_model_step_iri)))

        elif old_workflow_model_id != new_workflow_model_id: # We only need to change the workflow model's base name
            new_workflow_model_step_iri = crc_workflow_prefix[f"workflow_step_{uuid_for_name(old_step_name, new_workflow_model.creator_user_id)}_for_workflow_model_{new_workflow_model_id}"]

            queries.append((redirect_workflow_instance_steps_query
                            .replace("{old_workflow_model_step_iri}", old_workflow_model_step_iri)
                            .replace("{new_workflow_model_step_iri}", new_workflow_model_step_iri)))

    if old_workflow_model_id != new_workflow_model_id: # Redirect the instance as a whole
        queries.append((redirect_workflow_instances_query
                 .replace("{old_workflow_model_iri}", old_workflow_model_iri)
                 .replace("{new_workflow_model_iri}", new_workflow_model_iri)))

    if return_queries:
        return queries
    else:
        updates = [(query, UpdateType.query) for query in queries]
        await rdf_datastore_client.launch_updates(updates, graph_iri=WORKFLOWS_GRAPH_IRI)
        return None


async def delete_workflow_model(workflow_model: WorkflowModel,
                                return_query: bool = False) -> str | None:
    """
    Deletes the workflow model of a given user from the rdf_datastore_client

    Note that workflow instanced will not be redirected unless redirect_workflow_instance_steps is called
    """
    workflow_model_id = uuid_for_name(workflow_model.workflow_model_name, workflow_model.creator_user_id)
    workflow_model_iri = crc_workflow_prefix["workflow_model_" + workflow_model_id]

    query = delete_handover_workflow_model_query.replace("{handover_workflow_model_iri}", workflow_model_iri)
    if return_query:
        return query
    else:
        updates = [(query, UpdateType.query)]
        await rdf_datastore_client.launch_updates(updates, graph_iri=WORKFLOWS_GRAPH_IRI)
        return None


async def overwrite_workflow_model(workflow_model: WorkflowModel,
                                   original_workflow_model: WorkflowModel,
                                   step_renamings: dict[str, str] | None = None):
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
                                                                                               step_renamings,
                                                                                               return_queries=True))]

    await rdf_datastore_client.launch_updates(actions, graph_iri=WORKFLOWS_GRAPH_IRI, delete_files_after_upload=True)


async def get_workflow_instances_of_model(workflow_model: WorkflowModel) -> dict[tuple[str, int], WorkflowInstance]:
    """
        Returns a dict of (Workflow instance name, creator's user id) -> WorkflowInstance assigned to the provided model
    """
    workflow_model_id = uuid_for_name(workflow_model.workflow_model_name, workflow_model.creator_user_id)
    workflow_model_iri = crc_workflow_prefix["workflow_model_" + workflow_model_id]

    # Workflow (instance name, user_id) -> WorkflowInstance
    workflow_instances: dict[tuple[str, int], WorkflowInstance] = dict()
    query = workflow_instance_details_query.replace("{workflow_model_iri}", workflow_model_iri)
    result = await rdf_datastore_client.launch_query(query)
    data = result["results"]["bindings"]
    if not data:
        return {}

    for binding in data:
        workflow_instance_name: str = binding["workflow_instance_name"]["value"]
        step_name: str = binding["step_name"]["value"]
        # The step may not have any assigned objects
        object_id: int | None = None
        if "object_id" in binding:
            object_id: int = int(binding["object_id"]["value"])
        user_id: int = int(binding["user_id"]["value"])

        if (workflow_instance_name, user_id) not in workflow_instances:
            workflow_instance = WorkflowInstance()

            workflow_instance.workflow_instance_name = workflow_instance_name
            workflow_instance.workflow_model_name = workflow_model.workflow_model_name
            workflow_instance.creator_user_id = user_id

            workflow_instances[(workflow_instance_name, user_id)] = workflow_instance

        workflow_instance_to_modify = workflow_instances[(workflow_instance_name, user_id)]


        if step_name not in workflow_instance_to_modify.step_assignments:
            workflow_instance_to_modify.step_assignments[step_name] = []

        if object_id is not None:
            workflow_instance_to_modify.step_assignments[step_name].append(object_id)

    return workflow_instances


async def create_workflow_instance(workflow_instance: WorkflowInstance,
                                   workflow_model: WorkflowModel,
                                   return_file: bool = False) -> str | None:
    """
    Serializes the workflow instance into RDF and stores it
    """

    g = Graph()

    workflow_model_id = uuid_for_name(workflow_model.workflow_model_name, workflow_model.creator_user_id)
    workflow_model_iri = crc_workflow_prefix["workflow_model_" + workflow_model_id]

    workflow_instance_id = uuid_for_name(workflow_instance.workflow_instance_name, workflow_instance.creator_user_id)
    workflow_instance_iri = crc_workflow_prefix["workflow_instance_" + workflow_instance_id]

    # Type
    g.add((workflow_instance_iri, rdf_prefix.type, crc_prefix.HandoverWorkflowModelInstance))

    # Label
    g.add((workflow_instance_iri, crc_prefix.objectName, Literal(workflow_instance.workflow_instance_name, datatype=XSD.string)))

    # Attribution
    g.add((workflow_instance_iri, crc_prefix.creator, crc_user_prefix[str(workflow_instance.creator_user_id)]))

    # Link to workflow model
    g.add((workflow_instance_iri, crc_prefix.handoverWorkflowModelInstanceOf, workflow_model_iri))

    for i, step_name in enumerate(workflow_instance.step_assignments.keys()):
        assignment_iri = crc_workflow_prefix[f"step_assignment_{i}_of_workflow_instance_{workflow_instance_id}"]
        # Type
        g.add((assignment_iri, rdf_prefix.type, crc_prefix.HandoverWorkflowInstanceAssignment))

        # Attribution
        g.add((assignment_iri, crc_prefix.creator, crc_user_prefix[str(workflow_instance.creator_user_id)]))

        # Link to assignment
        g.add((workflow_instance_iri, crc_prefix.hasAssignment, assignment_iri))

        # Link to step
        step_iri = crc_workflow_prefix[f"workflow_step_{uuid_for_name(step_name, workflow_instance.creator_user_id)}_for_workflow_model_{workflow_model_id}"]
        g.add((assignment_iri, crc_prefix.relatesToHandoverWorkflowStep, step_iri))

    # We are guaranteed the same order of keys() and values()
    for i, object_ids in enumerate(workflow_instance.step_assignments.values()):
        assignment_iri = crc_workflow_prefix[f"step_assignment_{i}_of_workflow_instance_{workflow_instance_id}"]

        # Link to sample(s)
        for object_id in object_ids:
            g.add((assignment_iri, crc_prefix.assignedObject, crc_sample_prefix[str(object_id)]))

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
    workflow_instance_id = uuid_for_name(workflow_instance.workflow_instance_name, workflow_instance.creator_user_id)
    workflow_instance_iri = crc_workflow_prefix["workflow_instance_" + workflow_instance_id]

    query = delete_handover_workflow_instance_query.replace("{handover_workflow_instance_iri}", workflow_instance_iri)
    if return_query:
        return query
    else:
        updates = [(query, UpdateType.query)]
        await rdf_datastore_client.launch_updates(updates, workflow_instance_iri)


async def overwrite_workflow_instance(workflow_instance: WorkflowInstance,
                                      workflow_model: WorkflowModel,
                                      original_name: str=None):
    """
    Given an (updated) workflow instance and the workflow model it refers to, deletes the existing, corresponding workflow instance, and stores it again
    """
    actions = []
    if original_name is not None:
        workflow_instance_copy = deepcopy(workflow_instance)
        workflow_instance_copy.workflow_instance_name = original_name
        actions = [(await (delete_workflow_instance(workflow_instance_copy, return_query=True)), UpdateType.query),
                   (await (create_workflow_instance(workflow_instance, workflow_model, return_file=True)), UpdateType.file_upload)]
    else:
        actions.append((await delete_workflow_instance(workflow_instance, return_query=True), UpdateType.query))
        actions.append((await create_workflow_instance(workflow_instance, workflow_model, return_file=True), UpdateType.file_upload))

    await rdf_datastore_client.launch_updates(actions, graph_iri=WORKFLOWS_GRAPH_IRI, delete_files_after_upload=True)


async def get_handover_group_pairs(object_id: int,
                                   cached_object_handover_groups: dict[int, tuple[str, dict[str, str] | None]]) -> dict[str, str]:
    if object_id in cached_object_handover_groups and cached_object_handover_groups[object_id][1] is not None:
        return cached_object_handover_groups[object_id][1] # Return the handover group pairs

    handover_groups: dict[str, str] = {}

    if (await rdf_datastore_client.get_datastore_type()) == "virtuoso":
        # Virtuoso is very finicky when matching ints
        query = get_handover_group_pairs_query.replace('{object_id}', f'"{object_id}"^^xsd:integer')
    else:
        query = get_handover_group_pairs_query.replace('{object_id}', str(object_id))

    result = await rdf_datastore_client.launch_query(query)

    for binding in result["results"]["bindings"]:
        handover_groups[binding["handover_group_1"]["value"]] = binding["handover_group_2"]["value"]

    cached_object_handover_groups[object_id] = (cached_object_handover_groups[object_id][0], # First handover group
                                                handover_groups) # And all handover groups

    return handover_groups


async def get_first_handover_group(object_id: int,
                                   cached_object_handover_groups: dict[int, tuple[str, dict[str, str] | None]]) -> str:
    """
    Returns the IRI of the first handover group the given materials library or sample has
    """
    if object_id in cached_object_handover_groups:
        return cached_object_handover_groups[object_id][0] # Return the first handover group

    if (await rdf_datastore_client.get_datastore_type()) == "virtuoso":
        # Virtuoso is very finicky when matching ints
        query = get_first_handover_group_query.replace('{object_id}', f'"{object_id}"^^xsd:integer')
    else:
        query = get_first_handover_group_query.replace('{object_id}', str(object_id))

    result = await rdf_datastore_client.launch_query(query)
    if len(result["results"]["bindings"]) == 0:
        raise RuntimeError(f"No initial handover group found for sample {object_id}")

    first_handover_group = result["results"]["bindings"][0]["first_handover_group"]["value"]
    cached_object_handover_groups[object_id] = (first_handover_group, None)

    return first_handover_group


def generate_group_shape(workflow_model_step: WorkflowModelStep,
                         target_node: str) -> str:
    """
    Returns a SHACL shape string for validating the workflow model step, assigned to the target node
    """
    step_shape = str(handovers_group_shape_templated)

    # Generate the group shape
    placeholders = {
        '{handovers_group_shape_name}': uuid.uuid4().hex,
        '{target_node}': target_node,
        '{target_projects}': ' '.join([f'project:{project}' for project in workflow_model_step.projects]),
    }
    for key, val in placeholders.items():
        step_shape = step_shape.replace(key, val)

    # Is it restricted to only the specified activities?
    shape_restrict_number_of_activities = ''
    if not workflow_model_step.allow_other_activities:
        replacement = shape_restrict_number_of_activities_templated.replace('{number_of_activities}',
                                                                            f"{len(workflow_model_step.required_activities)}")
        shape_restrict_number_of_activities = replacement
    step_shape = step_shape.replace('{restrict_number_of_activities_shape}',
                                    shape_restrict_number_of_activities.lstrip())

    # Add a restriction for each activity
    activity_shapes = []
    for req_activity in workflow_model_step.required_activities:
        if req_activity in activity_name_to_iri:
            activity_shapes.append(
                shape_require_activity_templated
                .replace('{activity_class}', "<" + activity_name_to_iri[req_activity] + ">")
                .replace("{measurement_name}", req_activity)
            )
        else:
            raise ValueError(f"{req_activity} is not a valid activity type")

    # Terminate the list of SHACL conditions
    if len(activity_shapes) > 0:
        activity_shapes[-1] = activity_shapes[-1][:-1] + "."

    return step_shape.replace('{activity_shapes}', '\n\n'.join(activity_shapes).lstrip())


@dataclass
class StepValidationInfo:
    """
    Convenience class for a Workflow model step and its validation status wrt. an
    object and one of its handover groups
    """
    workflow_model_step: WorkflowModelStep = None
    object_id: int = 0 # ML / Sample ID
    target_node: str = "" # Handover group IRI


@dataclass
class StepToValidate:
    """
    Convenience class for a Workflow model step and its validation status wrt. an
    object and one of its handover groups, alongside the generated SHACL shape for it
    """
    step_information: StepValidationInfo = field(default_factory=StepValidationInfo)
    shacl_shape: str = "" # Syntactically valid SHACL shape as a string


async def generate_SHACL_shapes_for_workflow(workflow_model: WorkflowModel,
                                             workflow_instance: WorkflowInstance) -> tuple[list[StepToValidate], list[StepValidationInfo]]:
    """
    Returns a list of steps to validate for the workflow model, following the sample assignments of the workflow instance,
    and a list of references to workflow model steps for which an object did not have a corresponding handover group for it
    (i.e., more steps than handover groups).

    It will iteratively follow the steps chain and generating as many shapes for a step as there are samples assigned to it,
    without checking for loops. If a sample has less handover groups than steps, the remaining SHACL shapes will not be
    generated and the validation on that branch will stop.

    Only the SHACL shape string is needed for the validation, the rest of the entries are for traceability / debugging
    """

    # To simplify the algorithm, we employ two stacks
    #
    # Stack of steps to parse. The algorithm will iteratively extract
    # entries from this stack, and:
    #   - Generate their corresponding SHACL shapes and insert them into steps_to_validate
    #   - Insert in this list the following handover workflow model step to parse after the current step (one for every object they are assigned to).
    #     If any of the objects does not contain more handover groups to continue the validation, nothing will be inserted for that object in this list
    @dataclass
    class StepToParse:
        step_information: StepValidationInfo = field(default_factory=StepValidationInfo)
        handover_group_pairs: dict[str, str] = field(default_factory=dict)

    steps_to_parse: list[StepToParse] = []
    #
    # Stack of steps to validate, containing all the individual validation jobs that must be performed to completely validate the handover workflow
    # model with its corresponding handover workflow model instance
    steps_to_validate: list[StepToValidate] = []

    # Since we also allow arbitrary objects along the handover workflow models that may reappear at any time at any branch,
    # we also cache their information globally
    # dict of Object ID -> tuple of (first_handover_group_iri, dict of handover_group_iri -> next_handover_group_iri)
    cached_object_handover_groups: dict[int, tuple[str, dict[str, str] | None]] = {}

    # List of (handover workflow model step, object ID, target node), containing
    # references to the *first* workflow model steps for which the given object ID
    # did not have a target node for (i.e., not enough handover groups), alongside
    # the last handover group that exists
    steps_with_no_target_node: list[StepValidationInfo] = []

    # Start validating from the initial step, for every sample that is assigned to it
    initial_step = workflow_model.workflow_model_steps[workflow_model.workflow_model_options.initial_step_name]
    for object_id in workflow_instance.step_assignments[workflow_model.workflow_model_options.initial_step_name]:
        first_handover_group = await get_first_handover_group(object_id, cached_object_handover_groups)
        handover_group_pairs = await get_handover_group_pairs(object_id, cached_object_handover_groups)

        if first_handover_group is not None:  # Else, stop checking. If the object was generated via mappings, there is always an initial handover group
            step_to_parse = StepToParse()
            step_to_parse.step_information.workflow_model_step = initial_step
            step_to_parse.step_information.object_id = object_id
            step_to_parse.step_information.target_node = first_handover_group
            step_to_parse.handover_group_pairs = handover_group_pairs
            steps_to_parse.append(step_to_parse)

    while len(steps_to_parse) > 0:
        step_to_parse: StepToParse = steps_to_parse.pop()

        step_to_validate: StepToValidate = StepToValidate()
        step_to_validate.step_information = step_to_parse.step_information
        step_to_validate.shacl_shape = generate_group_shape(step_to_parse.step_information.workflow_model_step, step_to_validate.step_information.target_node)
        steps_to_validate.append(step_to_validate)

        current_step_information = step_to_parse.step_information
        for next_step_name in current_step_information.workflow_model_step.next_steps:
            next_step = workflow_model.workflow_model_steps[next_step_name]
            current_step_object_ids =  workflow_instance.step_assignments[current_step_information.workflow_model_step.step_name]
            next_step_object_ids = workflow_instance.step_assignments[next_step_name]

            for next_step_object_id in next_step_object_ids:
                # Continue the validation from its next handover group, if it exists
                if current_step_information.object_id == next_step_object_id:
                    new_target_node = step_to_parse.handover_group_pairs.get(current_step_information.target_node)
                    if new_target_node is not None:
                        new_step_to_parse = StepToParse()
                        new_step_to_parse.step_information.workflow_model_step = next_step # New step
                        new_step_to_parse.step_information.object_id = current_step_information.object_id # Same object
                        new_step_to_parse.step_information.target_node = new_target_node # New target node
                        new_step_to_parse.handover_group_pairs = step_to_parse.handover_group_pairs # Keep the cache



                        steps_to_parse.append(new_step_to_parse)
                    else:
                        # There are no further handover groups - we can stop validating this branch
                        #
                        # We save a reference to this to report it
                        step_with_no_target_node = StepValidationInfo()
                        step_with_no_target_node.workflow_model_step = next_step
                        step_with_no_target_node.target_node = step_to_parse.step_information.target_node
                        step_with_no_target_node.object_id = current_step_information.object_id
                        steps_with_no_target_node.append(step_with_no_target_node)

                # Continue the validation from any new objects that were not in the current step
                #
                # We ensure no duplicates are added when checking the same current step under any
                # of its other assigned objects
                else :
                    # For every object in next step that is not in the current step
                    for new_object_id in [obj_id for obj_id in next_step_object_ids if obj_id not in current_step_object_ids]:
                        new_step_to_parse = StepToParse()
                        new_step_to_parse.step_information.workflow_model_step = next_step  # New step
                        new_step_to_parse.step_information.object_id = new_object_id  # New object
                        new_step_to_parse.step_information.target_node = await get_first_handover_group(new_object_id, cached_object_handover_groups)
                        new_step_to_parse.handover_group_pairs = await get_handover_group_pairs(new_object_id, cached_object_handover_groups)

                        if new_step_to_parse not in steps_to_parse:
                            steps_to_parse.append(new_step_to_parse)

    return steps_to_validate, steps_with_no_target_node


async def get_data_graph_for_object_id(object_id: int) -> tuple[int, Graph]:
    """
    Generate a .ttl file for pySHACL by querying for the handover groups, handovers and activities of the given sample

    This circumvents pySHACL's lack of support for named graphs via SPARQL
    """
    if (await rdf_datastore_client.get_datastore_type()) == "virtuoso":
        # Virtuoso is very finicky when matching ints
        query = get_handovers_and_activities_for_sample_query.replace('{object_id}', f'"{object_id}"^^xsd:integer')
    else:
        query = get_handovers_and_activities_for_sample_query.replace('{object_id}', str(object_id))

    result = await rdf_datastore_client.launch_query(query)

    bindings = result["results"]["bindings"]
    if len(bindings) == 0:
        raise RuntimeError(f"No data found for sample {object_id}")

    g = Graph()
    for row in bindings:
        s = URIRef(row["s"]["value"])
        p = URIRef(row["p"]["value"])
        o_value = row["o"]["value"]

        # Determine if object is a URI or literal
        if row["o"]["type"] == "uri":
            o = URIRef(o_value)
        else:
            o = Literal(o_value)
        g.add((s, p, o))

    return object_id, g


async def generate_data_graphs_for_workfow_steps(steps_to_validate: list[StepToValidate]):
    data_graphs = dict()

    object_ids_to_fetch = {
        step.step_information.object_id
        for step in steps_to_validate
    }
    tasks = [get_data_graph_for_object_id(obj_id) for obj_id in object_ids_to_fetch]
    results = await asyncio.gather(*tasks)

    for object_id, data_graph in results:
        data_graphs[object_id] = data_graph

    return data_graphs


@dataclass
class ValidationResult:
    step_to_validate: StepToValidate = field(default_factory=StepToValidate)
    conforms: bool = False
    pyshacl_output: str = ""

def validate_workflow_model_step(data_graph, step_to_validate: StepToValidate, results: list[ValidationResult]):
    shacl_graph = Graph()
    shacl_graph.parse(data=step_to_validate.shacl_shape, format="turtle")

    conforms, results_graph, pyshacl_output = validate(data_graph=data_graph,
                                                       shacl_graph=shacl_graph,
                                                       ont_graph=ont_graph,
                                                       inference=None,  # 'rdfs',
                                                       abort_on_first=False,
                                                       allow_infos=False,
                                                       allow_warnings=False,
                                                       meta_shacl=False,
                                                       advanced=False,
                                                       js=False,
                                                       # sparql_mode=True, # TODO check it out, could it be faster this way?
                                                       debug=False)
    validation_result = ValidationResult()
    validation_result.step_to_validate = step_to_validate
    validation_result.conforms = conforms
    validation_result.pyshacl_output = pyshacl_output
    results.append(validation_result)


def validate_SHACL_rules(steps_to_validate: list[StepToValidate], data_graphs) -> list[ValidationResult]:
    results: list[ValidationResult] = []

    # (workflow_step, workflow_step_name, object_id, target_node, shacl_rules)
    for step_to_validate in steps_to_validate:
        validate_workflow_model_step(data_graphs[step_to_validate.step_information.object_id],
                                     step_to_validate,
                                     results)

    return results


def validation_task_wrapper(data_graphs, step_to_validate: StepToValidate) -> ValidationResult :
    local_results: list[ValidationResult] = []

    validate_workflow_model_step(data_graphs[step_to_validate.step_information.object_id],
                                 step_to_validate,
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
            ValidationStatus.Warning: "All steps were validated successfully, but some handover workflows were incomplete.",
            ValidationStatus.Error: "One or more steps failed validation."
        }
        return descriptions[self]

async def is_workflow_instance_valid(workflow_model, workflow_instance) -> ValidationStatus:
    """
    Returns a ValidationStatus of for the provided workflow model against its instance's assignments

    generate_SHACL_shapes_for_workflow and validate_SHACL_rules can be run separately if more details are needed (e.g., which
    steps are valid and which aren't, and the reasons why)

    Optimized for parallelism (or, at least, for python's "parallelism")
    """
    steps_to_validate, steps_with_no_target_node = await generate_SHACL_shapes_for_workflow(workflow_model, workflow_instance)
    data_graphs = await generate_data_graphs_for_workfow_steps(steps_to_validate)

    with ProcessPoolExecutor() as executor:
        tasks = []

        for step_to_validate in steps_to_validate:
            task = asyncio.get_running_loop().run_in_executor(
                executor,
                validation_task_wrapper,
                data_graphs,
                step_to_validate
            )
            tasks.append(task)

        results: list[ValidationResult] = await asyncio.gather(*tasks)

    all_steps_conform = all(result.conforms for result in results)
    if all_steps_conform and len(steps_with_no_target_node) == 0:
        return ValidationStatus.Valid
    elif all_steps_conform:
        return ValidationStatus.Warning
    else:
        return ValidationStatus.Error