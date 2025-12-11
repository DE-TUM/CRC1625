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
bootstrap them.

Note: no user-facing UI is available yet. The UI is meant to hook to this API and abstract
users from the actual representation of the workflows.
"""

import os
import uuid
from dataclasses import dataclass, field

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
get_next_handover_group_query = prefixes + open(os.path.join(module_dir, 'queries/get_next_handover_group.sparql'), 'r').read()
get_handovers_and_activities_for_sample_query = prefixes + open(os.path.join(module_dir, 'queries/get_handovers_and_activities_for_sample.sparql'), 'r').read()
delete_handover_workflow_model_query = prefixes + open(os.path.join(module_dir, 'queries/delete_handover_workflow_model.sparql'), 'r').read()
delete_handover_workflow_instance_query = prefixes + open(os.path.join(module_dir, 'queries/delete_handover_workflow_instance.sparql'), 'r').read()
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

activity_to_iri_prefixed = {
    "Annealing": ":AnnealingProcess",
    "APT": ":APTProcess",
    "Bandgap": ":BandgapProcess",
    "FIM": ":FIMProcess",
    "LEIS": ":LEISProcess",
    "Photo": ":PhotoProcess",
    "PSM": ":PSMProcess",
    "Report": ":Reportrocess",
    "Resistance": ":ResistanceProcess",
    "SDC": ":SDCProcess",
    "SECCM": ":SECCMProcess",
    "SEM": ":SEMProcess",
    "TEM": ":TEMProcess",
    "Thickness": ":ThicknessProcess",
    "XPS": ":XPSProcess",
    "XRD": ":XRDProcess",
    "EDX": ":EDXMicroscopyProcess",
    "Others": "pmdco:AnalysingProcess",
}

activity_to_iri = {
    "Annealing": str(crc_prefix.AnnealingProcess),
    "APT": str(crc_prefix.APTProcess),
    "Bandgap": str(crc_prefix.BandgapProcess),
    "FIM": str(crc_prefix.FIMProcess),
    "LEIS": str(crc_prefix.LEISProcess),
    "Photo": str(crc_prefix.PhotoProcess),
    "PSM": str(crc_prefix.PSMProcess),
    "Report": str(crc_prefix.Reportrocess),
    "Resistance": str(crc_prefix.ResistanceProcess),
    "SDC": str(crc_prefix.SDCProcess),
    "SECCM": str(crc_prefix.SECCMProcess),
    "SEM": str(crc_prefix.SEMProcess),
    "TEM": str(crc_prefix.TEMProcess),
    "Thickness": str(crc_prefix.ThicknessProcess),
    "XPS": str(crc_prefix.XPSProcess),
    "XRD": str(crc_prefix.XRDProcess),
    "EDX": str(crc_prefix.EDXMicroscopyProcess),
    "Others": str(pmdco_prefix.AnalysingProcess),
}

iri_to_activity = {v: k for k, v in activity_to_iri.items()}


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
    next_steps: set[str] = field(default_factory=set)

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
    workflow_model_steps: dict[str, WorkflowModelStep] = field(default_factory=dict)


@dataclass
class WorkflowInstance:
    workflow_instance_name: str = ""

    """
    Workflow model name it refers to. Note that this will change when workflow model names stop 
    being unique identifiers
    """
    workflow_model_name: str = ""

    """
    Dict of Step name -> List of sample IDs
    
    The step names must refer to the step names contained in the workflow model it is specifying
    """
    step_assignments: dict[str, list[int]] = field(default_factory=dict)


workflow_model_iri_to_config = {
    str(rdfs_prefix.label): "workflow_model_name",
    str(pmdco_prefix.subordinateProcess): "initial_step",
    str(crc_prefix.allowIntermediateHandoverGroups): "allow_intermediate_handover_groups"
}

workflow_model_step_iri_to_config = {
    str(crc_prefix.isHandoverWorkflowStepEnabled): "enabled",
    str(pmdco_prefix.nextProcess): "next_steps",
    str(rdfs_prefix.comment): "step_description",
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


async def read_workflow_model(workflow_model_name: str, user_id: int) -> None | WorkflowModel:
    """
    Returns the WorkflowModel identified by the provided name
    """

    workflow_model = WorkflowModel()

    workflow_model_id = uuid_for_name(workflow_model_name, user_id)
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

        if p == 'http://www.w3.org/2000/01/rdf-schema#label':
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
                elif p != 'http://www.w3.org/2000/01/rdf-schema#label': # We already set it, and the label is not an option
                    workflow_model.workflow_model_options.set_option(workflow_model_iri_to_config[p], o)

        else:  # It's a step
            step_name = labels_dict[s]
            if step_name not in workflow_model.workflow_model_steps:
                workflow_model.workflow_model_steps[step_name] = WorkflowModelStep()

            workflow_step = workflow_model.workflow_model_steps[step_name]
            if p in workflow_model_step_iri_to_config:
                match workflow_model_step_iri_to_config[p]:
                    case "next_steps":
                        workflow_step.next_steps.add(labels_dict[o])
                    case "projects":
                        workflow_step.projects.append(o.rsplit("/", 1)[-1])
                    case "required_activities":
                        workflow_step.required_activities.append(iri_to_activity[await get_activity_type(o)])
                    case _:
                        workflow_step.set_option(workflow_model_step_iri_to_config[p], o)

    return workflow_model


async def store_workflow_model(workflow_model: WorkflowModel,
                               user_id: int,
                               return_file: bool = False):
    """
    Serializes the workflow model into RDF and stores it
    """
    g = Graph()
    name_to_uid = dict()

    workflow_model_id = uuid_for_name(workflow_model.workflow_model_name, user_id)
    workflow_model_iri = crc_workflow_prefix["workflow_model_" + workflow_model_id]

    # Type
    g.add((workflow_model_iri, rdf_prefix.type, crc_prefix.HandoverWorkflowModel))

    # Label
    g.add((workflow_model_iri, rdfs_prefix.label, Literal(workflow_model.workflow_model_name, datatype=XSD.string)))

    # Attribution
    g.add((workflow_model_iri, prov_prefix.wasAttributedTo, crc_user_prefix[str(user_id)]))

    # Settings
    g.add((workflow_model_iri, crc_prefix.allowIntermediateHandoverGroups,
           Literal(workflow_model.workflow_model_options.allow_intermediate_handover_groups, datatype=XSD.boolean)))

    step_name: str
    step_options: WorkflowModelStep
    for step_name, step in workflow_model.workflow_model_steps.items():
        if step_name not in name_to_uid:
            name_to_uid[step_name] = uuid_for_name(step_name, user_id)

        step_iri = crc_workflow_prefix[f"workflow_step_{name_to_uid[step_name]}_for_workflow_model_{workflow_model_id}"]

        if step_name == workflow_model.workflow_model_options.initial_step_name:
            # Link to first step
            g.add((workflow_model_iri, pmdco_prefix.subordinateProcess, step_iri))

        # Type
        g.add((step_iri, rdf_prefix.type, crc_prefix.HandoverWorkflowModelStep))

        # Label
        g.add((step_iri, rdfs_prefix.label, Literal(step_name, datatype=XSD.string)))

        # Attribution
        g.add((step_iri, prov_prefix.wasAttributedTo, crc_user_prefix[str(user_id)]))

        # Comment
        g.add((step_iri, rdfs_prefix.comment, Literal(step.step_description, datatype=XSD.string)))

        # Enabled
        g.add((step_iri, crc_prefix.isHandoverWorkflowStepEnabled, Literal(step.enabled, datatype=XSD.boolean)))

        # Projects
        for project_name in step.projects:
            g.add((step_iri, crc_prefix.allowedProject, crc_project_prefix[project_name]))

        # Next steps
        for next_step_name in step.next_steps:
            if next_step_name not in name_to_uid:
                name_to_uid[next_step_name] = uuid_for_name(next_step_name, user_id)

            next_step_iri = crc_workflow_prefix[f"workflow_step_{name_to_uid[next_step_name]}_for_workflow_model_{workflow_model_id}"]
            g.add((step_iri, pmdco_prefix.nextProcess, next_step_iri))

        # Required activities
        for required_activity in step.required_activities:
            activity_iri = crc_prefix[f"{required_activity}_activity_for_workflow_step_{name_to_uid[step_name]}"]

            # Requirement
            g.add((step_iri, crc_prefix.requiresActivity, activity_iri))

            # Type(s) of the activity
            if required_activity == "Others":
                activity_type_iri = pmdco_prefix[activity_to_iri_prefixed[required_activity].replace("pmdco:", "")]
            else:
                activity_type_iri = crc_prefix[activity_to_iri_prefixed[required_activity].replace(":", "")]

            g.add((activity_iri, rdf_prefix.type, activity_type_iri))
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


async def delete_workflow_model(workflow_model: WorkflowModel,
                          user_id: int,
                          return_query: bool = False):
    """
    Deletes the workflow model of a given user from the rdf_datastore_client
    """
    workflow_model_id = uuid_for_name(workflow_model.workflow_model_name, user_id)
    workflow_model_iri = crc_workflow_prefix["workflow_model_" + workflow_model_id]

    query = delete_handover_workflow_model_query.replace("{handover_workflow_model_iri}", workflow_model_iri)
    if return_query:
        return query
    else:
        updates = [(query, UpdateType.query)]
        await rdf_datastore_client.launch_updates(updates, graph_iri=WORKFLOWS_GRAPH_IRI)


async def clean_workflow_instance_steps(workflow_model: WorkflowModel,
                                        user_id: int,
                                        return_query: bool = False):
    workflow_instances = await get_workflow_instances_of_model(workflow_model.workflow_model_name,
                                                               user_id)

    for (workflow_instance_name, user_id) in workflow_instances.keys():
        workflow_instance_id = uuid_for_name(workflow_instance_name, user_id)
        workflow_instance_iri = crc_workflow_prefix["workflow_instance_" + workflow_instance_id]

        query = clean_handover_workflow_instance_steps_query.replace("{handover_workflow_instance_iri}", workflow_instance_iri)
        if return_query:
            return query
        else:
            updates = [(query, UpdateType.query)]
            await rdf_datastore_client.launch_updates(updates, graph_iri=WORKFLOWS_GRAPH_IRI)


async def overwrite_workflow_model(workflow_model: WorkflowModel,
                             user_id: int):
    """
    Deletes the workflow model of a given user, and stores it again
    """
    actions = [(await (delete_workflow_model(workflow_model, user_id, return_query=True)), UpdateType.query),
                (await (store_workflow_model(workflow_model, user_id, return_file=True)), UpdateType.file_upload)]
    # actions.append((clean_workflow_instance_steps(workflow_model, user_id, rdf_datastore_client, return_query=True), UpdateType.query))

    # TODO FIX
    await rdf_datastore_client.launch_updates(actions, graph_iri=WORKFLOWS_GRAPH_IRI, delete_files_after_upload=True)


async def get_workflow_instances_of_model(workflow_model_name: str,
                                          user_id: int) -> dict[tuple[str, int], WorkflowInstance]:
    """
    Returns a dict of (Workflow instance name, creator's user id) -> WorkflowInstance assigned to the provided model
    """

    workflow_model_id = uuid_for_name(workflow_model_name, user_id)
    workflow_model_iri = crc_workflow_prefix["workflow_model_" + workflow_model_id]

    # Workflow (instance name, user_id) -> WorkflowInstance
    workflow_instances: dict[tuple[str, int], WorkflowInstance] = dict()
    query = workflow_instance_details_query.replace("{workflow_model_iri}", workflow_model_iri)
    result = await rdf_datastore_client.launch_query(query)
    data = result["results"]["bindings"]
    if not data:
        return dict()

    for binding in data:
        workflow_instance_name: str = binding["workflow_instance_name"]["value"]
        step_name: str = binding["step_name"]["value"]
        object_id: int = int(binding["object_id"]["value"])
        user_id: int = int(binding["user_id"]["value"])

        if (workflow_instance_name, user_id) not in workflow_instances:
            workflow_instance = WorkflowInstance()

            workflow_instance.workflow_instance_name = workflow_instance_name
            workflow_instance.workflow_model_name = workflow_model_name

            workflow_instances[(workflow_instance_name, user_id)] = workflow_instance

        workflow_instance_to_modify = workflow_instances[(workflow_instance_name, user_id)]

        if step_name not in workflow_instance_to_modify.step_assignments:
            workflow_instance_to_modify.step_assignments[step_name] = []

        workflow_instance_to_modify.step_assignments[step_name].append(object_id)

    return workflow_instances


async def create_workflow_instance(workflow_instance: WorkflowInstance,
                                   user_id: int,
                                   return_file: bool = False):
    """
    Serializes the workflow instance into RDF and stores it
    """

    g = Graph()

    workflow_model_id = uuid_for_name(workflow_instance.workflow_model_name, user_id)
    workflow_model_iri = crc_workflow_prefix["workflow_model_" + workflow_model_id]

    workflow_instance_id = uuid_for_name(workflow_instance.workflow_instance_name, user_id)
    workflow_instance_iri = crc_workflow_prefix["workflow_instance_" + workflow_instance_id]

    # Type
    g.add((workflow_instance_iri, rdf_prefix.type, crc_prefix.HandoverWorkflowModelInstance))

    # Label
    g.add((workflow_instance_iri, rdfs_prefix.label, Literal(workflow_instance.workflow_instance_name, datatype=XSD.string)))

    # Attribution
    g.add((workflow_instance_iri, prov_prefix.wasAttributedTo, crc_user_prefix[str(user_id)]))

    # Link to workflow model
    g.add((workflow_instance_iri, crc_prefix.handoverWorkflowModelInstanceOf, workflow_model_iri))

    for i, step_name in enumerate(workflow_instance.step_assignments.keys()):
        assignment_iri = crc_workflow_prefix[f"step_assignment_{i}_of_workflow_instance_{workflow_instance_id}"]
        # Type
        g.add((assignment_iri, rdf_prefix.type, crc_prefix.HandoverWorkflowInstanceAssignment))

        # Attribution
        g.add((assignment_iri, prov_prefix.wasAttributedTo, crc_user_prefix[str(user_id)]))

        # Link to assignment
        g.add((workflow_instance_iri, crc_prefix.hasAssignment, assignment_iri))

        # Link to step
        step_iri = crc_workflow_prefix[f"workflow_step_{uuid_for_name(step_name, user_id)}_for_workflow_model_{workflow_model_id}"]
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


async def delete_workflow_instance(workflow_instance: WorkflowInstance,
                                   user_id: int,
                                   return_query: bool = False):
    """
    Deletes the workflow instance corresponding to the provided one, and stores it again
    """
    workflow_instance_id = uuid_for_name(workflow_instance.workflow_instance_name, user_id)
    workflow_instance_iri = crc_workflow_prefix["workflow_instance_" + workflow_instance_id]

    query = delete_handover_workflow_instance_query.replace("{handover_workflow_instance_iri}", workflow_instance_iri)
    if return_query:
        return query
    else:
        updates = [(query, UpdateType.query)]
        await rdf_datastore_client.launch_updates(updates, workflow_instance_iri)


async def overwrite_workflow_instance(workflow_instance: WorkflowInstance, user_id: int):
    """
    Deletes the workflow model corresponding to the provided one, and stores it again
    """
    actions = []
    actions.append((await delete_workflow_instance(workflow_instance, user_id, return_query=True), UpdateType.query))
    actions.append((await create_workflow_instance(workflow_instance, user_id, return_file=True), UpdateType.file_upload))

    await rdf_datastore_client.launch_updates(actions, graph_iri=WORKFLOWS_GRAPH_IRI, delete_files_after_upload=True)


async def get_first_handover_group(object_id: int) -> str:
    """
    Returns the IRI of the first handover group the given materials library or sample has
    """
    if (await rdf_datastore_client.get_datastore_type()) == "virtuoso":
        # Virtuoso is very finicky when matching ints
        query = get_first_handover_group_query.replace('{object_id}', f'"{object_id}"^^xsd:integer')
    else:
        query = get_first_handover_group_query.replace('{object_id}', str(object_id))

    result = await rdf_datastore_client.launch_query(query)
    if len(result["results"]["bindings"]) == 0:
        raise RuntimeError(f"No initial handover group found for sample {object_id}")

    return result["results"]["bindings"][0]["first_handover_group"]["value"]


async def get_next_handover_group(current_handover_group_iri: str) -> str | None:
    """
    Returns the next handover group's IRI after the provided one if exists, else None
    """
    query = get_next_handover_group_query.replace('{current_handover_group}', current_handover_group_iri)
    result = await rdf_datastore_client.launch_query(query)
    if len(result["results"]["bindings"]) == 0:
        return None

    return result["results"]["bindings"][0]["next_handover_group"]["value"]


def generate_group_shape(workflow_model_step: WorkflowModelStep, target_node: str) -> str:
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
        if req_activity in activity_to_iri_prefixed:
            activity_shapes.append(
                shape_require_activity_templated
                .replace('{activity_class}', activity_to_iri_prefixed[req_activity])
                .replace("{measurement_name}", req_activity)
            )
        else:
            raise ValueError(f"{req_activity} is not a valid activity type")

    # Terminate the list of SHACL conditions
    if len(activity_shapes) > 0:
        activity_shapes[-1] = activity_shapes[-1][:-1] + "."

    return step_shape.replace('{activity_shapes}', '\n\n'.join(activity_shapes).lstrip())


async def get_next_validation_steps(workflow_model: WorkflowModel,
                              workflow_instance: WorkflowInstance,
                              current_workflow_step: WorkflowModelStep,
                              current_object_id: int,
                              target_node: str) -> list[tuple[WorkflowModelStep, str, int, str]]:
    """
    Returns a list of (WorkflowModelStep, next_step_name, object_id, target node IRI) tuples given the current
    step, sample ID and target node

    Depending on the configuration, multiple entries may be generated for the same step on different samples

    If the list is empty, no more steps need to be executed
    """
    if len(current_workflow_step.next_steps) > 0:
        next_step_name = next(iter(current_workflow_step.next_steps))  # TODO implement OR of n>1 steps (or do it via SHACL itself?)

        # List of (step, object_id, target_node)
        next_steps: list[(WorkflowModelStep, str, str)] = []

        for new_object_id in workflow_instance.step_assignments[next_step_name]:
            if current_object_id == new_object_id:  # The next target node is the next handover group for the target node
                new_target_node = await get_next_handover_group(target_node)
                if new_target_node is not None:  # Else, we stop checking TODO handle better
                    next_steps.append((workflow_model.workflow_model_steps[next_step_name], next_step_name, current_object_id, new_target_node))
            else:  # We have a new sample, so we must continue the workflow from the new sample's *first* handover group
                next_steps.append((workflow_model.workflow_model_steps[next_step_name], next_step_name, new_object_id,
                                   await get_first_handover_group(new_object_id)))

        return next_steps
    else:
        return []


async def generate_SHACL_shapes_for_workflow(workflow_model: WorkflowModel,
                                             workflow_instance: WorkflowInstance) -> list[tuple[WorkflowModelStep, str, int, str, str]]:
    """
    Returns a list of (WorkflowModelStep, workflow step name, sample id, target node IRI, SHACL shape string) for the
    workflow model, following the sample assignments of the workflow instance.

    It will iteratively follow the steps chain and generating as many shapes for a step as there are samples assigned to it,
    without checking for loops. If a sample has less handover groups than steps, the remaining SHACL shapes will not be
    generated and the validation on that branch will stop.

    Only the SHACL shape string is needed for the validation, the rest of the entries are for traceability / debugging

    :returns: A stack of (handover workflow model step, handover workflow model step name, object ID, target node, SHACL shape),
    containing all the individual validation jobs that must be performed to completely validate the handover workflow
    model with its corresponding handover workflow model instance
    """

    # To simplify the algorithm, we employ two stacks
    #
    # Stack of (handover workflow model step, handover workflow model step name, object ID, target node), containing workflow model steps and the target
    # node (ML or sample) they must validate. The algorithm will iteratively extract entries from this stack, and:
    #   - Generate their corresponding SHACL shapes and insert them into steps_to_validate
    #   - Insert in this list the following handover workflow model steps after the current step (one for every object they are assigned to).
    #     If any of the objects does not contain more handover groups to continue the validation, nothing will be inserted for that object
    steps_to_parse: list[tuple[WorkflowModelStep, str, int, str]] = []
    #
    # Stack of (handover workflow model step, handover workflow model step name, object ID, target node, SHACL shape),
    # containing all the individual validation jobs that must be performed to completely validate the handover workflow
    # model with its corresponding handover workflow model instance
    steps_to_validate: list[tuple[WorkflowModelStep, str, int, str, str]] = []

    # Start validating from the initial step, for every sample that is assigned to it
    for object_id in workflow_instance.step_assignments[workflow_model.workflow_model_options.initial_step_name]:
        first_handover_group = await get_first_handover_group(object_id)
        if first_handover_group is not None:  # Else, stop checking TODO handle better?
            initial_step = workflow_model.workflow_model_steps[workflow_model.workflow_model_options.initial_step_name]
            steps_to_parse.append((initial_step, workflow_model.workflow_model_options.initial_step_name, object_id, first_handover_group))

    while len(steps_to_parse) > 0:
        (workflow_step, workflow_step_name, object_id, target_node) = steps_to_parse.pop()

        steps_to_validate.append((workflow_step, workflow_step_name, object_id, target_node, generate_group_shape(workflow_step, target_node)))
        steps_to_parse.extend(await get_next_validation_steps(workflow_model, workflow_instance, workflow_step, object_id, target_node))

    return steps_to_validate


async def get_ttl_for_sample(object_id: str):
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

    path_to_ttl = f"{uuid.uuid4().hex}_{object_id}_data.ttl"
    g.serialize(destination=path_to_ttl, format="turtle")
    return path_to_ttl


async def validate_SHACL_rules(steps_to_validate: list[(WorkflowModelStep, str, str)]) -> list[(WorkflowModelStep, int, str, str, bool, str)]:
    results: list[(WorkflowModelStep, int, str, str, bool, str)] = []

    sample_ttls = dict()

    for (workflow_step, workflow_step_name, object_id, target_node, shacl_rules) in steps_to_validate:
        shacl_graph = Graph()
        shacl_graph.parse(data=shacl_rules, format="turtle")

        if object_id not in sample_ttls:
            sample_ttls[object_id] = await get_ttl_for_sample(object_id)

        conforms, results_graph, results_text = validate(data_graph=sample_ttls[object_id],
                                                         shacl_graph=shacl_graph,
                                                         ont_graph=ont_graph,
                                                         inference=None,  # 'rdfs',
                                                         abort_on_first=False,
                                                         allow_infos=False,
                                                         allow_warnings=False,
                                                         meta_shacl=False,
                                                         advanced=False,
                                                         js=False,
                                                         debug=False)
        results.append((workflow_step, workflow_step_name, object_id, target_node, shacl_rules, conforms, results_text))

    for path_to_ttl in sample_ttls.values():
        os.remove(path_to_ttl)

    return results


async def is_workflow_instance_valid(workflow_model, workflow_instance) -> bool:
    """
    Returns True if the workflow model and its model instance match with the handover workflow they refer to, False otherwise

    generate_SHACL_shapes_for_workflow and validate_SHACL_rules can be run separately if more details are needed (e.g., which
    steps are valid and which aren't, and the reasons why)
    """
    steps_to_validate = await generate_SHACL_shapes_for_workflow(workflow_model, workflow_instance)
    results = await validate_SHACL_rules(steps_to_validate)

    return all(result[5] for result in results)
