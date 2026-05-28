from dataclasses import dataclass, field

from nicegui import app
from nicegui.elements.row import Row
from nicegui.elements.column import Column
from nicegui.elements.input import Input
from rdflib import URIRef

from handover_workflows_validation_webui.common_functions import get_sample_object_id_of_handover_group_iri, get_handover_group_iri_of_sample_object_id
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent
from workflows_validation.workflows_validator import WorkflowInstance


@dataclass
class WorkflowInstancePageState:
    """
    State dataclass containing references to UI elements and contents of pages in this module.
    Initialized by accessing the main page and thus local to the user.
    """
    validation_paths_row: Row = None
    graph_component: CytoscapeComponent = None
    graph_component_column: Column = None
    node_controls_column: Column = None
    graph_controls_column: Column = None
    selected_node: str = None
    workflow_instance_name_input: Input = None
    changes_are_saved: bool = True
    original_workflow_instance: WorkflowInstance = None
    sample_object_id_to_hnd_group_iri: dict[int, URIRef] = field(default_factory=dict)
    hnd_group_iri_to_sample_object_id: dict[URIRef, int] = field(default_factory=dict)

    def undo_workflow_instance_changes(self):
        if self.original_workflow_instance is not None:
            app.storage.tab['current_workflow_instance'] = self.original_workflow_instance

    async def populate_sample_to_iri_correspondences(self):
        for step_assignment in app.storage.tab['current_workflow_instance'].step_assignments.values():
            for assigned_entity in step_assignment.assigned_entities:
                if assigned_entity not in self.hnd_group_iri_to_sample_object_id:
                    sample_internal_id = await get_sample_object_id_of_handover_group_iri(assigned_entity)
                    self.hnd_group_iri_to_sample_object_id[assigned_entity] = sample_internal_id
                    self.sample_object_id_to_hnd_group_iri[sample_internal_id] = assigned_entity

    async def add_sample_object_id_to_caches(self, internal_id: int):
        """
        Attempts to add a sample given by its internal object ID to the caches. If the object ID does not exist, nothing will be added.
        """
        if internal_id not in self.sample_object_id_to_hnd_group_iri:
            handover_group_iri = await get_handover_group_iri_of_sample_object_id(internal_id)
            if handover_group_iri != URIRef(""):
                self.hnd_group_iri_to_sample_object_id[handover_group_iri] = internal_id
                self.sample_object_id_to_hnd_group_iri[internal_id] = handover_group_iri
