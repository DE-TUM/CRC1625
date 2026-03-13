from copy import deepcopy
from dataclasses import dataclass, field

from nicegui import app
from nicegui.elements.column import Column
from nicegui.elements.input import Input

from handover_workflows_validation.handover_workflows_validation import WorkflowInstance
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent


@dataclass
class WorkflowInstancePageState:
    """
    State dataclass containing references to UI elements and contents of pages in this module.
    Initialized by accessing the main page and thus local to the user.
    """
    graph_component: CytoscapeComponent = None
    graph_component_column: Column = None
    node_controls_column: Column = None
    graph_controls_column: Column = None
    selected_node: str = None
    workflow_instance_name_input: Input = None
    changes_are_saved: bool = True
    original_workflow_instance: WorkflowInstance = None
    existing_objects: set[int] = field(default_factory=set)


    def undo_workflow_instance_changes(self):
        if self.original_workflow_instance is not None:
            app.storage.tab['current_workflow_instance'] = self.original_workflow_instance


    def calculate_existing_objects(self):
        self.existing_objects = set()

        for assignments in app.storage.tab['current_workflow_instance'].step_assignments.values():
            for assignment in assignments:
                self.existing_objects.add(assignment)