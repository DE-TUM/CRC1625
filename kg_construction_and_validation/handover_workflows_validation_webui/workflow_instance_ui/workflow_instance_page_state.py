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
    original_workflow_instance_name: str = None
    changes_are_saved: bool = True
    workflow_instance_page_state: bool = True
    workflow_instance_history: list[tuple[str, WorkflowInstance]] = field(default_factory=list)
    existing_objects: set[int] = field(default_factory=set)

    def save_workflow_instance_copy(self):
        self.workflow_instance_history.append((self.selected_node, deepcopy(app.storage.tab['current_workflow_instance'])))
        self.changes_are_saved = False


    def undo_workflow_instance_change(self):
        if len(self.workflow_instance_history) > 0:
            self.selected_node, app.storage.tab['current_workflow_instance'] = self.workflow_instance_history.pop()

        self.calculate_existing_objects()


    def calculate_existing_objects(self):
        self.existing_objects = set()

        for assignments in app.storage.tab['current_workflow_instance'].step_assignments.values():
            for assignment in assignments:
                self.existing_objects.add(assignment)