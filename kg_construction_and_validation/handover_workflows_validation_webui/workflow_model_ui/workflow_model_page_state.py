from copy import deepcopy
from dataclasses import dataclass, field

from nicegui import app
from nicegui.elements.column import Column
from nicegui.elements.input import Input

from handover_workflows_validation.handover_workflows_validation import WorkflowModel
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent


@dataclass
class WorkflowModelPageState:
    """
    State dataclass containing references to UI elements and contents of pages in this module.
    Initialized by accessing the main page and thus local to the user.
    """
    graph_component: CytoscapeComponent = None
    graph_component_column: Column = None
    node_controls_column: Column = None
    graph_controls_column: Column = None
    selected_node: str = None
    workflow_model_name_input: Input = None
    original_workflow_model_name: str = None
    changes_are_saved: bool = True
    workflow_model_history: list[tuple[str, WorkflowModel]] = field(default_factory=list)

    def save_workflow_model_copy(self):
        self.workflow_model_history.append((self.selected_node, deepcopy(app.storage.tab['current_workflow_model'])))
        self.changes_are_saved = False


    def undo_workflow_model_change(self):
        if len(self.workflow_model_history) > 0:
            self.selected_node, app.storage.tab['current_workflow_model'] = self.workflow_model_history.pop()