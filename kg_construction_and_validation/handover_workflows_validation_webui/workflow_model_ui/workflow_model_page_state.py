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
    step_renamings: dict[str, str] = field(default_factory=dict)
    changes_are_saved: bool = True
    original_workflow_model: WorkflowModel = None

    def undo_workflow_model_changes(self):
        if self.original_workflow_model is not None:
            app.storage.tab['current_workflow_model'] = self.original_workflow_model