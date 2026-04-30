from dataclasses import dataclass

from nicegui import app
from nicegui.elements.column import Column
from nicegui.elements.input import Input
from rdflib import URIRef

from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent
from workflows_validation.workflows_validator import WorkflowModel


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
    changes_are_saved: bool = True
    original_workflow_model: WorkflowModel = None

    def undo_workflow_model_changes(self):
        if self.original_workflow_model is not None:
            app.storage.tab['current_workflow_model'] = self.original_workflow_model


def get_iri_for_workflow_step_name(workflow_step_name: str) -> URIRef | None:
    """
    Helper function to get the IRI corresponding to a workflow model step name of the currently cached workflow model
    Since we work with names in the Web UI and Cytoscape, it is necessary to dereference them
    """
    for workflow_model_step in app.storage.tab['current_workflow_model'].workflow_model_steps.values():
        if workflow_step_name == workflow_model_step.name:
            return workflow_model_step.iri
    else:
        return None
