from copy import deepcopy
from dataclasses import dataclass, field

from nicegui import app, ui

from handover_workflows_validation.handover_workflows_validation import WorkflowInstance, WorkflowModel
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent


@dataclass
class UIElements:
    graph_component: CytoscapeComponent = None,
    graph_component_column: ui.column = None,
    node_controls_column: ui.column = None
    graph_controls_column: ui.column = None


ui_elements = UIElements()


@dataclass
class State:
    _storage: dict = field(init=False, repr=False)

    def __post_init__(self):
        self._storage = app.storage.client

        defaults = {
            'selected_node': "",
            'workflow_instances_of_current_workflow_model': {},
            'current_workflow_instance': None,
            'existing_objects': set(),
            'current_workflow_model': None,
            'graph_component': None,
            'graph_component_column': None,
            'node_controls_column': None,
            'graph_controls_column': None,
            'user_id': 0,
            'changes_are_saved': True,
            'store': None,
            'workflow_model_history': [],
            'workflow_instance_history': [],
        }

        for key, default_value in defaults.items():
            if key not in self._storage:
                self._storage[key] = default_value

    # Cytoscape
    @property
    def selected_node(self) -> str:
        return self._storage['selected_node']

    @selected_node.setter
    def selected_node(self, value: str):
        self._storage['selected_node'] = value

    # Workflow Model instances
    @property
    def workflow_instances_of_current_workflow_model(self) -> dict[tuple[str, int], WorkflowInstance]:
        return self._storage['workflow_instances_of_current_workflow_model']

    @workflow_instances_of_current_workflow_model.setter
    def workflow_instances_of_current_workflow_model(self, value: dict[tuple[str, int], WorkflowInstance]):
        self._storage['workflow_instances_of_current_workflow_model'] = value

    @property
    def current_workflow_instance(self) -> WorkflowInstance:
        return self._storage['current_workflow_instance']

    @current_workflow_instance.setter
    def current_workflow_instance(self, value: WorkflowInstance):
        self._storage['current_workflow_instance'] = value

    @property
    def existing_objects(self) -> set[int]:
        return self._storage['existing_objects']

    @existing_objects.setter
    def existing_objects(self, value: set[int]):
        self._storage['existing_objects'] = value

    # Workflow Models
    @property
    def current_workflow_model(self) -> WorkflowModel:
        return self._storage['current_workflow_model']

    @current_workflow_model.setter
    def current_workflow_model(self, value: WorkflowModel):
        self._storage['current_workflow_model'] = value

    @property
    def graph_component(self):
        return self._storage['graph_component']

    @graph_component.setter
    def graph_component(self, value):
        self._storage['graph_component'] = value

    @property
    def graph_component_column(self):
        return self._storage['graph_component_column']

    @graph_component_column.setter
    def graph_component_column(self, value):
        self._storage['graph_component_column'] = value

    @property
    def node_controls_column(self):
        return self._storage['node_controls_column']

    @node_controls_column.setter
    def node_controls_column(self, value):
        self._storage['node_controls_column'] = value

    @property
    def graph_controls_column(self):
        return self._storage['graph_controls_column']

    @graph_controls_column.setter
    def graph_controls_column(self, value):
        self._storage['graph_controls_column'] = value

    # Both
    @property
    def user_id(self) -> int:
        return self._storage['user_id']

    @user_id.setter
    def user_id(self, value: int):
        self._storage['user_id'] = value

    @property
    def changes_are_saved(self) -> bool:
        return self._storage['changes_are_saved']

    @changes_are_saved.setter
    def changes_are_saved(self, value: bool):
        self._storage['changes_are_saved'] = value

    # History
    @property
    def workflow_model_history(self) -> list[tuple[str, WorkflowModel]]:
        return self._storage['workflow_model_history']

    @workflow_model_history.setter
    def workflow_model_history(self, value: list[tuple[str, WorkflowModel]]):
        self._storage['workflow_model_history'] = value

    @property
    def workflow_instance_history(self) -> list[tuple[str, WorkflowInstance]]:
        return self._storage['workflow_instance_history']

    @workflow_instance_history.setter
    def workflow_instance_history(self, value: list[tuple[str, WorkflowInstance]]):
        self._storage['workflow_instance_history'] = value

    def calculate_existing_objects(self):
        self.existing_objects = set()

        for assignments in self.current_workflow_instance.step_assignments.values():
            for assignment in assignments:
                self.existing_objects.add(assignment)

    def save_workflow_model_copy(self):
        self.workflow_model_history.append((self.selected_node, deepcopy(self.current_workflow_model)))
        self.changes_are_saved = False

    def undo_workflow_model_change(self):
        if len(self.workflow_model_history) > 0:
            self.selected_node, self.current_workflow_model = self.workflow_model_history.pop()

    def save_workflow_instance_copy(self):
        self.workflow_instance_history.append((self.selected_node, deepcopy(self.current_workflow_instance)))
        self.changes_are_saved = False

    def undo_workflow_instance_change(self):
        if len(self.workflow_instance_history) > 0:
            self.selected_node, self.current_workflow_instance = self.workflow_instance_history.pop()

        self.calculate_existing_objects()
