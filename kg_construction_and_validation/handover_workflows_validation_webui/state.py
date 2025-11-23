from copy import deepcopy
from dataclasses import dataclass, field

from datastores.rdf.rdf_datastore import RDFDatastore
from handover_workflows_validation.handover_workflows_validation import WorkflowModel, WorkflowInstance


@dataclass
class State:
    # Cytoscape
    selected_node: str = ""

    # Instances
    workflow_instances_of_current_workflow_model: dict[tuple[str, int], WorkflowInstance] = field(default_factory=dict)
    current_workflow_instance: WorkflowInstance = None
    existing_objects: set[int] = field(default_factory=set)

    # Models
    current_workflow_model: WorkflowModel = None
    graph_component = None
    graph_component_column = None
    node_controls_column = None
    graph_controls_column = None

    # Both
    user_id: int = 0
    changes_are_saved: bool = True
    store: RDFDatastore = None

    # History
    workflow_model_history: list[tuple[str, WorkflowModel]] = field(default_factory=list)  # Selected node and workflow model
    workflow_instance_history: list[tuple[str, WorkflowInstance]] = field(default_factory=list)  # Selected node and workflow instance

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


state = State()
