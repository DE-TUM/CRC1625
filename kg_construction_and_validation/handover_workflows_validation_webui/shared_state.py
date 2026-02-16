from dataclasses import dataclass, field

from nicegui import app

from handover_workflows_validation.handover_workflows_validation import WorkflowInstance, WorkflowModel

@dataclass
class SharedState:
    """
    State class for shared user data. Anything that needs to be preserved across page navigation belongs here.
    """
    _storage: dict = field(init=False, repr=False)

    def __post_init__(self):
        self._storage = app.storage.client

        defaults = {
            'current_workflow_model': None,
            'workflow_instances_of_current_workflow_model': {},
            'current_workflow_instance': None,
            'user_id': 0,
            'demo_mode': False
        }

        for key, default_value in defaults.items():
            if key not in self._storage:
                self._storage[key] = default_value

    # Workflow Models
    @property
    def current_workflow_model(self) -> WorkflowModel:
        return self._storage['current_workflow_model']

    @current_workflow_model.setter
    def current_workflow_model(self, value: WorkflowModel):
        self._storage['current_workflow_model'] = value

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

    # User auth (TODO)
    @property
    def user_id(self) -> int:
        return self._storage['user_id']

    @user_id.setter
    def user_id(self, value: int):
        self._storage['user_id'] = value

    @property
    def demo_mode(self) -> bool:
        return self._storage['demo_mode']

    @demo_mode.setter
    def demo_mode(self, value: bool):
        self._storage['demo_mode'] = value


# Use a singleton pattern
_state_instance = None

def shared_state():
    global _state_instance

    if _state_instance is None:
        _state_instance = SharedState()

    return _state_instance