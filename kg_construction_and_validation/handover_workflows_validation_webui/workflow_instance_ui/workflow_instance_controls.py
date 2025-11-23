from nicegui import ui

from handover_workflows_validation_webui.state import State, ui_elements
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_step_controls import \
    create_workflow_instance_step_controls


def add_object_action(new_object_id: str | None):
    if not new_object_id:
        ui.notify("Please indicate a Materials Library / Sample ID to add", type='warning')
        return

    if int(new_object_id) in State().existing_objects:
        ui.notify(f"The Materials Library / Sample ID '{new_object_id}' is already present in the Workflow Instance",
                  type='negative')
        return

    State().save_workflow_instance_copy()

    ui_elements.graph_component.add_node(new_object_id, new_object_id)
    State().existing_objects.add(int(new_object_id))

    ui.notify(f"Added Materials Library / Sample ID '{new_object_id}'", type='positive')

    ui_elements.graph_controls_column.clear()
    with ui_elements.graph_controls_column:
        create_graph_controls()

    ui_elements.node_controls_column.clear()
    with ui_elements.node_controls_column:
        create_workflow_instance_step_controls()


def remove_object_action(object_id_to_remove: str):
    if not object_id_to_remove:
        ui.notify("Please indicate a Materials Library / Sample ID to remove", type='warning')
        return

    State().save_workflow_model_copy()

    for assignment in State().current_workflow_instance.step_assignments.values():
        if int(object_id_to_remove) in assignment:
            assignment.remove(int(object_id_to_remove))

    ui_elements.graph_component.remove_node(object_id_to_remove)
    State().existing_objects.remove(int(object_id_to_remove))

    ui.notify(f"Removed Materials Library / Sample ID '{object_id_to_remove}'", type='positive')

    ui_elements.graph_controls_column.clear()
    with ui_elements.graph_controls_column:
        create_graph_controls()

    ui_elements.node_controls_column.clear()
    with ui_elements.node_controls_column:
        create_workflow_instance_step_controls()


def create_graph_controls():
    with ui.card().classes('w-full'):
        ui.label('Workflow Instance options').classes('text-lg font-semibold')

        ui.label('Add a Materials Library / Sample ID').classes('text-sm font-bold text-gray-600')
        with ui.row().classes('w-full items-center'):
            new_object_input = ui.input('Workflow step name').classes('flex-grow')
            ui.button('Add step', on_click=lambda: add_object_action(
                new_object_input.value
            ))

        ui.separator().classes('my-2')

        ui.label('Remove a Materials Library / Sample ID').classes('text-sm font-bold text-gray-600')
        with ui.row().classes('w-full items-center'):
            remove_object_select = ui.select(
                options=[str(obj) for obj in sorted(list(State().existing_objects))])
            ui.button('Remove step', on_click=lambda: remove_object_action(
                remove_object_select.value
            ))
