from nicegui import ui, app

from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import NodeType
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_page_state import WorkflowInstancePageState
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_step_controls import \
    create_workflow_instance_step_controls


def add_object_action(new_object_id: str | None, 
                      workflow_instance_page_state: WorkflowInstancePageState):
    # Remove the previous node colors
    workflow_instance_page_state.graph_component.clear_validation_results()

    if not new_object_id:
        ui.notify("Please indicate a Materials Library / Sample ID to add", type='warning')
        return

    if int(new_object_id) in workflow_instance_page_state.existing_objects:
        ui.notify(f"The Materials Library / Sample ID '{new_object_id}' is already present in the Workflow Instance",
                  type='negative')
        return

    workflow_instance_page_state.save_workflow_instance_copy()

    workflow_instance_page_state.graph_component.add_node(new_object_id,
                                                       new_object_id,
                                                       NodeType.node_type_object,
                                                       coloring_ids=['object'])
    workflow_instance_page_state.existing_objects.add(int(new_object_id))

    ui.notify(f"Added Materials Library / Sample ID '{new_object_id}'", type='positive')

    workflow_instance_page_state.graph_controls_column.clear()
    with workflow_instance_page_state.graph_controls_column:
        create_graph_controls(workflow_instance_page_state)

    workflow_instance_page_state.node_controls_column.clear()
    with workflow_instance_page_state.node_controls_column:
        create_workflow_instance_step_controls(workflow_instance_page_state)


def remove_object_action(object_id_to_remove: str, 
                         workflow_instance_page_state: WorkflowInstancePageState):
    # Remove the previous node colors
    workflow_instance_page_state.graph_component.clear_validation_results()

    if not object_id_to_remove:
        ui.notify("Please indicate a Materials Library / Sample ID to remove", type='warning')
        return

    workflow_instance_page_state.save_workflow_instance_copy()

    for assignment in app.storage.tab['current_workflow_instance'].step_assignments.values():
        if int(object_id_to_remove) in assignment:
            assignment.remove(int(object_id_to_remove))

    workflow_instance_page_state.graph_component.remove_node(object_id_to_remove)
    workflow_instance_page_state.existing_objects.remove(int(object_id_to_remove))

    ui.notify(f"Removed Materials Library / Sample ID '{object_id_to_remove}'", type='positive')

    workflow_instance_page_state.graph_controls_column.clear()
    with workflow_instance_page_state.graph_controls_column:
        create_graph_controls(workflow_instance_page_state)

    workflow_instance_page_state.node_controls_column.clear()
    with workflow_instance_page_state.node_controls_column:
        create_workflow_instance_step_controls(workflow_instance_page_state)


def create_graph_controls(workflow_instance_page_state: WorkflowInstancePageState):
    with ui.card().classes('w-full bg-secondary'):
        ui.label('Workflow Instance options').classes('text-lg font-semibold')

        ui.label('Add a Materials Library / Sample ID').classes('text-sm font-bold text-gray-600')
        with ui.row().classes('w-full items-center'):
            new_object_input = ui.input('Materials Library / Sample ID').classes('flex-grow')
            ui.button('Add step', color='info', on_click=lambda: add_object_action(
                new_object_input.value,
                workflow_instance_page_state
            ))

        ui.separator().classes('my-2')

        ui.label('Remove a Materials Library / Sample ID').classes('text-sm font-bold text-gray-600')
        with ui.row().classes('w-full items-center'):
            remove_object_select = ui.select(
                options=[str(obj) for obj in sorted(list(workflow_instance_page_state.existing_objects))])
            ui.button('Remove ML / Sample', color='negative', on_click=lambda: remove_object_action(
                remove_object_select.value,
                workflow_instance_page_state
            ))
