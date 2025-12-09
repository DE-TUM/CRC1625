from nicegui import ui

from handover_workflows_validation_webui.state import State, ui_elements


async def add_edge_action(step_name: str | None,
                          object_id: str | None):
    if not step_name or not object_id:
        ui.notify("Please indicate both a Materials Library / Sample ID and a step", type='warning')
        return
    elif await ui_elements.graph_component.exists_edge(step_name, object_id):
        ui.notify("The Materials Library / Sample is already assigned to the step", type='negative')
        return

    State().save_workflow_model_copy()

    ui_elements.graph_component.add_edge(step_name, object_id)
    State().current_workflow_instance.step_assignments[step_name].append(int(object_id))

    ui.notify(f"Assigned Materials Library / Sample ID '{object_id}' to step '{step_name}'", type='positive')


async def remove_edge_action(step_name: str,
                             object_id: str):
    if not step_name or not object_id:
        ui.notify("Please indicate both a Materials Library / Sample ID and a step", type='warning')
        return
    elif not await ui_elements.graph_component.exists_edge(step_name, object_id):
        ui.notify("The Materials Library / Sample is not assigned to the step", type='negative')
        return

    State().save_workflow_model_copy()

    ui_elements.graph_component.remove_edge(step_name, object_id)
    State().current_workflow_instance.step_assignments[step_name].remove(int(object_id))

    ui.notify(f"Removed assignment of Materials Library / Sample ID '{object_id}' from step '{step_name}'",
              type='positive')


def create_workflow_instance_step_controls():
    ui_elements.node_controls_column.clear()

    with ui_elements.node_controls_column:
        with (ui.card().classes('w-full')):
            ui.label(f"Step options for '{State().selected_node}'").classes('text-lg font-semibold')

            ui.label('Assign Materials Library / Sample to step').classes('text-sm font-bold text-gray-600')
            with ui.grid(columns=3).classes('w-full items-center gap-4'):
                with ui.column(align_items='center'):
                    ui.label("Step")
                    source_node_input_add = ui.select(
                        options=sorted(list(State().current_workflow_model.workflow_model_steps.keys())))
                with ui.column(align_items='center'):
                    ui.label("Materials Library / Sample ID")
                    target_node_input_add = ui.select(
                        options=[str(obj) for obj in sorted(State().existing_objects)])

                with ui.column():
                    ui.button('Assign', on_click=lambda:
                    add_edge_action(source_node_input_add.value, target_node_input_add.value)
                              ).classes('w-full mt-2')

            ui.label('Disconnect steps').classes('text-sm font-bold text-gray-600')
            with ui.grid(columns=3).classes('w-full items-center gap-4'):
                with ui.column(align_items='center'):
                    ui.label("Step")
                    source_node_input_remove = ui.select(
                        options=sorted(list(State().current_workflow_model.workflow_model_steps.keys())))
                with ui.column(align_items='center'):
                    ui.label("Materials Library / Sample ID")
                    target_node_input_remove = ui.select(
                        options=[str(obj) for obj in sorted(State().existing_objects)])

                with ui.column():
                    ui.button('Unassign', on_click=lambda:
                    remove_edge_action(source_node_input_remove.value, target_node_input_remove.value)
                              ).classes('w-full mt-2').props("color=red")
