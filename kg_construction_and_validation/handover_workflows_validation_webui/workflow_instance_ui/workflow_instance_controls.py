from nicegui import ui, app

from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import NodeType
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_page_state import WorkflowInstancePageState
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_step_controls import \
    create_workflow_instance_step_controls


async def add_object_action(new_object_id: str | None,
                            workflow_instance_page_state: WorkflowInstancePageState):
    # Remove the previous node colors
    workflow_instance_page_state.graph_component.clear_validation_results()

    if not new_object_id:
        ui.notify("Please indicate a Materials Library / Sample ID to add", type='warning')
        return

    if int(new_object_id) in workflow_instance_page_state.sample_object_id_to_hnd_group_iri:
        ui.notify(f"The Materials Library / Sample ID '{new_object_id}' is already present in the Workflow Instance",
                  type='negative')
        return

    await workflow_instance_page_state.add_sample_object_id_to_caches(int(new_object_id))

    if int(new_object_id) not in workflow_instance_page_state.sample_object_id_to_hnd_group_iri:
        ui.notify(f"The Materials Library / Sample ID '{new_object_id}' does not exist. Please make sure you are using its internal / object ID.",
                  type='negative')
        return

    workflow_instance_page_state.graph_component.add_node(f'ML / Sample {new_object_id}',
                                                          NodeType.node_type_object,
                                                          coloring_ids=['object'])

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

    handover_group_iri = workflow_instance_page_state.sample_object_id_to_hnd_group_iri[int(object_id_to_remove)]
    for step_assignment in app.storage.tab['current_workflow_instance'].step_assignments.values():
        if handover_group_iri in step_assignment.assigned_entities:
            step_assignment.assigned_entities.remove(handover_group_iri)

    workflow_instance_page_state.graph_component.remove_node(f'ML / Sample {object_id_to_remove}')
    del workflow_instance_page_state.sample_object_id_to_hnd_group_iri[int(object_id_to_remove)]

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
                options=[str(obj) for obj in sorted(list(workflow_instance_page_state.sample_object_id_to_hnd_group_iri.keys()))])
            ui.button('Remove ML / Sample', color='negative', on_click=lambda: remove_object_action(
                remove_object_select.value,
                workflow_instance_page_state
            ))
