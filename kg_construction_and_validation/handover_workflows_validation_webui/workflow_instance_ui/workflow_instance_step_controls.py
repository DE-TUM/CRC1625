from nicegui import ui, app

from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_page_state import WorkflowInstancePageState
from handover_workflows_validation_webui.workflow_model_ui.workflow_model_page_state import get_iri_for_workflow_step_name
from workflows_validation.CRC_1625_workflows_validator.CRC_1625_workflows_validator import crc_prefix
from workflows_validation.workflow_instance import StepAssignment


async def add_edge_action(step_name: str | None,
                          object_id: str | None,
                          workflow_instance_page_state: WorkflowInstancePageState):
    # Remove the previous node colors
    workflow_instance_page_state.graph_component.clear_validation_results()

    if not step_name or not object_id:
        ui.notify("Please indicate both a Materials Library / Sample ID and a step", type='warning')
        return
    elif await workflow_instance_page_state.graph_component.exists_edge(step_name, f'ML / Sample {object_id}'):
        ui.notify("The Materials Library / Sample is already assigned to the step", type='negative')
        return

    workflow_step_iri = get_iri_for_workflow_step_name(step_name)

    workflow_instance_page_state.graph_component.add_edge(step_name, f'ML / Sample {object_id}')
    handover_group_iri = workflow_instance_page_state.sample_object_id_to_hnd_group_iri[int(object_id)]
    if workflow_step_iri not in app.storage.tab['current_workflow_instance'].step_assignments:
        # We need to create a new one
        step_assignment = StepAssignment()
        step_assignment.create_new_iri()
        step_assignment.assigned_entities = []
        step_assignment.property_to_follow = crc_prefix.nextStep
        step_assignment.workflow_step_iri = workflow_step_iri
        app.storage.tab['current_workflow_instance'].step_assignments[workflow_step_iri] = step_assignment

    app.storage.tab['current_workflow_instance'].step_assignments[workflow_step_iri].assigned_entities.append(handover_group_iri)

    ui.notify(f"Assigned Materials Library / Sample ID '{object_id}' to step '{step_name}'", type='positive')


async def remove_edge_action(step_name: str,
                             object_id: str,
                             workflow_instance_page_state: WorkflowInstancePageState):
    # Remove the previous node colors
    workflow_instance_page_state.graph_component.clear_validation_results()

    if not step_name or not object_id:
        ui.notify("Please indicate both a Materials Library / Sample ID and a step", type='warning')
        return
    elif not await workflow_instance_page_state.graph_component.exists_edge(step_name, f'ML / Sample {object_id}'):
        ui.notify(f"The ML / Sample {object_id} is not assigned to {step_name}", type='negative')
        return

    workflow_instance_page_state.graph_component.remove_edge(step_name, f'ML / Sample {object_id}')
    handover_group_iri = workflow_instance_page_state.sample_object_id_to_hnd_group_iri[int(object_id)]
    app.storage.tab['current_workflow_instance'].step_assignments[get_iri_for_workflow_step_name(step_name)].assigned_entities.remove(handover_group_iri)

    ui.notify(f"Removed assignment of Materials Library / Sample ID '{object_id}' from step '{step_name}'",
              type='positive')


def create_workflow_instance_step_controls(workflow_instance_page_state: WorkflowInstancePageState):
    workflow_instance_page_state.node_controls_column.clear()

    with workflow_instance_page_state.node_controls_column:
        with ui.card().classes('w-full bg-secondary'):
            ui.label(f"Step options for '{workflow_instance_page_state.selected_node}'").classes('text-lg font-semibold')

            sample_ids = [str(obj) for obj in sorted(list(workflow_instance_page_state.sample_object_id_to_hnd_group_iri.keys()))]
            workflow_step_names = sorted([step.name for step in app.storage.tab['current_workflow_model'].workflow_model_steps.values()])

            ui.label('Assign Materials Library / Sample to step').classes('text-sm font-bold text-gray-600')
            with ui.grid(columns=3).classes('w-full items-center gap-4'):
                with ui.column(align_items='center'):
                    ui.label("Step")
                    source_node_input_add = ui.select(options=workflow_step_names)
                with ui.column(align_items='center'):
                    ui.label("Materials Library / Sample ID")
                    target_node_input_add = ui.select(options=sample_ids)

                with ui.column():
                    ui.button('Assign', color='info', on_click=lambda:
                    add_edge_action(source_node_input_add.value, target_node_input_add.value, workflow_instance_page_state)
                              ).classes('w-full mt-2')

            ui.label('Disconnect steps').classes('text-sm font-bold text-gray-600')
            with ui.grid(columns=3).classes('w-full items-center gap-4'):
                with ui.column(align_items='center'):
                    ui.label("Step")
                    source_node_input_remove = ui.select(options=workflow_step_names)
                with ui.column(align_items='center'):
                    ui.label("Materials Library / Sample ID")
                    target_node_input_remove = ui.select(options=sample_ids)

                with ui.column():
                    ui.button('Unassign', color='negative', on_click=lambda:
                    remove_edge_action(source_node_input_remove.value, target_node_input_remove.value, workflow_instance_page_state)
                              ).classes('w-full mt-2')
