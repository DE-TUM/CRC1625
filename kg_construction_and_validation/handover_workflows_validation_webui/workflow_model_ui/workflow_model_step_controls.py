from nicegui import ui, app

from handover_workflows_validation_webui.workflow_model_ui.workflow_model_controls import create_graph_controls
from handover_workflows_validation_webui.workflow_model_ui.workflow_model_page_state import WorkflowModelPageState, get_iri_for_workflow_step_name
from workflows_validation.CRC_1625_workflows_validator.CRC_1625_workflows_validator import CRC1625WorkflowModelStep, project_name_to_iri, \
    activity_name_to_class_iri


def change_step_name_action(new_name: str,
                            workflow_model_page_state: WorkflowModelPageState):
    if not workflow_model_page_state.selected_node:
        ui.notify("No step selected", type='warning')
        return

    if get_iri_for_workflow_step_name(new_name) is not None:
        ui.notify("There is already another step with this name", type='negative')
        return

    # Change the selected node
    previous_name = workflow_model_page_state.selected_node
    workflow_model_page_state.selected_node = new_name

    # Modify the workflow model
    step_iri = get_iri_for_workflow_step_name(previous_name)
    app.storage.tab['current_workflow_model'].workflow_model_steps[step_iri].name = new_name

    # Change the node's name in Cytoscape
    workflow_model_page_state.graph_component.rename_node(previous_name, new_name)

    # Reflect the changes in the left tab
    workflow_model_page_state.graph_controls_column.clear()
    with workflow_model_page_state.graph_controls_column:
        create_graph_controls(workflow_model_page_state)

    ui.notify(f"Renamed '{previous_name}' to '{new_name}'", type='positive')


def change_step_description_action(new_description: str, workflow_model_page_state: WorkflowModelPageState):
    if not workflow_model_page_state.selected_node:
        ui.notify("No step selected", type='warning')
        return

    step_iri = get_iri_for_workflow_step_name(workflow_model_page_state.selected_node)

    app.storage.tab['current_workflow_model'].workflow_model_steps[step_iri].description = new_description
    ui.notify("Description modified", type='positive')


def other_activities_switch_action(switch_value: bool, workflow_model_page_state: WorkflowModelPageState):
    if not workflow_model_page_state.selected_node:
        ui.notify("No step selected", type='warning')
        return

    step_iri = get_iri_for_workflow_step_name(workflow_model_page_state.selected_node)
    step = app.storage.tab['current_workflow_model'].workflow_model_steps[step_iri]
    if not isinstance(step, CRC1625WorkflowModelStep):
        step = CRC1625WorkflowModelStep.from_step(step)
    step.set_other_activities_allowed_flag(switch_value)

    app.storage.tab['current_workflow_model'].workflow_model_steps[step_iri] = step

    if switch_value:
        ui.notify(f"Other activities are now enabled on {workflow_model_page_state.selected_node}", type='positive')
    else:
        ui.notify(f"Other activities are now disabled on {workflow_model_page_state.selected_node}", type='positive')


def add_activity_action(activities_select_values,
                        workflow_model_page_state: WorkflowModelPageState):
    if not workflow_model_page_state.selected_node:
        ui.notify("No step selected", type='warning')
        return

    step_iri = get_iri_for_workflow_step_name(workflow_model_page_state.selected_node)
    step = app.storage.tab['current_workflow_model'].workflow_model_steps[step_iri]
    if not isinstance(step, CRC1625WorkflowModelStep):
        step = CRC1625WorkflowModelStep.from_step(step)
    step.set_allowed_activity_names(activities_select_values)

    app.storage.tab['current_workflow_model'].workflow_model_steps[step_iri] = step

    # Reflect it in Cytoscape
    workflow_model_page_state.graph_component.replace_activities(workflow_model_page_state.selected_node, sorted(activities_select_values))


def add_project_action(projects_select_values,
                       workflow_model_page_state: WorkflowModelPageState):
    if not workflow_model_page_state.selected_node:
        ui.notify("No step selected", type='warning')
        return

    step_iri = get_iri_for_workflow_step_name(workflow_model_page_state.selected_node)
    step = app.storage.tab['current_workflow_model'].workflow_model_steps[step_iri]
    if not isinstance(step, CRC1625WorkflowModelStep):
        step = CRC1625WorkflowModelStep.from_step(step)
    step.set_allowed_project_names(projects_select_values)

    app.storage.tab['current_workflow_model'].workflow_model_steps[step_iri] = step

    # Reflect it in Cytoscape
    workflow_model_page_state.graph_component.replace_projects(workflow_model_page_state.selected_node, sorted(projects_select_values))


def create_workflow_model_step_controls(workflow_model_page_state: WorkflowModelPageState):
    workflow_model_page_state.node_controls_column.clear()

    if workflow_model_page_state.selected_node:
        selected_step_iri = get_iri_for_workflow_step_name(workflow_model_page_state.selected_node)
        selected_step = app.storage.tab['current_workflow_model'].workflow_model_steps[selected_step_iri]
        if not isinstance(selected_step, CRC1625WorkflowModelStep):
            selected_step = CRC1625WorkflowModelStep.from_step(selected_step)

    with workflow_model_page_state.node_controls_column:
        with ui.card().classes('w-full bg-secondary'):
            # If the model is empty or nothing is selected somehow, so empty controls
            if workflow_model_page_state.selected_node:
                ui.label(f"Step options for '{workflow_model_page_state.selected_node}'").classes('text-lg font-semibold')
            else:
                ui.label("Please select or create a new step")

            with ui.grid(columns=2).classes('w-full'):
                with ui.column(align_items='center'):
                    ui.label('Workflow step name').classes('text-sm font-bold text-gray-600')

                    with ui.row():
                        rename_input = ui.input('New step name')
                        rename_input.value = workflow_model_page_state.selected_node
                        ui.button('Rename', color='info', on_click=lambda: change_step_name_action(
                            rename_input.value,
                            workflow_model_page_state
                        ))

                with ui.column(align_items='center'):
                    ui.label('Workflow step description').classes('text-sm font-bold text-gray-600')
                    with ui.row():
                        description_input = ui.input('Description').props('type=textarea')
                        if workflow_model_page_state.selected_node:
                            description_input.value = selected_step.description
                        else:
                            description_input.value = "Please select or create a new step"
                        ui.button('Rename', color='info', on_click=lambda: change_step_description_action(
                            description_input.value,
                            workflow_model_page_state
                        ))

            ui.separator().classes('my-2')

            with ui.grid(columns=3).classes('w-full'):
                with ui.column(align_items='center'):
                    ui.label('Project').classes('text-sm font-bold text-gray-600')
                    projects_select = ui.select(list(project_name_to_iri.keys()),
                                                multiple=True,
                                                label='Select one or more projects',
                                                on_change=lambda: add_project_action(projects_select.value, workflow_model_page_state)).classes(
                        'w-64').props('use-chips')
                    if workflow_model_page_state.selected_node:
                        projects_select.value = selected_step.get_allowed_project_names()
                    else:
                        projects_select.value = []

                with ui.column(align_items='center'):
                    ui.label('Characterization activities').classes('text-sm font-bold text-gray-600')
                    activities_select = ui.select(list(activity_name_to_class_iri.keys()),
                                                  multiple=True,
                                                  label='Select one or more activities',
                                                  on_change=lambda: add_activity_action(activities_select.value, workflow_model_page_state)).classes(
                        'w-64').props('use-chips')
                    if workflow_model_page_state.selected_node:
                        activities_select.value = selected_step.get_allowed_activity_names()
                    else:
                        activities_select.value = []

                with ui.column(align_items='center'):
                    ui.label('Allow other activities?').classes('text-sm font-bold text-gray-600')
                    if workflow_model_page_state.selected_node:
                        allow_other_activities = selected_step.are_other_activities_allowed()
                    else:
                        allow_other_activities = False
                    switch = ui.switch('Allow other activities', value=allow_other_activities,
                                       on_change=lambda: other_activities_switch_action(switch.value, workflow_model_page_state))
