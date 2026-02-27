from nicegui import ui, app

from handover_workflows_validation_webui.workflow_model_ui.workflow_model_controls import create_graph_controls
from handover_workflows_validation_webui.workflow_model_ui.workflow_model_page_state import WorkflowModelPageState

allowed_activities = ["Photo",
                      "EDX",
                      "XRD",
                      "XPS",
                      "Annealing",
                      "LEIS",
                      "Thickness",
                      "SEM",
                      "Resistance",
                      "Bandgap",
                      "APT",
                      "TEM",
                      "SDC",
                      "SECCM",
                      "FIM",
                      "PSM",
                      "Report",
                      "Others"]
allowed_activities.sort()

allowed_projects = [
    "A01",
    "A02",
    "A03",
    "A04",
    "A05",
    "A06",

    "B01",
    "B02",
    "B03",
    "B04",
    "B05",

    "C01",
    "C02",
    "C03",
    "C04",

    "INF",
    "S",
    "Z"
]


def change_step_name_action(new_name: str,
                            workflow_model_page_state: WorkflowModelPageState):
    if not workflow_model_page_state.selected_node:
        ui.notify("No step selected", type='warning')
        return

    if not new_name in app.storage.tab['current_workflow_model'].workflow_model_steps:
        ui.notify("There is already another step with this name", type='negative')
        return

    previous_name = workflow_model_page_state.selected_node
    workflow_model_page_state.selected_node = new_name

    workflow_model_page_state.save_workflow_model_copy()
    app.storage.tab['current_workflow_model'].workflow_model_steps[new_name] = app.storage.tab['current_workflow_model'].workflow_model_steps.pop(previous_name)
    app.storage.tab['current_workflow_model'].workflow_model_steps[new_name].step_name = new_name
    for step_name, step in app.storage.tab['current_workflow_model'].workflow_model_steps.items():
        step.next_steps = [new_name if name == previous_name else name for name in step.next_steps]

    if previous_name == app.storage.tab['current_workflow_model'].workflow_model_options.initial_step_name:
        app.storage.tab['current_workflow_model'].workflow_model_options.initial_step_name = new_name

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

    workflow_model_page_state.save_workflow_model_copy()
    app.storage.tab['current_workflow_model'].workflow_model_steps[workflow_model_page_state.selected_node].step_description = new_description
    ui.notify("Description modified", type='positive')



def enable_switch_action(switch_value: bool, workflow_model_page_state: WorkflowModelPageState):
    if not workflow_model_page_state.selected_node:
        ui.notify("No step selected", type='warning')
        return

    workflow_model_page_state.save_workflow_model_copy()

    app.storage.tab['current_workflow_model'].workflow_model_steps[workflow_model_page_state.selected_node].enabled = switch_value


def other_activities_switch_action(switch_value: bool, workflow_model_page_state: WorkflowModelPageState):
    if not workflow_model_page_state.selected_node:
        ui.notify("No step selected", type='warning')
        return

    workflow_model_page_state.save_workflow_model_copy()
    app.storage.tab['current_workflow_model'].workflow_model_steps[workflow_model_page_state.selected_node].allow_other_activities = switch_value


def add_activity_action(activities_select_values,
                       workflow_model_page_state: WorkflowModelPageState):
    if not workflow_model_page_state.selected_node:
        ui.notify("No step selected", type='warning')
        return

    workflow_model_page_state.graph_component.replace_activities(workflow_model_page_state.selected_node, sorted(activities_select_values))

    if sorted(app.storage.tab['current_workflow_model'].workflow_model_steps[workflow_model_page_state.selected_node].required_activities) != sorted(activities_select_values):
        workflow_model_page_state.save_workflow_model_copy()

        app.storage.tab['current_workflow_model'].workflow_model_steps[
            workflow_model_page_state.selected_node].required_activities = activities_select_values
    #  else: the selector's callback is a bit wonky and sometimes triggers with no changes

def add_project_action(projects_select_values,
                       workflow_model_page_state: WorkflowModelPageState):
    if not workflow_model_page_state.selected_node:
        ui.notify("No step selected", type='warning')
        return

    workflow_model_page_state.graph_component.replace_projects(workflow_model_page_state.selected_node, sorted(projects_select_values))

    if sorted(app.storage.tab['current_workflow_model'].workflow_model_steps[workflow_model_page_state.selected_node].projects) != sorted(projects_select_values):
        workflow_model_page_state.save_workflow_model_copy()

        app.storage.tab['current_workflow_model'].workflow_model_steps[
            workflow_model_page_state.selected_node].projects = projects_select_values

def create_workflow_model_step_controls(workflow_model_page_state: WorkflowModelPageState):
    workflow_model_page_state.node_controls_column.clear()

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
                            description_input.value = app.storage.tab['current_workflow_model'].workflow_model_steps[
                                workflow_model_page_state.selected_node].step_description
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
                    projects_select = ui.select(allowed_projects,
                                                  multiple=True,
                                                  label='Select one or more projects',
                                                  on_change=lambda: add_project_action(projects_select.value, workflow_model_page_state)).classes(
                        'w-64').props('use-chips')
                    if workflow_model_page_state.selected_node:
                        projects_select.value = app.storage.tab['current_workflow_model'].workflow_model_steps[
                            workflow_model_page_state.selected_node].projects
                    else:
                        projects_select.value = []

                with ui.column(align_items='center'):
                    ui.label('Characterization activities').classes('text-sm font-bold text-gray-600')
                    activities_select = ui.select(allowed_activities,
                                                  multiple=True,
                                                  label='Select one or more activities',
                                                  on_change=lambda: add_activity_action(activities_select.value, workflow_model_page_state)).classes(
                        'w-64').props('use-chips')
                    if workflow_model_page_state.selected_node:
                        activities_select.value = app.storage.tab['current_workflow_model'].workflow_model_steps[
                            workflow_model_page_state.selected_node].required_activities
                    else:
                        activities_select.value = []

                with ui.column(align_items='center'):
                    ui.label('Allow other activities?').classes('text-sm font-bold text-gray-600')
                    if workflow_model_page_state.selected_node:
                        allow_other_activities = app.storage.tab['current_workflow_model'].workflow_model_steps[
                            workflow_model_page_state.selected_node].allow_other_activities
                    else:
                        allow_other_activities = False
                    switch = ui.switch('Allow other activities', value=allow_other_activities,
                                       on_change=lambda: other_activities_switch_action(switch.value, workflow_model_page_state))
