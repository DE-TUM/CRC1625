from nicegui import ui
from nicegui.elements.select import Select

from handover_workflows_validation.handover_workflows_validation import WorkflowModel
from handover_workflows_validation_webui.state import State, ui_elements

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


def change_step_name_action(new_name: str):
    State().save_workflow_model_copy()

    if State().selected_node == "":
        ui.notify("No node selected (click a node first)", type='warning')
        return

    State().current_workflow_model.workflow_model_steps[new_name] = State().current_workflow_model.workflow_model_steps.pop(
        State().selected_node)
    for step_name, step in State().current_workflow_model.workflow_model_steps.items():
        if State().selected_node in step.next_steps:
            step.next_steps.pop(step.next_steps.index(State().selected_node))
            step.next_steps.append(new_name)

    ui_elements.graph_component.rename_node(State().selected_node, new_name)

    previous_name = State().selected_node
    State().selected_node = new_name

    ui.notify(f"Renamed '{previous_name}' to '{new_name}'", type='positive')


def change_step_description_action(new_description: str):
    State().save_workflow_model_copy()

    State().current_workflow_model.workflow_model_steps[State().selected_node].step_description = new_description

    ui.notify("Description modified", type='positive')


def enable_switch_action(workflow_model: WorkflowModel,
                         switch_value: bool):
    State().save_workflow_model_copy()

    workflow_model.workflow_model_steps[State().selected_node].enabled = switch_value


def other_activities_switch_action(workflow_model: WorkflowModel,
                                   switch_value: bool):
    State().save_workflow_model_copy()

    workflow_model.workflow_model_steps[State().selected_node].allow_other_activities = switch_value


def add_activity_action(activities_select: Select):
    #for old_activity in State().current_workflow_model.workflow_model_steps[State().selected_node].required_activities:
    #    ui_elements.graph_component.remove_activity(State().selected_node, old_activity)

    #for new_activity in activities_select.value:
    #    ui_elements.graph_component.add_activity(State().selected_node, new_activity)

    ui_elements.graph_component.replace_activities(State().selected_node, sorted(activities_select.value))

    if sorted(State().current_workflow_model.workflow_model_steps[State().selected_node].required_activities) != sorted(activities_select.value):
        State().save_workflow_model_copy()

        State().current_workflow_model.workflow_model_steps[
            State().selected_node].required_activities = activities_select.value
    #  else: the selector's callback is a bit wonky and sometimes triggers with no changes


def create_workflow_model_step_controls():
    ui_elements.node_controls_column.clear()

    with ui_elements.node_controls_column:
        with (ui.card().classes('w-full')):
            ui.label(f"Step options for '{State().selected_node}'").classes('text-lg font-semibold')
            with ui.grid(columns=2).classes('w-full'):
                with ui.column(align_items='center'):
                    ui.label('Workflow step name').classes('text-sm font-bold text-gray-600')

                    with ui.row():
                        rename_input = ui.input('New step name')
                        rename_input.value = State().selected_node
                        ui.button('Rename', on_click=lambda: change_step_name_action(
                            rename_input.value
                        ))

                with ui.column(align_items='center'):
                    ui.label('Workflow step description').classes('text-sm font-bold text-gray-600')
                    with ui.row():
                        description_input = ui.input('Description').props('type=textarea')
                        description_input.value = State().current_workflow_model.workflow_model_steps[
                            State().selected_node].step_description
                        ui.button('Rename', on_click=lambda: change_step_description_action(
                            description_input.value,
                        ))

            ui.separator().classes('my-2')

            with ui.grid(columns=2).classes('w-full'):
                with ui.column(align_items='center'):
                    ui.label('Characterization activities required').classes('text-sm font-bold text-gray-600')
                    activities_select = ui.select(allowed_activities,
                                                  multiple=True,
                                                  label='Select one or more activities',
                                                  on_change=lambda: add_activity_action(activities_select)).classes(
                        'w-64').props('use-chips')
                    activities_select.value = State().current_workflow_model.workflow_model_steps[
                        State().selected_node].required_activities

                with ui.column(align_items='center'):
                    ui.label('Allow other activities?').classes('text-sm font-bold text-gray-600')
                    allow_other_activities = State().current_workflow_model.workflow_model_steps[
                        State().selected_node].allow_other_activities
                    switch = ui.switch('Allow other activities', value=allow_other_activities,
                                       on_change=lambda: other_activities_switch_action(State().current_workflow_model,
                                                                                        switch.value))
