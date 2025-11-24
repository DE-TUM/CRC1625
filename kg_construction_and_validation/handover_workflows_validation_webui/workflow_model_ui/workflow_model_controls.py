from nicegui import ui

from handover_workflows_validation.handover_workflows_validation import WorkflowModelStep
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import NodeType
from handover_workflows_validation_webui.state import State, ui_elements


async def add_edge_action(source: str,
                          target: str):
    if not source or not target:
        ui.notify("Please enter both source and target steps", type='warning')
        return
    elif source == target:
        ui.notify("It is not possible to connect the steps to itself", type='negative')
        return
    elif await ui_elements.graph_component.exists_edge(source, target):
        ui.notify("The two steps are already connected", type='negative')
        return

    State().save_workflow_model_copy()

    ui_elements.graph_component.add_edge(source, target)
    State().current_workflow_model.workflow_model_steps[source].next_steps.add(target)

    ui.notify(f"Added edge from '{source}' to '{target}'", type='positive')

    ui_elements.graph_controls_column.clear()
    with ui_elements.graph_controls_column:
        create_graph_controls()


async def remove_edge_action(source: str,
                             target: str):
    if not source or not target:
        ui.notify("Please enter both source and target steps", type='warning')
        return
    elif not await ui_elements.graph_component.exists_edge(source, target):
        ui.notify("The two steps are not connected", type='negative')
        return

    State().save_workflow_model_copy()

    ui_elements.graph_component.remove_edge(source, target)
    State().current_workflow_model.workflow_model_steps[source].next_steps.remove(target)

    ui.notify(f"Removed edge from '{source}' to '{target}'", type='positive')

    ui_elements.graph_controls_column.clear()
    with ui_elements.graph_controls_column:
        create_graph_controls()


def add_step_action(new_step_name: str):
    if not new_step_name:
        ui.notify("Please enter a step name", type='warning')
        return

    if new_step_name in State().current_workflow_model.workflow_model_steps:
        ui.notify(f"Node '{new_step_name}' already exists", type='negative')
        return

    State().save_workflow_model_copy()

    new_step = WorkflowModelStep(next_steps=set())
    State().current_workflow_model.workflow_model_steps[new_step_name] = new_step
    ui_elements.graph_component.add_node(new_step_name, new_step_name, NodeType.node_type_step)

    ui.notify(f"Added step '{new_step_name}'", type='positive')

    ui_elements.graph_controls_column.clear()
    with ui_elements.graph_controls_column:
        create_graph_controls()


def remove_step_action(node_to_remove: str):
    if not node_to_remove:
        ui.notify("Please indicate the step to remove", type='warning')
        return

    State().save_workflow_model_copy()

    del State().current_workflow_model.workflow_model_steps[node_to_remove]
    for (step_name, workflow_step) in State().current_workflow_model.workflow_model_steps.items():
        if node_to_remove in workflow_step.next_steps:
            workflow_step.next_steps.remove(node_to_remove)

    ui_elements.graph_component.remove_node(node_to_remove)

    ui.notify(f"Removed step '{node_to_remove}'", type='positive')

    ui_elements.graph_controls_column.clear()
    with ui_elements.graph_controls_column:
        create_graph_controls()


def create_graph_controls():
    with ui.card().classes('w-full'):
        ui.label('Workflow Model options').classes('text-lg font-semibold')

        ui.label('Add workflow step').classes('text-sm font-bold text-gray-600')
        with ui.row().classes('w-full items-center'):
            new_step_input = ui.input('Workflow step name').classes('flex-grow')
            ui.button('Add step', on_click=lambda: add_step_action(
                new_step_input.value
            ))

        ui.separator().classes('my-2')

        ui.label('Remove workflow step').classes('text-sm font-bold text-gray-600')
        with ui.row().classes('w-full items-center'):
            remove_step_select = ui.select(
                options=sorted(list(State().current_workflow_model.workflow_model_steps.keys())))
            ui.button('Remove step', on_click=lambda: remove_step_action(
                remove_step_select.value
            ))

        ui.separator().classes('my-2')

        ui.label('Connect steps').classes('text-sm font-bold text-gray-600')
        with ui.grid(columns=3).classes('w-full items-center gap-4'):
            with ui.column(align_items='center'):
                ui.label("Source step")
                source_node_input_add = ui.select(
                    options=sorted(list(State().current_workflow_model.workflow_model_steps.keys())))
            with ui.column(align_items='center'):
                ui.label("Target step")
                target_node_input_add = ui.select(
                    options=sorted(list(State().current_workflow_model.workflow_model_steps.keys())))

            with ui.column():
                ui.button('Connect', on_click=lambda:
                add_edge_action(source_node_input_add.value, target_node_input_add.value)
                          ).classes('w-full mt-2')

        ui.label('Disconnect steps').classes('text-sm font-bold text-gray-600')
        with ui.grid(columns=3).classes('w-full items-center gap-4'):
            with ui.column(align_items='center'):
                ui.label("Source step")
                source_node_input_remove = ui.select(
                    options=sorted(list(State().current_workflow_model.workflow_model_steps.keys())))
            with ui.column(align_items='center'):
                ui.label("Target step")
                target_node_input_remove = ui.select(
                    options=sorted(list(State().current_workflow_model.workflow_model_steps.keys())))

            with ui.column():
                ui.button('Disconnect', on_click=lambda:
                remove_edge_action(source_node_input_remove.value, target_node_input_remove.value)
                          ).classes('w-full mt-2').props("color=red")
