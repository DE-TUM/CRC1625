from nicegui import ui, app

from handover_workflows_validation.handover_workflows_validation import WorkflowModelStep
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import NodeType
from handover_workflows_validation_webui.workflow_model_ui.workflow_model_page_state import WorkflowModelPageState


async def add_edge_action(source: str,
                          target: str,
                          workflow_model_page_state: WorkflowModelPageState):
    if not source or not target:
        ui.notify("Please enter both source and target steps", type='warning')
        return
    elif source == target:
        ui.notify("It is not possible to connect the steps to itself", type='negative')
        return
    elif await workflow_model_page_state.graph_component.exists_edge(source, target):
        ui.notify("The two steps are already connected", type='negative')
        return

    workflow_model_page_state.save_workflow_model_copy()

    workflow_model_page_state.graph_component.add_edge(source, target)
    app.storage.tab['current_workflow_model'].workflow_model_steps[source].next_steps.append(target)

    ui.notify(f"Added edge from '{source}' to '{target}'", type='positive')

    workflow_model_page_state.graph_controls_column.clear()
    with workflow_model_page_state.graph_controls_column:
        create_graph_controls(workflow_model_page_state)


async def remove_edge_action(source: str,
                             target: str,
                             workflow_model_page_state: WorkflowModelPageState):
    if not source or not target:
        ui.notify("Please enter both source and target steps", type='warning')
        return
    elif not await workflow_model_page_state.graph_component.exists_edge(source, target):
        ui.notify("The two steps are not connected", type='negative')
        return

    workflow_model_page_state.save_workflow_model_copy()

    workflow_model_page_state.graph_component.remove_edge(source, target)
    app.storage.tab['current_workflow_model'].workflow_model_steps[source].next_steps.remove(target)

    ui.notify(f"Removed edge from '{source}' to '{target}'", type='positive')

    workflow_model_page_state.graph_controls_column.clear()
    with workflow_model_page_state.graph_controls_column:
        create_graph_controls(workflow_model_page_state)


def add_step_action(new_step_name: str, workflow_model_page_state: WorkflowModelPageState):
    if not new_step_name:
        ui.notify("Please enter a step name", type='warning')
        return

    if new_step_name in app.storage.tab['current_workflow_model'].workflow_model_steps:
        ui.notify(f"Node '{new_step_name}' already exists", type='negative')
        return

    workflow_model_page_state.save_workflow_model_copy()

    new_step = WorkflowModelStep(next_steps=list())
    app.storage.tab['current_workflow_model'].workflow_model_steps[new_step_name] = new_step
    workflow_model_page_state.graph_component.add_node(new_step_name,
                                                       new_step_name,
                                                       NodeType.node_type_step,
                                                       coloring_ids=[''])

    ui.notify(f"Added step '{new_step_name}'", type='positive')

    workflow_model_page_state.graph_controls_column.clear()
    with workflow_model_page_state.graph_controls_column:
        create_graph_controls(workflow_model_page_state)


def set_initial_step_action(initial_step_node: str, workflow_model_page_state: WorkflowModelPageState):
    if not initial_step_node:
        ui.notify("Please indicate the initial step", type='warning')
        return

    for workflow_model_step in app.storage.tab['current_workflow_model'].workflow_model_steps.values():
        if initial_step_node in workflow_model_step.next_steps:
            ui.notify("The initial step node should not be preceded by any steps", type='negative')
            return

    workflow_model_page_state.save_workflow_model_copy()

    app.storage.tab['current_workflow_model'].workflow_model_options.initial_step_name = initial_step_node

    ui.notify(f"Set '{initial_step_node}' as the initial step", type='positive')

    workflow_model_page_state.graph_controls_column.clear()
    with workflow_model_page_state.graph_controls_column:
        create_graph_controls(workflow_model_page_state)


def remove_step_action(node_to_remove: str, workflow_model_page_state: WorkflowModelPageState):
    if not node_to_remove:
        ui.notify("Please indicate the step to remove", type='warning')
        return

    workflow_model_page_state.save_workflow_model_copy()

    del app.storage.tab['current_workflow_model'].workflow_model_steps[node_to_remove]
    for (step_name, workflow_step) in app.storage.tab['current_workflow_model'].workflow_model_steps.items():
        if node_to_remove in workflow_step.next_steps:
            workflow_step.next_steps.remove(node_to_remove)

    workflow_model_page_state.graph_component.remove_node(node_to_remove)

    ui.notify(f"Removed step '{node_to_remove}'", type='positive')

    workflow_model_page_state.graph_controls_column.clear()
    with workflow_model_page_state.graph_controls_column:
        create_graph_controls(workflow_model_page_state)


def create_graph_controls(workflow_model_page_state: WorkflowModelPageState):
    with ui.card().classes('w-full bg-secondary'):
        ui.label('Workflow Model options').classes('text-lg font-semibold')

        ui.label('Set initial workflow step').classes('text-sm font-bold text-gray-600')
        with ui.row().classes('w-full items-center'):
            if app.storage.tab['current_workflow_model'].workflow_model_options.initial_step_name:
                initial_step_select = ui.select(
                    options=sorted(list(app.storage.tab['current_workflow_model'].workflow_model_steps.keys())),
                    value=app.storage.tab['current_workflow_model'].workflow_model_options.initial_step_name)
            else:
                initial_step_select = ui.select(
                    options=sorted(list(app.storage.tab['current_workflow_model'].workflow_model_steps.keys())))
            ui.button('Set initial step', color='info', on_click=lambda: set_initial_step_action(
                initial_step_select.value,
                workflow_model_page_state
            ))

        ui.separator().classes('my-2')

        ui.label('Add workflow step').classes('text-sm font-bold text-gray-600')
        with ui.row().classes('w-full items-center'):
            new_step_input = ui.input('Workflow step name').classes('flex-grow')
            ui.button('Add step', color='info', on_click=lambda: add_step_action(
                new_step_input.value,
                workflow_model_page_state
            ))

        ui.separator().classes('my-2')

        ui.label('Remove workflow step').classes('text-sm font-bold text-gray-600')
        with ui.row().classes('w-full items-center'):
            remove_step_select = ui.select(
                options=sorted(list(app.storage.tab['current_workflow_model'].workflow_model_steps.keys())))
            ui.button('Remove step', color='negative', on_click=lambda: remove_step_action(
                remove_step_select.value,
                workflow_model_page_state
            ))

        ui.separator().classes('my-2')

        ui.label('Connect steps').classes('text-sm font-bold text-gray-600')
        with ui.grid(columns=3).classes('w-full items-center gap-4'):
            with ui.column(align_items='center'):
                ui.label("Source step")
                source_node_input_add = ui.select(
                    options=sorted(list(app.storage.tab['current_workflow_model'].workflow_model_steps.keys())))
            with ui.column(align_items='center'):
                ui.label("Target step")
                target_node_input_add = ui.select(
                    options=sorted(list(app.storage.tab['current_workflow_model'].workflow_model_steps.keys())))

            with ui.column():
                ui.button('Connect', color='info', on_click=lambda:
                add_edge_action(source_node_input_add.value, target_node_input_add.value, workflow_model_page_state)
                          ).classes('w-full mt-2')

        ui.label('Disconnect steps').classes('text-sm font-bold text-gray-600')
        with ui.grid(columns=3).classes('w-full items-center gap-4'):
            with ui.column(align_items='center'):
                ui.label("Source step")
                source_node_input_remove = ui.select(
                    options=sorted(list(app.storage.tab['current_workflow_model'].workflow_model_steps.keys())))
            with ui.column(align_items='center'):
                ui.label("Target step")
                target_node_input_remove = ui.select(
                    options=sorted(list(app.storage.tab['current_workflow_model'].workflow_model_steps.keys())))

            with ui.column():
                ui.button('Disconnect', color='negative', on_click=lambda:
                remove_edge_action(source_node_input_remove.value, target_node_input_remove.value, workflow_model_page_state)
                          ).classes('w-full mt-2')
