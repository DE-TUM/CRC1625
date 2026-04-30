from enum import Enum

from nicegui import ui, app
from rdflib import URIRef

from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import NodeType
from handover_workflows_validation_webui.workflow_model_ui.workflow_model_page_state import WorkflowModelPageState, get_iri_for_workflow_step_name
from workflows_validation.CRC_1625_workflows_validator.CRC_1625_workflows_validator import CRC1625WorkflowModelStep


def current_workflow_model_contains_cycles():
    """
    Checks if the current workflow model contains cycles by
    doing a DFS traversal
    """

    class visit_status(Enum):
        unvisited = 0
        visited_in_current_path = 1
        already_evaluated = 2

    status: dict[URIRef, visit_status] = {
        step_iri: visit_status.unvisited for step_iri in app.storage.tab['current_workflow_model'].workflow_model_steps.keys()
    }

    def visit_step(step_iri: URIRef) -> bool:
        if status[step_iri] == visit_status.visited_in_current_path:
            return True
        if status[step_iri] == visit_status.already_evaluated:
            return False

        status[step_iri] = visit_status.visited_in_current_path

        for next_step_iri in app.storage.tab['current_workflow_model'].workflow_model_steps[step_iri].next_steps:
            if visit_step(next_step_iri):
                return True

        status[step_iri] = visit_status.already_evaluated
        return False

    return visit_step(app.storage.tab['current_workflow_model'].initial_step_iri)


def get_reachable_steps(start_step_iri: URIRef) -> list[URIRef]:
    """
    Returns a list of all unique step IRIs reachable from the
    given starting step's IRI by doing a DFS traversal
    """
    reachable_steps: set[URIRef] = set()
    workflow_steps = app.storage.tab['current_workflow_model'].workflow_model_steps

    def traverse(current_iri: URIRef):
        if current_iri in reachable_steps:
            return

        reachable_steps.add(current_iri)

        step_data = workflow_steps.get(current_iri)
        if step_data:
            for next_step_iri in step_data.next_steps:
                traverse(next_step_iri)

    # Start traversal if the initial IRI exists in the model
    if start_step_iri in workflow_steps:
        traverse(start_step_iri)

    return list(reachable_steps)


async def add_edge_action(source: str,
                          target: str,
                          workflow_model_page_state: WorkflowModelPageState):
    source_iri = get_iri_for_workflow_step_name(source)
    target_iri = get_iri_for_workflow_step_name(target)

    if not source or not target:
        ui.notify("Please enter both source and target steps", type='warning')
        return
    elif source == target:
        ui.notify("It is not possible to connect the steps to itself", type='negative')
        return
    else:
        if target_iri in app.storage.tab['current_workflow_model'].workflow_model_steps[source_iri].next_steps:
            ui.notify("The two steps are already connected", type='negative')
            return

    app.storage.tab['current_workflow_model'].workflow_model_steps[source_iri].next_steps.append(target_iri)
    if current_workflow_model_contains_cycles():
        ui.notify("Creating cycles between steps is not allowed", type='negative')
        app.storage.tab['current_workflow_model'].workflow_model_steps[source_iri].next_steps.remove(target_iri)
        return

    workflow_model_page_state.graph_component.add_edge(source, target)

    ui.notify(f"Added edge from '{source}' to '{target}'", type='positive')

    workflow_model_page_state.graph_controls_column.clear()
    with workflow_model_page_state.graph_controls_column:
        create_graph_controls(workflow_model_page_state)


async def remove_edge_action(source: str,
                             target: str,
                             workflow_model_page_state: WorkflowModelPageState):
    source_iri = get_iri_for_workflow_step_name(source)
    target_iri = get_iri_for_workflow_step_name(target)

    if not source or not target:
        ui.notify("Please enter both source and target steps", type='warning')
        return
    elif not await workflow_model_page_state.graph_component.exists_edge(source, target):
        ui.notify("The two steps are not connected", type='negative')
        return

    workflow_model_page_state.graph_component.remove_edge(source, target)
    app.storage.tab['current_workflow_model'].workflow_model_steps[source_iri].next_steps.remove(target_iri)

    ui.notify(f"Removed edge from '{source}' to '{target}'", type='positive')

    workflow_model_page_state.graph_controls_column.clear()
    with workflow_model_page_state.graph_controls_column:
        create_graph_controls(workflow_model_page_state)


def add_step_action(new_step_name: str, workflow_model_page_state: WorkflowModelPageState):
    if not new_step_name:
        ui.notify("Please enter a step name", type='warning')
        return

    if get_iri_for_workflow_step_name(new_step_name) is not None:
        ui.notify(f"Node '{new_step_name}' already exists", type='negative')
        return

    new_step = CRC1625WorkflowModelStep()
    new_step.create_new_iri()
    new_step.name = new_step_name
    app.storage.tab['current_workflow_model'].workflow_model_steps[new_step.iri] = new_step
    workflow_model_page_state.graph_component.add_node(new_step_name,
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

    initial_step_iri = get_iri_for_workflow_step_name(initial_step_node)
    initial_step = app.storage.tab['current_workflow_model'].workflow_model_steps[initial_step_iri]

    for workflow_model_step in app.storage.tab['current_workflow_model'].workflow_model_steps.values():
        if initial_step_iri in workflow_model_step.next_steps:
            ui.notify("The initial step node should not be preceded by any steps", type='negative')
            return

    for step_after_initial_step in initial_step.next_steps:
        for workflow_model_step in app.storage.tab['current_workflow_model'].workflow_model_steps.values():
            if workflow_model_step.iri != initial_step_iri and step_after_initial_step in workflow_model_step.next_steps:
                ui.notify("The next step(s) after the initial step cannot have preceding steps", type='negative')
                return

    reachable_steps = get_reachable_steps(initial_step_iri)
    if len(reachable_steps) != len(app.storage.tab['current_workflow_model'].workflow_model_steps):
        ui.notify("There cannot be any unreachable steps from the initial step", type='negative')
        return

    app.storage.tab['current_workflow_model'].initial_step_iri = initial_step_iri

    ui.notify(f"Set '{initial_step_node}' as the initial step", type='positive')

    workflow_model_page_state.graph_controls_column.clear()
    with workflow_model_page_state.graph_controls_column:
        create_graph_controls(workflow_model_page_state)


def remove_step_action(node_to_remove: str, workflow_model_page_state: WorkflowModelPageState):
    if not node_to_remove:
        ui.notify("Please indicate the step to remove", type='warning')
        return

    step_to_remove_iri = get_iri_for_workflow_step_name(node_to_remove)

    if step_to_remove_iri == app.storage.tab['current_workflow_model'].initial_step_iri:
        ui.notify("You cannot delete the initial step. Please set another step as the initial one and try again.", type='warning')
        return

    # Remove entries from next steps
    del app.storage.tab['current_workflow_model'].workflow_model_steps[step_to_remove_iri]
    for (step_iri, workflow_step) in app.storage.tab['current_workflow_model'].workflow_model_steps.items():
        if step_to_remove_iri in workflow_step.next_steps:
            workflow_step.next_steps.remove(step_to_remove_iri)

    workflow_model_page_state.graph_component.remove_node(node_to_remove)

    ui.notify(f"Removed step '{node_to_remove}'", type='positive')

    workflow_model_page_state.graph_controls_column.clear()
    with workflow_model_page_state.graph_controls_column:
        create_graph_controls(workflow_model_page_state)


def create_graph_controls(workflow_model_page_state: WorkflowModelPageState):
    with ui.card().classes('w-full bg-secondary'):
        ui.label('Workflow options').classes('text-lg font-semibold')

        workflow_step_names = sorted([step.name for step in app.storage.tab['current_workflow_model'].workflow_model_steps.values()])

        ui.label('Set initial workflow step').classes('text-sm font-bold text-gray-600')
        with ui.row().classes('w-full items-center'):
            if app.storage.tab['current_workflow_model'].initial_step_iri:
                initial_step_select = ui.select(
                    options=workflow_step_names,
                    value=app.storage.tab['current_workflow_model'].workflow_model_steps[app.storage.tab['current_workflow_model'].initial_step_iri].name)
            else:
                initial_step_select = ui.select(
                    options=workflow_step_names)
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
                options=workflow_step_names)
            ui.button('Remove step', color='negative', on_click=lambda: remove_step_action(
                remove_step_select.value,
                workflow_model_page_state
            ))

        ui.separator().classes('my-2')

        workflow_step_names = sorted([step.name for step in app.storage.tab['current_workflow_model'].workflow_model_steps.values()])

        ui.label('Connect steps').classes('text-sm font-bold text-gray-600')
        with ui.grid(columns=3).classes('w-full items-center gap-4'):
            with ui.column(align_items='center'):
                ui.label("Source step")
                source_node_input_add = ui.select(
                    options=workflow_step_names)
            with ui.column(align_items='center'):
                ui.label("Target step")
                target_node_input_add = ui.select(
                    options=workflow_step_names)

            with ui.column():
                ui.button('Connect',
                          color='info',
                          on_click=lambda: add_edge_action(source_node_input_add.value, target_node_input_add.value, workflow_model_page_state)
                          ).classes('w-full mt-2')

        ui.label('Disconnect steps').classes('text-sm font-bold text-gray-600')
        with ui.grid(columns=3).classes('w-full items-center gap-4'):
            with ui.column(align_items='center'):
                ui.label("Source step")
                source_node_input_remove = ui.select(
                    options=workflow_step_names)
            with ui.column(align_items='center'):
                ui.label("Target step")
                target_node_input_remove = ui.select(
                    options=workflow_step_names)

            with ui.column():
                ui.button('Disconnect',
                          color='negative',
                          on_click=lambda: remove_edge_action(source_node_input_remove.value, target_node_input_remove.value, workflow_model_page_state)
                          ).classes('w-full mt-2')
