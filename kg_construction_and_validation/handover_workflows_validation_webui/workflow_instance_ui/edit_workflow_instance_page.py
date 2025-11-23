from nicegui import ui

from handover_workflows_validation.handover_workflows_validation import read_workflow_model, WorkflowModel, \
    get_workflow_instances_of_model, WorkflowInstance, overwrite_workflow_instance
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent
from handover_workflows_validation_webui.state import state
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_controls import create_graph_controls
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_step_controls import \
    create_workflow_instance_step_controls


def workflow_model_and_instance_to_nodes_and_edges(workflow_model: WorkflowModel,
                                                   workflow_instance: WorkflowInstance):
    """
    Converts a workflow model to nodes and edges JSON that Cytoscape can consume

    TODO improve this: coloring, labels, alternative layout... (maybe not even having objects as nodes?)
    """
    nodes = list()
    edges = list()
    for step_name, step in workflow_model.workflow_model_steps.items():
        nodes.append({'data': {'id': step_name, 'label': step_name}})
        for next_step_name in step.next_steps:
            edges.append({'data': {'source': step_name, 'target': next_step_name}})

    for assigned_step_name, assigned_objects in workflow_instance.step_assignments.items():
        for assigned_object in assigned_objects:
            if {'data': {'id': assigned_object, 'label': assigned_object}} not in nodes:
                nodes.append({'data': {'id': assigned_object, 'label': f'ML / Sample {assigned_object}'}})

            edges.append({'data': {'source': assigned_step_name, 'target': assigned_object}})

    return {
        'nodes': nodes,
        'edges': edges
    }


def handle_node_click(e):
    node_id = e.get('id')
    node_label = e.get('label')

    if node_id in list(state.current_workflow_model.workflow_model_steps.keys()):
        state.selected_node = node_id
        create_workflow_instance_step_controls()
        ui.notify(f"Step selected: {node_label}", type='info')
    else:
        ui.notify(f"Only workflow steps are selectable", type='warning')


def save_and_exit():
    overwrite_workflow_instance(state.current_workflow_instance, state.user_id, state.store)
    ui.navigate.to('/')


def handle_return_button():
    if not state.changes_are_saved:
        with ui.dialog() as return_dialog:
            with ui.card(align_items='center'):
                with ui.row(align_items='center').classes('w-full justify-center'):
                    ui.label('The workflow model has been modified. Save changes and exit?')

                    def save_and_exit_and_close():
                        save_and_exit()
                        return_dialog.close()

                    def navigate_without_saving():
                        return_dialog.close()
                        ui.navigate.to('/')

                    ui.button('Save and exit', on_click=save_and_exit_and_close).props("color='green'")
                    ui.button('Exit without saving', on_click=navigate_without_saving).props("color='red'")
                    ui.button('Cancel', on_click=return_dialog.close)

        # 4. Open the dialog immediately after definition
        return_dialog.open()
    else:
        ui.navigate.to('/')


def handle_undo_button():
    if len(state.workflow_model_history) == 0:
        ui.notify("No changes have been performed yet!", type='warning')
    else:
        state.undo_workflow_model_change()

        # Reload Cytoscape
        graph_data = workflow_model_and_instance_to_nodes_and_edges(state.current_workflow_model,
                                                                    state.current_workflow_instance)
        state.graph_component_column.clear()
        with state.graph_component_column:
            state.graph_component = CytoscapeComponent(
                graph_data['nodes'],
                graph_data['edges'],
                state,
                handle_node_click
            )

        # Reload the UI
        state.graph_controls_column.clear()
        with state.graph_controls_column:
            create_graph_controls()

        state.node_controls_column.clear()
        with state.node_controls_column:
            create_workflow_instance_step_controls()

        state.graph_component.select_node(state.selected_node)
        ui.notify("The last change has been undone", type='positive')


def handle_save_button():
    overwrite_workflow_instance(state.current_workflow_instance, state.user_id, state.store)
    state.changes_are_saved = True
    state.workflow_model_history = []
    ui.notify("The changes have been saved", type='positive')


@ui.page('/edit_workflow_instance/{workflow_model_name}/{workflow_instance_name}/{user_id}')
async def edit_workflow_instance_page(workflow_model_name: str, workflow_instance_name: str, user_id: int):
    state.user_id = user_id  # TODO

    if state.current_workflow_model is None:  # The page has been reloaded
        state.current_workflow_model = read_workflow_model(workflow_model_name, user_id, state.store)  # TODO
        state.workflow_instances_of_current_workflow_model = get_workflow_instances_of_model(workflow_model_name,
                                                                                             user_id,
                                                                                             state.store)  # TODO
        state.current_workflow_instance = state.workflow_instances_of_current_workflow_model[
            (workflow_instance_name, user_id)]

    state.calculate_existing_objects()

    ui.label(f"Editing Workflow Instance '{state.current_workflow_instance.workflow_instance_name}'").classes(
        'text-2xl font-bold mb-4')
    with ui.grid(columns=3):
        with ui.column(align_items='stretch'):
            ui.button('Return to main page', on_click=lambda: handle_return_button())
        with ui.column(align_items='stretch'):
            ui.button('Undo last change', on_click=lambda: handle_undo_button()).props("color=red")
        with ui.column(align_items='stretch'):
            ui.button('Save all changes', on_click=lambda: handle_save_button()).props("color=green")

    graph_data = workflow_model_and_instance_to_nodes_and_edges(state.current_workflow_model,
                                                                state.current_workflow_instance)

    with ui.grid(columns=1).classes('w-full gap-8'):
        graph_component_column = ui.column()
        with graph_component_column:
            graph_component = CytoscapeComponent(
                graph_data['nodes'],
                graph_data['edges'],
                state,  # It does not depend on state for initialization
                handle_node_click
            )

        with ui.grid(columns=2).classes('w-full gap-8'):
            graph_controls_column = ui.column()
            node_controls_column = ui.column()

        if graph_data['nodes']:
            state.selected_node = state.current_workflow_model.workflow_model_options.initial_step_name

        state.graph_component = graph_component
        state.graph_component_column = graph_component_column
        state.node_controls_column = node_controls_column
        state.graph_controls_column = graph_controls_column

        with graph_controls_column:
            create_graph_controls()

        with node_controls_column:
            create_workflow_instance_step_controls()

        graph_component.select_node(state.selected_node)
