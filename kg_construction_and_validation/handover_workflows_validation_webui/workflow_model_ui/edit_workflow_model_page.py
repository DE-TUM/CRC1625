from nicegui import ui

from datastores.rdf.virtuoso_datastore import VirtuosoRDFDatastore
from handover_workflows_validation.handover_workflows_validation import read_workflow_model, WorkflowModel, \
    overwrite_workflow_model
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent, NodeType
from handover_workflows_validation_webui.state import State, ui_elements
from handover_workflows_validation_webui.workflow_model_ui.workflow_model_controls import create_graph_controls
from handover_workflows_validation_webui.workflow_model_ui.workflow_model_step_controls import \
    create_workflow_model_step_controls


def workflow_model_to_nodes_and_edges(workflow_model: WorkflowModel):
    """
    Converts a workflow model to nodes and edges JSON that Cytoscape can consume
    """
    nodes = []
    edges = []
    for step_name, step in workflow_model.workflow_model_steps.items():
        nodes.append({'data': {'id': step_name, 'label': step_name, 'activities': step.required_activities, 'identifiers_for_coloring': step.required_activities}, 'classes': [NodeType.node_type_step.value]})
        for next_step_name in step.next_steps:
            edges.append({'data': {'source': step_name, 'target': next_step_name}})

    return {
        'nodes': nodes,
        'edges': edges
    }


def handle_node_click(e):
    node_id = e.get('id')
    node_label = e.get('label')

    State().selected_node = node_id
    create_workflow_model_step_controls()
    ui.notify(f"Step selected: {node_label}", type='info')


def save_and_exit():
    overwrite_workflow_model(State().current_workflow_model, State().user_id, VirtuosoRDFDatastore())
    ui.navigate.to('/')


def handle_return_button():
    if not State().changes_are_saved:
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
    if len(State().workflow_model_history) == 0:
        ui.notify("No changes have been performed yet!", type='warning')
    else:
        State().undo_workflow_model_change()

        # Reload Cytoscape
        graph_data = workflow_model_to_nodes_and_edges(State().current_workflow_model)
        ui_elements.graph_component_column.clear()
        with ui_elements.graph_component_column:
            ui_elements.graph_component = CytoscapeComponent(
                graph_data['nodes'],
                graph_data['edges'],
                handle_node_click
            )

        # Reload the UI
        ui_elements.graph_controls_column.clear()
        with ui_elements.graph_controls_column:
            create_graph_controls()

        ui_elements.node_controls_column.clear()
        with ui_elements.node_controls_column:
            create_workflow_model_step_controls()

        ui_elements.graph_component.select_node(State().selected_node)
        ui.notify("The last change has been undone", type='positive')


def handle_save_button():
    overwrite_workflow_model(State().current_workflow_model, State().user_id, VirtuosoRDFDatastore())
    State().changes_are_saved = True
    State().workflow_model_history = []
    ui.notify("The changes have been saved", type='positive')


@ui.page('/edit_workflow_model/{workflow_model_name}/{user_id}')
async def edit_workflow_model_page(workflow_model_name: str, user_id: int):
    State().user_id = user_id  # TODO

    if State().current_workflow_model is None:  # The page has been reloaded
        State().current_workflow_model = read_workflow_model(workflow_model_name, user_id, VirtuosoRDFDatastore())

    ui.label(f"Editing Workflow Model '{workflow_model_name}'").classes('text-2xl font-bold mb-4')
    with ui.grid(columns=3):
        with ui.column(align_items='stretch'):
            ui.button('Return to main page', on_click=lambda: handle_return_button())
        with ui.column(align_items='stretch'):
            ui.button('Undo last change', on_click=lambda: handle_undo_button()).props("color=red")
        with ui.column(align_items='stretch'):
            ui.button('Save all changes', on_click=lambda: handle_save_button()).props("color=green")

    graph_data = workflow_model_to_nodes_and_edges(State().current_workflow_model)

    with ui.grid(columns=1).classes('w-full gap-8'):
        graph_component_column = ui.column()
        with graph_component_column:
            graph_component = CytoscapeComponent(
                graph_data['nodes'],
                graph_data['edges'],
                handle_node_click
            )

        graph_and_node_controls_grid = ui.grid(columns=2).classes('w-full gap-8')
        with graph_and_node_controls_grid:
            graph_controls_column = ui.column()
            node_controls_column = ui.column()

        if graph_data['nodes']:
            State().selected_node = State().current_workflow_model.workflow_model_options.initial_step_name

        ui_elements.graph_component = graph_component
        ui_elements.graph_component_column = graph_component_column
        ui_elements.node_controls_column = node_controls_column
        ui_elements.graph_controls_column = graph_controls_column

        with graph_controls_column:
            create_graph_controls()

        with node_controls_column:
            create_workflow_model_step_controls()

        graph_component.select_node(State().selected_node)
