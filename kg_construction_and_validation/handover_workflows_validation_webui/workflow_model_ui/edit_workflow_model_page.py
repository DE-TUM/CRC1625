from nicegui import ui
from nicegui.elements.input import Input

from handover_workflows_validation.handover_workflows_validation import read_workflow_model, WorkflowModel, \
    overwrite_workflow_model
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent, NodeType
from handover_workflows_validation_webui.state import ui_elements, get_state
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
        nodes.append({
            'data': {
                'id': step_name,
                'label': step_name,
                'projects': step.projects,
                'activities': step.required_activities,
                'identifiers_for_coloring': step.required_activities
            },
            'classes': [NodeType.node_type_step.value]})
        for next_step_name in step.next_steps:
            edges.append({'data': {'source': step_name, 'target': next_step_name}})

    return {
        'nodes': nodes,
        'edges': edges
    }


def handle_node_click(e):
    node_id = e.get('id')
    node_label = e.get('label')

    get_state().selected_node = node_id
    create_workflow_model_step_controls()
    ui.notify(f"Step selected: {node_label}", type='info')


def handle_workflow_model_name_button(new_name):
    get_state().save_workflow_model_copy()
    get_state().current_workflow_model.workflow_model_name = new_name


async def handle_return_button():
    if not get_state().changes_are_saved and not get_state().demo_mode:
        with ui.dialog() as return_dialog:
            with ui.card(align_items='center'):
                with ui.row(align_items='center').classes('w-full justify-center'):
                    ui.label('The workflow model has been modified. Save changes and exit?')

                    async def save_and_exit_and_close():
                        await overwrite_workflow_model(get_state().current_workflow_model)

                        return_dialog.close()
                        ui.navigate.to('/workflows')

                    async def navigate_without_saving():
                        return_dialog.close()
                        ui.navigate.to('/workflows')

                    ui.button('Save and exit', color='positive', on_click=save_and_exit_and_close)
                    ui.button('Exit without saving', color='negative', on_click=navigate_without_saving)
                    ui.button('Cancel', color='negative', on_click=return_dialog.close)

        # 4. Open the dialog immediately after definition
        return_dialog.open()
    else:
        ui.navigate.to('/workflows')


def handle_undo_button(workflow_model_name_input: Input):
    if len(get_state().workflow_model_history) == 0:
        ui.notify("No changes have been performed yet", type='warning')
    else:
        get_state().undo_workflow_model_change()

        # Reload Cytoscape
        graph_data = workflow_model_to_nodes_and_edges(get_state().current_workflow_model)
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

        ui_elements.graph_component.select_node(get_state().selected_node)

        workflow_model_name_input.value = get_state().current_workflow_model.workflow_model_name

        ui.notify("The last change has been undone", type='positive')


async def handle_save_button():
    if get_state().demo_mode:
        ui.notify("You cannot save changes as a demo user", type='negative')
    else:
        await overwrite_workflow_model(get_state().current_workflow_model)
        get_state().changes_are_saved = True
        get_state().workflow_model_history = []
        ui.notify("The changes have been saved", type='positive')


@ui.page('/workflows/edit_workflow_model/{workflow_model_name}/{user_id}')
async def edit_workflow_model_page(workflow_model_name: str, user_id: int):
    if get_state().current_workflow_model is None:  # The page has been reloaded
        get_state().current_workflow_model = await read_workflow_model(workflow_model_name, user_id)

    with ui.header().classes('items-center p-2 h-14'):
        ui.label("Workflow Model Editor").classes('text-xl').style('color: #000000')
        ui.space()
        if True:  # TODO integrate auth
            ui.label('Welcome, Sir SHACLot (demo user)').classes('text-xl').style('color: #000000')
            ui.button('Log out', color='negative', on_click=lambda: ui.navigate.to("/")).props('size=m').style('color: #000000')
        else:
            ui.button('Log in', color='info').props('size=m')
            ui.button('Log in (as demo user)', color='info').props('size=m')

    with ui.footer().classes('items-center p-2 h-11'):
        ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-m').style('color: #000000')
        ui.space()
        ui.image('/assets/crc_logo_black_letters_wide.png').classes('w-26')

    with ui.row().classes('w-full items-center'):
        workflow_model_name_input = ui.input(label='Workflow Model name',
                                             value=get_state().current_workflow_model.workflow_model_name,
                                             on_change=lambda i: handle_workflow_model_name_button(i.value)).classes('grow')
        ui.button('Return to main page', color='info', on_click=handle_return_button)
        ui.button('Undo last change', color='negative', on_click=lambda: handle_undo_button(workflow_model_name_input))
        ui.button('Save all changes', color='positive', on_click=handle_save_button)

    graph_data = workflow_model_to_nodes_and_edges(get_state().current_workflow_model)

    with ui.grid(columns=1).classes('w-full gap-8'):
        graph_component_column = ui.column()
        with graph_component_column:
            graph_component = CytoscapeComponent(
                graph_data['nodes'],
                graph_data['edges'],
                handle_node_click
            )

        graph_and_node_controls_grid = ui.grid(columns='auto auto').classes('w-full gap-8')
        with graph_and_node_controls_grid:
            graph_controls_column = ui.column()
            node_controls_column = ui.column()

        if graph_data['nodes']:
            get_state().selected_node = get_state().current_workflow_model.workflow_model_options.initial_step_name
        else:
            # We need to reset it anyways
            get_state().selected_node = ""

        ui_elements.graph_component = graph_component
        ui_elements.graph_component_column = graph_component_column
        ui_elements.node_controls_column = node_controls_column
        ui_elements.graph_controls_column = graph_controls_column

        with graph_controls_column:
            create_graph_controls()

        with node_controls_column:
            create_workflow_model_step_controls()

        graph_component.select_node(get_state().selected_node)
