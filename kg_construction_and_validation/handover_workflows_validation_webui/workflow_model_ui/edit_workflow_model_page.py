from copy import deepcopy

from nicegui import ui, app
from rdflib import URIRef

from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent, NodeType
from handover_workflows_validation_webui.middleware import matinf_or_demo_login_required, log_out
from handover_workflows_validation_webui.workflow_model_ui.workflow_model_controls import create_graph_controls
from handover_workflows_validation_webui.workflow_model_ui.workflow_model_page_state import WorkflowModelPageState
from handover_workflows_validation_webui.workflow_model_ui.workflow_model_step_controls import \
    create_workflow_model_step_controls
from workflows_validation.CRC_1625_workflows_validator.CRC_1625_workflows_validator import CRC1625WorkflowModelStep
from workflows_validation.common import crc_prefix
from workflows_validation.workflow_model import WorkflowModel, overwrite_workflow_model, read_workflow_model


def workflow_model_to_nodes_and_edges(workflow_model: WorkflowModel):
    """
    Converts a workflow model to nodes and edges JSON that Cytoscape can consume
    """
    nodes = []
    edges = []
    for step_iri, step in workflow_model.workflow_model_steps.items():
        crc_1625_step = CRC1625WorkflowModelStep.from_step(step)
        nodes.append({
            'data': {
                'id': step.name,
                'label': step.name,
                'projects': crc_1625_step.get_allowed_project_names(),
                'activities': crc_1625_step.get_allowed_activity_names(),
                'identifiers_for_coloring': crc_1625_step.get_allowed_activity_names()
            },
            'classes': [NodeType.node_type_step.value]})
        for next_step_iri in crc_1625_step.next_steps:
            edges.append({
                'data': {
                    'source': step.name,
                    'target': workflow_model.workflow_model_steps[next_step_iri].name
                }
            })

    return {
        'nodes': nodes,
        'edges': edges
    }


def handle_node_click(e, workflow_model_page_state: WorkflowModelPageState):
    # node_id = e.get('id') # We use the label, as it may have been renamed
    node_label = e.get('label')

    workflow_model_page_state.selected_node = node_label
    create_workflow_model_step_controls(workflow_model_page_state)


def handle_workflow_model_name_button(new_name: str):
    app.storage.tab['current_workflow_model'].name = new_name


def can_current_workflow_model_be_saved():
    has_steps = len(app.storage.tab['current_workflow_model'].workflow_model_steps) == 0
    has_initial_step = app.storage.tab['current_workflow_model'].initial_step_iri != ""

    has_two_or_more_steps = len(app.storage.tab['current_workflow_model'].workflow_model_steps) >= 2

    pointed_at_step_iris = {dest for step in app.storage.tab['current_workflow_model'].workflow_model_steps.values() for dest in step.next_steps}
    has_unconnected_steps = any(
        len(step.next_steps) == 0 and step.iri not in pointed_at_step_iris  # Doesn't point at any step and is not pointed at either
        for step in app.storage.tab['current_workflow_model'].workflow_model_steps.values()
    )

    return ((has_steps or has_initial_step) and
            (has_two_or_more_steps and not has_unconnected_steps) or (not has_two_or_more_steps))


async def handle_return_button(workflow_model_page_state: WorkflowModelPageState):
    if not workflow_model_page_state.changes_are_saved and not app.storage.tab['demo_mode']:
        with ui.dialog() as return_dialog:
            with ui.card(align_items='center'):
                with ui.row(align_items='center').classes('w-full justify-center'):
                    ui.label('The workflow has been modified. Save changes and exit?')

                    async def save_and_exit_and_close():
                        return_dialog.close()

                        if can_current_workflow_model_be_saved():
                            await overwrite_workflow_model(app.storage.tab['current_workflow_model'],
                                                           workflow_model_page_state.original_workflow_model)
                            ui.navigate.to('/workflows')
                        else:
                            ui.notify("You must select an initial step first and have no unconnected steps if there are multiple of them", type='negative')

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


def handle_undo_button(workflow_model_page_state: WorkflowModelPageState):
    workflow_model_page_state.undo_workflow_model_changes()

    # Unselect the node to prevent stale references
    workflow_model_page_state.selected_node = ""

    # Reload Cytoscape
    graph_data = workflow_model_to_nodes_and_edges(app.storage.tab['current_workflow_model'])
    workflow_model_page_state.graph_component_column.clear()
    with workflow_model_page_state.graph_component_column:
        workflow_model_page_state.graph_component = CytoscapeComponent(
            graph_data['nodes'],
            graph_data['edges'],
            handle_node_click,
            workflow_model_page_state
        )

    # Reload the UI
    workflow_model_page_state.graph_controls_column.clear()
    with workflow_model_page_state.graph_controls_column:
        create_graph_controls(workflow_model_page_state)

    workflow_model_page_state.node_controls_column.clear()
    with workflow_model_page_state.node_controls_column:
        create_workflow_model_step_controls(workflow_model_page_state)

    workflow_model_page_state.graph_component.select_node(workflow_model_page_state.selected_node)

    workflow_model_page_state.workflow_model_name_input.value = app.storage.tab['current_workflow_model'].workflow_model_name

    ui.notify("All changes have been undone", type='positive')


async def handle_save_button(workflow_model_page_state: WorkflowModelPageState):
    if app.storage.tab['demo_mode']:
        ui.notify("You cannot save changes as a demo user", type='negative')
    else:
        if can_current_workflow_model_be_saved():
            await overwrite_workflow_model(app.storage.tab['current_workflow_model'],
                                           workflow_model_page_state.original_workflow_model)
            workflow_model_page_state.changes_are_saved = True
            workflow_model_page_state.original_workflow_model = app.storage.tab['current_workflow_model']
            ui.notify("The changes have been saved", type='positive')
        else:
            ui.notify("You must select an initial step first and have no unconnected steps if there are multiple of them", type='negative')


@ui.page('/workflows/edit_workflow_model/{workflow_model_uuid}')
@matinf_or_demo_login_required
async def edit_workflow_model_page(workflow_model_uuid: str):
    await ui.context.client.connected()

    workflow_model_page_state = WorkflowModelPageState()

    if app.storage.tab.get('current_workflow_model', None):  # The page has been reloaded
        app.storage.tab['current_workflow_model'] = await read_workflow_model(URIRef(crc_prefix[workflow_model_uuid]))
    workflow_model_page_state.original_workflow_model = deepcopy(app.storage.tab['current_workflow_model'])

    with ui.header().classes('items-center p-2 h-14'):
        ui.label("Workflow Editor").classes('text-xl').style('color: #000000')
        ui.space()
        ui.label(f'Welcome, {app.storage.tab['user_name']} ({app.storage.tab['user_project']})').classes('text-xl').style('color: #000000')
        ui.button('Log out', color='negative', on_click=lambda: log_out()).props('size=m')
        ui.button('Return to the previous page', color='info', on_click=lambda: ui.navigate.to("/")).props('size=m')

    with ui.footer().classes('items-center p-2 h-11'):
        ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-m').style('color: #000000')
        ui.space()
        ui.image('/assets/crc_logo_black_letters_wide.png').classes('w-26')

    with ui.row().classes('w-full items-center'):
        workflow_model_page_state.workflow_model_name_input = ui.input(label='Workflow name',
                                                                       value=app.storage.tab['current_workflow_model'].name,
                                                                       on_change=lambda i: handle_workflow_model_name_button(i.value)).classes('grow')
        ui.button('Return to main page', color='info', on_click=lambda: handle_return_button(workflow_model_page_state))
        ui.button('Undo all changes', color='negative', on_click=lambda: handle_undo_button(workflow_model_page_state))
        ui.button('Save all changes', color='positive', on_click=lambda: handle_save_button(workflow_model_page_state))

    graph_data = workflow_model_to_nodes_and_edges(app.storage.tab['current_workflow_model'])

    with ui.grid(columns=1).classes('w-full gap-8'):
        graph_component_column = ui.column()
        with graph_component_column:
            graph_component = CytoscapeComponent(
                graph_data['nodes'],
                graph_data['edges'],
                handle_node_click,
                workflow_model_page_state
            )

        graph_and_node_controls_grid = ui.grid(columns='auto auto').classes('w-full gap-8')
        with graph_and_node_controls_grid:
            graph_controls_column = ui.column()
            node_controls_column = ui.column()

        if graph_data['nodes']:
            workflow_model_page_state.selected_node = app.storage.tab['current_workflow_model'].workflow_model_steps[
                app.storage.tab['current_workflow_model'].initial_step_iri].name
        else:
            # We need to reset it anyways
            workflow_model_page_state.selected_node = ""

        workflow_model_page_state.graph_component = graph_component
        workflow_model_page_state.graph_component_column = graph_component_column
        workflow_model_page_state.node_controls_column = node_controls_column
        workflow_model_page_state.graph_controls_column = graph_controls_column

        with graph_controls_column:
            create_graph_controls(workflow_model_page_state)

        with node_controls_column:
            create_workflow_model_step_controls(workflow_model_page_state)

        graph_component.select_node(workflow_model_page_state.selected_node)
