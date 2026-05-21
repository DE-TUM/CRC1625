from copy import deepcopy

from nicegui import ui, app
from rdflib import URIRef

from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent, NodeType
from handover_workflows_validation_webui.middleware import matinf_or_demo_login_required, log_out
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_controls import create_graph_controls
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_page_state import WorkflowInstancePageState
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_step_controls import \
    create_workflow_instance_step_controls
from workflows_validation.CRC_1625_workflows_validator.CRC_1625_workflows_validator import CRC1625WorkflowModelStep, get_creator_user_id, crc_prefix
from workflows_validation.workflow_instance import overwrite_workflow_instance, get_workflow_instances_of_model
from workflows_validation.workflow_model import read_workflow_model
from workflows_validation.workflows_validator import WorkflowModel, WorkflowInstance, is_workflow_instance_valid


def workflow_model_and_instance_to_nodes_and_edges(workflow_model: WorkflowModel,
                                                   workflow_instance: WorkflowInstance,
                                                   workflow_instance_page_state: WorkflowInstancePageState):
    """
    Converts a workflow model to nodes and edges JSON that Cytoscape can consume

    TODO improve this: coloring, labels, alternative layout... (maybe not even having objects as nodes?)
    """
    nodes = list()
    edges = list()

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

    for assigned_step_iri, step_assignment in workflow_instance.step_assignments.items():
        for assigned_entity in step_assignment.assigned_entities:
            sample_object_id = str(workflow_instance_page_state.hnd_group_iri_to_sample_object_id[assigned_entity])
            # We set as the node's ids for coloring a single list containing 'object'
            if {
                'data': {
                    'id': f"ML / Sample {sample_object_id}",
                    'label': f"ML / Sample {sample_object_id}",
                    'identifiers_for_coloring': ['object']
                },
                'classes': [NodeType.node_type_step.value]
            } not in nodes:
                nodes.append({
                    'data': {
                        'id': f"ML / Sample {sample_object_id}",
                        'label': f'ML / Sample {sample_object_id}',
                        'identifiers_for_coloring': ['object']
                    },
                    'classes': [NodeType.node_type_object.value]
                })

            edges.append({
                'data': {
                    'source': workflow_model.workflow_model_steps[assigned_step_iri].name,
                    'target': f"ML / Sample {sample_object_id}"
                }
            })

    return {
        'nodes': nodes,
        'edges': edges
    }


def handle_node_click(e, workflow_instance_page_state: WorkflowInstancePageState):
    # node_id = e.get('id') # We use the label, as it may have been renamed
    node_label = e.get('label')

    if node_label in list(step.name for step in app.storage.tab['current_workflow_model'].workflow_model_steps.values()):
        workflow_instance_page_state.selected_node = node_label
        create_workflow_instance_step_controls(workflow_instance_page_state)
    else:
        ui.notify(f"Only workflow steps are selectable", type='warning')


def handle_workflow_instance_name_button(new_name: str):
    app.storage.tab['current_workflow_instance'].name = new_name


def handle_return_button(workflow_instance_page_state: WorkflowInstancePageState):
    if not workflow_instance_page_state.changes_are_saved and not app.storage.tab['demo_mode']:
        with ui.dialog() as return_dialog:
            with ui.card(align_items='center'):
                with ui.row(align_items='center').classes('w-full justify-center'):
                    ui.label('The workflow model has been modified. Save changes and exit?')

                    async def save_and_exit_and_close():
                        await overwrite_workflow_instance(app.storage.tab['current_workflow_instance'])
                        return_dialog.close()
                        ui.navigate.to('/workflows')

                    def navigate_without_saving():
                        return_dialog.close()
                        ui.navigate.to('/workflows')

                    ui.button('Save and exit', color='positive', on_click=save_and_exit_and_close)
                    ui.button('Exit without saving', color='negative', on_click=navigate_without_saving)
                    ui.button('Cancel', color='info', on_click=return_dialog.close)

        # 4. Open the dialog immediately after definition
        return_dialog.open()
    else:
        ui.navigate.to('/workflows')


def handle_undo_button(workflow_instance_page_state: WorkflowInstancePageState):
    workflow_instance_page_state.undo_workflow_instance_changes()

    # Unselect the node to prevent stale references
    workflow_instance_page_state.selected_node = ""

    # Reload Cytoscape
    graph_data = workflow_model_and_instance_to_nodes_and_edges(app.storage.tab['current_workflow_model'],
                                                                app.storage.tab['current_workflow_instance'])
    workflow_instance_page_state.graph_component_column.clear()
    with workflow_instance_page_state.graph_component_column:
        workflow_instance_page_state.graph_component = CytoscapeComponent(
            graph_data['nodes'],
            graph_data['edges'],
            handle_node_click,
            workflow_instance_page_state
        )

    # Reload the UI
    workflow_instance_page_state.graph_controls_column.clear()
    with workflow_instance_page_state.graph_controls_column:
        create_graph_controls(workflow_instance_page_state)

    workflow_instance_page_state.node_controls_column.clear()
    with workflow_instance_page_state.node_controls_column:
        create_workflow_instance_step_controls(workflow_instance_page_state)

    workflow_instance_page_state.graph_component.select_node(workflow_instance_page_state.selected_node)

    workflow_instance_page_state.workflow_instance_name_input.value = app.storage.tab['current_workflow_instance'].name

    ui.notify("All changes have been undone", type='positive')


async def handle_save_button(workflow_instance_page_state: WorkflowInstancePageState):
    if app.storage.tab['demo_mode']:
        ui.notify("You cannot save changes as a demo user", type='negative')
    if app.storage.tab['user_id'] != get_creator_user_id(app.storage.tab['current_workflow_instance']):
        ui.notify("You are not the owner of this workflow instance, so you cannot edit it.", type='negative')
    else:
        await overwrite_workflow_instance(app.storage.tab['current_workflow_instance'])
        workflow_instance_page_state.changes_are_saved = True
        workflow_instance_page_state.original_workflow_instance = app.storage.tab['current_workflow_instance']
        ui.notify("The changes have been saved", type='positive')


async def run_validation(workflow_instance_page_state: WorkflowInstancePageState):
    # Remove the previous node colors
    workflow_instance_page_state.graph_component.clear_validation_results()

    validation_results, steps_with_no_target_node = await is_workflow_instance_valid(app.storage.tab['current_workflow_model'],
                                                                                     app.storage.tab['current_workflow_instance'],
                                                                                     return_individual_results=True)

    # TODO we should distinguish between objects
    # TODO we can also show the individual validation path breakdowns across objects
    colored_steps = set()
    for entity_iri, validation_paths in validation_results.items():
        for validation_path in validation_paths:
            for validation_result in validation_path:
                step_name = validation_result.validation_job.paired_step.workflow_model_step.name
                # object_id = validation_result.step_to_validate.step_information.object_id
                if validation_result.conforms:
                    workflow_instance_page_state.graph_component.set_node_as_valid(step_name, "This step is valid")
                else:
                    workflow_instance_page_state.graph_component.set_node_as_invalid(step_name, validation_result.pyshacl_output)

                colored_steps.add(step_name)

    for step_with_no_target_node in steps_with_no_target_node:
        workflow_instance_page_state.graph_component.set_node_as_missing(step_with_no_target_node.workflow_model_step.name,
                                                                         # TODO show the actual ML / Sample ID instead of its first handover group
                                                                         f"ML / Sample with object ID {step_with_no_target_node.entity} had no matching handover group for this step")
        colored_steps.add(step_with_no_target_node.workflow_model_step.name)

    # All remaining workflow model steps did not have any object assigned to them
    for step in app.storage.tab['current_workflow_model'].workflow_model_steps.values():
        if step.name not in colored_steps:
            workflow_instance_page_state.graph_component.set_node_as_not_checked(step.name,
                                                                                 "This step was not assigned to any ML/Sample")


@ui.page('/workflows/edit_workflow_instance/{workflow_model_uuid}/{workflow_instance_uuid}')
@matinf_or_demo_login_required
async def edit_workflow_instance_page(workflow_model_uuid: str,
                                      workflow_instance_uuid: str):
    await ui.context.client.connected()

    workflow_instance_iri = URIRef(crc_prefix[workflow_instance_uuid])

    workflow_instance_page_state = WorkflowInstancePageState()

    if app.storage.tab.get('current_workflow_model', None):  # The page has been reloaded
        app.storage.tab['current_workflow_model'] = await read_workflow_model(URIRef(crc_prefix[workflow_model_uuid]))

    workflow_instances_of_current_workflow_model = await get_workflow_instances_of_model(app.storage.tab['current_workflow_model'])
    app.storage.tab['workflow_instances_of_current_workflow_model'] = list(workflow_instances_of_current_workflow_model.values())
    app.storage.tab['current_workflow_instance'] = workflow_instances_of_current_workflow_model[workflow_instance_iri]

    workflow_instance_page_state.original_workflow_instance = deepcopy(app.storage.tab['current_workflow_instance'])

    await workflow_instance_page_state.populate_sample_to_iri_correspondences()

    with ui.header().classes('items-center p-2 h-14'):
        ui.label("Workflow Instance Editor").classes('text-xl').style('color: #000000')
        ui.space()
        ui.label(f'Welcome, {app.storage.tab['user_name']} ({app.storage.tab['user_project']})').classes('text-xl').style('color: #000000')
        ui.button('Log out', color='negative', on_click=lambda: log_out()).props('size=m')
        ui.button('Return to the previous page', color='info', on_click=lambda: ui.navigate.to("/")).props('size=m')

    with ui.footer().classes('items-center p-2 h-11'):
        ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-m').style('color: #000000')
        ui.space()
        ui.image('/assets/crc_logo_black_letters_wide.png').classes('w-26')

    with ui.row().classes('w-full items-center'):
        workflow_instance_page_state.workflow_instance_name_input = ui.input(label='Workflow Instance name',
                                                                             value=app.storage.tab['current_workflow_instance'].name,
                                                                             on_change=lambda i: handle_workflow_instance_name_button(i.value)).classes('grow')
        ui.button('Return to main page', color='info', on_click=lambda: handle_return_button(workflow_instance_page_state))
        ui.button('Undo all changes', color='negative', on_click=lambda: handle_undo_button(workflow_instance_page_state))
        ui.button('Save all changes', color='positive', on_click=lambda: handle_save_button(workflow_instance_page_state))
        ui.button('Validate workflow', color='info', on_click=lambda: run_validation(workflow_instance_page_state))

    graph_data = workflow_model_and_instance_to_nodes_and_edges(app.storage.tab['current_workflow_model'],
                                                                app.storage.tab['current_workflow_instance'],
                                                                workflow_instance_page_state)

    with ui.grid(columns=1).classes('w-full gap-8'):
        graph_component_column = ui.column()
        with graph_component_column:
            graph_component = CytoscapeComponent(
                graph_data['nodes'],
                graph_data['edges'],
                handle_node_click,
                workflow_instance_page_state
            )

        with ui.grid(columns=2).classes('w-full gap-8'):
            graph_controls_column = ui.column()
            node_controls_column = ui.column()

        if graph_data['nodes']:
            workflow_instance_page_state.selected_node = app.storage.tab['current_workflow_model'].workflow_model_steps[
                app.storage.tab['current_workflow_model'].initial_step_iri].name

        workflow_instance_page_state.graph_component = graph_component
        workflow_instance_page_state.graph_component_column = graph_component_column
        workflow_instance_page_state.node_controls_column = node_controls_column
        workflow_instance_page_state.graph_controls_column = graph_controls_column

        with graph_controls_column:
            create_graph_controls(workflow_instance_page_state)

        with node_controls_column:
            create_workflow_instance_step_controls(workflow_instance_page_state)

        graph_component.select_node(workflow_instance_page_state.selected_node)
