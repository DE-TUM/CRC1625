from copy import deepcopy

from nicegui import ui, app
from rdflib import URIRef

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore import UpdateType
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent, NodeType
from handover_workflows_validation_webui.middleware import matinf_or_demo_login_required, log_out
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_controls import create_graph_controls
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_page_state import WorkflowInstancePageState
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_step_controls import \
    create_workflow_instance_step_controls
from workflows_validation.CRC_1625_workflows_validator.CRC_1625_workflows_validator import CRC1625WorkflowModelStep, get_creator_user_id, dw_prefix
from workflows_validation.extra_functions import read_workflow_model, get_workflow_instances_assigned_to_model
from workflows_validation.workflows_validator import WorkflowModel, WorkflowInstance, is_workflow_instance_valid, generate_SHACL_shapes_for_workflow, \
    generate_validation_paths, ValidationJob


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


async def check_for_objects_with_multiple_paths(workflow_model, workflow_instance):
    validation_jobs = await generate_SHACL_shapes_for_workflow(workflow_model, workflow_instance)
    validation_paths: dict[URIRef, list[list[ValidationJob]]] = generate_validation_paths(workflow_model, validation_jobs)
    for entity_iri, paths in validation_paths.items():
        if len(paths) > 1:
            ui.notification("A Materials Library or Sample is assigned to non-consecutive workflow steps. Are you sure this is intended?",
                            type='warning',
                            timeout=None,
                            close_button=True)
            return


def handle_return_button(workflow_instance_page_state: WorkflowInstancePageState):
    if not workflow_instance_page_state.changes_are_saved and not app.storage.tab['demo_mode']:
        with ui.dialog() as return_dialog:
            with ui.card(align_items='center'):
                with ui.row(align_items='center').classes('w-full justify-center'):
                    ui.label('The workflow model has been modified. Save changes and exit?')

                    async def save_and_exit_and_close():
                        is_valid, msg = app.storage.tab['current_workflow_instance'].is_definition_valid(app.storage.tab['current_workflow_model'])
                        if not is_valid:
                            ui.notification(msg,
                                            type='negative',
                                            timeout=None,
                                            close_button=True)
                            return_dialog.close()
                        else:
                            await check_for_objects_with_multiple_paths(app.storage.tab['current_workflow_model'],
                                                                        app.storage.tab['current_workflow_instance'])
                            await rdf_datastore_client.launch_updates([(q, UpdateType.query) for q in app.storage.tab['current_workflow_instance'].get_overwrite_queries()])
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
    elif app.storage.tab['user_id'] != get_creator_user_id(app.storage.tab['current_workflow_instance']):
        ui.notify("You are not the owner of this workflow instance, so you cannot edit it.", type='negative')
    else:
        is_valid, msg = app.storage.tab['current_workflow_instance'].is_definition_valid(app.storage.tab['current_workflow_model'])
        if not is_valid:
            ui.notification(msg,
                            type='negative',
                            timeout=None,
                            close_button=True)
        else:
            await check_for_objects_with_multiple_paths(app.storage.tab['current_workflow_model'],
                                                        app.storage.tab['current_workflow_instance'])
            await rdf_datastore_client.launch_updates([(q, UpdateType.query) for q in app.storage.tab['current_workflow_instance'].get_overwrite_queries()])
            workflow_instance_page_state.changes_are_saved = True
            workflow_instance_page_state.original_workflow_instance = app.storage.tab['current_workflow_instance']
            ui.notify("The changes have been saved", type='positive')


async def run_validation(workflow_instance_page_state: WorkflowInstancePageState):
    # Remove the previous node colors
    workflow_instance_page_state.graph_component.clear_validation_results()

    validation_results = await is_workflow_instance_valid(app.storage.tab['current_workflow_model'],
                                                          app.storage.tab['current_workflow_instance'],
                                                          return_individual_results=True)

    # TODOs:
    #  - Show prettified ML / Sample names
    #  - Make the table scrollable

    # Update the nodes in the Cytoscape view
    colored_steps = set()
    for entity_iri, validation_paths in validation_results.items():
        for validation_path in validation_paths:
            for _, validation_results_for_step in validation_path.items():
                for validation_result in validation_results_for_step:
                    step_name = validation_result.validation_job.paired_step.workflow_model_step.name
                    if validation_result.conforms:
                        workflow_instance_page_state.graph_component.set_node_as_valid(step_name, "This step is valid")
                    elif validation_result.is_missing_data:
                        workflow_instance_page_state.graph_component.set_node_as_missing(step_name,"There is missing data for MLs / Samples assigned to this step")
                    else:
                        workflow_instance_page_state.graph_component.set_node_as_invalid(step_name, validation_result.pyshacl_output)

                    colored_steps.add(step_name)

    # All remaining workflow model steps did not have any object assigned to them
    for step in app.storage.tab['current_workflow_model'].workflow_model_steps.values():
        if step.name not in colored_steps:
            workflow_instance_page_state.graph_component.set_node_as_not_checked(step.name, "This step was not assigned to any ML/Sample")

    # Show the individual validation paths at the top
    table_rows = []
    for entity_iri, validation_paths in validation_results.items():
        steps_data = []
        for validation_path in validation_paths:
            for step_iri, validation_results_for_step in validation_path.items():
                for validation_result in validation_results_for_step:
                    trace_message = f'\nTarget node: {validation_result.validation_job.target_node}' if not validation_result.is_missing_data else ''

                    steps_data.append({
                        'name': str(validation_result.validation_job.paired_step.workflow_model_step.name),
                        'status': 'valid' if validation_result.conforms else ('missing_data' if validation_result.is_missing_data else 'invalid'),
                        'tooltip': f'This step is valid{trace_message}' if validation_result.conforms else (f'This step could not be matched to a handover group{trace_message}' if validation_result.is_missing_data else f'{validation_result.pyshacl_output}{trace_message}'),
                    })

        table_rows.append({
            'entity': str(entity_iri),
            'steps': steps_data
        })

    table_columns = [
        {'name': 'entity', 'label': 'Entity', 'field': 'entity', 'align': 'left', 'sortable': True},
        {'name': 'trace', 'label': 'Trace Steps', 'field': 'steps', 'align': 'left', 'sortable': False}
    ]

    with workflow_instance_page_state.validation_paths_row:
        workflow_instance_page_state.validation_paths_row.clear()

    if table_rows:
        table = ui.table(columns=table_columns, rows=table_rows).classes('w-full')

        # TODO the styling is a bit ugly, we cannot 100% mimic the border and background node styles from Cytoscape
        table.add_slot('body-cell-trace', '''
            <q-td :props="props">
                <div class="row items-center q-gutter-sm">
                    <div v-for="(step, index) in props.value" :key="index" class="row items-center no-wrap">
                        
                        <div class="column items-center cursor-pointer" style="width: 100px;">
                            
                            <div :style="step.status === 'valid' ? {
                                     backgroundColor: '#369c4e',
                                     backgroundImage: 'url(/assets/check_circle.svg)',
                                     backgroundSize: 'contain',
                                     backgroundRepeat: 'no-repeat',
                                     backgroundPosition: 'center',
                                     backgroundBlendMode: 'soft-light'
                                 } : step.status === 'invalid' ? {
                                     backgroundColor: '#dc7682',
                                     backgroundImage: 'url(/assets/error.svg)',
                                     backgroundSize: 'contain',
                                     backgroundRepeat: 'no-repeat',
                                     backgroundPosition: 'center',
                                     backgroundBlendMode: 'soft-light'
                                 } : {
                                     backgroundColor: '#e6b772',
                                     backgroundImage: 'url(/assets/error.svg)',
                                     backgroundSize: 'contain',
                                     backgroundRepeat: 'no-repeat',
                                     backgroundPosition: 'center',
                                     backgroundBlendMode: 'soft-light'
                                 }"
                                 class="rounded-borders text-white q-mb-xs"
                                 style="width: 100px; height: 100px; border-radius: 50%;">
                            </div>
                            
                            <div class="text-center text-grey-9 text-weight-medium" 
                                 style="font-size: 11px; line-height: 1.2; word-break: break-word; width: 100px; padding: 0 4px;">
                                {{ step.name }}
                            </div>

                            <q-tooltip class="bg-black text-body2" style="white-space: pre-line;" anchor="top middle" self="bottom middle" :offset="[10, 10]">
                                {{ step.tooltip }}
                            </q-tooltip>
                        </div>
                        
                        <q-icon v-if="index < props.value.length - 1" 
                                name="arrow_forward" 
                                size="sm" 
                                class="text-grey q-mx-xs" 
                                style="transform: translateY(-10px);" />
                    </div>
                </div>
            </q-td>
        ''')
    else:
        ui.label('No validation results available.')

@ui.page('/workflows/edit_workflow_instance/{workflow_model_uuid}/{workflow_instance_uuid}')
@matinf_or_demo_login_required
async def edit_workflow_instance_page(workflow_model_uuid: str,
                                      workflow_instance_uuid: str):
    await ui.context.client.connected()

    workflow_instance_iri = URIRef(dw_prefix[workflow_instance_uuid])

    workflow_instance_page_state = WorkflowInstancePageState()

    if app.storage.tab.get('current_workflow_model', None):  # The page has been reloaded
        app.storage.tab['current_workflow_model'] = await read_workflow_model(URIRef(dw_prefix[workflow_model_uuid]), rdf_datastore_client.launch_query)

    workflow_instances_of_current_workflow_model = await get_workflow_instances_assigned_to_model(app.storage.tab['current_workflow_model'], rdf_datastore_client.launch_query)
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

    workflow_instance_page_state.validation_paths_row = ui.row().classes('w-full items-center')

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
