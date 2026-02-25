from nicegui import ui

from handover_workflows_validation.handover_workflows_validation import read_workflow_model, WorkflowModel, \
    get_workflow_instances_of_model, WorkflowInstance, overwrite_workflow_instance, generate_SHACL_shapes_for_workflow, generate_data_graphs_for_workfow_steps, \
    validate_SHACL_rules
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent, NodeType
from handover_workflows_validation_webui.middleware import matinf_or_demo_login_required, log_out
from handover_workflows_validation_webui.shared_state import shared_state
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_controls import create_graph_controls
from handover_workflows_validation_webui.workflow_instance_ui.workflow_instance_page_state import WorkflowInstancePageState
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
        nodes.append({
            'data': {
                'id': step_name,
                'label': step_name,
                'projects': step.projects,
                'activities': step.required_activities,
                'identifiers_for_coloring': step.required_activities
            },
            'classes': [NodeType.node_type_step.value]
        })

        for next_step_name in step.next_steps:
            edges.append({'data': {'source': step_name, 'target': next_step_name}})

    for assigned_step_name, assigned_objects in workflow_instance.step_assignments.items():
        for assigned_object in assigned_objects:
            # We set as the node's ids for coloring a single list containing 'object'
            if {
                'data': {
                    'id': assigned_object,
                    'label': assigned_object,
                    'identifiers_for_coloring': ['object']
                },
                'classes': [NodeType.node_type_step.value]
            } not in nodes:
                nodes.append({
                    'data': {
                        'id': assigned_object,
                        'label': f'ML / Sample {assigned_object}',
                        'identifiers_for_coloring': ['object']
                    },
                    'classes': [NodeType.node_type_object.value]
                })

            edges.append({
                'data': {
                    'source': assigned_step_name,
                    'target': assigned_object
                }
            })

    return {
        'nodes': nodes,
        'edges': edges
    }


def handle_node_click(e, workflow_instance_page_state: WorkflowInstancePageState):
    #node_id = e.get('id') # We use the label, as it may have been renamed
    node_label = e.get('label')

    if node_label in list(shared_state().current_workflow_model.workflow_model_steps.keys()):
        workflow_instance_page_state.selected_node = node_label
        create_workflow_instance_step_controls(workflow_instance_page_state)
    else:
        ui.notify(f"Only workflow steps are selectable", type='warning')


def handle_workflow_instance_name_button(new_name: str, workflow_instance_page_state: WorkflowInstancePageState):
    workflow_instance_page_state.save_workflow_instance_copy()
    shared_state().current_workflow_instance.workflow_instance_name = new_name


def handle_return_button(workflow_instance_page_state: WorkflowInstancePageState):
    if not workflow_instance_page_state.changes_are_saved and not shared_state().demo_mode:
        with ui.dialog() as return_dialog:
            with ui.card(align_items='center'):
                with ui.row(align_items='center').classes('w-full justify-center'):
                    ui.label('The workflow model has been modified. Save changes and exit?')

                    async def save_and_exit_and_close():
                        await overwrite_workflow_instance(shared_state().current_workflow_instance, shared_state().current_workflow_model)
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
    if len(workflow_instance_page_state.workflow_instance_history) == 0:
        ui.notify("No changes have been performed yet", type='warning')
    else:
        workflow_instance_page_state.undo_workflow_instance_change()

        # Reload Cytoscape
        graph_data = workflow_model_and_instance_to_nodes_and_edges(shared_state().current_workflow_model,
                                                                    shared_state().current_workflow_instance)
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

        workflow_instance_page_state.workflow_instance_name_input.value = shared_state().current_workflow_instance.workflow_instance_name

        ui.notify("The last change has been undone", type='positive')


async def handle_save_button(workflow_instance_page_state: WorkflowInstancePageState):
    if shared_state().demo_mode:
        ui.notify("You cannot save changes as a demo user", type='negative')
    else:
        await overwrite_workflow_instance(shared_state().current_workflow_instance, shared_state().current_workflow_model)
        workflow_instance_page_state.changes_are_saved = True
        workflow_instance_page_state.workflow_model_history = []
        ui.notify("The changes have been saved", type='positive')


async def run_validation(workflow_instance_page_state: WorkflowInstancePageState):
    # Remove the previous node colors
    workflow_instance_page_state.graph_component.clear_validation_results()

    steps_to_validate, steps_with_no_target_node = await generate_SHACL_shapes_for_workflow(shared_state().current_workflow_model,
                                                                                            shared_state().current_workflow_instance)
    data_graphs = await generate_data_graphs_for_workfow_steps(steps_to_validate)
    results = validate_SHACL_rules(steps_to_validate, data_graphs)

    colored_steps = set()
    for validation_result in results:
        step_name = validation_result.step_to_validate.step_information.workflow_model_step.step_name
        # TODO we can additionally distinguish between objects
        #object_id = validation_result.step_to_validate.step_information.object_id
        if validation_result.conforms:
            workflow_instance_page_state.graph_component.set_node_as_valid(step_name, "This step is valid")
        else:
            workflow_instance_page_state.graph_component.set_node_as_invalid(step_name, validation_result.pyshacl_output)

        colored_steps.add(step_name)

    for step_with_no_target_node in steps_with_no_target_node:
        workflow_instance_page_state.graph_component.set_node_as_missing(step_with_no_target_node.workflow_model_step.step_name,
                                                                      f"ML / Sample with object ID {step_with_no_target_node.object_id} had no matching handover group for this step")
        colored_steps.add(step_with_no_target_node.workflow_model_step.step_name)

    # Same as above, with less detailed tooltips
    for step_name in shared_state().current_workflow_model.workflow_model_steps:
        if step_name not in colored_steps:
            workflow_instance_page_state.graph_component.set_node_as_not_checked(step_name,
                                                                              "No data was available to check this step")


@ui.page('/workflows/edit_workflow_instance/{workflow_model_name}/{workflow_model_creator_user_id}/{workflow_instance_name}/{workflow_instance_creator_user_id}')
@matinf_or_demo_login_required
async def edit_workflow_instance_page(workflow_model_name: str,
                                      workflow_model_creator_user_id: int,
                                      workflow_instance_name: str,
                                      workflow_instance_creator_user_id: int):
    workflow_instance_page_state = WorkflowInstancePageState()

    if shared_state().current_workflow_model is None:  # The page has been reloaded
        shared_state().current_workflow_model = await read_workflow_model(workflow_model_name, workflow_model_creator_user_id)

    shared_state().workflow_instances_of_current_workflow_model = await get_workflow_instances_of_model(shared_state().current_workflow_model)
    shared_state().current_workflow_instance = shared_state().workflow_instances_of_current_workflow_model[(workflow_instance_name, workflow_instance_creator_user_id)]

    workflow_instance_page_state.calculate_existing_objects()

    with ui.header().classes('items-center p-2 h-14'):
        ui.label("Workflow Instance Editor").classes('text-xl').style('color: #000000')
        ui.space()
        ui.label(f'Welcome, {shared_state().user_name} ({shared_state().user_project})').classes('text-xl').style('color: #000000')
        ui.button('Log out', color='negative', on_click=lambda: log_out()).props('size=m')
        ui.button('Return to the previous page', color='info', on_click=lambda: ui.navigate.to("/")).props('size=m')

    with ui.footer().classes('items-center p-2 h-11'):
        ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-m').style('color: #000000')
        ui.space()
        ui.image('/assets/crc_logo_black_letters_wide.png').classes('w-26')

    with ui.row().classes('w-full items-center'):
        workflow_instance_page_state.workflow_instance_name_input = ui.input(label='Workflow Instance name',
                                                                             value=shared_state().current_workflow_instance.workflow_instance_name,
                                                                             on_change=lambda i: handle_workflow_instance_name_button(i.value, workflow_instance_page_state)).classes('grow')
        ui.button('Return to main page', color='info', on_click=lambda: handle_return_button(workflow_instance_page_state))
        ui.button('Undo last change', color='negative', on_click=lambda: handle_undo_button(workflow_instance_page_state))
        ui.button('Save all changes', color='positive', on_click=lambda: handle_save_button(workflow_instance_page_state))
        ui.button('Validate workflow', color='info', on_click=lambda: run_validation(workflow_instance_page_state))

    graph_data = workflow_model_and_instance_to_nodes_and_edges(shared_state().current_workflow_model,
                                                                shared_state().current_workflow_instance)

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
            workflow_instance_page_state.selected_node = shared_state().current_workflow_model.workflow_model_options.initial_step_name

        workflow_instance_page_state.graph_component = graph_component
        workflow_instance_page_state.graph_component_column = graph_component_column
        workflow_instance_page_state.node_controls_column = node_controls_column
        workflow_instance_page_state.graph_controls_column = graph_controls_column

        with graph_controls_column:
            create_graph_controls(workflow_instance_page_state)

        with node_controls_column:
            create_workflow_instance_step_controls(workflow_instance_page_state)

        graph_component.select_node(workflow_instance_page_state.selected_node)
