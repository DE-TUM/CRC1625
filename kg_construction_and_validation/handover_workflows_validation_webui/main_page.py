import asyncio
import os
from dataclasses import dataclass, field

from nicegui import ui, app
from nicegui.elements.column import Column
from nicegui.elements.drawer import RightDrawer
from nicegui.elements.input import Input
from nicegui.elements.select import Select
from rdflib import URIRef

from datastores.rdf import rdf_datastore_client
from handover_workflows_validation_webui.common_functions import get_sample_object_id_of_handover_group_iri
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent
from handover_workflows_validation_webui.middleware import matinf_or_demo_login_required, activate_demo_mode, log_out, show_materialization_card
from handover_workflows_validation_webui.workflow_model_ui.edit_workflow_model_page import workflow_model_to_nodes_and_edges
from workflows_validation.CRC_1625_workflows_validator.CRC_1625_workflows_validator import get_creator_user_id, set_creator_user_id, CRC1625WorkflowModelStep, \
    crc_prefix
from workflows_validation.workflow_instance import WorkflowInstance, delete_workflow_instance, store_workflow_instance, get_workflow_instances_of_model, \
    StepAssignment, is_workflow_instance_definition_valid
from workflows_validation.workflow_model import store_workflow_model, read_workflow_model, WorkflowModel
from workflows_validation.workflows_validator import is_workflow_instance_valid

module_dir = os.path.dirname(__file__)
prefixes: str = open(os.path.join(module_dir, '../workflows_validation/queries/prefixes.sparql')).read()
details_all_users_query = prefixes + open(os.path.join(module_dir, 'queries/details_all_users.sparql'), 'r').read()
get_workflow_models_and_creators_query = prefixes + open(os.path.join(module_dir, 'queries/get_workflow_models_and_creators.sparql'), 'r').read()


@dataclass
class WorkflowsPageState:
    """
    State dataclass containing references to UI elements and contents of pages in this module.
    Initialized by accessing the main page and thus local to the user.
    """
    graph_component: CytoscapeComponent = None
    graph_component_column: Column = None
    main_content: Column = None
    right_drawer: RightDrawer = None
    search_input_workflow_models: Input = None
    creator_selector_workflow_models: Select = None
    workflow_models_table_container: Column = None
    workflow_model_details: dict[URIRef, tuple[str, str]] = field(default_factory=dict)
    filtered_workflow_instances_of_current_workflow_model: list[WorkflowInstance] = field(default_factory=list)
    workflow_instances_container: Column = None
    search_input_workflow_instances: Input = None
    sample_input_workflow_instances: Select = None


demo_warning_message = """
## **Welcome to the CRC 1625 Knowledge Graph demo!**
This demo contains **real mock data stored in the Virtuoso endpoint**, including two workflow models and three workflow instances:  

* *Demo Workflow* contains a workflow instance with missing data and a workflow instance with mismatching data.
* *Demo Workflow 2* contains a workflow instance that correctly matches and validates the data of all demo MLs and samples.

You can visualize the demo data diagrams by clicking the *Visualize data* button.

### The following functionalities are <span style="color:green">**enabled**</span>

* Browse Workflow models and instances.
* Access the workflow model editor. 
* Access and **validate** a workflow instance against its model within the workflow instance editor.  

###  The following functionalities are <span style="color:red">**disabled**</span>

* Creation of new workflow models and instances
* Saving changes inside the editors  

###  The following functionalities are still a <span style="color:orange">**Work In Progress**</span>:

* The Web UI style is not finalized and thus is subject to change. 
* The graph layout algorithms don't yet render workflow instances correctly as a hierarchical view.
* The workflow model and instance settings are handled via forms and controls. Interaction via the graph view itself (Cytoscape) is planned but not yet implemented.
"""


def edit_handover_workflow_instance_button_click():
    workflow_model_uuid = app.storage.tab['current_workflow_model'].iri.rsplit('/', 1)[-1]
    workflow_instance_uuid = app.storage.tab['current_workflow_instance'].iri.rsplit('/', 1)[-1]
    ui.navigate.to(f'/workflows/edit_workflow_instance/{workflow_model_uuid}/{workflow_instance_uuid}')


async def handle_workflow_instance_deletion(workflows_page_state: WorkflowsPageState):
    await delete_workflow_instance(app.storage.tab['current_workflow_instance'])
    ui.notify(f'Workflow Instance {app.storage.tab['current_workflow_instance'].name} deleted', color='positive')
    # Force the suer to select the workflow model again
    workflows_page_state.main_content.clear()
    workflows_page_state.right_drawer.clear()


async def delete_workflow_instance_button_click(workflows_page_state: WorkflowsPageState):
    if app.storage.tab['demo_mode']:
        ui.notify("You cannot delete workflow instances as a demo user", type='negative')
    elif app.storage.tab['user_id'] != get_creator_user_id(app.storage.tab['current_workflow_instance']):
        ui.notify(f"You are not the owner of this workflow instance, so you cannot delete it", type='negative')
    else:
        with ui.dialog() as workflow_instance_deletion_dialog:
            with ui.card(align_items='center'):
                with ui.row(align_items='center').classes('w-full justify-center'):
                    ui.label('Are you sure you want to delete this Workflow Instance?')
                    ui.button('Yes', color='positive', on_click=lambda: handle_workflow_instance_deletion(workflows_page_state))
                    ui.button('Cancel', color='negative', on_click=workflow_instance_deletion_dialog.close)

        workflow_instance_deletion_dialog.open()


def edit_handover_workflow_model_button_click():
    workflow_model_uuid = app.storage.tab['current_workflow_model'].iri.rsplit('/', 1)[-1]
    ui.navigate.to(f'/workflows/edit_workflow_model/{workflow_model_uuid}')


async def copy_handover_workflow_model(workflows_page_state: WorkflowsPageState):
    copy_of_current_workflow_model = app.storage.tab['current_workflow_model'].create_copy()
    set_creator_user_id(copy_of_current_workflow_model, app.storage.tab['user_id'])

    await store_workflow_model(copy_of_current_workflow_model)

    ui.notify(f'Workflow copied as {copy_of_current_workflow_model.name}', color='positive')

    # Add it to the left sidebar's table
    workflows_page_state.workflow_model_details[copy_of_current_workflow_model.iri] = (copy_of_current_workflow_model.name,
                                                                                       str(get_creator_user_id(copy_of_current_workflow_model)))

    # Apply the filters again and show it
    populate_workflow_models_table(workflows_page_state)


def handle_workflow_instance_table_click(workflow_instance: WorkflowInstance, workflows_page_state: WorkflowsPageState):
    """
    Workflow instance edit options
    """
    workflows_page_state.right_drawer.clear()

    app.storage.tab['current_workflow_instance'] = workflow_instance

    with workflows_page_state.right_drawer:
        right_drawer_label = ui.label('Workflow instance options').classes('text-xl font-bold')

        with ui.column().classes('w-full items-center gap-2'):
            ui.button("View Workflow Instance", color='info').classes('w-full p-0').on_click(
                lambda: edit_handover_workflow_instance_button_click()
            )
            ui.button("Delete Workflow Instance", color='negative').classes('w-full p-0').on_click(
                lambda: delete_workflow_instance_button_click(workflows_page_state)
            )

        right_drawer_label.set_text(
            f"Workflow instance '{app.storage.tab['current_workflow_instance'].name}' options")

    ui.notify(f'Selected Workflow Instance {app.storage.tab['current_workflow_instance'].name}', color='info')


def populate_workflow_models_table(workflows_page_state: WorkflowsPageState):
    search_term = "" if workflows_page_state.search_input_workflow_models.value is None else workflows_page_state.search_input_workflow_models.value.lower()

    filtered_workflow_models_list = list(workflows_page_state.workflow_model_details.items())

    # Filter them by name
    if search_term:
        filtered_workflow_models_list = [
            (uri, (name, user_id)) for (uri, (name, user_id)) in filtered_workflow_models_list if search_term in name.lower()
        ]

    # Filter them by user ID
    if workflows_page_state.creator_selector_workflow_models.value:
        filtered_workflow_models_list = [
            (uri, (name, user_id)) for (uri, (name, user_id)) in filtered_workflow_models_list if
            str(workflows_page_state.creator_selector_workflow_models.value) == user_id
        ]

    # Sort them by name
    filtered_workflow_models_list.sort(key=lambda x: x[1][0].lower())

    # Show them
    workflows_page_state.workflow_models_table_container.clear()

    with (workflows_page_state.workflow_models_table_container):
        for (uri, (name, user_id)) in filtered_workflow_models_list:
            with ui.button(on_click=lambda f, u=uri: show_workflow_model_instances(u, workflows_page_state)
                           ).props('no-caps unelevated').classes('w-full'):
                with ui.row().classes('w-full py-1 items-center'):
                    ui.label(str(name)).classes('w-1/2 text-left').style('color: #000000')
                    ui.space()
                    ui.label(str(user_id)).classes('w-0 flex-grow text-right').style('color: #000000')


async def create_empty_workflow_instance(workflow_instance_name: str,
                                         workflows_page_state: WorkflowsPageState):
    if app.storage.tab['demo_mode']:
        ui.notify("You cannot create Workflow Instances as a demo user", type='negative')
        return
    elif app.storage.tab['user_id'] != get_creator_user_id(app.storage.tab['current_workflow_model']):
        ui.notify(f"You are not the owner of this workflow, so you cannot add workflow instances to it. You can create a copy of it", type='negative')
        return

    workflow_instance = WorkflowInstance()
    workflow_instance.create_new_iri()
    workflow_instance.name = workflow_instance_name
    set_creator_user_id(workflow_instance, app.storage.tab['user_id'])
    workflow_instance.workflow_model_iri = app.storage.tab['current_workflow_model'].iri

    # We must add step assignments, even if they don't hold references to any objects
    for step_iri in app.storage.tab['current_workflow_model'].workflow_model_steps.keys():
        workflow_instance.step_assignments[step_iri] = StepAssignment()
        workflow_instance.step_assignments[step_iri].create_new_iri()
        workflow_instance.step_assignments[step_iri].workflow_step_iri = step_iri
        workflow_instance.step_assignments[step_iri].property_to_follow = crc_prefix.nextStep

    is_valid, msg = await is_workflow_instance_definition_valid(app.storage.tab['current_workflow_instance'])
    if not is_valid:
        ui.notify(msg, type='negative')
    else:
        await store_workflow_instance(workflow_instance)

        # Show them again
        await show_workflow_model_instances(app.storage.tab['current_workflow_model'].iri,
                                            workflows_page_state)

        ui.notify(f'Workflow Instance {workflow_instance_name} created', color='positive')


async def populate_workflow_instances_table(workflows_page_state: WorkflowsPageState):
    workflows_page_state.workflow_instances_container.clear()

    with workflows_page_state.workflow_instances_container:
        # Workflow instances list
        with ui.row().classes('w-full border-b-2 border-gray-400 py-1 font-bold'):
            ui.label('Workflow Instance name').classes('w-1/3 text-left')
            ui.label('Associated Materials libraries or Samples').classes('w-1/3 text-left')
            ui.label('Validation status').classes('w-0 flex-grow text-left')

        async def validate_workflow(validation_icon_column, workflow_model, workflow_instance):
            """
            Runs validation for the workflow model and instance pair, and updates the corresponding icon according to the results
            """
            validation_status = await is_workflow_instance_valid(workflow_model,
                                                                 workflow_instance,
                                                                 return_individual_results=False)

            validation_icon_column.clear()
            with validation_icon_column:
                with ui.row():
                    if validation_status == validation_status.Valid:
                        ui.icon('check_circle').classes('text-green-6')
                    elif validation_status == validation_status.Warning:
                        ui.icon('warning').classes('text-orange-6')
                    else:
                        ui.icon('error').classes('text-red-6')

                    with ui.icon('o_help').classes('text-sm'):
                        ui.tooltip(validation_status.description)

        # First pass over workflow instances to populate the sample ID filter
        search_input_workflow_instances_options: list[str] = []
        handover_group_iri_to_sample_id: dict[URIRef, int] = dict()
        for workflow_instance in app.storage.tab['workflow_instances_of_current_workflow_model']:
            search_input_workflow_instances_options.append(workflow_instance.name)
            for step_assignment in workflow_instance.step_assignments.values():
                for assigned_entity in step_assignment.assigned_entities:
                    handover_group_iri_to_sample_id[assigned_entity] = await get_sample_object_id_of_handover_group_iri(assigned_entity)

        workflows_page_state.search_input_workflow_instances.set_autocomplete(search_input_workflow_instances_options)
        workflows_page_state.sample_input_workflow_instances.set_options(handover_group_iri_to_sample_id)

        # Second pass over the workflow instances to show the able and run validation jobs
        validation_jobs = []
        for workflow_instance in app.storage.tab['workflow_instances_of_current_workflow_model']:
            # Filter by name
            if workflows_page_state.search_input_workflow_instances.value:
                if workflows_page_state.search_input_workflow_instances.value not in workflow_instance.name.lower():
                    continue

            if workflows_page_state.sample_input_workflow_instances.value:
                has_sample = False
                for step_assignment in workflow_instance.step_assignments.values():
                    for selection_value in workflows_page_state.sample_input_workflow_instances.value:
                        if selection_value in step_assignment.assigned_entities:
                            has_sample = True
                            break
                    if has_sample:
                        break

                if not has_sample:
                    continue

            with ui.button(on_click=lambda r=workflow_instance: handle_workflow_instance_table_click(r, workflows_page_state)).props(
                    'no-caps unelevated color=secondary').classes('w-full p-1'):
                with ui.row().classes('w-full py-1 items-center'):
                    ui.label(workflow_instance.name).classes('w-1/3 text-left').style('color: #000000')

                    associated_objects = set()
                    for step_assignment in workflow_instance.step_assignments.values():
                        for assigned_entity in step_assignment.assigned_entities:
                            associated_objects.add(str(await get_sample_object_id_of_handover_group_iri(assigned_entity)))

                    ui.label(', '.join(sorted(associated_objects))).classes('w-1/3 text-left').style('color: #000000')

                    # The third column is shown as a spinner until the async validation jobs are run
                    validation_icon_column = ui.column().classes('w-0 flex-grow text-left').style('color: #000000')
                    with validation_icon_column:
                        ui.spinner()

                    validation_jobs.append(validate_workflow(validation_icon_column, app.storage.tab['current_workflow_model'], workflow_instance))

        if not app.storage.tab['demo_mode']:
            ui.button("Create a new workflow instance", color='info',
                      on_click=lambda: create_empty_workflow_instance(
                          f"New workflow instance of {app.storage.tab['current_workflow_model'].name}",
                          workflows_page_state))
        else:
            ui.button("Add a new workflow instance", color='gray',
                      on_click=lambda: ui.notify("You cannot create new workflow instances as a demo user", type='negative'))

        # Run validation and update the spinners
        await asyncio.gather(*validation_jobs)


async def show_workflow_model_instances(workflow_model_iri: URIRef,
                                        workflows_page_state: WorkflowsPageState):
    """
    Workflow instances of the selected workflow model from the left drawer, also allowing to edit or copy the model and to create a new instance. Empty until so
    """
    # Load the selected workflow model and its instances
    app.storage.tab['current_workflow_model'] = await read_workflow_model(workflow_model_iri)
    workflow_instances_of_current_workflow_model = await get_workflow_instances_of_model(app.storage.tab['current_workflow_model'])
    app.storage.tab['workflow_instances_of_current_workflow_model'] = list(workflow_instances_of_current_workflow_model.values())
    workflows_page_state.filtered_workflow_instances_of_current_workflow_model = app.storage.tab['workflow_instances_of_current_workflow_model']

    workflow_model_name = app.storage.tab['current_workflow_model'].name

    workflows_page_state.main_content.clear()
    with workflows_page_state.main_content:
        with ui.row():
            if len(workflow_model_name) > 100:
                ui.label(f"Overview of '{workflow_model_name[0:100] + "..."}'").classes('text-lg font-semibold')
            else:
                ui.label(f"Overview of '{workflow_model_name}'").classes('text-lg font-semibold')

            # Workflow model edit and copy buttons
            with ui.row():
                if get_creator_user_id(app.storage.tab['current_workflow_model']) == app.storage.tab['user_id']:
                    ui.button("Edit Workflow", color='info').on_click(
                        lambda: edit_handover_workflow_model_button_click()
                    )
                else:
                    ui.button("Edit Workflow", color='gray').on_click(
                        lambda: ui.notify("You are not the owner of this workflow, so you cannot edit it. You can create a copy of it", type='negative')
                    )

                if not app.storage.tab['demo_mode']:
                    ui.button("Create a copy", color='info').on_click(
                        lambda: copy_handover_workflow_model(workflows_page_state)
                    )
                else:
                    ui.button("Create a copy", color='gray').on_click(
                        lambda: ui.notify("You cannot copy workflows as a demo user", type='negative')
                    )

        # Workflow model overview
        with ui.grid(columns=1).classes('w-full gap-8'):
            workflows_page_state.graph_component_column = ui.column()
            with workflows_page_state.graph_component_column:
                graph_data = workflow_model_to_nodes_and_edges(app.storage.tab['current_workflow_model'])
                workflows_page_state.graph_component = CytoscapeComponent(
                    graph_data['nodes'],
                    graph_data['edges'],
                    lambda: None,
                    None
                )

        # Workflow instances view
        with ui.row():
            ui.label("Workflow instances").classes('text-lg font-semibold')

        # Search and filtering
        with ui.row():
            with ui.column():
                ui.label("Filter by workflow instance name:")
                workflows_page_state.search_input_workflow_instances = ui.input(placeholder='Name...').props('clearable').classes('w-full')
                workflows_page_state.search_input_workflow_instances.on_value_change(lambda: populate_workflow_instances_table(workflows_page_state))
            with ui.column():
                ui.label("Filter by Sample ID:")
                workflows_page_state.sample_input_workflow_instances = ui.select(with_input=True,
                                                                                 multiple=True,
                                                                                 clearable=True,
                                                                                 options=[]).on_value_change(
                    lambda: populate_workflow_instances_table(workflows_page_state))

        workflows_page_state.workflow_instances_container = ui.column().classes('w-full')

    await populate_workflow_instances_table(workflows_page_state)


async def get_workflow_model_names_and_creator_user_ids() -> dict[URIRef, tuple[str, str]]:
    workflow_details: dict[URIRef, tuple[str, str]] = dict()

    result = await rdf_datastore_client.launch_query(get_workflow_models_and_creators_query)
    results = result["results"]["bindings"]
    for result in results:
        workflow_model_iri = URIRef(result["workflow_model"]["value"])
        workflow_model_name = result["workflow_model_name"]["value"]
        user_id = result["user_id"]["value"]

        workflow_details[workflow_model_iri] = (workflow_model_name, user_id)

    return workflow_details


async def create_workflows_model_left_drawer(workflows_page_state: WorkflowsPageState):
    """
    Workflow models table, allowing the user to search/filter workflow models, to select one of them or to create a new one
    """
    ui.label('Workflows list').classes('text-xl font-bold')

    # Search and filtering
    async def get_user_details() -> dict[int, tuple[str, str]]:
        user_details: dict[int, tuple[str, str]] = dict()

        result = await rdf_datastore_client.launch_query(details_all_users_query)
        results = result["results"]["bindings"]
        for result in results:
            user_id = int(result["user_id"]["value"])
            name = result["user_name"]["value"]
            project = result["project_name"]["value"]

            user_details[user_id] = (name, project)

        return user_details

    async def create_empty_workflow_model(workflow_model_name: str,
                                          workflows_page_state: WorkflowsPageState):
        if app.storage.tab['demo_mode']:
            ui.notify("You cannot create new workflows as a demo user", type='negative')
            return

        workflow_model = WorkflowModel()
        workflow_model.create_new_iri()
        workflow_model.name = workflow_model_name
        set_creator_user_id(workflow_model, app.storage.tab['user_id'])

        # Add an initial, empty step
        workflow_model_step = CRC1625WorkflowModelStep()
        workflow_model_step.create_new_iri()
        workflow_model_step.name = "Initial step"
        workflow_model_step.description = "The starting point of the workflow"
        workflow_model.workflow_model_steps[workflow_model_step.iri] = workflow_model_step
        workflow_model.initial_step_iri = workflow_model_step.iri

        await store_workflow_model(workflow_model)

        workflows_page_state.workflow_model_details[workflow_model.iri] = (workflow_model_name, str(app.storage.tab['user_id']))

        # Apply the filters again and show it
        populate_workflow_models_table(workflows_page_state)

        ui.notify(f'Workflow {workflow_model_name} created', color='positive')

    with ui.column():
        ui.label("Filter by workflow name:")
        workflows_page_state.search_input_workflow_models = ui.input(placeholder='Name...').props('clearable').classes('w-full')

    with ui.column():
        ui.label("Filter by owner:")
        user_details = await get_user_details()
        creator_selector_dict = {
            user_id: f"{f'{user_name} (You)' if user_id == app.storage.tab['user_id'] else user_name} ({project.replace('_', ', ')})"
            for user_id, (user_name, project) in sorted(
                user_details.items(),
                # Alphabetical sort based on project and then user name
                key=lambda item: (item[1][1], item[1][0])
            )
        }
        workflows_page_state.creator_selector_workflow_models = ui.select(options=creator_selector_dict,
                                                                          with_input=True,
                                                                          clearable=True,
                                                                          value=app.storage.tab['user_id'])

    workflows_page_state.creator_selector_workflow_models.on_value_change(lambda: populate_workflow_models_table(workflows_page_state))
    workflows_page_state.search_input_workflow_models.on_value_change(lambda: populate_workflow_models_table(workflows_page_state))

    with ui.row().classes('w-full border-b-2 border-gray-400 py-2 font-bold'):
        ui.label("Workflows").classes('align-center')

    workflows_page_state.workflow_models_table_container = ui.column().classes('w-full')
    workflows_page_state.workflow_model_details = await get_workflow_model_names_and_creator_user_ids()
    populate_workflow_models_table(workflows_page_state)

    if not app.storage.tab['demo_mode']:
        ui.button("Add a new workflow",
                  color='info',
                  on_click=lambda: create_empty_workflow_model("New workflow", workflows_page_state))
    else:
        ui.button("Create a new workflow",
                  color='gray',
                  on_click=lambda: ui.notify("You cannot create new workflows as a demo user", type='negative'))


@ui.page('/workflows')
@matinf_or_demo_login_required
async def workflows_page():
    """
    Main workflows page
        - Left drawer: Workflow models table, allowing the user to search/filter workflow models, to select one of them or to create a new one
        - Main content: Workflow instances of the selected workflow model from the left drawer, also allowing to edit or copy the model and to create a new instance. Empty until so
        - Right drawer: Workflow instance edit options
    """
    await ui.context.client.connected()

    if await rdf_datastore_client.is_materialization_active():
        show_materialization_card(lambda: ui.navigate.reload())
    else:
        if app.storage.tab['demo_mode']:
            def show_demo_data_diagram():
                with ui.dialog().classes('w-full h-full') as demo_data_diagram:
                    with ui.card(align_items='center').classes('w-full h-full').style('max-width: 90%; max-height: 90%'):
                        with ui.row().classes('w-full h-full'):
                            ui.html(
                                f'<embed src="/assets/diagrams/demo_data.pdf" type="application/pdf" style="width:100%; height:100%; border:none;">',
                                sanitize=False
                            ).classes('w-full h-full')

                        with ui.row():
                            ui.button('Close', color='positive', on_click=lambda: demo_data_diagram.close())

                demo_data_diagram.open()

            with ui.dialog() as demo_warning_dialog:
                with ui.card(align_items='center').classes('w-full').style('max-width: 60%'):
                    with ui.row(align_items='center').classes('w-full justify-center'):
                        ui.markdown(demo_warning_message)

                    with ui.row():
                        ui.button('Visualize data', color='info', on_click=lambda: show_demo_data_diagram())
                        ui.button('Understood', color='positive', on_click=lambda: demo_warning_dialog.close())

            demo_warning_dialog.open()

        workflows_page_state = WorkflowsPageState()
        workflows_page_state.main_content = ui.column().classes('w-full')

        with ui.header().classes('items-center p-2 h-14'):
            ui.label('Handover workflows validation prototype UI').classes('text-xl').style('color: #000000')
            ui.space()
            ui.label(f'Welcome, {app.storage.tab['user_name']} ({app.storage.tab['user_project']})').classes('text-xl').style('color: #000000')
            ui.button('Log out', color='negative', on_click=lambda: log_out()).props('size=m')
            ui.button('Return to the previous page', color='info', on_click=lambda: ui.navigate.to("/")).props('size=m')

        with ui.footer().classes('items-center p-2 h-11'):
            ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-m').style('color: #000000')
            ui.space()
            ui.image('/assets/crc_logo_black_letters_wide.png').classes('w-26')

        workflows_page_state.right_drawer = ui.right_drawer().classes('bg-secondary')
        # TODO: We can hide it and show it only when clicked, but for now the graph component cannot adapt to the new width when doing so
        # workflows_page_state.right_drawer.hide()

        with ui.left_drawer().classes('bg-secondary'):
            await create_workflows_model_left_drawer(workflows_page_state)


def handle_demo_mode_button_click():
    activate_demo_mode()
    ui.navigate.to('/workflows')


@ui.page('/')
async def landing_page():
    """
    Landing page where the user selects whether to access the workflows dashboard or the SPARQL editor
    """
    await ui.context.client.connected()

    with ui.header().classes('items-center p-2 h-14'):
        ui.label('Handover workflows validation prototype UI').classes('text-xl').style('color: #000000')
        ui.space()
        if app.storage.tab.get('user_id', 0):  # Not logged in yet
            ui.label(f'Welcome, {app.storage.tab['user_name']} ({app.storage.tab['user_project']})').classes('text-xl').style('color: #000000')
            ui.button('Log out',
                      color='negative',
                      on_click=lambda: log_out()).props('size=m')
        else:
            ui.button('Log in',
                      color='positive',
                      on_click=lambda: ui.navigate.to('/login?redirect_to=/')).props('size=m')
            ui.button('Log in as demo user',
                      color='positive',
                      on_click=lambda: handle_demo_mode_button_click()).props('size=m')

    with ui.footer().classes('items-center p-2 h-11'):
        ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-m').style('color: #000000')
        ui.space()
        ui.image('/assets/crc_logo_black_letters_wide.png').classes('w-26')

    with ui.row().classes('w-full justify-center gap-8 p-8'):
        with ui.card().tight().classes('w-128 h-100 cursor-pointer hover:shadow-lg') \
                .on('click', lambda: ui.navigate.to('/workflows')):
            ui.image('assets/workflows_validation_header.png').props('fit=scale-down').classes('h-90')
            with ui.column().classes('p-4 w-full bg-secondary'):
                ui.label('Access the workflows dashboard').classes('text-h6')

        with ui.card().tight().classes('w-128 h-100 cursor-pointer hover:shadow-lg') \
                .on('click', lambda: ui.navigate.to('/sparql')):
            ui.image('assets/sparql_endpoint_header.png').props('fit=scale-down').classes('h-90')
            with ui.column().classes('p-4 w-full bg-secondary'):
                ui.label('Access the SPARQL querying interface').classes('text-h6')

        with ui.card().tight().classes('w-128 h-100 cursor-pointer hover:shadow-lg') \
                .on('click', lambda: ui.navigate.to('/assets/ontology_widoco/index-en.html')):
            ui.image('assets/crc_logo_black_letters.png').props('fit=scale-down').classes('h-90')
            with ui.column().classes('p-4 w-full bg-secondary'):
                ui.label('Access the CRC 1625 ontology documentation').classes('text-h6')

        with ui.card().tight().classes('w-128 h-100 cursor-pointer hover:shadow-lg') \
                .on('click', lambda: ui.navigate.to('https://github.com/DE-TUM/CRC1625')):
            ui.image('assets/git_logo.svg').props('fit=scale-down').classes('h-90')
            with ui.column().classes('p-4 w-full bg-secondary'):
                ui.label('Access the code repository').classes('text-h6')
