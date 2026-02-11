import asyncio

from nicegui import ui

from handover_workflows_validation.handover_workflows_validation import get_workflow_model_names_and_creator_user_ids, \
    get_workflow_instances_of_model, read_workflow_model, store_workflow_model, WorkflowInstance, is_workflow_instance_valid, WorkflowModel, \
    create_workflow_instance
from handover_workflows_validation_webui.state import get_state


def edit_handover_workflow_instance_button_click():
    ui.navigate.to(
        f'/workflows/edit_workflow_instance/{get_state().current_workflow_model.workflow_model_name}/{get_state().current_workflow_model.creator_user_id}/{get_state().current_workflow_instance.workflow_instance_name}/{get_state().current_workflow_instance.creator_user_id}')


def edit_handover_workflow_model_button_click():
    ui.navigate.to(f'/workflows/edit_workflow_model/{get_state().current_workflow_model.workflow_model_name}/{get_state().user_id}')


def handle_workflow_instance_table_click(workflow_instance: WorkflowInstance, right_drawer):
    right_drawer.clear()

    get_state().current_workflow_instance = workflow_instance

    with right_drawer:
        right_drawer_label = ui.label('Workflow instance options').classes('text-xl font-bold')

        with ui.column().classes('w-full items-center gap-2'):
            ui.button("Edit", color='info').classes('w-full p-0').on_click(
                lambda: edit_handover_workflow_instance_button_click()
            )
            ui.button("Delete", color='negative').classes('w-full p-0')

        right_drawer_label.set_text(
            f"Workflow instance '{get_state().current_workflow_instance.workflow_instance_name}' options")
    right_drawer.show()

    ui.notify(f'Selected Workflow Instance {get_state().current_workflow_instance.workflow_instance_name}', color='info')


async def create_workflow_models_table(main_content, right_drawer):
    ui.label('Your workflow models').classes('text-xl font-bold')

    async def create_workflow_models_table():
        workflow_models_table = set()

        for workflow_model_name, user_id in await get_workflow_model_names_and_creator_user_ids():
            workflow_models_table.add(
                (
                    workflow_model_name,
                    user_id,
                )
            )

        return workflow_models_table

    def show_table(results_container: ui.column, rows):
        results_container.clear()

        with results_container:
            for row in rows:
                with ui.button(on_click=lambda r=row: show_workflow_model_instances(r[0], r[1], main_content, right_drawer)).props('no-caps unelevated').classes('w-full'):
                    with ui.row().classes('w-full py-1 items-center'):
                        ui.label(str(row[0])).classes('w-1/2 text-left').style('color: #000000')
                        ui.space()
                        ui.label(str(row[1])).classes('w-0 flex-grow text-right').style('color: #000000')

    def filter_table(search_input: ui.input, workflow_models_table):
        search_term = search_input.value.lower()

        filtered_rows = [
            row for row in workflow_models_table if search_term in row[0].lower()
        ]

        show_table(results_container, filtered_rows)

    async def create_empty_workflow_model(workflow_model_name: str, results_container, workflow_models_table):
        if get_state().demo_mode:
            ui.notify("You cannot create new models as a demo user", type='warning')
            return

        workflow_model = WorkflowModel()
        workflow_model.workflow_model_name = workflow_model_name

        await store_workflow_model(workflow_model, get_state().user_id)

        workflow_models_table.add(
            (
                workflow_model_name,
                get_state().user_id,
            )
        )
        show_table(results_container, workflow_models_table)

    search_input = ui.input(placeholder='Search by name...').classes('w-full').on_value_change(lambda: filter_table(search_input, workflow_models_table))

    with ui.row().classes('w-full border-b-2 border-gray-400 py-2 font-bold'):
        ui.label('Workflow model').classes('w-1/2 text-left')
        ui.label('User ID').classes('w-0 flex-grow text-right')

    results_container = ui.column().classes('w-full')

    workflow_models_table = await create_workflow_models_table()
    show_table(results_container, workflow_models_table)

    ui.button("Add a new workflow model", color='info', on_click=lambda: create_empty_workflow_model("New workflow model", results_container, workflow_models_table))


async def check_and_update_icon(validation_icon_column: ui.column, workflow_model, workflow_instance):
    valid = await is_workflow_instance_valid(workflow_model, workflow_instance)

    validation_icon_column.clear()
    with validation_icon_column:
        if valid:
            ui.icon('check_circle').classes('text-green-6')
            #ui.image('/assets/heppy.png').classes('w-8')
        else:
            ui.icon('error').classes('text-red-6')

async def show_workflow_model_instances(workflow_model_name: str, workflow_model_creator_user_id: int, main_content, right_drawer):
    # Load the selected workflow model
    get_state().current_workflow_model = await read_workflow_model(workflow_model_name, workflow_model_creator_user_id)
    get_state().workflow_instances_of_current_workflow_model = await get_workflow_instances_of_model(get_state().current_workflow_model)

    main_content.clear()


    with main_content:
        with ui.grid(columns=2):
            ui.label(f"Workflow instances of '{workflow_model_name}'").classes('text-lg font-semibold')
            ui.button("Edit Workflow model", color='info').on_click(
                lambda: edit_handover_workflow_model_button_click()
            )

        with ui.row().classes('w-full border-b-2 border-gray-400 py-1 font-bold'):
            ui.label('Workflow Instance name').classes('w-1/3 text-left')
            ui.label('Associated Materials libraries or Samples').classes('w-1/3 text-left')
            ui.label('Validation status').classes('w-0 flex-grow text-left')

        validation_jobs = []
        for workflow_instance in get_state().workflow_instances_of_current_workflow_model.values():
            with ui.button(on_click=lambda r=workflow_instance: handle_workflow_instance_table_click(r, right_drawer)).props('no-caps unelevated color=secondary').classes('w-full p-1'):
                with ui.row().classes('w-full py-1 items-center'):
                    ui.label(workflow_instance.workflow_instance_name).classes('w-1/3 text-left').style('color: #000000')

                    associated_objects = set()
                    for assignments in workflow_instance.step_assignments.values():
                        for assignment in assignments:
                            associated_objects.add(str(assignment))

                    ui.label(', '.join(sorted(associated_objects))).classes('w-1/3 text-left').style('color: #000000')

                    validation_icon_column = ui.column().classes('w-0 flex-grow text-left').style('color: #000000')
                    with validation_icon_column:
                        ui.spinner()

                    validation_jobs.append(check_and_update_icon(validation_icon_column, get_state().current_workflow_model, workflow_instance))

        async def create_empty_workflow_instance(workflow_instance_name: str, main_content, right_drawer):
            workflow_instance = WorkflowInstance()
            workflow_instance.workflow_model_name = get_state().current_workflow_model.workflow_model_name
            workflow_instance.workflow_instance_name = workflow_instance_name

            # We must add step assignments, even if they don't hold references to any objects
            for step_name in get_state().current_workflow_model.workflow_model_steps:
                workflow_instance.step_assignments[step_name] = []

            await create_workflow_instance(workflow_instance, get_state().current_workflow_model)
            # Show them again
            await show_workflow_model_instances(workflow_model_name, workflow_model_creator_user_id, main_content, right_drawer)

        ui.button("Add a new workflow instance", color='info',
                  on_click=lambda: create_empty_workflow_instance(f"New workflow instance of {get_state().current_workflow_model.workflow_model_name}", main_content, right_drawer))

        # Run validation and update the spinners
        await asyncio.gather(*validation_jobs)

@ui.page('/workflows')
async def workflows_page(demo: str = ""):
    if demo == "demo":
        # Become Sir SHACLot
        get_state().demo_mode = True
        get_state().user_id = -1
    else: # TODO until auth
        get_state().user_id = 0

    main_content = ui.column().classes('w-full')

    with ui.header().classes('items-center p-2 h-14'):
        ui.label('Handover workflows validation prototype UI').classes('text-xl').style('color: #000000')
        ui.space()
        if True:  # TODO integrate auth
            ui.label('Welcome, Sir SHACLot (demo user)!').classes('text-xl').style('color: #000000')
            ui.button('Log out', color='negative', on_click=lambda: ui.navigate.to("/")).props('size=m')
            ui.button('Return to main page', color='info', on_click=lambda: ui.navigate.to("/")).props('size=m')
        else:
            ui.button('Log in', color='info').props('size=m').style('color: #000000')
            ui.button('Log in (as demo user)', color='info').props('size=m').style('color: #000000')

    with ui.footer().classes('items-center p-2 h-11'):
        ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-m').style('color: #000000')
        ui.space()
        ui.image('/assets/crc_logo_black_letters_wide.png').classes('w-26')

    right_drawer = ui.right_drawer(fixed=False).classes('bg-secondary')
    right_drawer.hide()

    with ui.left_drawer().classes('bg-secondary'):
        await create_workflow_models_table(main_content, right_drawer)


@ui.page('/')
async def landing_page():
    with ui.header().classes('items-center p-2 h-14'):
        ui.label('Handover workflows validation prototype UI').classes('text-xl').style('color: #000000')
        ui.space()
        if False: # TODO integrate auth
            ui.button('Log out', color='negative', on_click=lambda: ui.navigate.to("/")).props('size=m')
        else:
            ui.button('Log in', color='info').props('size=m')
            ui.button('Log in (as demo user)', color='info', on_click=lambda: ui.navigate.to("/workflows?demo=demo")).props('size=m')

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