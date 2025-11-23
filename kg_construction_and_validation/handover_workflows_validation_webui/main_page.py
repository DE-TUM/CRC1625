from nicegui import ui

from datastores.rdf.virtuoso_datastore import VirtuosoRDFDatastore
from handover_workflows_validation.handover_workflows_validation import get_workflow_model_names_and_creator_user_ids, \
    get_workflow_instances_of_model, read_workflow_model, WorkflowInstance
from handover_workflows_validation_webui.state import state


def edit_handover_workflow_instance_button_click():
    ui.navigate.to(
        f'/edit_workflow_instance/{state.current_workflow_model.workflow_model_name}/{state.current_workflow_instance.workflow_instance_name}/{state.user_id}')


def edit_handover_workflow_model_button_click():
    ui.navigate.to(f'/edit_workflow_model/{state.current_workflow_model.workflow_model_name}/{state.user_id}')


def handle_workflow_instance_table_click(workflow_instance: WorkflowInstance, right_drawer):
    right_drawer.clear()

    state.current_workflow_instance = workflow_instance

    with right_drawer:
        right_drawer_label = ui.label('Workflow instance options').classes('text-xl font-bold')

        with ui.column().classes('w-full items-center gap-2'):
            ui.button("Edit").props("color='green-6'").classes('w-full p-0').on_click(
                lambda: edit_handover_workflow_instance_button_click()
            )
            ui.button("Save as predefined workflow").classes('w-full p-0').props("color='blue-6'")
            ui.button("Delete").props("color='red-6'").classes('w-full p-0')

        right_drawer_label.set_text(
            f"Workflow instance '{state.current_workflow_instance.workflow_instance_name}' options")
    right_drawer.show()

    ui.notify(f'Selected Workflow Instance {state.current_workflow_instance.workflow_instance_name}', color='info')


def create_workflow_models_table(main_content, right_drawer):
    ui.label('List of predefined workflows').classes('text-xl font-bold')
    search_input = ui.input(placeholder='Search workflows...').classes('w-full')  # TODO

    workflow_models_table = []
    for workflow_model_name, user_id in get_workflow_model_names_and_creator_user_ids(state.store):
        workflow_models_table.append(
            {
                "workflow_model_name": workflow_model_name,
                "user_id": user_id,
            }
        )

    with ui.row().classes('w-full border-b-2 border-gray-400 py-2 font-bold'):
        ui.label('Workflow model name').classes('w-1/2 text-left')
        ui.label('User ID').classes('w-0 flex-grow text-left')

    for row in workflow_models_table:
        with ui.button(on_click=lambda r=row: handle_workflow_models_table_click(r['workflow_model_name'], user_id,
                                                                                 main_content, right_drawer)).props(
            'flat').classes('w-full p-0'):
            with ui.row().classes('w-full border-b border-gray-200 py-2 items-center'):
                ui.label(str(row['workflow_model_name'])).classes('w-1/2 text-left')

                ui.label(str(row['user_id'])).classes('w-0 flex-grow text-right')


def handle_workflow_models_table_click(workflow_model_name: str, user_id: int, main_content, right_drawer):
    state.current_workflow_model = read_workflow_model(workflow_model_name, user_id, state.store)  # TODO
    state.workflow_instances_of_selected_workflow_model = get_workflow_instances_of_model(workflow_model_name, user_id,
                                                                                          state.store)  # TODO
    state.user_id = user_id  # TODO where better? + Auth

    main_content.clear()

    with main_content:
        with ui.grid(columns=2):
            ui.label(f"Workflow instances of '{workflow_model_name}'").classes('text-lg font-semibold')
            ui.button("Edit Workflow model").on_click(
                lambda: edit_handover_workflow_model_button_click()
            )

        with ui.row().classes('w-full border-b-2 border-gray-400 py-2 font-bold'):
            ui.label('Workflow Instance name').classes('w-1/3 text-left')
            ui.label('Associated Objects').classes('w-1/3 text-left')
            ui.label('Status').classes('w-0 flex-grow text-left')

        for (workflow_instance_name,
             user_id), workflow_instance in state.workflow_instances_of_selected_workflow_model.items():
            with ui.button(
                    on_click=lambda r=workflow_instance: handle_workflow_instance_table_click(r, right_drawer)).props(
                'flat').classes(
                'w-full p-0'):
                with ui.row().classes('w-full border-b border-gray-200 py-2 items-center'):
                    ui.label(workflow_instance_name).classes('w-1/3 text-left')

                    associated_objects = set()
                    for assignments in workflow_instance.step_assignments.values():
                        for assignment in assignments:
                            associated_objects.add(str(assignment))

                    ui.label(', '.join(sorted(associated_objects))).classes('w-1/3 text-left')

                    with ui.column().classes('w-0 flex-grow text-left'):
                        valid = False  # is_workflow_instance_valid(state.current_workflow_model, workflow_instance, state.store) # TODO
                        if valid:
                            ui.icon('check_circle').classes('text-green-6')
                        else:
                            ui.icon('error').classes('text-red-6')


@ui.page('/')
async def main_page():
    state.store = VirtuosoRDFDatastore()

    main_content = ui.column().classes('w-full')

    with ui.header(elevated=True).style('background-color: #3874c8').classes('items-center justify-between'):
        ui.label('Handover workflows validation prototype UI').classes('text-2xl font-bold')

    right_drawer = ui.right_drawer(fixed=False).style('background-color: #ebf1fa').props('bordered')
    right_drawer.hide()

    with ui.left_drawer().style('background-color: #d7e3f4'):
        create_workflow_models_table(main_content, right_drawer)

    with ui.footer().style('background-color: #3874c8'):
        ui.label('© 2025-2027 - CRC 1625 Knowleddge Graph (WIP)')

    with ui.page_scroller(position='bottom-right', x_offset=20, y_offset=20):
        ui.button('Scroll to Top')
