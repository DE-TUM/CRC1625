import os
from functools import wraps

from nicegui import ui

from datastores.rdf import rdf_datastore_client
from handover_workflows_validation_webui.shared_state import shared_state

module_dir = os.path.dirname(__file__)
prefixes: str = open(os.path.join(module_dir, '../handover_workflows_validation/queries/prefixes.sparql')).read()
details_single_user_query = prefixes + open(os.path.join(module_dir, 'queries/details_single_user.sparql'), 'r').read()


def activate_demo_mode():
    # Become Sir SHACLot and "bypass" authentication
    # No data will be editable
    shared_state().demo_mode = True
    shared_state().user_id = -1
    shared_state().user_name = "Sir SHACLot"
    shared_state().user_project = "Demo user"


def handle_log_out_confirm():
    shared_state().demo_mode = False
    shared_state().user_id = 0
    shared_state().user_name = ''
    shared_state().user_project = ''

    ui.navigate.to("/")


def log_out():
    log_out_dialog = ui.dialog()
    with log_out_dialog:
        with ui.card(align_items='center').classes('absolute-center'):
            with ui.column(align_items='center'):
                ui.label("Are you sure you want to log out? Any unsaved changes will be lost.")
                with ui.row(align_items='center'):
                    ui.button("Cancel",
                              color='info',
                              on_click=lambda: log_out_dialog.close())
                    ui.button("Log out",
                              color='negative',
                              on_click=lambda: handle_log_out_confirm())

    log_out_dialog.open()


async def check_authentication_in_matinf():
    js_code = '''
        try {
            const response = await fetch('https://crc1625.mdi.ruhr-uni-bochum.de/ajax/getstate', { 
                credentials: 'include' 
            });
            return { success: true, content: await response.json() };
        } catch (error) {
            return { success: false, content: error.message };
        }
    '''
    return await ui.run_javascript(js_code)


def matinf_login_required(func):
    """
    Active page guard that forces the user to be logged in MatInf before continuing
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if shared_state().demo_mode:
            return await func(*args, **kwargs)
        else:
            matinf_response = await check_authentication_in_matinf()

            if matinf_response.get('success') and matinf_response.get('content') and matinf_response['content'].get('isUserAuthentificated'):
                user_id = matinf_response.get("id")

                result = await rdf_datastore_client.launch_query(details_single_user_query.replace("{user_id}", str(user_id)))
                results = result["results"]["bindings"]

                shared_state().user_id = user_id
                shared_state().user_name = results[0]["user_name"]["value"]
                shared_state().user_project = result[0]["project_name"]["value"]

                return await func(*args, **kwargs)
            else: # Redirect to the login page
                ui.navigate.to(f'/login?redirect_to={ui.context.client.page.path}')

    return wrapper


def handle_demo_mode_button_click(redirect_to: str):
    activate_demo_mode()
    ui.navigate.to(redirect_to)


@ui.page('/login')
async def login(redirect_to: str = "/"):
    with ui.card(align_items='center').classes('absolute-center'):
        with ui.column():
            ui.markdown(f"""
            **You need to be authenticated in MatInf to continue. Please log in to MatInf and return to the previous page.**

            Alternatively, you can log in as the demo user.
            """)

        with ui.row().classes('items-center'):
            ui.button("Open MatInf's login page",
                      color='info',
                      on_click=lambda: ui.navigate.to('https://crc1625.mdi.ruhr-uni-bochum.de/identity/account/login', new_tab=True))
            ui.button("Return to the previous page",
                      color='info',
                      on_click=lambda: ui.navigate.to(redirect_to))
            ui.button('Log in as demo user',
                      color='positive',
                      on_click=lambda: handle_demo_mode_button_click(redirect_to)).props('size=m')