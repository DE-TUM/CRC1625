import argparse
import asyncio
import os

from dotenv import load_dotenv
from starlette.middleware.base import BaseHTTPMiddleware

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore import WORKFLOWS_GRAPH_IRI
from handover_workflows_validation_webui import demo_data_loader, middleware

# Required for serving their pages
import handover_workflows_validation_webui.workflow_instance_ui.edit_workflow_instance_page
import handover_workflows_validation_webui.workflow_model_ui.edit_workflow_model_page
import handover_workflows_validation_webui.main_page
import handover_workflows_validation_webui.sparql_ui.yasgui_wrapper
import handover_workflows_validation_webui.middleware
import handover_workflows_validation_webui.visualization_ui.visualization_ui


from nicegui import ui, app

from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import load_cytoscape_js_libs
from handover_workflows_validation_webui.demo_data_loader import is_demo_data_already_loaded

module_dir = os.path.dirname(__file__)
load_dotenv(os.path.join(module_dir, '.env'))

ASSETS_FOLDER = os.path.join(module_dir, 'handover_workflows_validation_webui/assets/')

async def setup_debug_files(clear_main_graph: bool = False,
                            clear_workflows_graph: bool = False):
    if clear_main_graph:
        await rdf_datastore_client.clear_triples()

        # Load the ontologies at the very least
        ontology_files: list[dict[str, str]] = [
            {
                "name": "PMD-core",
                "file": "../ontologies/pmd_core.ttl",
                "content_type": "text/turtle"
            },
            {
                "name": "CRC",
                "file": "../ontologies/crc.ttl",
                "content_type": "text/turtle"
            },
            {
                "name": "OCE",
                "file": "../ontologies/oce.ttl",
                "content_type": "text/turtle"
            }
        ]

        await rdf_datastore_client.bulk_file_load([f["file"] for f in ontology_files], delete_files_after_upload=False)

    if clear_workflows_graph:
        await rdf_datastore_client.clear_triples(WORKFLOWS_GRAPH_IRI)

    # Avoid uploading demo data if this was a webserver restart
    if not await is_demo_data_already_loaded():
        # Load Sir SHACLot's workflows
        await demo_data_loader.load_demo_workflows()
        # Load the triples for the main graph just it just in case if we are doing a cold boot
        await demo_data_loader.load_demo_user_data()

if __name__ in {"__main__", "__mp_main__"}:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--upload_debug_files",
        action="store_true",
        default=os.getenv("WEBUI_UPLOAD_DEBUG_FILES", "false").lower() == "true",
        help="Debugging mode: Upload demo files"
    )

    parser.add_argument(
        "--bypass_auth",
        action="store_true",
        default=os.getenv("WEBUI_BYPASS_AUTH", "false").lower() == "true",
        help="Debugging mode: Bypass authentication"
    )

    parser.add_argument(
        "--clear_main_graph",
        action="store_true",
        default=os.getenv("WEBUI_CLEAR_MAIN_GRAPH", "false").lower() == "true",
        help="Debugging mode: clear the main graph before starting the WebUI"
    )

    parser.add_argument(
        "--clear_workflows_graph",
        action="store_true",
        default=os.getenv("WEBUI_CLEAR_WORKFLOWS_GRAPH", "false").lower() == "true",
        help="Debugging mode: clear the workflows graph before starting the WebUI"
    )

    parser.add_argument(
        "--reload_on_changes",
        action="store_true",
        default=False,
        help="Debugging mode: reload the webserver when files are changed. Note that it may go haywire when running validation."
    )


    args = parser.parse_args()

    uvicorn_logging_level = 'warning'
    access_log = False
    if args.upload_debug_files:
        asyncio.run(setup_debug_files(clear_main_graph=args.clear_main_graph,
                                      clear_workflows_graph=args.clear_workflows_graph))
        uvicorn_logging_level = 'debug'
        access_log = True

    if args.bypass_auth:
        middleware.DEBUGGING_MODE = True # Bypass authentication as a non-demo user (i.e., access everything with edit permissions)

    app.add_static_files("/assets", ASSETS_FOLDER)


    class SecurityHeaderMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            response = await call_next(request)
            # Prevent other domains from iframing us
            response.headers['Content-Security-Policy'] = "frame-ancestors 'self'"
            response.headers['X-Frame-Options'] = 'SAMEORIGIN'
            return response


    app.add_middleware(SecurityHeaderMiddleware)

    app.colors(primary='#dbdbdb',
               secondary='#f0f0f0',
               positive='#369c4e',
               negative='#d40820',
               info='#5898d4',
               warning='#e88b00')

    load_cytoscape_js_libs()

    ui.run(host="0.0.0.0",
           port=int(os.environ.get("WEBUI_PORT")),
           title="CRC1625 Handover workflows validation prototype",
           reload=args.reload_on_changes, # Do not enable this for now, it freaks out when detecting changes on .ttl files
           storage_secret=os.environ.get("WEBUI_STORAGE_SECRET"),
           uvicorn_logging_level=uvicorn_logging_level,
           access_log=access_log)