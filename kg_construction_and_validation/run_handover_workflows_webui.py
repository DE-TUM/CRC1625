import argparse
import asyncio
import os

from datastores.rdf import rdf_datastore_client
from datastores.rdf.virtuoso_datastore import VirtuosoRDFDatastore
from handover_workflows_validation import handover_workflows_validation

# Required for serving their pages
import handover_workflows_validation_webui.workflow_instance_ui.edit_workflow_instance_page
import handover_workflows_validation_webui.workflow_model_ui.edit_workflow_model_page
import handover_workflows_validation_webui.main_page
import handover_workflows_validation_webui.sparql_ui.yasgui_wrapper

from nicegui import ui, app

from handover_workflows_validation_webui import state

module_dir = os.path.dirname(__file__)

ASSETS_FOLDER = os.path.join(module_dir, 'handover_workflows_validation_webui/assets/')

async def setup_debug_files():
    await rdf_datastore_client.clear_triples()
    await rdf_datastore_client.clear_triples(handover_workflows_validation.WORKFLOWS_GRAPH_IRI)

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

    await rdf_datastore_client.upload_file(os.path.join(module_dir, "handover_workflows_validation/validation_test/validation_test_triples_webui.ttl"))

    test_file_path = os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/')

    await rdf_datastore_client.upload_file(test_file_path + "workflow_models_webui.ttl", graph_iri=handover_workflows_validation.WORKFLOWS_GRAPH_IRI)
    await rdf_datastore_client.upload_file(test_file_path + "workflow_instances_webui.ttl", graph_iri=handover_workflows_validation.WORKFLOWS_GRAPH_IRI)


if __name__ in {"__main__", "__mp_main__"}:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Debugging mode: clear all stores before running and upload test files"
    )

    args = parser.parse_args()

    uvicorn_logging_level = 'warning'
    access_log = False
    if args.debug:
        asyncio.run(setup_debug_files())
        uvicorn_logging_level = 'debug'
        access_log = True

    app.add_static_files("/assets", ASSETS_FOLDER)

    ui.run(title="CRC1625 Handover workflows validation prototype",
           reload=args.debug,
           uvicorn_logging_level=uvicorn_logging_level,
           access_log=access_log)