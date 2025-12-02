import argparse
import os

from handover_workflows_validation import handover_workflows_validation
import handover_workflows_validation_webui.workflow_instance_ui.edit_workflow_instance_page
import handover_workflows_validation_webui.workflow_model_ui.edit_workflow_model_page
import handover_workflows_validation_webui.main_page
import handover_workflows_validation_webui.sparql_ui.yasgui_wrapper

from nicegui import ui

from datastores.rdf.virtuoso_datastore import VirtuosoRDFDatastore

module_dir = os.path.dirname(__file__)


if __name__ in {"__main__", "__mp_main__"}:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Debugging mode: clear all stores before running and upload test files"
    )

    args = parser.parse_args()

    store = VirtuosoRDFDatastore()

    reload = False
    if args.debug:
        reload = True

        store.clear_triples()
        store.clear_triples(handover_workflows_validation.WORKFLOWS_GRAPH_IRI)

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

        store.bulk_file_load([f["file"] for f in ontology_files], delete_files_after_upload=False)

        store.upload_file(os.path.join(module_dir, "handover_workflows_validation/validation_test/validation_test_triples_webui.ttl"))

        test_file_path = os.path.join(module_dir, 'handover_workflows_validation/validation_test/workflow_config_files/')

        store.upload_file(test_file_path + "workflow_models_webui.ttl", graph_iri=handover_workflows_validation.WORKFLOWS_GRAPH_IRI)
        store.upload_file(test_file_path + "workflow_instances_webui.ttl", graph_iri=handover_workflows_validation.WORKFLOWS_GRAPH_IRI)

    ui.run(title="CRC1625 Handover workflows validation prototype",
           reload=False)