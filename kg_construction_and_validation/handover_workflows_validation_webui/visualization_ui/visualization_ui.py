from datastores.rdf import rdf_datastore_client
from handover_workflows_validation_webui.visualization_ui.cytoscape_visualization.cytoscape_component_visualization import CytoscapeComponent, NodeType 
from handover_workflows_validation_webui.main_page import WorkflowsPageState
from handover_workflows_validation_webui.middleware import log_out, matinf_or_demo_login_required

from handover_workflows_validation_webui.visualization_ui.sparql_graph_adapter import (
    translate_sparql_results_to_generic_edges,
    build_cytoscape_graph_from_generic_edges,
)

from handover_workflows_validation_webui.visualization_ui.graph_specs import (
    HANDOVER_WORKFLOW_EDGE_SPECS,
)

import asyncio
import os
from copy import copy
from dataclasses import dataclass, field
from urllib.parse import quote


from nicegui import ui, app
from nicegui.elements.button import Button
from nicegui.elements.column import Column
from nicegui.elements.drawer import RightDrawer
from nicegui.elements.input import Input
from nicegui.elements.select import Select

module_dir = os.path.dirname(__file__)
prefixes: str = open(os.path.join(module_dir, '../queries/prefixes.sparql')).read()
handover_viz_query = (
    prefixes.rstrip()
    + "\n\n"
    + open(os.path.join(module_dir, 'queries/handover_visualisation.sparql'), 'r').read()
)


# Helper function section
entity_details_query = (
    prefixes.rstrip()
    + "\n\n"
    + open(os.path.join(module_dir, 'queries/entity_details.sparql'), 'r').read()
)


DETAIL_PROPERTIES_BY_KIND = {
    "ho_g": [
        "objectName",
        "assignedTo",
    ],
    "ho_o_g": [
        "internalID",
        "creationDate",
        "acceptedDate",
        "objectName",
        "objectDescription",
        "assignedTo",
    ],
    "act": [
        "objectName",
        "objectDescription",
        "creationDate",
        "acceptedDate",
        "assignedTo",
        "output",
    ],
    "output": [
        "internalID",
        "objectName",
        "objectDescription",
        "creationDate",
    ],
    "sample": [
        "internalID",
        "creationDate",
        "acceptedDate",
        "objectName",
        "objectDescription",
    ],
}


def short_uri(value: str) -> str:
    if not value:
        return ""

    if "#" in value:
        return value.split("#")[-1]

    if "/" in value:
        return value.split("/")[-1]

    return value

def sparql_string_literal(value: str) -> str:
    """
    Safely turn a Python string into a SPARQL string literal.
    This is needed because some RDF entity URIs contain spaces and cannot
    be injected as <IRI>.
    """
    if value is None:
        value = ""

    escaped = (
        value
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )

    return f'"{escaped}"'

def property_name(property_uri: str) -> str:
    return short_uri(property_uri)


def display_value(value_binding: dict, value_name_binding: dict | None = None) -> str:
    if value_name_binding and value_name_binding.get("value"):
        return value_name_binding["value"]

    value = value_binding.get("value", "")

    if value_binding.get("type") == "uri":
        return short_uri(value)

    return value


def get_event_args(event) -> dict:
    """
    NiceGUI custom event payloads usually arrive as event.args.
    This helper also handles the case where the callback receives a dict directly.
    """
    if isinstance(event, dict):
        return event

    args = getattr(event, "args", None)

    if isinstance(args, dict):
        return args

    if isinstance(args, list) and args and isinstance(args[0], dict):
        return args[0]

    return {}


def get_kind_from_event_args(args: dict) -> str:
    kind = args.get("kind")

    if kind:
        return kind

    identifiers = args.get("identifiers_for_coloring") or []

    if isinstance(identifiers, str):
        identifiers = [identifiers]

    for candidate in ["sample", "ho_g", "ho_o_g", "act", "output"]:
        if candidate in identifiers:
            return candidate

    return "generic"


def filter_detail_rows(rows: list[dict], kind: str) -> list[dict]:
    allowed_properties = DETAIL_PROPERTIES_BY_KIND.get(kind)

    result = []

    for index, row in enumerate(rows):
        prop_uri = row["property"]["value"]
        prop_name = property_name(prop_uri)

        if allowed_properties and prop_name not in allowed_properties:
            continue

        value = display_value(
            row["value"],
            row.get("value_name"),
        )

        result.append({"row_id": index,
                       "property": prop_name, 
                       "value": value,
                       "property_uri": prop_uri})

    return result
def truncate_text(value: str, max_length: int = 20) -> str:
    if not value:
        return ""

    if len(value) <= max_length:
        return value

    return value[:max_length].rstrip() + "..."


def render_entity_title(entity_label: str, max_length: int = 20):
    short_label = truncate_text(entity_label, max_length)

    label = ui.label(short_label).classes(
        "text-lg font-bold w-full cursor-help"
    ).style(
        """
        white-space: normal;
        overflow-wrap: anywhere;
        word-break: break-word;
        line-height: 1.25;
        max-width: 100%;
        """
    )

    if entity_label and entity_label != short_label:
        label.tooltip(entity_label)

    

@ui.page('/visualization_ui')
@matinf_or_demo_login_required
async def visualization_launcher():
    await ui.context.client.connected()

    with ui.header().classes('items-center p-2 h-14'):
        ui.label('Workflow visualization').classes('text-xl').style('color: #000000')
        ui.space()
        ui.label(
            f"Welcome, {app.storage.tab['user_name']} ({app.storage.tab['user_project']})"
        ).classes('text-xl').style('color: #000000')
        ui.button('Log out', color='negative', on_click=lambda: log_out()).props('size=m')
        ui.button('Return to the previous page', color='info', on_click=lambda: ui.navigate.to("/")).props('size=m')

    with ui.column().classes('w-full items-center gap-4 p-8'):
        ui.label('Choose sample').classes('text-2xl')

        sample_input = ui.input(
            label='Sample internal ID',
            placeholder='Enter sample number',
        ).classes('w-96')

        def open_visualization():
            sample_id = (sample_input.value or '').strip()

            if not sample_id:
                ui.notify('Please enter a sample number.', color='warning')
                return

            ui.navigate.to(f'/visualization_ui/{quote(sample_id)}')

        ui.button(
            'Render workflow graph',
            color='info',
            on_click=open_visualization,
        )

    with ui.footer().classes('items-center p-2 h-11'):
        ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-m').style('color: #000000')
        ui.space()
        ui.image('/assets/crc_logo_black_letters_wide.png').classes('w-26')


@ui.page('/visualization_ui/{object_ID}')
@matinf_or_demo_login_required
async def visualization(object_ID:str):
    await ui.context.client.connected()
    workflows_page_state = WorkflowsPageState()


    query = handover_viz_query.replace("{internalID}", object_ID)
    
    result = await rdf_datastore_client.launch_query(query)
    results = result["results"]["bindings"]



    workflows_page_state.main_content = ui.column().classes('w-full')


    with ui.header().classes('items-center p-2 h-18'):
            ui.label('This is a testing page, mlem!').classes('text-xl').style('color: #000000')
            sample_input = ui.input(
                            label='Sample internal ID',
                            placeholder='Enter sample number',
                        ).classes('w-48')
            def open_visualization():
                            sample_id = (sample_input.value or '').strip()
                
                            if not sample_id:
                                ui.notify('Please enter a sample number.', color='warning')
                                return
                
                            ui.navigate.to(f'/visualization_ui/{quote(sample_id)}')
            ui.button(
                            'Render workflow graph',
                            color='info',
                            on_click=open_visualization
                        )
            ui.space()
            ui.label(f'Welcome, {app.storage.tab['user_name']} ({app.storage.tab['user_project']})').classes('text-xl').style('color: #000000')
            ui.button('Log out', color='negative', on_click=lambda: log_out()).props('size=m')
            ui.button('Return to the previous page', color='info', on_click=lambda: ui.navigate.to("/")).props('size=m')
            
    #drawer for entity details
    details_drawer = ui.right_drawer(value=False).classes("p-4 bg-white").props("width=400px bordered")
    with details_drawer:
        with ui.row().classes("w-full items-center justify-between"):
            ui.label("Entity details").classes("text-xl font-bold")
            ui.button(icon="close", on_click=lambda: setattr(details_drawer, "value", False)).props("flat round dense")
        details_content = ui.column().classes("w-full gap-2")

    #handler for said drawer on click
    async def show_entity_details(event, page_state: WorkflowsPageState = workflows_page_state):
        args = get_event_args(event)

        entity_uri = args.get("id")
        entity_label = args.get("label") or short_uri(entity_uri)
        entity_kind = get_kind_from_event_args(args)

        if not entity_uri:
            ui.notify("No entity URI found.", color="warning")
            return

        if not entity_uri.startswith("https://crc1625.mdi.ruhr-uni-bochum.de/"):
            ui.notify("Invalid entity URI.", color="negative")
            return

        details_drawer.value = True
        details_content.clear()

        with details_content:
            render_entity_title(entity_label)
            ui.label(entity_kind).classes("text-sm text-gray-500")
            ui.separator()
            ui.spinner(size="md")
            ui.label("Loading entity details...")

        query = entity_details_query.replace(
            "{entityURI_LITERAL}",
            sparql_string_literal(entity_uri),
        )

        try:
            result = await rdf_datastore_client.launch_query(query)
            rows = result["results"]["bindings"]
        except Exception as error:
            details_content.clear()

            with details_content:
                render_entity_title(entity_label)
                ui.label(entity_kind).classes("text-sm text-gray-500")
                ui.separator()
                ui.label("Could not load entity details.").classes("text-negative")
                ui.label(str(error)).classes("break-all text-xs")

            return

        detail_rows = filter_detail_rows(rows, entity_kind)

        details_content.clear()

        with details_content:
            render_entity_title(entity_label)
            ui.label(entity_kind).classes("text-sm text-gray-500")
            ui.separator()

            if not detail_rows:
                ui.label("No selected details found for this entity.")
                return

            columns = [
                {
                    "name": "property",
                    "label": "Property",
                    "field": "property",
                    "align": "left",
                    "sortable": True,
                },
                {
                    "name": "value",
                    "label": "Value",
                    "field": "value",
                    "align": "left",
                    "sortable": False,
                },
            ]

            details_table = ui.table(
                columns=columns,
                rows=detail_rows,
                row_key="row_id",
            ).classes("w-full").props("flat bordered dense wrap-cells")

            details_table.add_slot(
                "body-cell-property",
                """
                <q-td :props="props" style="width: 130px; max-width: 130px; vertical-align: top;">
                    <span class="text-weight-bold cursor-help">
                        {{ props.row.property }}
                        <q-tooltip anchor="top middle" self="bottom middle">
                            {{ props.row.property_iri }}
                        </q-tooltip>
                    </span>
                </q-td>
                """
            )

            details_table.add_slot(
                "body-cell-value",
                """
                <q-td :props="props" style="vertical-align: top; max-width: 320px;">
                    <div
                        style="
                            max-width: 320px;
                            white-space: pre-wrap;
                            overflow-wrap: anywhere;
                            word-break: break-word;
                            line-height: 1.35;
                        "
                    >
                        {{ props.row.value }}
                    </div>
                </q-td>
                """
            )



    with ui.grid(columns=1).classes('w-full gap-8'):
        workflows_page_state.graph_component_column = ui.column().classes('w-full')

        with workflows_page_state.graph_component_column:
            generic_rows = translate_sparql_results_to_generic_edges(
                results,
                edge_specs=HANDOVER_WORKFLOW_EDGE_SPECS,
            )

            nodes, edges = build_cytoscape_graph_from_generic_edges(
                generic_rows,
                object_id=object_ID,
                node_class=NodeType.node_type_step.value,
                use_sample_parent=True,
            )

            workflows_page_state.graph_component = CytoscapeComponent(
                nodes,
                edges,
                show_entity_details,
                None,
            )

          


    with ui.footer().classes('items-center p-2 h-11'):
            ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-m').style('color: #000000')
            ui.space()
            ui.image('/assets/crc_logo_black_letters_wide.png').classes('w-26')

            
