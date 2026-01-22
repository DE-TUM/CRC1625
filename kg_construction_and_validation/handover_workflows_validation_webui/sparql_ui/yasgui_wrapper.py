import os
import urllib.parse
from pathlib import Path

from nicegui import app, ui
from starlette.requests import Request
from starlette.responses import JSONResponse

from datastores.rdf import rdf_datastore_client

LOCAL_SPARQL_PROXY_ROUTE = "/api/sparql"

module_dir = os.path.dirname(__file__)
DEFAULT_QUERY = open(os.path.join(module_dir, "./default_query.sparql"), 'r').read()


@app.post(LOCAL_SPARQL_PROXY_ROUTE)
async def sparql_proxy(request: Request):
    """
    Proxies a request from YASGUI to the internal CRC 1625 SPARQL endpoint
    """
    if request.method == 'POST':
        data = await request.body()

        parsed_data = urllib.parse.parse_qs(data.decode('utf-8'))
    else:
        return JSONResponse({"error": f"No requests other than POST are allowed"}, status_code=400)

    try:
        query = parsed_data['query'][0]

        if app.storage.user.get('use_inference', False):
            query = 'DEFINE input:inference "inference_rules"\n' + query
        response = await rdf_datastore_client.launch_query(query, return_full_response=True)

        return JSONResponse(
            content=response['data'],
            status_code=response['status']
        )

    except Exception as e:  # TODO: I know, but we are not exposing anything critical
        return JSONResponse({"error": f"An unexpected error occurred: {e}"}, status_code=500)


# New route for the iframe content
@ui.page('/yasgui_frame', title='YASGUI Embed')
def yasgui_frame_page():
    content = f"""
        <link href="https://unpkg.com/@triply/yasgui/build/yasgui.min.css" rel="stylesheet" type="text/css" />
        <script src="https://unpkg.com/@triply/yasgui/build/yasgui.min.js"></script>

        <style>
            /* Ensure the content fills the entire iframe body */
            html, body, #yasgui {{
                height: 100%;
                width: 100%;
                margin: 0; /* Remove default body margins */
                padding: 0;
                overflow: auto; 
            }}
        </style>

        <div id="yasgui"></div>

        <script>
            function initializeYasqe(yasgui) {{
                const yasqe = yasgui.getTab().yasqe;

                yasqe.addPrefixes({{
                    pmdco:   "https://w3id.org/pmd/co/",
                    pmd:   "https://w3id.org/pmd/co/",
                    chebi: "http://purl.obolibrary.org/obo/chebi/",
                    crc: "https://crc1625.mdi.ruhr-uni-bochum.de/", 
                    user:    "https://crc1625.mdi.ruhr-uni-bochum.de/user/",
                    project:    "https://crc1625.mdi.ruhr-uni-bochum.de/project/",
                    object: "https://crc1625.mdi.ruhr-uni-bochum.de/object/",
                    substrate: "https://crc1625.mdi.ruhr-uni-bochum.de/substrate/",
                    measurement: "https://crc1625.mdi.ruhr-uni-bochum.de/measurement/",
                    measurement_type: "https://crc1625.mdi.ruhr-uni-bochum.de/measurement_type/",
                    measurement_area: "https://crc1625.mdi.ruhr-uni-bochum.de/measurement_area/",
                    bulk_composition: "https://crc1625.mdi.ruhr-uni-bochum.de/bulk_composition/",
                    EDX_composition: "https://crc1625.mdi.ruhr-uni-bochum.de/EDX_composition/",
                    idea_or_experiment_plan: "https://crc1625.mdi.ruhr-uni-bochum.de/idea_or_experiment_plan/",
                    request_for_synthesis: "https://crc1625.mdi.ruhr-uni-bochum.de/request_for_synthesis/",
                    workflow_instance: "https://crc1625.mdi.ruhr-uni-bochum.de/workflow_instance/",
                    workflow_model: "https://crc1625.mdi.ruhr-uni-bochum.de/workflow_model/",
                    handover: "https://crc1625.mdi.ruhr-uni-bochum.de/handover/",
                    activity: "https://crc1625.mdi.ruhr-uni-bochum.de/activity/",
                    rdf:     "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                    xsd:     "http://www.w3.org/2001/XMLSchema#",
                    foaf:    "http://xmlns.com/foaf/0.1/",
                    prov:    "http://www.w3.org/ns/prov#",
                    rdfs:    "http://www.w3.org/2000/01/rdf-schema#",
                }});
                
                yasqe.collapsePrefixes(true);
            }}
        
            function initializeYasgui() {{
                const yasgui = new Yasgui(document.getElementById("yasgui"), {{
                    requestConfig: {{
                        endpoint: "{LOCAL_SPARQL_PROXY_ROUTE}"
                    }},
                    copyEndpointOnNewTab: true,
                    showEndpointInput: false,
                }});

                const defaultQuery = `{DEFAULT_QUERY}`;
                const tab = yasgui.getTab();
                tab.setQuery(defaultQuery);
                
                return yasgui;
            }}
            
            yasgui = initializeYasgui();
            initializeYasqe(yasgui);
            
            // Listener for the example queries selector
            window.addEventListener('message', (event) => {{
                console.log(event);
                if (event.data.type === 'set_query') {{
                    yasgui.getTab().setQuery(event.data.query);
                    initializeYasqe(yasgui);
                }}
            }});
        </script>
    """

    ui.add_body_html(content)


def load_example_queries() -> list[tuple[str, list[tuple[str, str]]]]:
    example_queries: dict[str, list[tuple[str, str]]] = {}

    for subdir in Path(os.path.join(module_dir, './example_queries')).iterdir():
        if subdir.is_dir():
            for file in subdir.iterdir():
                if file.is_file():
                    category = (subdir.name[:1].upper() + subdir.name[1:]).replace("_", " ")
                    query_name = (file.name[:1].upper() + file.name[1:]).replace("_", " ").replace(".sparql", "")

                    if category not in example_queries:
                        example_queries[category] = []

                    example_queries[category].append((query_name, open(file).read()))

    # Order by query name
    for queries_data in example_queries.values():
        queries_data.sort(key=lambda x: x[0])

    # Turn the dict into a hand-ordered array based on the categories
    order_mapping = {
        "Users and projects": 0,
        "Materials libraries and samples": 1,
        "Measurements": 2,
        "Compositions": 3,
        "Handovers and characterization activities": 4
    }

    final_ordered_queries = sorted(
        example_queries.items(),
        key=lambda item: order_mapping.get(item[0], 99)
    )

    return final_ordered_queries


@ui.page('/sparql')
async def main_page():
    def set_query(query_text: str):
        ui.run_javascript(f'''
            const iframe = document.querySelector('iframe');
            
            if (iframe && iframe.contentWindow) {{
                iframe.contentWindow.postMessage({{
                    type: 'set_query',
                    query: `{query_text}`
                }}, '*');
            }}
        ''')

    ui.page_title('CRC 1625 SPARQL Endpoint')

    example_queries: list[tuple[str, list[tuple[str, str]]]] = load_example_queries()
    with ui.row().classes('items-center gap-2 mb-4'):
        ui.label('Load an example query:')
        for category, queries in example_queries:
            with ui.button(category).props('outline color=primary size=sm'):
                with ui.menu():
                    for (query_name, query_content) in queries:
                        ui.menu_item(query_name, on_click=lambda q=query_content: set_query(q))

    with ui.row().classes('items-center gap-2'):
        ui.label('(Optional) Enable inference:')
        ui.switch().bind_value(app.storage.user, 'use_inference')
        with ui.icon('help'):
            ui.tooltip('Enabling inference allows querying the data using the PMDco ontology')

    with ui.column().classes('w-full h-screen overflow-hidden flex'):
        with ui.row().classes('w-full h-full flex-grow overflow-hidden'):
            iframe_html = """
                    <iframe 
                        src="/yasgui_frame"
                        style="width: 100%; height: 100%; border: none; border-radius: 0.25rem; overflow: hidden;"
                        title="YASGUI SPARQL Editor"
                    ></iframe>
                """

            ui.html(iframe_html, sanitize=False).classes('w-full h-full flex-grow')

    with ui.header(elevated=True).style('background-color: #17365c').classes('items-center justify-between p-2 h-15'):
        ui.label(f"CRC 1625 SPARQL endpoint").classes('text-xl font-medium')

    with ui.footer().style('background-color: #17365c').classes('items-center justify-between p-2 h-15'):
        ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-xl font-medium')
        ui.image('/assets/crc_logo_white_letters.png').classes('w-15')
