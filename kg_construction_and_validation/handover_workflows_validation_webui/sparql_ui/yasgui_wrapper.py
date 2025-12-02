import urllib.parse

from nicegui import app, ui
from starlette.requests import Request
from starlette.responses import JSONResponse

from datastores.rdf.virtuoso_datastore import VirtuosoRDFDatastore

LOCAL_SPARQL_PROXY_ROUTE = "/api/sparql"


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
        response = VirtuosoRDFDatastore().launch_query(parsed_data['query'][0])

        return JSONResponse(
            content=response.json(),
            status_code=response.status_code
        )

    except Exception as e: # TODO: I know, but we are not exposing anything critical
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
                overflow: hidden; /* Prevent double scrollbars */
            }}
        </style>

        <div id="yasgui"></div>

        <script>
            function initializeYasgui() {{
                const config = {{
                    requestConfig: {{
                        // IMPORTANT: Use the full absolute URL or a relative URL that resolves correctly *from within the iframe*
                        endpoint: "{LOCAL_SPARQL_PROXY_ROUTE}" 
                    }},
                    copyEndpointOnNewTab: true,
                    showEndpointInput: false,
                    yasqe: {{
                        value: "SELECT ?s ?p ?o WHERE {{ ?s ?p ?o }} LIMIT 10"
                    }}
                }};

                new Yasgui(document.getElementById("yasgui"), config);
            }}

            initializeYasgui();
        </script>
    """

    ui.add_body_html(content)


@ui.page('/sparql')
async def main_page():
    ui.page_title('CRC 1625 SPARQL Endpoint')

    with ui.column().classes('w-full h-screen flex'):
        with ui.row().classes('w-full flex-grow p-4'):
            iframe_html = """
                    <iframe 
                        src="/yasgui_frame"
                        style="width: 100%; height: 100%; border: none; border-radius: 0.25rem;"
                        title="YASGUI SPARQL Editor"
                    ></iframe>
                """

            ui.html(iframe_html, sanitize=False).classes('w-full h-full flex-grow')

    with ui.header(elevated=True).style('background-color: #3874c8').classes('items-center justify-between'):
        ui.label('SPARQL Query Interface').classes('text-2xl font-bold')

    with ui.footer().style('background-color: #3874c8'):
        ui.label('© 2025-2027 - CRC 1625 Knowleddge Graph (WIP)')

    with ui.page_scroller(position='bottom-right', x_offset=20, y_offset=20):
        ui.button('Scroll to Top')