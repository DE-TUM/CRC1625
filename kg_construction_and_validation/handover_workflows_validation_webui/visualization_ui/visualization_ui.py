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
handover_viz_query = prefixes + open(os.path.join(module_dir, 'queries/handover_visualisation.sparql'), 'r').read()
#hand_viz_compl_query = prefixes + open(os.path.join(module_dir, 'queries/hand_viz_compl.sparql'), 'r').read()



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
    #print(results)


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
                lambda: None,
                None,
            )

          


    with ui.footer().classes('items-center p-2 h-11'):
            ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-m').style('color: #000000')
            ui.space()
            ui.image('/assets/crc_logo_black_letters_wide.png').classes('w-26')

            
