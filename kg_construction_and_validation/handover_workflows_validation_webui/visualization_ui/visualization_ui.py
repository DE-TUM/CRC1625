#maybe we move this to a separate file for handling uris or sparql or smth
from datastores.rdf import rdf_datastore_client
from handover_workflows_validation_webui.cytoscape_component.cytoscape_component import CytoscapeComponent, NodeType
from handover_workflows_validation_webui.main_page import WorkflowsPageState
from handover_workflows_validation_webui.middleware import log_out, matinf_or_demo_login_required

import asyncio
import os
from copy import copy
from dataclasses import dataclass, field


from nicegui import ui, app
from nicegui.elements.button import Button
from nicegui.elements.column import Column
from nicegui.elements.drawer import RightDrawer
from nicegui.elements.input import Input
from nicegui.elements.select import Select

module_dir = os.path.dirname(__file__)
prefixes: str = open(os.path.join(module_dir, '../queries/prefixes.sparql')).read()
handover_viz_query = prefixes + open(os.path.join(module_dir, 'queries/handover_visualisation.sparql'), 'r').read()


def extract_label_from_uri(uri: str) -> str:
            """Extract the readable label from a URI"""
            # If it's a URI like "http://example.com/handover_group#Activity"
            # or "http://example.com/handover_group/Activity_1"
            # Extract just the last part after # or /
            
            if '#' in uri:
                return uri.split('#')[-1]
            elif '/' in uri:
                return uri.split('/')[-1]
            else:
                return uri

@ui.page('/visualization_ui/{object_ID}')
@matinf_or_demo_login_required
async def visualization(object_ID:str):
    await ui.context.client.connected()
    workflows_page_state = WorkflowsPageState()


    query = handover_viz_query.replace("{internalID}", object_ID)
    
    result = await rdf_datastore_client.launch_query(query)
    results = result["results"]["bindings"]

    entries = []
    incoming_edges = set()
    root_node = None

    for result in results:
        handover_group = result["handover_group"]["value"]
        handover_of_group = result["handover_of_group"]["value"]
        activity = result["activity"]["value"]
        nextGroup = result.get("nextGroup", {}).get("value", None)

        entries.append((handover_group, handover_of_group, activity, nextGroup))

    
        # First, collect all nextGroup values to identify which nodes have incoming edges
        
    for handover_group, handover_of_group, activity, nextGroup in entries:
        if nextGroup:
            incoming_edges.add(nextGroup)

        # Find the root (handover_group that's NOT in incoming_edges)
    for handover_group, handover_of_group, activity, nextGroup in entries:
        if handover_group not in incoming_edges:
            root_node = handover_group
            break


    workflows_page_state.main_content = ui.column().classes('w-full')

    with ui.header().classes('items-center p-2 h-14'):
            ui.label('This is a testing page, mlem!').classes('text-xl').style('color: #000000')
            ui.space()
            ui.label(f'Welcome, {app.storage.tab['user_name']} ({app.storage.tab['user_project']})').classes('text-xl').style('color: #000000')
            ui.button('Log out', color='negative', on_click=lambda: log_out()).props('size=m')
            ui.button('Return to the previous page', color='info', on_click=lambda: ui.navigate.to("/")).props('size=m')

    with ui.grid(columns=1).classes('w-full gap-8'):
            workflows_page_state.graph_component_column = ui.column()
            with workflows_page_state.graph_component_column:                
                nodes = []
                edges = []

                entry_map = {hg: (hog, act, ng) for hg, hog, act, ng in entries}

                # Create the main sample node (like "Main demo ML (ID: -10)")
                sample_node_id = f"sample_{object_ID}"
                nodes.append({
                    'data': {
                        'id': sample_node_id,
                        'label': f"Sample (ID: {object_ID})",
                        'projects': [],
                        'activities': [],
                        'identifiers_for_coloring': ["sample"]
                    },
                    'classes': [NodeType.node_type_step.value]
                })

                
                    
        # LAYER 1: Create nodes for handover groups (the sequence chain)
                current = root_node
                while current:
                    handover_of_group, activity, nextGroup = entry_map[current]

                #for handover_group, handover_of_group, activity, nextGroup in entries:
                    hg_label = extract_label_from_uri(current)
                    # Extract readable labels but keep full URIs in data

                    #hg_label = extract_label_from_uri(handover_group)
                    #hog_label = extract_label_from_uri(handover_of_group)
                    #act_label = extract_label_from_uri(activity)
                    

                    nodes.append({
                        'data': {
                            'id': current,
                            'label': hg_label,
                            'parent': sample_node_id, 
                            'identifiers_for_coloring': ["ho_g"]
                        },
                        'classes': [NodeType.node_type_step.value]
                    })

                    # Get next in sequence
                    handover_of_group, activity, nextGroup = entry_map[current]
                    current = nextGroup


                     # LAYER 2 & 3: Create handover_of_group and activity nodes, and wire them up
                current = root_node    

                while current:
                    handover_of_group, activity, nextGroup = entry_map[current]
                    hog_label = extract_label_from_uri(handover_of_group)
                    act_label = extract_label_from_uri(activity)

                # Layer 2: Handover of group (child of this handover_group)
                    nodes.append({
                        'data': {
                            'id': handover_of_group,
                            'label': hog_label,
                            'parent': sample_node_id,
                            'identifiers_for_coloring': ["ho_o_g"]
                        },
                        'classes': [NodeType.node_type_step.value]
                    })

                    # Layer 3: Activity (child of handover_of_group)        
                    nodes.append({
                        'data': {
                            'id': activity,
                            'label': act_label,
                            'parent': sample_node_id,
                            'identifiers_for_coloring': ["act"]
                        },
                        'classes': [NodeType.node_type_step.value]
                    })
                    current = nextGroup

                current = root_node
                while current:
                    handover_of_group, activity, nextGroup = entry_map[current]
                        
                    #  Edge: handover_group → handover_of_group → activity
                    edges.append({'data': {'source': current, 'target': handover_of_group}})
                    edges.append({'data': {'source': handover_of_group, 'target': activity}})
                    if nextGroup:
                            edges.append({'data': {'source': current, 'target': nextGroup}})

                    current = nextGroup


                    """edges.append({'data': {'source': handover_group, 'target': handover_of_group}})
                    edges.append({'data': {'source': handover_of_group, 'target': activity}})
                    if nextGroup:
                            edges.append({'data': {'source': handover_group, 'target': nextGroup}})"""
                workflows_page_state.graph_component = CytoscapeComponent(
                    nodes,
                    edges,
                    lambda: None,
                    None
                )


    with ui.footer().classes('items-center p-2 h-11'):
            ui.label('© 2025-2027 - CRC 1625 A06 Project - Work in progress').classes('text-m').style('color: #000000')
            ui.space()
            ui.image('/assets/crc_logo_black_letters_wide.png').classes('w-26')

            
