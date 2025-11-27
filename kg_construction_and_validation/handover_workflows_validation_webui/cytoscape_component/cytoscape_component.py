from enum import Enum
from typing import List, Dict, Callable, Optional

from nicegui import ui
from nicegui.element import Element


class NodeType(Enum):
    node_type_object = "object"
    node_type_step = "step"
    node_type_invisible = "invisible"


class CytoscapeComponent(Element, component='cytoscape_component.js'):
    """
    NiceGUI Element implementation for integrating Cytoscape as a custom Vue component
    """

    # Adapted from: https: // github.com / stardog - union / stardog - examples / blob / develop / weblog / stardog - d3 / js / stardogd3.js

    # Adapted from: https://github.com/stardog-union/stardog-examples/blob/develop/weblog/stardog-d3/js/stardogd3.js
    colors = ['#68bdf6',  # light blue
              '#6dce9e',  # green #1
              '#faafc2',  # light pink
              '#f2baf6',  # purple
              '#ff928c',  # light red
              '#fcea7e',  # light yellow
              '#ffc766',  # light orange
              '#405f9e',  # navy blue
              '#a5abb6',  # dark gray
              '#78cecb',  # green #2,
              '#b88cbb',  # dark purple
              '#ced2d9',  # light gray
              '#e84646',  # dark red
              '#fa5f86',  # dark pink
              '#ffab1a',  # dark orange
              '#fcda19',  # dark yellow
              '#797b80',  # black
              '#c9d96f',  # pistacchio
              '#47991f',  # green #3
              '#70edee',  # turquoise
              '#ff75ea']  # pink

    color_i = 0
    id_to_color = dict()

    def __init__(self,
                 nodes: List[Dict],
                 edges: List[Dict],
                 on_node_click: Optional[Callable]) -> None:
        super().__init__()

        # Load dependencies
        ui.add_head_html('''
            <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.33.1/cytoscape.min.js"></script>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/dagre/0.8.5/dagre.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.5.0/cytoscape-dagre.js"></script>
        ''')

        self._colour_nodes(nodes)

        self._props['nodes'] = nodes
        self._props['edges'] = edges

        # Register event listeners
        if on_node_click:
            self.on('nodeClick', lambda e: on_node_click(e.args))

        self.run_method('rerun_layout_and_fit')

    # Hooks into the javascript functions declared in the Vue component

    def _colour_nodes(self, nodes):
        node_coloring_ids = set([' '.join(str(node['data']['identifiers_for_coloring'])) for node in nodes])

        for coloring_id in node_coloring_ids:
            self.id_to_color[coloring_id] = self.colors[self.color_i]
            if self.color_i == len(self.colors) - 1:
                self.color_i = 0
            else:
                self.color_i += 1

        for node in nodes:
            node['data']['color'] = self.id_to_color[' '.join(str(node['data']['identifiers_for_coloring']))]

    def _get_node_color(self, ids_for_coloring):
        coloring_id = ' '.join(str(id) for id in ids_for_coloring)

        if coloring_id not in self.id_to_color:
            self.id_to_color[coloring_id] = self.colors[self.color_i]
            if self.color_i == len(self.colors) - 1:
                self.color_i = 0
            else:
                self.color_i += 1

        return self.id_to_color[coloring_id]

    def add_edge(self, source: str, target: str) -> None:
        self.run_method('addEdge', source, target)

    async def exists_edge(self, source: str, target: str) -> bool:
        return await self.run_method('existsEdge', source, target)

    def remove_edge(self, source: str, target: str) -> None:
        self.run_method('removeEdge', source, target)

    def rename_node(self, node_id: str, new_label: str) -> None:
        self.run_method('renameNode', node_id, new_label)

    def add_node(self, node_id: str, label: str, node_type: NodeType, activities: list[str] = None) -> None:
        node_color = self.colors[0]
        if activities is not None:
            node_color = self._get_node_color(activities)

        self.run_method('addNode', node_id, label, node_type.value, node_color)

    def remove_node(self, node_id: str) -> None:
        self.run_method('removeNode', node_id)

    def select_node(self, node_id: str) -> None:
        self.run_method('selectNode', node_id)

    def add_activity(self, node_id: str, new_activities: list[str], added_activity: str) -> None:
        self.run_method('addActivity', node_id, added_activity, self._get_node_color(new_activities))

    def remove_activity(self, node_id: str, new_activities: list[str], removed_activity: str) -> None:
        self.run_method('removeActivity', node_id, removed_activity, self._get_node_color(new_activities))

    def replace_activities(self, node_id: str, activities: [str]) -> None:
        new_node_color = self.colors[0]
        if activities is not None:
            new_node_color = self._get_node_color(activities)

        self.run_method('replaceActivities', node_id, activities, new_node_color)