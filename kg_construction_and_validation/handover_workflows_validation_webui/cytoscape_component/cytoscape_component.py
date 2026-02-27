import os
from enum import Enum
from typing import List, Dict, Callable, Optional

from nicegui import ui
from nicegui.element import Element


def load_cytoscape_js_libs():
    """
    Injects the js libs needed for cytoscape on all pages
    TODO: This is not really ideal, as there can be pages that don't use Cytoscape (e.g the SPARQL endpoint),
          but this solves the headache of timing conflicts with complex pages or wrt middleware, redirects, etc.
    """
    ui.add_head_html('''
        <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.33.1/cytoscape.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/dagre/0.8.5/dagre.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.5.0/cytoscape-dagre.js"></script>
    ''', shared=True)

class NodeType(Enum):
    node_type_object = "object"
    node_type_step = "step"
    node_type_invisible = "invisible"


class CytoscapeComponent(Element, component=os.path.join(os.path.dirname(__file__), 'cytoscape_component.js')):
    """
    NiceGUI Element implementation for integrating Cytoscape as a custom Vue component
    """

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
    # Label to ID correspondences, to keep
    # track of original IDs after renamings
    name_to_id = dict()

    def __init__(self,
                 nodes: List[Dict],
                 edges: List[Dict],
                 # Node click callable and page state to pass to it
                 on_node_click: Optional[Callable] = None,
                 page_state = None) -> None:
        super().__init__()

        self.name_to_id = {node['data']['label']: node['data']['id'] for node in nodes}

        self._colour_nodes(nodes)

        self._props['nodes'] = nodes
        self._props['edges'] = edges

        # Register event listeners
        if on_node_click is not None:
            self.on('nodeClick', lambda e: on_node_click(e.args, page_state))

        self._rerun_layout_and_fit()

    # Hooks into the javascript functions declared in the Vue component
    def _rerun_layout_and_fit(self):
        self.run_method('rerun_layout_and_fit')

    def _colour_nodes(self, nodes):
        node_coloring_ids = set([' '.join([str(node_id) for node_id in node['data']['identifiers_for_coloring']]) for node in nodes])

        for coloring_id in node_coloring_ids:
            self.id_to_color[coloring_id] = self.colors[self.color_i]
            if self.color_i == len(self.colors) - 1:
                self.color_i = 0
            else:
                self.color_i += 1

        for node in nodes:
            node['data']['color'] = self.id_to_color[' '.join([str(node_id) for node_id in node['data']['identifiers_for_coloring']])]

    def _get_node_color(self, ids_for_coloring):
        coloring_id = ' '.join([str(id) for id in ids_for_coloring])

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

    def rename_node(self, current_label: str, new_label: str) -> None:
        """
        Changes the node's label (avoids performing a full renaming by changing the ID,
        as that would be more complex in Cytoscape)
        """
        node_id = self.name_to_id.get(current_label)
        self.run_method('renameNode', node_id, new_label)

        del self.name_to_id[current_label]
        self.name_to_id[new_label] = node_id

    def add_node(self, label: str, node_type: NodeType, coloring_ids: list[str] = None) -> None:
        if label not in self.name_to_id[label]:
            self.name_to_id[label] = label # Its ID is the label

            node_color = self.colors[0]
            if coloring_ids is not None:
                node_color = self._get_node_color(coloring_ids)

            self.run_method('addNode', label, label, node_type.value, node_color)

    def remove_node(self, node_label: str) -> None:
        node_id = self.name_to_id.get(node_label)

        self.run_method('removeNode', node_id)

    def select_node(self, node_label: str) -> None:
        node_id = self.name_to_id.get(node_label)

        self.run_method('selectNode', node_id)

    def set_node_as_valid(self, node_label: str, tooltip: str) -> None:
        node_id = self.name_to_id.get(node_label)

        self.run_method('setNodeAsValid', node_id, tooltip)

    def set_node_as_invalid(self, node_label: str, tooltip: str) -> None:
        node_id = self.name_to_id.get(node_label)

        self.run_method('setNodeAsInvalid', node_id, tooltip)

    def set_node_as_missing(self, node_label: str, tooltip: str) -> None:
        node_id = self.name_to_id.get(node_label)

        self.run_method('setNodeAsMissing', node_id, tooltip)

    def set_node_as_not_checked(self, node_label: str, tooltip: str) -> None:
        node_id = self.name_to_id.get(node_label)

        self.run_method('setNodeAsNotChecked', node_id, tooltip)

    def clear_validation_results(self) -> None:
        self.run_method('clearValidationResults')

    def add_activity(self, node_label: str, new_activities: list[str], added_activity: str) -> None:
        node_id = self.name_to_id.get(node_label)

        self.run_method('addActivity', node_id, added_activity, self._get_node_color(new_activities))

    def remove_activity(self, node_label: str, new_activities: list[str], removed_activity: str) -> None:
        node_id = self.name_to_id.get(node_label)

        self.run_method('removeActivity', node_id, removed_activity, self._get_node_color(new_activities))

    def replace_activities(self, node_label: str, activities: list[str]) -> None:
        node_id = self.name_to_id.get(node_label)

        new_node_color = self.colors[0]
        if activities is not None:
            new_node_color = self._get_node_color(activities)

        self.run_method('replaceActivities', node_id, activities, new_node_color)

    def replace_projects(self, node_label: str, projects: list[str]) -> None:
        node_id = self.name_to_id.get(node_label)

        self.run_method('replaceProjects', node_id, projects)