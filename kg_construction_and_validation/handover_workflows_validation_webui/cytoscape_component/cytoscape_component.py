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

    Follows the example at https://github.com/zauberzeug/nicegui/blob/main/examples/custom_vue_component/main.py
    """



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

        self._props['nodes'] = nodes
        self._props['edges'] = edges

        # Register event listeners
        if on_node_click:
            self.on('nodeClick', lambda e: on_node_click(e.args))

        self.run_method('rerun_layout_and_fit')

    # Hooks into the javascript functions declared in the Vue component

    def add_edge(self, source: str, target: str) -> None:
        self.run_method('addEdge', source, target)

    async def exists_edge(self, source: str, target: str) -> bool:
        return await self.run_method('existsEdge', source, target)

    def remove_edge(self, source: str, target: str) -> None:
        self.run_method('removeEdge', source, target)

    def rename_node(self, node_id: str, new_label: str) -> None:
        self.run_method('renameNode', node_id, new_label)

    def add_node(self, node_id: str, label: str, node_type: NodeType) -> None:
        self.run_method('addNode', node_id, label, node_type.value)

    def remove_node(self, node_id: str) -> None:
        self.run_method('removeNode', node_id)

    def select_node(self, node_id: str) -> None:
        self.run_method('selectNode', node_id)
