from handover_workflows_validation_webui.visualization_ui.sparql_graph_adapter import EdgeSpec


HANDOVER_WORKFLOW_EDGE_SPECS = [
    EdgeSpec(
        source="handover_group",
        target="nextGroup",
        source_kind="ho_g",
        target_kind="ho_g",
    ),
    EdgeSpec(
        source="handover_group",
        target="handover_of_group",
        source_kind="ho_g",
        target_kind="ho_o_g",
    ),
    EdgeSpec(
        source="handover_of_group",
        target="activity",
        source_kind="ho_o_g",
        target_kind="act",
    ),
    EdgeSpec(
        source="activity",
        target="output",
        source_kind="act",
        target_kind="output",
    ),
]


QUERY_EDGE_SPECS = {
    "handover_workflow": HANDOVER_WORKFLOW_EDGE_SPECS,
}