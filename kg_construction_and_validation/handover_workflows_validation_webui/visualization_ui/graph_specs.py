from handover_workflows_validation_webui.visualization_ui.sparql_graph_adapter import EdgeSpec


HANDOVER_WORKFLOW_EDGE_SPECS = [
    EdgeSpec(
        source="handover_group",
        target="nextGroup",
        source_kind="ho_g",
        target_kind="ho_g",
        source_label="handover_group_name",
        target_label="nextGroup_name"
    ),
    EdgeSpec(
        source="handover_group",
        target="handover_of_group",
        source_kind="ho_g",
        target_kind="ho_o_g",
        source_label="handover_group_name",
        target_label="handover_of_group_name",
    ),
    EdgeSpec(
        source="handover_of_group",
        target="activity",
        source_kind="ho_o_g",
        target_kind="act",
        source_label="handover_of_group_name",
        target_label="activity_name",
    ),
    EdgeSpec(
        source="activity",
        target="output",
        source_kind="act",
        target_kind="output",
        source_label="activity_name",
        target_label="output_name",
    ),
]


QUERY_EDGE_SPECS = {
    "handover_workflow": HANDOVER_WORKFLOW_EDGE_SPECS,
}