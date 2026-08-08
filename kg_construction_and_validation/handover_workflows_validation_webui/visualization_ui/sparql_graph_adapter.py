from dataclasses import dataclass
import re

#from prettytable.__main__ import row


@dataclass(frozen=True)
class EdgeSpec:
    source: str
    target: str
    source_kind: str
    target_kind: str
    source_label: str | None = None
    target_label: str | None = None


def extract_label_from_uri(uri: str) -> str:
    """Extract readable label from URI or return string as-is."""
    
    if not uri:
        return ""

    if "#" in uri:
        return uri.split("#")[-1]

    if "/" in uri:
        return uri.split("/")[-1]

    return uri


def binding_value(row: dict, key: str, default=None):
    """Safely read one value from a SPARQL JSON binding row."""
    value = row.get(key)

    if isinstance(value, dict):
        return value.get("value", default)

    return value if value is not None else default


def make_binding(value, value_type: str = "uri") -> dict:
    """Create a SPARQL-result-like binding dict."""
    return {
        "type": value_type,
        "value": value,
    }


def normalize_kind(kind: str | None) -> str:
    """
    Normalize query/RDF kind names into identifiers used by JS.

    JS uses `ho_g` to detect the horizontal sequence.
    Everything else is treated as a downward branch.
    """
    if not kind:
        return "generic"

    readable = extract_label_from_uri(kind)

    kind_map = {
        "handover_group": "ho_g",
        "HandoverGroup": "ho_g",
        "ho_g": "ho_g",

        "handover_of_group": "ho_o_g",
        "handoverOfGroup": "ho_o_g",
        "HandoverOfGroup": "ho_o_g",
        "ho_o_g": "ho_o_g",

        "activity": "act",
        "Activity": "act",
        "act": "act",

        "output": "output",
        "Output": "output",
    }

    return kind_map.get(readable, readable)


def infer_kind_from_variable(variable_name: str, level_index: int | None = None) -> str:
    """Infer kind from a query variable name."""
    name = variable_name.lower()

    if name in {"handover_group", "nextgroup", "next_group"}:
        return "ho_g"

    if name in {"handover_of_group", "handoverofgroup"}:
        return "ho_o_g"

    if name in {"activity", "act"}:
        return "act"

    if name in {"output", "measurement", "result"}:
        return name

    if level_index == 0:
        return "ho_g"

    return "generic"


def ordered_level_keys(row: dict) -> list[str]:
    """
    Detect path-style result variables:
      level_0, level_1, level_2 ...
      node_0, node_1, node_2 ...
      step_0, step_1, step_2 ...
    """
    patterns = [
        re.compile(r"^(level|node|step|entity)_(\d+)$"),
        re.compile(r"^(level|node|step|entity)(\d+)$"),
    ]

    found = []

    for key in row.keys():
        for pattern in patterns:
            match = pattern.match(key)
            if match:
                found.append((int(match.group(2)), key))
                break

    found.sort(key=lambda item: item[0])
    return [key for _, key in found]


def kind_for_path_key(row: dict, key: str, index: int) -> str:
    """
    Supports optional kind/type variables:
      level_0, level_0_kind
      level_1, level_1_kind

    Or:
      level_0, kind_0
      level_1, kind_1
    """
    possible_kind_keys = [
        f"{key}_kind",
        f"{key}_type",
        f"kind_{index}",
        f"type_{index}",
    ]

    for kind_key in possible_kind_keys:
        kind_value = binding_value(row, kind_key)
        if kind_value:
            return normalize_kind(kind_value)

    return infer_kind_from_variable(key, index)


def translate_rows_to_generic_edges(
    results: list[dict],
    edge_specs: list[EdgeSpec],
) -> list[dict]:
    """
    Convert normal/wide SPARQL rows into generic graph edge rows.

    Output shape:
      source | target | source_kind | target_kind

    Missing variables are skipped.
    """
    generic_rows = []
    seen_edges = set()

    for row in results:
        for spec in edge_specs:
            source = binding_value(row, spec.source)
            target = binding_value(row, spec.target)
            source_label = binding_value(row, spec.source_label) if spec.source_label else None
            target_label = binding_value(row, spec.target_label) if spec.target_label else None

            if not source or not target:
                continue

            if source == target:
                continue

            edge_key = (
                source,
                target,
                spec.source_kind,
                spec.target_kind,
            )

            if edge_key in seen_edges:
                continue


            generic_row = {
                "source": make_binding(source),
                "target": make_binding(target),
                "source_kind": make_binding(spec.source_kind, "literal"),
                "target_kind": make_binding(spec.target_kind, "literal"),
            }

            if source_label:
                generic_row["source_label"] = make_binding(source_label, "literal")

            if target_label:
                generic_row["target_label"] = make_binding(target_label, "literal")


            generic_rows.append(generic_row)

            seen_edges.add(edge_key)

    return generic_rows


def translate_path_rows_to_generic_edges(results: list[dict]) -> list[dict]:
    """
    Convert path-style rows into generic graph edges.

    Example:
      level_0 | level_1 | level_2 | level_3

    Becomes:
      level_0 -> level_1
      level_1 -> level_2
      level_2 -> level_3
    """
    generic_rows = []
    seen_edges = set()

    for row in results:
        level_keys = ordered_level_keys(row)

        if len(level_keys) < 2:
            continue

        for index in range(len(level_keys) - 1):
            source_key = level_keys[index]
            target_key = level_keys[index + 1]

            source = binding_value(row, source_key)
            target = binding_value(row, target_key)

            if not source or not target:
                continue

            if source == target:
                continue

            source_kind = kind_for_path_key(row, source_key, index)
            target_kind = kind_for_path_key(row, target_key, index + 1)

            edge_key = (source, target, source_kind, target_kind)

            if edge_key in seen_edges:
                continue

            generic_rows.append({
                "source": make_binding(source),
                "target": make_binding(target),
                "source_kind": make_binding(source_kind, "literal"),
                "target_kind": make_binding(target_kind, "literal"),
            })

            seen_edges.add(edge_key)

    return generic_rows


def translate_sparql_results_to_generic_edges(
    results: list[dict],
    edge_specs: list[EdgeSpec] | None = None,
) -> list[dict]:
    """
    Universal adapter.

    1. If rows already contain source/target, return them.
    2. If explicit EdgeSpec mapping is provided, use it.
    3. If rows contain level_0/level_1/... variables, convert as path.
    4. Otherwise return empty list.
    """
    if not results:
        return []

    first_row = results[0]

    if "source" in first_row and "target" in first_row:
        return results

    if edge_specs:
        return translate_rows_to_generic_edges(results, edge_specs)

    if ordered_level_keys(first_row):
        return translate_path_rows_to_generic_edges(results)

    print("WARNING: Could not translate SPARQL results into generic graph edges.")
    print("Available columns:", list(first_row.keys()))

    return []


def build_cytoscape_graph_from_generic_edges(
    generic_rows: list[dict],
    object_id: str,
    node_class: str,
    use_sample_parent: bool = True,
):
    nodes = []
    edges = []

    seen_nodes = {}
    seen_edges = set()

    sample_node_id = f"sample_{object_id}"

    def add_node(
        node_id: str,
        kind: str = "generic",
        parent_id: str | None = None,
        label: str | None = None,
    ):
        if not node_id:
            return

        kind = normalize_kind(kind)
        fallback_label = extract_label_from_uri(node_id)
        clean_label = label or fallback_label

        if node_id in seen_nodes:
            existing_node = seen_nodes[node_id]
            existing_label = existing_node["data"].get("label")

            # If the node was first created with URI fallback,
            # but later appears with crc:objectName, update it.
            if label and (
                not existing_label
                or existing_label == fallback_label
            ):
                existing_node["data"]["label"] = label

            existing_ids = existing_node["data"].get("identifiers_for_coloring", [])
            if kind not in existing_ids:
                existing_ids.append(kind)

            existing_node["data"]["identifiers_for_coloring"] = existing_ids

            if not existing_node["data"].get("kind"):
                existing_node["data"]["kind"] = kind

            return

        data = {
            "id": node_id,
            "label": clean_label,
            "kind": kind,
            "projects": [],
            "activities": [],
            "identifiers_for_coloring": [kind],
        }

        if parent_id:
            data["parent"] = parent_id

        node = {
            "data": data,
            "classes": f"{node_class} {kind}",
        }

        nodes.append(node)
        seen_nodes[node_id] = node

    def add_edge(source: str, target: str):
        if not source or not target:
            return

        if source == target:
            return

        edge_key = (source, target)

        if edge_key in seen_edges:
            return

        edges.append({
            "data": {
                "id": f"edge_{len(edges)}",
                "source": source,
                "target": target,
            }
        })

        seen_edges.add(edge_key)

    parent_id = None

    if use_sample_parent:
        sample_node = {
            "data": {
                "id": sample_node_id,
                "label": f"Sample (ID: {object_id})",
                "kind": "sample",
                "projects": [],
                "activities": [],
                "identifiers_for_coloring": ["sample"],
            },
            "classes": f"{node_class} sample",
        }

        nodes.append(sample_node)
        seen_nodes[sample_node_id] = sample_node
        parent_id = sample_node_id

    for row in generic_rows:
        source = binding_value(row, "source")
        target = binding_value(row, "target")

        source_kind = normalize_kind(binding_value(row, "source_kind"))
        target_kind = normalize_kind(binding_value(row, "target_kind"))


        source_label = binding_value(row, "source_label")
        target_label = binding_value(row, "target_label")

        add_node(source, source_kind, parent_id, source_label)
        add_node(target, target_kind, parent_id, target_label)
        add_edge(source, target)

    return nodes, edges