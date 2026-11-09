# Visualisation UI Documentation
This document provides an overall view of the visualisation user interface, aka folder visualization.ui.
The relevant contents of the folder are:
- Cytoscape visualization files: javascript file with custom cytoscape layout and a python file that functions as a wrapper for the javascript, as the majority of visualisation code and its logic are written in python, as nobody could be bothered with working in pure js.
- Queries folder: contains all queries that are used in the workflow visualisation. To learn more about them go directly to the queries files and read their descriptions.
- graph specs and adapter files: these contain logic to help transform retrieved data into a format that works for cytoscape layout.
- Visualization file: handles the landing page and transfers between samples.

The next section of this document will be dealing with architecture of each file mentioned above, and a bit more in depth insight on what's going on in there. Though, the explanation of each function logic will be handled directly in the respective file as in-line comments/documentation comments.

TODO: write the rest of the markdown

## Larger functions
Here I will be documenting some of the larger functions of each class, that require more in-depth explanation.
### **Visualization_ui**


#### *foo*
___
### **Sparql_graph_adapter**
### *translate_sparql_results_to_generic_edges*
def translate_sparql_results_to_generic_edges(
    results: list[dict],
    edge_specs: list[EdgeSpec] | None = None,) -> list[dict]:

    Parameters:
    results : list[dict]
        Raw SPARQL result bindings.
    edge_specs : list[EdgeSpec] | None
        Optional mappings for converting non-standard SPARQL
        result columns into source-target relationships.

    Returns:
    list[dict]
        Normalized graph rows containing complete relationships
        and, where applicable, source-only entries.

    Idea: Normalize SPARQL query results into a generic graph representation.

The function supports three input formats:
1. Direct-edge results:
    Rows containing source, target, source_kind and target_kind.
2. EdgeSpec-based results:
    Rows with arbitrary variable names interpreted using predefined EdgeSpec mappings.
3. Path-based results:
    Rows containing ordered path variables such as level_0, level_1 and level_2.

For direct-edge results, the function processes each row independently and:
- Extracts source and target entity identifiers.
- Normalizes source and target entity types.
- Determines the relationship type (structural orsecondary_sequence).
- Preserves source-only rows when no target exists.
- Filters self-loops.
- Removes duplicate relationships and source-only entries.
- Generates fallback labels from entity URIs when needed.

The resulting rows retain a SPARQL-binding-like structure and can be passed to build_cytoscape_graph_from_generic_edges() for Cytoscape node and edge construction.
  
Notes:
This function does not create Cytoscape elements or calculate graph positions. Those operations are handled by subsequent graph-building and layout functions.


    Function in a graph:

    START
    │
    ▼
    Receive SPARQL results
    │
    ▼
    Are results empty?
    │
    ├── YES ──► Return []
    │
    ▼ NO
    Detect result format
    │
    ├── Direct edges ────────────────────────┐
    │                                        │
    │                                        ▼
    │                              FOR EACH RESULT ROW
    │                                        │
    │                                        ▼
    │                              Extract source/target
    │                                        │
    │                              Is source missing?
    │                                        │
    │                              YES ──► Skip row
    │                                        │
    │                                       NO
    │                                        │
    │                                        ▼
    │                              Normalize entity kinds
    │                                        │
    │                                        ▼
    │                              Determine edge kind
    │                                        │
    │                                        ▼
    │                              Is target missing?
    │                                        │
    │                            ┌───────────┴───────────┐
    │                           YES                     NO
    │                            │                       │
    │                            ▼                       ▼
    │                     Source-only row        Check self-loop
    │                            │                       │
    │                            ▼                       ▼
    │                     Check duplicate         Check duplicate
    │                            │                       │
    │                            ▼                       ▼
    │                     Add fallback label      Add fallback labels
    │                            │                       │
    │                            ▼                       ▼
    │                     Preserve source row     Preserve edge row
    │                            │                       │
    │                            └───────────┬───────────┘
    │                                        │
    │                                        ▼
    │                                   NEXT ROW
    │                                        │
    │                                        ▼
    │                              All rows processed?
    │                                        │
    │                                        ▼
    │                                Print statistics
    │                                        │
    │                                        ▼
    │                               Return valid_rows
    │
    ├── EdgeSpec format ──► Translate using EdgeSpecs
    │
    ├── Path format ──────► Translate consecutive path levels
    │
    └── Unknown ─────────► Warning + return []


### **build_cytoscape_graph_from_generic_edges**
    def build_cytoscape_graph_from_generic_edges(
        generic_rows: list[dict],
        object_id: str,
        node_class: str,
        use_sample_parent: bool = True,
    ):

Build Cytoscape-compatible graph elements from normalized edge rows.

This function transforms generic graph rows into two collections: unique nodes and unique directed edges.

Each input row describes either a relationship between two entities or a source entity without a bound target.

    Parameters:
        generic_rows : list[dict]
            Normalized graph rows containing source and target identifiers,
            node classifications, optional labels, and edge classifications.
        object_id : str
            Internal ID of the sample being visualized.
        node_class : str
            CSS class applied to generated Cytoscape nodes.
        use_sample_parent : bool, optional
            If True, create a compound sample container and assign
            all workflow nodes to it. Defaults to True.

    Returns:
    tuple[list[dict], list[dict]]
        A tuple containing:
        nodes:
            Cytoscape-compatible node dictionaries with unique
            identifiers, labels, classifications, and optional parents.
        edges:
            Cytoscape-compatible directed edge dictionaries containing
            source and target identifiers and relationship classifications.


Graph construction proceeds as follows:
1. Initialize node and edge collections and deduplication structures.
2. Optionally create a compound sample node that acts as thevisual parent of all workflow entities.
3. Iterate over the normalized graph rows and extract source,target, entity kinds, labels, and relationship classifications.
4. Create source and target nodes using their entity URIs as unique identifiers. Reuse existing nodes when the same entity appears in multiple relationships.
5. Update previously created nodes when additional information, such as a meaningful label or entity classification, becomes available.
6. Create directed edges between existing source and target nodes. Skip incomplete relationships, self-loops, and duplicate edges.
7. Return the completed node and edge collections.

Node deduplication is based on the entity URI.

Edge deduplication is based on the combination of source URI, target URI, and edge classification.
Nodes are assigned a primary kind and a list of identifiers used for coloring and classification.

When use_sample_parent is enabled, a synthetic sample node is created. All workflow nodes are assigned to this container using Cytoscape's compound-node parent property.
The compound parent relationship is distinct from workflow relationships represented by graph edges.

Notes: 
 The function constructs graph data but does not instantiate Cytoscape.js elements or calculate node positions.
 Layout and rendering are handled subsequently by the Cytoscape visualization component.


    Processing logic in graph:
        START
          │
          ▼
        Receive generic graph rows
          │
          ▼
        Initialize:
        - nodes
        - edges
        - seen_nodes
        - seen_edges
          │
          ▼
        Define add_node() helper
          │
          ▼
        Define add_edge() helper
          │
          ▼
        use_sample_parent == True?
          │
          ├── YES
          │     │
          │     ▼
          │   Create sample container
          │     │
          │     ▼
          │   Register sample as parent
          │
          └── NO
                │
                ▼
            No parent assigned
                │
                ▼
        ┌─────────────────────────────┐
        │ FOR EACH GENERIC ROW        │
        └─────────────────────────────┘
                │
                ▼
        Extract:
        - source
        - target
        - source_kind
        - target_kind
        - source_label
        - target_label
        - edge_kind
                │
                ▼
            add_node(source)
                │
                ▼
        Does source already exist?
                │
            ┌───┴────┐
        YES       NO
            │         │
            ▼         ▼
        Update      Create
        existing    new node
        node
            │         │
            └────┬────┘
                │
                ▼
        Does target exist?
                │
            ┌────┴────┐
        YES        NO
            │          │
            ▼          │
        add_node(target)
            │          │
            ▼          │
        Create or      │
        update target  │
            │          │
            ▼          │
        add_edge()     │
            │          │
            ▼          │
        Validate edge  │
            │          │
            ▼          │
        Check duplicate│
            │          │
            ▼          │
        Append edge    │
            │          │
            └────┬─────┘
                │
                ▼
            NEXT ROW
                │
                ▼
        All rows processed?
                │
                ▼
        Return (nodes, edges)
                │
                ▼
                END

