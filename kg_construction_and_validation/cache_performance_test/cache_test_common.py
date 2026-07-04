"""
Shared store-setup scaffolding for the validation-cache tests:
  - run_cache_test.py             (correctness)
  - run_cache_performance_test.py (performance)

Holds only the boring, identical plumbing both tests need to get the datastore into a known state:
clearing the graphs, building one model with N instances that each own their data, uploading it,
reloading instances from the store, and storing a cache result. Test-specific logic stays in the test
files (correctness mutations and assertions; performance measurement and plotting).

Assumes the RDF datastore is running. These helpers clear datastore contents.
"""
import uuid

from rdflib import Graph, URIRef

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore import WORKFLOWS_GRAPH_IRI, MAIN_GRAPH_IRI
from workflows_validation.common import dw_prefix
from workflows_validation.extra_functions import get_workflow_instances_assigned_to_model
from workflows_validation.validation_cache import compute_footprint_hash
from workflows_validation.workflow_instance import WorkflowInstance
from workflows_validation.workflow_model import WorkflowModel
from workflows_validation.workflows_validator import ValidationStatus

from run_handover_workflows_validation_test import (
    generate_handover_group_definition,
    generate_handover_group_triples,
    generate_workflow_model_and_instance_for_handover_group_definition,
)


async def clear_cache_test_graphs() -> None:
    """Wipes both the main data graph and the workflows graph, for a clean, known starting state."""
    await rdf_datastore_client.clear_triples()
    await rdf_datastore_client.clear_triples(WORKFLOWS_GRAPH_IRI)


def make_entity_graph_unique(graph: Graph, entity_iri: URIRef, suffix: str) -> tuple[Graph, URIRef]:
    """
    `generate_handover_group_triples` reuses fixed IRIs (handover_workflow_instance, handover_group_<i>)
    on every call, so several instances would otherwise share the same entity data. This rewrites those
    colliding IRIs with a per-instance suffix, yielding a distinct entity graph (and its entity IRI) so
    every instance validates against its own data, like in reality. The uuid-based handover/activity
    nodes are already unique and left untouched.
    """
    workflow_instance_iri = str(dw_prefix.handover_workflow_instance)
    handover_group_prefix = str(dw_prefix) + "handover_group_"

    def remap(node):
        if isinstance(node, URIRef):
            node_str = str(node)
            if node_str == workflow_instance_iri or node_str.startswith(handover_group_prefix):
                return URIRef(f"{node_str}__inst{suffix}")
        return node

    unique_graph = Graph()
    for s, p, o in graph:
        unique_graph.add((remap(s), remap(p), remap(o)))

    return unique_graph, remap(entity_iri)


async def upload_entity_graph(graph: Graph) -> None:
    """Serializes an entity data graph to a temporary Turtle file and bulk-loads it into the main graph."""
    ttl_path = f"{uuid.uuid4().hex}.ttl"
    graph.serialize(destination=ttl_path, format='turtle')
    await rdf_datastore_client.upload_file(ttl_path, graph_iri=MAIN_GRAPH_IRI, delete_file_after_upload=True)


async def build_model_with_instances(n_instances: int,
                                     n_steps: int) -> tuple[WorkflowModel, list[WorkflowInstance], list[URIRef]]:
    """
    Clears both graphs and builds one workflow model with `n_instances` instances assigned to it, each
    owning its own (valid) handover-group data. Loads the data, the model and the instances into the
    store. The instances have no cache yet, so their first validation is a miss.

    Returns (model, instances, entity_iris), where entity_iris[i] is the entity that instances[i]
    validates against.
    """
    await clear_cache_test_graphs()

    # One shared model. The generator is deterministic for a given definition (fixed step IRIs), so we
    # build the model once and reuse it across every instance.
    definition = generate_handover_group_definition(n_steps)
    model, _ = generate_workflow_model_and_instance_for_handover_group_definition(definition, dw_prefix["__model_seed_entity"])

    instances: list[WorkflowInstance] = []
    entity_iris: list[URIRef] = []
    for i in range(n_instances):
        main_graph, entity_iri = generate_handover_group_triples(definition)
        main_graph, entity_iri = make_entity_graph_unique(main_graph, entity_iri, str(i))

        # Reuse the generator to build the instance + its step assignments, then point it at the shared
        # model. The model it returns is identical to `model` (same fixed step IRIs) and is discarded.
        _, instance = generate_workflow_model_and_instance_for_handover_group_definition(definition, entity_iri)
        instance.workflow_model_iri = model.iri

        await upload_entity_graph(main_graph)
        instances.append(instance)
        entity_iris.append(entity_iri)

    await rdf_datastore_client.launch_update(model.get_insert_query())
    for instance in instances:
        await rdf_datastore_client.launch_update(instance.get_insert_query())

    return model, instances, entity_iris


async def reload_instances(model: WorkflowModel) -> dict[URIRef, WorkflowInstance]:
    """
    Re-reads the model's instances from the store (indexed by IRI), so their cache fields match what the
    UI sees on reload.
    """
    return await get_workflow_instances_assigned_to_model(model, rdf_datastore_client.launch_query)


async def store_in_cache(instance: WorkflowInstance | list[WorkflowInstance],
                         status_name: str = ValidationStatus.Valid.name) -> str | list[str]:
    """
    Does what the UI does after validating: hashes each instance's current data and persists its cache
    fields, returning the stored footprint hash(es).

    Accepts either a single instance (returns its hash) or a list of them (returns a list of hashes, e.g.
    to prime the cache of every instance of a model for an all-hits run). `status_name` defaults to a
    valid result, but may be a deliberately wrong status to "poison" the cache for hit tests.
    """
    if isinstance(instance, WorkflowInstance):
        return await _store_one_in_cache(instance, status_name)
    return [await _store_one_in_cache(single, status_name) for single in instance]


async def _store_one_in_cache(instance: WorkflowInstance, status_name: str) -> str:
    footprint_hash = await compute_footprint_hash(instance.iri)
    instance.mark_validated(status_name, footprint_hash)
    await rdf_datastore_client.launch_update(instance.get_cache_update_query())
    return footprint_hash


async def mark_instances_stale(instances: list[WorkflowInstance]) -> None:
    """
    Marks the given instances' caches stale in a single update, so their next validation is a miss.

    Used by the performance test to establish a chosen cache-hit ratio: validation re-caches misses
    during a measured pass, so the miss subset must be re-staled before every pass.
    """
    if not instances:
        return

    iris = ", ".join(f"<{instance.iri}>" for instance in instances)
    await rdf_datastore_client.launch_update(f"""
        DELETE {{ GRAPH <{WORKFLOWS_GRAPH_IRI}> {{ ?instance <{dw_prefix.validationCacheStale}> ?stale }} }}
        INSERT {{ GRAPH <{WORKFLOWS_GRAPH_IRI}> {{ ?instance <{dw_prefix.validationCacheStale}> true }} }}
        WHERE  {{ GRAPH <{WORKFLOWS_GRAPH_IRI}> {{
            ?instance <{dw_prefix.workflowModelInstanceOf}> ?model .
            OPTIONAL {{ ?instance <{dw_prefix.validationCacheStale}> ?stale }}
            FILTER(?instance IN ({iris}))
        }} }}
    """)


async def mark_model_instances_stale(model: WorkflowModel) -> None:
    """
    Marks every instance of `model` as having a stale cache, so the next validation of each is a guaranteed
    miss. The no-cache performance regime calls this before each pass: validation now persists its result on
    a miss, so without it a reloaded instance would hit the cache on the following pass.
    """
    await rdf_datastore_client.launch_update(f"""
        DELETE {{ GRAPH <{WORKFLOWS_GRAPH_IRI}> {{ ?instance <{dw_prefix.validationCacheStale}> ?stale }} }}
        INSERT {{ GRAPH <{WORKFLOWS_GRAPH_IRI}> {{ ?instance <{dw_prefix.validationCacheStale}> true }} }}
        WHERE  {{ GRAPH <{WORKFLOWS_GRAPH_IRI}> {{
            ?instance <{dw_prefix.workflowModelInstanceOf}> <{model.iri}> .
            OPTIONAL {{ ?instance <{dw_prefix.validationCacheStale}> ?stale }}
        }} }}
    """)
