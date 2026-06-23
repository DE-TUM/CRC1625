"""
Correctness tests for the workflow-instance validation cache:

  - storing a result in the cache (persistence)
  - cache hit  (no recomputing)
  - cache miss (a fresh / stale instance is recomputed)
  - invalidation through a workflow_instance change   (WorkflowInstance.get_overwrite_queries)
  - invalidation through a workflow_model change       (WorkflowModel.get_overwrite_queries)
  - invalidation through a data-tuple change         (validation_cache.invalidate_stale_validation_caches),
  - outside change: a change OUTSIDE the validation footprint must NOT invalidate footprint-hash
  - unchanged footprint is kept, already-stale is left alone, missing hash => changed
  - re-validation clearing the stale flag and computing the new hash
"""
import asyncio
import logging
import sys

from rdflib import URIRef

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore import WORKFLOWS_GRAPH_IRI, MAIN_GRAPH_IRI
from workflows_validation.common import dw_prefix
from workflows_validation.validation_cache import compute_footprint_hash, invalidate_stale_validation_caches
from workflows_validation.workflow_instance import WorkflowInstance
from workflows_validation.workflow_model import WorkflowModel
from workflows_validation.workflows_validator import is_workflow_instance_valid, ValidationStatus

from cache_performance_test.cache_test_common import (
    build_model_with_instances,
    reload_instances,
    store_in_cache,
)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

DEFAULT_N_STEPS = 5

# The shared store-setup scaffolding (building the scenario, reloading instances, storing a cache result)
# lives in cache_performance_test.cache_test_common, alongside `store_in_cache` (imported above). The
# wrappers below adapt the multi-instance helpers to this test's single-instance scenarios.
async def setup_scenario(n_steps: int = DEFAULT_N_STEPS) -> tuple[WorkflowModel, WorkflowInstance, URIRef]:
    """
    Builds and loads a single-instance valid scenario (data + model + instance). Returns
    (model, instance, entity_iri). The instance has no cache yet, so its first check is a miss.
    """
    model, instances, entity_iris = await build_model_with_instances(1, n_steps)
    return model, instances[0], entity_iris[0]


async def reload_instance(model: WorkflowModel, instance_iri: URIRef) -> WorkflowInstance:
    """Re-reads the instance from the store, so its cache fields match what the UI sees on reload."""
    return (await reload_instances(model))[instance_iri]


async def overall_status(model: WorkflowModel, instance: WorkflowInstance, individual: bool) -> ValidationStatus:
    """
    Validates and returns one status for both modes.
    """
    result = await is_workflow_instance_valid(model, instance, return_individual_results=individual, persist_cache=False)
    return result if not individual else _status_from_trace(result)


def _status_from_trace(trace) -> ValidationStatus:
    """Boils the detailed trace down to one overall status."""
    results = [r for paths in trace.values() for path in paths for reses in path.values() for r in reses]
    if not all(r.conforms for r in results):
        return ValidationStatus.Error
    if any(getattr(r, "is_missing_data", False) for r in results):
        return ValidationStatus.Warning
    return ValidationStatus.Valid


def _trace_shape(trace) -> dict:
    """
    A comparable summary of a detailed trace: per entity, per path, the (sorted) set of per-result fields.
    Within-step result ordering is not preserved by the cache, so each path's results are sorted.
    """
    shape = {}
    for entity, paths in trace.items():
        shape[str(entity)] = []
        for path in paths:
            step_results = []
            for step_iri, results in path.items():
                for r in results:
                    target = r.validation_job.target_node if r.validation_job else None
                    step_results.append((str(step_iri), r.conforms, r.is_missing_data,
                                         r.pyshacl_output, str(target) if target is not None else None))
            shape[str(entity)].append(sorted(step_results))
    return shape


# SPARQL for mutating the graphs from a test
async def change_group_project(group_iri: URIRef) -> None:
    """In-footprint change: changes a group's project (something the footprint looks at)."""
    await rdf_datastore_client.launch_update(f"""
        DELETE {{ GRAPH <{MAIN_GRAPH_IRI}> {{ <{group_iri}> <{dw_prefix.assignedTo}> ?p }} }}
        INSERT {{ GRAPH <{MAIN_GRAPH_IRI}> {{ <{group_iri}> <{dw_prefix.assignedTo}> <{dw_prefix.changed_project}> }} }}
        WHERE  {{ GRAPH <{MAIN_GRAPH_IRI}> {{ <{group_iri}> <{dw_prefix.assignedTo}> ?p }} }}
    """)


async def add_unrelated_triple() -> None:
    """Out-of-footprint change: a triple the footprint doesn't look at."""
    await rdf_datastore_client.launch_update(f"""
        INSERT DATA {{ GRAPH <{MAIN_GRAPH_IRI}> {{
            <{dw_prefix.unrelated_subject}> <{dw_prefix.unrelated_property}> "irrelevant"
        }} }}
    """)


async def force_stale(instance_iri: URIRef) -> None:
    """Marks the cache stale directly (stands in for an edit)."""
    await rdf_datastore_client.launch_update(f"""
        DELETE {{ GRAPH <{WORKFLOWS_GRAPH_IRI}> {{ <{instance_iri}> <{dw_prefix.validationCacheStale}> ?s }} }}
        INSERT {{ GRAPH <{WORKFLOWS_GRAPH_IRI}> {{ <{instance_iri}> <{dw_prefix.validationCacheStale}> true }} }}
        WHERE  {{ GRAPH <{WORKFLOWS_GRAPH_IRI}> {{ OPTIONAL {{ <{instance_iri}> <{dw_prefix.validationCacheStale}> ?s }} }} }}
    """)


async def delete_cache_hash(instance_iri: URIRef) -> None:
    """Removes the saved hash, like an instance cached before hashing existed."""
    await rdf_datastore_client.launch_update(f"""
        DELETE WHERE {{ GRAPH <{WORKFLOWS_GRAPH_IRI}> {{ <{instance_iri}> <{dw_prefix.validationCacheHash}> ?h }} }}
    """)


async def run_queries(queries: list[str]) -> None:
    for query in queries:
        await rdf_datastore_client.launch_update(query)


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
async def test_store_in_cache():
    """Storing a result saves all the cache fields, read back with the right types."""
    model, instance, _ = await setup_scenario()

    status = await is_workflow_instance_valid(model, instance, return_individual_results=False, persist_cache=False)  # miss -> Valid
    stored_hash = await store_in_cache(instance, status.name)

    reloaded = await reload_instance(model, instance.iri)
    assert reloaded.last_validated_at, "lastValidatedAt should be set after storing"
    assert reloaded.cached_validation_status == status.name, "cached status mismatch"
    assert reloaded.validation_cache_stale is False, "a freshly stored cache must not be stale"
    assert reloaded.validation_cache_hash == stored_hash, "persisted hash mismatch"
    assert reloaded.has_valid_cache(), "reloaded instance should report a usable cache"


async def test_cache_miss():
    """A fresh instance is recomputed; both modes give the true status."""
    model, instance, _ = await setup_scenario()
    for individual in (False, True):
        reloaded = await reload_instance(model, instance.iri)
        assert not reloaded.has_valid_cache(), "fresh instance must not have a usable cache"
        assert (await overall_status(model, reloaded, individual)) == ValidationStatus.Valid


async def test_cache_hit():
    """A poisoned cache is returned as-is, showing the cache is really used."""
    model, instance, _ = await setup_scenario()
    await store_in_cache(instance, ValidationStatus.Error.name)  # poison (true status is Valid)
    reloaded = await reload_instance(model, instance.iri)
    assert reloaded.has_valid_cache()

    # the simple mode uses the cache, so we get the poisoned status back
    assert (await overall_status(model, reloaded, individual=False)) == ValidationStatus.Error

    # the detailed mode reconstructs from cached per-step result nodes, but store_in_cache only writes the
    # overall status (no trace nodes), so here it finds no cached trace and recomputes the true status.
    # The detailed-trace cache hit is covered by test_individual_results_cache.
    assert (await overall_status(model, reloaded, individual=True)) == ValidationStatus.Valid


async def test_invalidation_via_instance_edit():
    """Editing the instance marks its cache stale, so it gets recomputed."""
    model, instance, _ = await setup_scenario()
    await store_in_cache(instance, ValidationStatus.Error.name)  # poison

    edited = await reload_instance(model, instance.iri)
    await run_queries(edited.get_overwrite_queries())  # marks the cache stale

    reloaded = await reload_instance(model, instance.iri)
    assert reloaded.validation_cache_stale is True, "instance edit must mark the cache stale"
    assert not reloaded.has_valid_cache()
    assert (await overall_status(model, reloaded, individual=False)) == ValidationStatus.Valid  # recomputed


async def test_invalidation_via_model_edit():
    """Editing the model marks the caches of all its instances stale."""
    model, instance, _ = await setup_scenario()
    await store_in_cache(instance, ValidationStatus.Error.name)  # poison

    await run_queries(model.get_overwrite_queries(model))  # also invalidates the model's instances

    reloaded = await reload_instance(model, instance.iri)
    assert reloaded.validation_cache_stale is True, "model edit must mark instance caches stale"
    assert (await overall_status(model, reloaded, individual=False)) == ValidationStatus.Valid  # recomputed


async def test_invalidation_via_tuple_change():
    """The data re-check marks stale only when the footprint really changed."""
    # change inside the footprint -> invalidated
    model, instance, entity_iri = await setup_scenario()
    status = await is_workflow_instance_valid(model, instance, return_individual_results=False, persist_cache=False)
    await store_in_cache(instance, status.name)
    await change_group_project(entity_iri)            # changes something the footprint looks at
    n = await invalidate_stale_validation_caches()
    reloaded = await reload_instance(model, instance.iri)
    assert n == 1 and reloaded.validation_cache_stale is True, "footprint change must invalidate"

    # change outside the footprint -> not invalidated
    model, instance, _ = await setup_scenario()
    status = await is_workflow_instance_valid(model, instance, return_individual_results=False, persist_cache=False)
    await store_in_cache(instance, status.name)
    await add_unrelated_triple()                      # changes something the footprint ignores
    n = await invalidate_stale_validation_caches()
    reloaded = await reload_instance(model, instance.iri)
    assert n == 0 and reloaded.validation_cache_stale is False, "non-footprint change must not invalidate"


async def test_footprint_hash_determinism_and_scope():
    """The hash stays the same for the same data, ignores unrelated changes, reacts to relevant ones."""
    model, instance, entity_iri = await setup_scenario()

    h1 = await compute_footprint_hash(instance.iri)
    h2 = await compute_footprint_hash(instance.iri)
    assert h1 == h2, "hash must be deterministic for unchanged data"

    await add_unrelated_triple()
    assert (await compute_footprint_hash(instance.iri)) == h1, "out-of-footprint change must not affect the hash"

    await change_group_project(entity_iri)
    assert (await compute_footprint_hash(instance.iri)) != h1, "in-footprint change must change the hash"


async def test_etl_invariants():
    """Unchanged stays cached; already-stale is left alone; a missing hash counts as changed."""
    # unchanged data: stays cached, still hits
    model, instance, _ = await setup_scenario()
    await store_in_cache(instance, ValidationStatus.Error.name)  # poison so we can spot a hit
    assert (await invalidate_stale_validation_caches()) == 0, "unchanged footprint must not be invalidated"
    reloaded = await reload_instance(model, instance.iri)
    assert reloaded.validation_cache_stale is False
    assert (await overall_status(model, reloaded, individual=False)) == ValidationStatus.Error  # still a hit

    # already-stale is skipped and stays stale (the re-check never clears it)
    model, instance, _ = await setup_scenario()
    await store_in_cache(instance, status_name=ValidationStatus.Valid.name)
    await force_stale(instance.iri)
    assert (await invalidate_stale_validation_caches()) == 0, "already-stale instances must be skipped"
    assert (await reload_instance(model, instance.iri)).validation_cache_stale is True

    # no saved hash (an old cache) -> treated as changed
    model, instance, _ = await setup_scenario()
    await store_in_cache(instance, ValidationStatus.Valid.name)
    await delete_cache_hash(instance.iri)
    assert (await invalidate_stale_validation_caches()) == 1, "missing hash must be treated as changed"
    assert (await reload_instance(model, instance.iri)).validation_cache_stale is True


async def test_revalidation_clears_stale():
    """Re-validating a stale instance clears the flag and saves the new hash."""
    model, instance, _ = await setup_scenario()
    await store_in_cache(instance, ValidationStatus.Valid.name)
    await force_stale(instance.iri)

    stale_instance = await reload_instance(model, instance.iri)
    assert stale_instance.validation_cache_stale is True

    status = await is_workflow_instance_valid(model, stale_instance, return_individual_results=False, persist_cache=False)  # miss
    new_hash = await store_in_cache(stale_instance, status.name)

    reloaded = await reload_instance(model, instance.iri)
    assert reloaded.validation_cache_stale is False, "re-validation must clear the stale flag"
    assert reloaded.validation_cache_hash == new_hash == await compute_footprint_hash(instance.iri)


async def test_individual_results_cache():
    """The per-step trace is persisted on a miss, served on a hit, and recomputed when stale."""
    model, instance, _ = await setup_scenario()

    # A detailed validation is a miss: it persists the overall status AND the per-step trace
    fresh = await is_workflow_instance_valid(model, instance, return_individual_results=True, persist_cache=True)
    reloaded = await reload_instance(model, instance.iri)
    assert reloaded.has_valid_cache(), "a detailed validation must populate the cache"

    # Served from the cached trace now (persist_cache=False proves it does not recompute and rewrite)
    cached = await is_workflow_instance_valid(model, reloaded, return_individual_results=True, persist_cache=False)
    assert _trace_shape(cached) == _trace_shape(fresh), "the cached trace must match the freshly computed one"

    # Marking the cache stale forces the detailed trace to be recomputed
    await force_stale(instance.iri)
    stale = await reload_instance(model, instance.iri)
    recomputed = await is_workflow_instance_valid(model, stale, return_individual_results=True, persist_cache=False)
    assert _status_from_trace(recomputed) == ValidationStatus.Valid, "a stale instance must be recomputed"


if __name__ == "__main__":
    tests = [
        test_store_in_cache,
        test_cache_miss,
        test_cache_hit,
        test_individual_results_cache,
        test_invalidation_via_instance_edit,
        test_invalidation_via_model_edit,
        test_invalidation_via_tuple_change,
        test_footprint_hash_determinism_and_scope,
        test_etl_invariants,
        test_revalidation_clears_stale,
    ]
    for test in tests:
        asyncio.run(test())
        logging.info("PASSED: %s", test.__name__)
    logging.info("All %d cache tests passed.", len(tests))
