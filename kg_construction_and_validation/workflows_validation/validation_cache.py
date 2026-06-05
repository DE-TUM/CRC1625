"""
Keeps the workflow-instance validation cache correct when the underlying data changes.

Each cached instance stores (in the workflows graph):
    crc:lastValidatedAt        - when it was cached
    crc:cachedValidationStatus - the cached status (Valid/Warning/Error)
    crc:validationCacheStale   - whether it must be recomputed
    crc:validationCacheHash    - hash of the data tuples the result was based on

Edits to an instance/model already flip the stale flag elsewhere. This module handles the other case:
after a re-materialization, it re-hashes each cached instance's data and marks it stale if the hash
changed.

Used by both the WebUI (to store the hash when caching) and the ETL pipeline (to compare hashes). Both
reach the store via rdf_datastore_client, so this works from either process.

The footprint query mirrors the SHACL shape in
CRC_1625_workflows_validator/CRC_1625_handover_workflow_group_shape.shacl - keep them in sync.
"""
import hashlib
import logging
import os

from rdflib import URIRef

from datastores.rdf import rdf_datastore_client
from workflows_validation.common import prefixes

logger = logging.getLogger(__name__)

module_dir = os.path.dirname(__file__)

validation_footprint_query = prefixes + open(os.path.join(module_dir, 'queries/validation_footprint.sparql'), 'r').read()
cached_workflow_instances_query = prefixes + open(os.path.join(module_dir, 'queries/cached_workflow_instances.sparql'), 'r').read()
mark_workflow_instance_cache_stale_query = prefixes + open(os.path.join(module_dir, 'queries/mark_workflow_instance_cache_stale.sparql'), 'r').read()


async def compute_footprint_hash(workflow_instance_iri: URIRef | str) -> str:
    """
    Hashes the exact set of data tuples this instance's validation depends on (see
    queries/validation_footprint.sparql). Same data -> same hash, so a changed hash means the data
    changed.

    The tuples are sorted before hashing so the result doesn't depend on SPARQL row order. SHA-256 is
    used (not Python's hash()) because hash() differs between processes.
    """
    query = validation_footprint_query.replace("{workflow_instance_iri}", str(workflow_instance_iri))
    result = await rdf_datastore_client.launch_query(query)

    lines = [
        f'{binding["s"]["value"]}\x01{binding["p"]["value"]}\x01{binding["o"]["value"]}'
        for binding in result["results"]["bindings"]
    ]
    lines.sort()

    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


async def invalidate_stale_validation_caches() -> int:
    """
    Re-hashes every non-stale cached instance and marks the ones whose data changed as stale. Run once
    after a re-materialization.

    Returns how many instances were marked as stale.
    """
    result = await rdf_datastore_client.launch_query(cached_workflow_instances_query)
    bindings = result["results"]["bindings"]

    invalidated = 0
    for binding in bindings:
        instance_iri = binding["workflow_instance"]["value"]
        # No stored hash (e.g. cached before this feature) -> treat as changed
        stored_hash = binding["hash"]["value"] if "hash" in binding else None

        current_hash = await compute_footprint_hash(instance_iri)
        if current_hash != stored_hash:
            await rdf_datastore_client.launch_update(
                mark_workflow_instance_cache_stale_query.replace("{workflow_instance_iri}", instance_iri))
            invalidated += 1

    logger.info("Validation cache: marked %d of %d cached instance(s) stale after re-materialization",
                invalidated, len(bindings))
    return invalidated
