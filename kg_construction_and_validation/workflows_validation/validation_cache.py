"""
In-process cache for workflow validation results, keyed on (model_iri, instance_iri).

The cached value is the rich per-path validation trace; the rolled-up ValidationStatus
is derived from it on demand, so both UI entry points share the same cache.

KNOWN LIMITATION: re-materialization (main.py) runs in a separate process from the
webserver and cannot invalidate this cache. Restart the webserver after re-materializing
to drop stale entries.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING
import logging

from rdflib import URIRef

if TYPE_CHECKING:
    from workflows_validation.workflows_validator import ValidationResult, ValidationJobWithMissingData

logger = logging.getLogger(__name__)


@dataclass
class CachedValidation:
    validation_results: dict[URIRef, list[dict[URIRef, list[ValidationResult]]]]
    steps_with_no_target_node: list[ValidationJobWithMissingData]


@dataclass
class ValidationCache:
    entries: dict[tuple[URIRef, URIRef], CachedValidation] = field(default_factory=dict)

    def get(self, model_iri: URIRef, instance_iri: URIRef) -> CachedValidation | None:
        result = self.entries.get((model_iri, instance_iri))
        if result is None:
            logger.debug("[CACHE miss]: model=%s instance=%s", model_iri, instance_iri)
        else:
            logger.debug("[CACHE hit]:  model=%s instance=%s", model_iri, instance_iri)
        return result

    def put(self, model_iri: URIRef, instance_iri: URIRef, value: CachedValidation) -> None:
        self.entries[(model_iri, instance_iri)] = value
        logger.debug("[CACHE put instance]: model=%s instance=%s", model_iri, instance_iri)

    def invalidate_instance(self, model_iri: URIRef, instance_iri: URIRef) -> None:
        result = self.entries.pop((model_iri, instance_iri), None)
        if result is None:
            logger.debug("[CACHE invalidate instance (not in cache)]: model=%s instance=%s", model_iri, instance_iri)
        else:
            logger.debug("[CACHE invalidate instance]: model=%s instance=%s", model_iri, instance_iri)

    def invalidate_model(self, model_iri: URIRef) -> None:
        deleted = []
        for key in [k for k in self.entries if k[0] == model_iri]:
            deleted.append(key)
            del self.entries[key]
        if not deleted:
            logger.debug("[CACHE invalidate model (not in cache)]: model=%s", model_iri)
        else:
            logger.debug("[CACHE invalidate model]: model=%s count=%d", model_iri, len(deleted))


cache = ValidationCache()