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

from rdflib import URIRef

if TYPE_CHECKING:
    from workflows_validation.workflows_validator import ValidationResult, ValidationJobWithMissingData


@dataclass
class CachedValidation:
    validation_results: dict[URIRef, list[dict[URIRef, list[ValidationResult]]]]
    steps_with_no_target_node: list[ValidationJobWithMissingData]


@dataclass
class ValidationCache:
    entries: dict[tuple[URIRef, URIRef], CachedValidation] = field(default_factory=dict)

    def get(self, model_iri: URIRef, instance_iri: URIRef) -> CachedValidation | None:
        return self.entries.get((model_iri, instance_iri))

    def put(self, model_iri: URIRef, instance_iri: URIRef, value: CachedValidation) -> None:
        self.entries[(model_iri, instance_iri)] = value

    def invalidate_instance(self, model_iri: URIRef, instance_iri: URIRef) -> None:
        self.entries.pop((model_iri, instance_iri), None)

    def invalidate_model(self, model_iri: URIRef) -> None:
        for key in [k for k in self.entries if k[0] == model_iri]:
            del self.entries[key]


cache = ValidationCache()
