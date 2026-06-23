import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging

from rdflib import URIRef, Graph, Literal, XSD

from datastores.rdf.rdf_datastore import WORKFLOWS_GRAPH_IRI
from workflows_validation.common import BaseWorkflowElement, dw_prefix, base_workflow_element_iri_to_config_key, prefixes, rdf_prefix, generate_unique_identifier
from workflows_validation.workflow_model import WorkflowModel, WorkflowModelStep

module_dir = os.path.dirname(__file__)

delete_workflow_instance_query = prefixes + open(os.path.join(module_dir, 'queries/delete_workflow_instance.sparql'), 'r').read()
update_workflow_instance_validation_cache_query = prefixes + open(os.path.join(module_dir, 'queries/update_workflow_instance_validation_cache.sparql'), 'r').read()
delete_cached_validation_results_query = prefixes + open(os.path.join(module_dir, 'queries/delete_cached_validation_results.sparql'), 'r').read()


workflow_instance_iri_to_config_key = {
    **base_workflow_element_iri_to_config_key,
    str(dw_prefix.workflowModelInstanceOf): "workflow_model_iri",
    str(dw_prefix.lastValidatedAt): "last_validated_at",
    str(dw_prefix.cachedValidationStatus): "cached_validation_status",
    str(dw_prefix.validationCacheStale): "validation_cache_stale",
    str(dw_prefix.validationCacheHash): "validation_cache_hash",
}
workflow_instance_config_key_to_iri = {v: k for k, v in workflow_instance_iri_to_config_key.items()}

step_assignment_iri_to_config_key = {
    **base_workflow_element_iri_to_config_key,
    # step_templates actually belongs to the workflow instance, but we handle their creation outside the automated config methods
    str(dw_prefix.hasAssignment): "step_assignments",
    str(dw_prefix.assignedWorkflowModelStep): "workflow_step_iri",
    str(dw_prefix.assignedEntity): "assigned_entities",
    str(dw_prefix.propertyToFollow): "property_to_follow",
}
step_assignment_config_key_to_iri = {v: k for k, v in step_assignment_iri_to_config_key.items()}

@dataclass
class StepAssignment(BaseWorkflowElement):
    """
    An assignment of entities for a given step
    """

    """
    Workflow model step it refers to
    """
    workflow_step_iri: URIRef = ""

    """
    Entity IRIs that are to be validated by the step this assignment refers to
    """
    assigned_entities: list[URIRef] = field(default_factory=list)

    """
    Property through which we will get the next focus node corresponding to the previous
    one, or the entity itself from this step assignment if it wasn't validated in the 
    previous step
    """
    property_to_follow: URIRef = ""


@dataclass
class WorkflowInstance(BaseWorkflowElement):
    """
    Workflow model it refers to
    """
    workflow_model_iri: URIRef = ""

    """
    Step assignments, indexed by the workflow model step IRI they are assigned to
    """
    step_assignments: dict[URIRef, StepAssignment] = field(default_factory=dict)

    """
    ISO 8601 timestamp of the last time this instance was validated and its result cached.
    Empty if the instance has never been validated
    """
    last_validated_at: str = ""

    """
    Cached overall validation status name (e.g. "Valid", "Warning", "Error") from the last validation.
    Empty if the instance has never been validated
    """
    cached_validation_status: str = ""

    """
    Whether the cached validation result is out of date (e.g. the instance or its model was edited) and
    the instance should be re-validated instead of returning the cache
    """
    validation_cache_stale: bool = False

    """
    SHA-256 hash of the exact set of data-graph tuples the cached validation result depended on (its
    validation footprint). Used by the ETL pipeline to detect, after a re-materialization, whether the
    underlying data changed and the cache should be marked stale. Empty if never validated.
    See workflows_validation.validation_cache.
    """
    validation_cache_hash: str = ""


    def normalize_cache_fields(self) -> None:
        """
        Coerces the cache fields to their proper Python types after being read from RDF, where they
        arrive as plain strings.

        xsd:boolean has four valid lexical forms ("true"/"false"/"1"/"0"), and the store may return
        either "true"/"false" or "1"/"0", so we accept all truthy forms instead of only "true".
        """
        if isinstance(self.validation_cache_stale, str):
            self.validation_cache_stale = self.validation_cache_stale.strip().lower() in ("true", "1")


    def has_valid_cache(self) -> bool:
        """
        Whether this instance holds a usable cached validation result, i.e. it has been validated at
        least once and the cache has not been marked stale
        """
        return bool(self.last_validated_at) and not self.validation_cache_stale


    def mark_validated(self, validation_status_name: str, footprint_hash: str) -> None:
        """
        Updates the in-memory cache fields to reflect a freshly computed validation result, storing the
        hash of the validation footprint that result was computed against. Use `get_cache_update_query`
        afterwards to persist them
        """
        logging.info("[CACHE put] instance=%s status=%s hash=%s",
                     self.iri or self.name, validation_status_name, footprint_hash)
        self.last_validated_at = datetime.now(timezone.utc).isoformat()
        self.cached_validation_status = validation_status_name
        self.validation_cache_stale = False
        self.validation_cache_hash = footprint_hash


    def get_cache_update_query(self) -> str:
        """
        Yields a SPARQL update that overwrites the cached validation fields of this instance in the KG
        with their current in-memory values
        """
        return (update_workflow_instance_validation_cache_query
                .replace("{workflow_instance_iri}", self.iri)
                .replace("{last_validated_at}", self.last_validated_at)
                .replace("{cached_validation_status}", self.cached_validation_status)
                .replace("{validation_cache_stale}", "true" if self.validation_cache_stale else "false")
                .replace("{validation_cache_hash}", self.validation_cache_hash))


    def get_validation_trace_update_queries(self, individual_results: dict) -> list[str]:
        """
        Yields the SPARQL updates that overwrite this instance's cached per-step validation trace with a
        freshly computed one. `individual_results` is the structure returned by `is_workflow_instance_valid`
        with `return_individual_results=True`: entity IRI -> list of validation paths, each an ordered dict
        of workflow model step IRI -> list of ValidationResult.

        One `crc:CachedValidationResult` node is stored per ValidationResult, hung off the step assignment
        of its step, carrying its entity, target node, conforms/missing/output, and the path/step indices
        needed to rebuild the nested structure exactly. Existing results are deleted first, so re-validation
        overwrites rather than accumulates.

        The result triples are built via rdflib so arbitrary pySHACL output (quotes, newlines) is stored
        verbatim and safely.
        """
        queries = [delete_cached_validation_results_query.replace("{workflow_instance_iri}", self.iri)]

        g = Graph()
        for entity_iri, validation_paths in individual_results.items():
            for path_index, validation_path in enumerate(validation_paths):
                for step_seq, (step_iri, results_for_step) in enumerate(validation_path.items()):
                    step_assignment = self.step_assignments.get(step_iri)
                    if step_assignment is None:
                        continue  # No assignment to hang the result off (shouldn't happen); skip defensively

                    for result in results_for_step:
                        result_iri = dw_prefix[generate_unique_identifier()]
                        g.add((step_assignment.iri, dw_prefix.hasCachedValidationResult, result_iri))
                        g.add((result_iri, rdf_prefix.type, dw_prefix.CachedValidationResult))
                        g.add((result_iri, dw_prefix.resultForEntity, URIRef(entity_iri)))
                        g.add((result_iri, dw_prefix.resultConforms, Literal(result.conforms, datatype=XSD.boolean)))
                        g.add((result_iri, dw_prefix.resultIsMissingData, Literal(result.is_missing_data, datatype=XSD.boolean)))
                        g.add((result_iri, dw_prefix.resultPyshaclOutput, Literal(result.pyshacl_output or "", datatype=XSD.string)))
                        g.add((result_iri, dw_prefix.resultPathIndex, Literal(path_index, datatype=XSD.integer)))
                        g.add((result_iri, dw_prefix.resultStepSeq, Literal(step_seq, datatype=XSD.integer)))

                        target_node = result.validation_job.target_node if result.validation_job else None
                        if target_node is not None:
                            g.add((result_iri, dw_prefix.resultTargetNode, URIRef(target_node)))

        if len(g) > 0:
            queries.append(f"""
            INSERT DATA {{
                GRAPH <{WORKFLOWS_GRAPH_IRI}> {{
                    {g.serialize(format='nt')}
                }}
            }}
            """)

        return queries


    def is_definition_valid(self, assigned_workflow_model: WorkflowModel) -> tuple[bool, str]:
        def all_step_successors_have_no_overlapping_assigned_entities(current_step: WorkflowModelStep) -> bool:
            assigned_entities_in_next_steps: list[URIRef] = []

            for next_step_iri in current_step.next_steps:
                if next_step_iri in self.step_assignments:
                    for entity in self.step_assignments[next_step_iri].assigned_entities:
                        assigned_entities_in_next_steps.append(entity)
            if len(assigned_entities_in_next_steps) != len(set(assigned_entities_in_next_steps)):
                return False # There are overlapping entities between the successors

            for next_step_iri in current_step.next_steps:
                if not all_step_successors_have_no_overlapping_assigned_entities(assigned_workflow_model.workflow_model_steps[next_step_iri]):
                    return False

            return True

        if not all_step_successors_have_no_overlapping_assigned_entities(assigned_workflow_model.workflow_model_steps[assigned_workflow_model.initial_step_iri]):
            return False, "An entity cannot be assigned to two or more consecutive steps in different branches"

        return True, ""


    def get_insert_query(self) -> str:
        """
        Yields a SPARQL query string that inserts the workflow instance
        """
        # TODO: For now, we don't enforce these constraints. They will generate hard to
        #       understand validation traces, though
        #valid, msg = await is_workflow_instance_definition_valid(workflow_instance)
        #if not valid:
        #    raise ValueError(msg)

        g = Graph()

        if not self.iri:  # The instance is new
            self.create_new_iri()

        # Type
        g.add((self.iri, rdf_prefix.type, dw_prefix.workflowModelInstance))

        # Label
        g.add((self.iri, URIRef(workflow_instance_config_key_to_iri["name"]), Literal(self.name, datatype=XSD.string)))

        # Description
        g.add((self.iri, URIRef(workflow_instance_config_key_to_iri["description"]), Literal(self.description, datatype=XSD.string)))

        # Link to workflow model
        g.add((self.iri, URIRef(workflow_instance_config_key_to_iri["workflow_model_iri"]), self.workflow_model_iri))

        # Cached validation result (only serialize the timestamp/status/hash if the instance has been validated)
        if self.last_validated_at:
            g.add((self.iri, URIRef(workflow_instance_config_key_to_iri["last_validated_at"]), Literal(self.last_validated_at, datatype=XSD.dateTime)))
            g.add((self.iri, URIRef(workflow_instance_config_key_to_iri["cached_validation_status"]), Literal(self.cached_validation_status, datatype=XSD.string)))
            g.add((self.iri, URIRef(workflow_instance_config_key_to_iri["validation_cache_hash"]), Literal(self.validation_cache_hash, datatype=XSD.string)))
        g.add((self.iri, URIRef(workflow_instance_config_key_to_iri["validation_cache_stale"]), Literal(self.validation_cache_stale, datatype=XSD.boolean)))

        for step_assignment in self.step_assignments.values():
            # Type
            g.add((step_assignment.iri, rdf_prefix.type, dw_prefix.WorkflowModelStepAssignment))

            # Label
            g.add((step_assignment.iri, URIRef(step_assignment_config_key_to_iri["name"]), Literal(step_assignment.name, datatype=XSD.string)))

            # Description
            g.add((step_assignment.iri, URIRef(step_assignment_config_key_to_iri["description"]), Literal(step_assignment.description, datatype=XSD.string)))

            # Link to assignment
            g.add((self.iri, URIRef(step_assignment_config_key_to_iri["step_assignments"]), step_assignment.iri))

            # Link to step
            g.add((step_assignment.iri, URIRef(step_assignment_config_key_to_iri["workflow_step_iri"]), step_assignment.workflow_step_iri))

            # Assigned entities
            for assigned_entity_iri in step_assignment.assigned_entities:
                g.add((step_assignment.iri, URIRef(step_assignment_config_key_to_iri["assigned_entities"]), assigned_entity_iri))

            # Property to follow
            g.add((step_assignment.iri, URIRef(step_assignment_config_key_to_iri["property_to_follow"]), step_assignment.property_to_follow))

            # User-defined metadata
            for (p, objs) in step_assignment.provenance_records.items():
                for o in objs:
                    g.add((step_assignment.iri, URIRef(p), o))

        # User-defined metadata
        for (p, objs) in self.provenance_records.items():
            for o in objs:
                g.add((self.iri, URIRef(p), o))

        return f"""
        INSERT DATA {{
            GRAPH <{WORKFLOWS_GRAPH_IRI}> {{
                {g.serialize(format='nt')}
            }}
        }}
        """


    def get_delete_query(self) -> str:
        """
        Yields a SPARQL query string that completely deletes the workflow instance
        """
        return delete_workflow_instance_query.replace("{workflow_instance_iri}", self.iri)


    def get_overwrite_queries(self) -> list[str]:
        """
        Yields the SPARQL query strings required to delete the existing workflow instance and store it again

        Overwriting an instance means its definition changed, so its cached validation result is invalidated
        """
        self.validation_cache_stale = True
        logging.info("[CACHE invalidate instance (edit)] instance=%s", self.iri or self.name)
        return [self.get_delete_query(), self.get_insert_query()]