import logging
import os
from copy import deepcopy
from dataclasses import dataclass, field

from rdflib import URIRef, Graph, Literal, XSD

from datastores.rdf.rdf_datastore import WORKFLOWS_GRAPH_IRI
from workflows_validation.common import BaseWorkflowElement, base_workflow_element_iri_to_config_key, dw_prefix, prefixes, rdf_prefix, generate_unique_identifier

module_dir = os.path.dirname(__file__)
delete_workflow_model_query = prefixes + open(os.path.join(module_dir, 'queries/delete_workflow_model.sparql'), 'r').read()
redirect_workflow_instances_query = prefixes + open(os.path.join(module_dir, 'queries/redirect_workflow_instances.sparql'), 'r').read()
invalidate_workflow_instance_caches_query = prefixes + open(os.path.join(module_dir, 'queries/invalidate_workflow_instance_caches.sparql'), 'r').read()
redirect_workflow_instance_steps_query = prefixes + open(os.path.join(module_dir, 'queries/redirect_workflow_instance_steps.sparql'), 'r').read()
delete_workflow_instance_assignments_related_to_step_query = prefixes + open(os.path.join(module_dir, 'queries/delete_workflow_instance_assignments_related_to_step.sparql'), 'r').read()


workflow_model_iri_to_config_key = {
    **base_workflow_element_iri_to_config_key,
    str(dw_prefix.substep): "initial_step_iri",
}
workflow_model_config_key_to_iri = {v: k for k, v in workflow_model_iri_to_config_key.items()}

workflow_model_step_iri_to_config_key = {
    **base_workflow_element_iri_to_config_key,
    str(dw_prefix.nextStep): "next_steps",
    str(dw_prefix.hasTemplate): "step_templates",
    str(dw_prefix.assignedShape): "SHACL_shape",
}
workflow_model_step_config_key_to_iri = {v: k for k, v in workflow_model_step_iri_to_config_key.items()}

# The templates don't have a dataclass nor provenance metadata,
# they could be modeled as blank nodes or triple terms
workflow_model_step_template_iri_to_config_key = {
    # **base_workflow_element_iri_to_config_key,
    str(dw_prefix.templateKey): "key",
    str(dw_prefix.templateValue): "value",
}
workflow_model_step_template_config_key_to_iri = {v: k for k, v in workflow_model_step_template_iri_to_config_key.items()}


@dataclass
class WorkflowModelStep(BaseWorkflowElement):
    """
    A step of a workflow model, containing restrictions for a tree of consecutive
    entities in the same order. The restrictions are formulated via a SHACL shape
    and, optionally, a collection of key->value replacements to apply on the SHACL
    shape, allowing the former to be reused.
    """

    """
    List of workflow model step IRIs that follow this one. Note that the system does not check for loops
    """
    next_steps: list[URIRef] = field(default_factory=list)

    """
    Key->value dict to replace in the step's SHACL shape, if any. The values can be either a list
    (multiple values being assigned to the same key), or a single str. This is treated as multiple 
    template triples when serializing the workflow model, and inferred when reading it
    """
    step_templates: dict[str, list[str] | str] = field(default_factory=dict)

    """
    SHACL shape serialized string as a Jinja template, to which the step templates will be applied.
    
    Important: At the very least, the template must contain a `target_node` replacement entry. When validated
    against a workflow instance, this entry will be replaced at validation time with the corresponding 
    target node from the assigned entities to this step.
    """
    SHACL_shape: str = ""


@dataclass
class WorkflowModel(BaseWorkflowElement):
    """
    A workflow model, consisting of a list of steps and a pointer to the initial step
    """

    """
    Initial step's IRI
    """
    initial_step_iri: URIRef = ""

    """
    Steps of the workflow, indexed by their IRI. The steps themselves indicate their successors, if any
    """
    workflow_model_steps: dict[URIRef, WorkflowModelStep] = field(default_factory=dict)


    def create_copy(self) -> "WorkflowModel":
        """
        Creates a copy of this workflow model ready to be serialized,
        ensuring that all of its entities have different URIs
        """
        workflow_model_copy = deepcopy(self)
        workflow_model_copy.create_new_iri()
        workflow_model_copy.name = "Copy of " + self.name

        for workflow_model_step in workflow_model_copy.workflow_model_steps.values():
            workflow_model_step.create_new_iri()

        return workflow_model_copy


    def get_insert_query(self) -> str:
        """
        Yields a SPARQL query string that inserts the workflow model
        """
        g = Graph()

        # Type
        g.add((self.iri, rdf_prefix.type, dw_prefix.WorkflowModel))

        # Label
        g.add((self.iri, URIRef(workflow_model_config_key_to_iri["name"]), Literal(self.name, datatype=XSD.string)))

        # Comment
        g.add((self.iri, URIRef(workflow_model_config_key_to_iri["description"]), Literal(self.description, datatype=XSD.string)))

        # First step
        g.add((self.iri, URIRef(workflow_model_config_key_to_iri["initial_step_iri"]), self.initial_step_iri))

        for step in self.workflow_model_steps.values():
            # Type
            g.add((step.iri, rdf_prefix.type, dw_prefix.WorkflowModelStep))

            # Label
            g.add((step.iri, URIRef(workflow_model_step_config_key_to_iri["name"]), Literal(step.name, datatype=XSD.string)))

            # Comment
            g.add((step.iri, URIRef(workflow_model_step_config_key_to_iri["description"]), Literal(step.description, datatype=XSD.string)))

            # Next steps
            for next_step_iri in step.next_steps:
                g.add((step.iri, URIRef(workflow_model_step_config_key_to_iri["next_steps"]), next_step_iri))

            # SHACL shape
            g.add((step.iri, URIRef(workflow_model_step_config_key_to_iri["SHACL_shape"]), Literal(step.SHACL_shape, datatype=XSD.string)))

            # Templates
            for key, replacement in step.step_templates.items():
                template_iri = dw_prefix[generate_unique_identifier()]
                g.add((step.iri, URIRef(workflow_model_step_config_key_to_iri["step_templates"]), template_iri))
                g.add((template_iri, URIRef(workflow_model_step_template_config_key_to_iri["key"]), Literal(key)))

                if isinstance(replacement, str):
                    g.add((template_iri, URIRef(workflow_model_step_template_config_key_to_iri["value"]), Literal(replacement)))
                elif isinstance(replacement, list):
                    for value in replacement:
                        g.add((template_iri, URIRef(workflow_model_step_template_config_key_to_iri["value"]), Literal(value)))

            # User-defined metadata
            for (p, objs) in step.provenance_records.items():
                for o in objs:
                    g.add((step.iri, URIRef(p), o))

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
        Yields a SPARQL query string that completely deletes the workflow model

        IMPORTANT: The associated workflow instances will *NOT* be deleted!
                   You should query for them beforehand and delete them
        """
        return delete_workflow_model_query.replace("{workflow_model_iri}", self.iri)


    def get_overwrite_queries(self,
                              original_workflow_model: "WorkflowModel"):
        """
        Given the current workflow model and its original version, yields the queries required to
        delete the existing, corresponding workflow model, and to store it again, while keeping the
        workflow instances references to it consistent

        If the workflow model itself has been renamed or one of its steps deleted, the original workflow model will be
        used to redirect or remove its workflow instance assignments accordingly. If any steps have been renamed,
        a dictionary of renamed_step_name -> original_step_name must also be provided
        """
        queries = [self.get_delete_query(), self.get_insert_query()]
        queries += _get_redirection_queries(self, original_workflow_model)

        return queries


def _get_redirection_queries(new_workflow_model: WorkflowModel,
                             old_workflow_model: WorkflowModel) -> list[str]:
    """
    Yields the queries required to adapt all workflow instance step assignments of the old workflow model
    to the new one, removing those that don't have a corresponding workflow model step IRI in the new model
    """
    queries = []

    # Delete assignments referencing workflow model steps that don't exist anymore
    for old_step_iri, old_step in old_workflow_model.workflow_model_steps.items():
        if old_step_iri not in new_workflow_model.workflow_model_steps:
            queries.append((delete_workflow_instance_assignments_related_to_step_query
                            .replace("{workflow_model_step_iri}", old_step_iri)))

    # Redirect the instance as a whole, if applicable
    if old_workflow_model.iri != new_workflow_model.iri:
        queries.append((redirect_workflow_instances_query
                        .replace("{old_workflow_model_iri}", old_workflow_model.iri)
                        .replace("{new_workflow_model_iri}", new_workflow_model.iri)))

    # The model definition changed, so the cached validation results of all its instances are now stale.
    # This runs after the redirection above, so the instances already point to the new model IRI
    logging.info("[CACHE invalidate model (edit)] model=%s", new_workflow_model.iri or new_workflow_model.name)
    queries.append(invalidate_workflow_instance_caches_query
                   .replace("{workflow_model_iri}", new_workflow_model.iri))

    return queries