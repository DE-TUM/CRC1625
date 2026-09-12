import os
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass, field

from rdflib import URIRef, Graph, Literal, XSD

from datastores.rdf.rdf_datastore import WORKFLOWS_GRAPH_IRI
from workflows_validation.common import BaseWorkflowElement, base_workflow_element_iri_to_config_key, dw_prefix, prefixes, rdf_prefix, generate_unique_identifier

module_dir = os.path.dirname(__file__)
delete_workflow_model_query = prefixes + open(os.path.join(module_dir, 'queries/delete_workflow_model.sparql'), 'r').read()
redirect_workflow_instances_query = prefixes + open(os.path.join(module_dir, 'queries/redirect_workflow_instances.sparql'), 'r').read()
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
    str(dw_prefix.hasRepetition): "repetition",
}
workflow_model_step_config_key_to_iri = {v: k for k, v in workflow_model_step_iri_to_config_key.items()}

workflow_model_step_repetition_iri_to_config_key = {
    str(dw_prefix.minRepetitions): "min_repetitions",
    str(dw_prefix.maxRepetitions): "max_repetitions",
    str(dw_prefix.repeatsUntil): "repeats_until",
}
workflow_model_step_repetition_config_key_to_iri = {v: k for k, v in workflow_model_step_repetition_iri_to_config_key.items()}

# The templates don't have a dataclass nor provenance metadata,
# they could be modeled as blank nodes or triple terms
workflow_model_step_template_iri_to_config_key = {
    # **base_workflow_element_iri_to_config_key,
    str(dw_prefix.templateKey): "key",
    str(dw_prefix.templateValue): "value",
}
workflow_model_step_template_config_key_to_iri = {v: k for k, v in workflow_model_step_template_iri_to_config_key.items()}


@dataclass
class Repetition(BaseWorkflowElement):
    """
    Marks the workflow model step it is attached to as repeatable, and bounds how often it may repeat

    The model only gives the bounds. How often the step actually repeats is decided per workflow
    instance, through the `repetition_count` of the StepAssignment that refers to the step. One model
    can therefore serve several instances that repeat the same step a different number of times

    The repetition is resolved before every validation run, by cloning the step as often as needed.
    See `workflows_validation.rewriting`
    """

    """
    Lowest number of repetitions an instance may ask for. A value of 0 lets an instance skip the step
    """
    min_repetitions: int = 1

    """
    Highest number of repetitions an instance may ask for. An unbounded number is not supported,
    because the step has to be cloned a known number of times before the validation starts
    """
    max_repetitions: int = 1

    """
    Last step of the repeated sequence. Everything from the step this repetition hangs on up to
    this one is repeated together

    `None` repeats only the step this repetition hangs on. The sequence has to be linear, because
    it needs exactly one step to continue from after the last repetition
    """
    repeats_until: URIRef | None = None


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
    Bounds for how often this step may be repeated, if it may be repeated at all.
    `None` means the step runs exactly once
    """
    repetition: Repetition | None = None

    """
    Step this one was cloned from while resolving a repetition, and which occurrence of it this
    step is. There is one number per repetition the step sits in, outermost first, so a step in a
    repeated sequence inside another repeated sequence carries two of them. An empty list means the
    step was not part of any repetition

    Both fields are filled in memory before a validation run and are never stored in the KG. They
    exist so that a validation result can be traced back to the step the user actually wrote
    """
    original_step_iri: URIRef | None = None
    occurrences: list[int] = field(default_factory=list)

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


def remap_step_iris(workflow_model_steps: Iterable[WorkflowModelStep],
                    step_iri_mapping: dict[URIRef, URIRef]) -> None:
    """
    Replaces the IRIs of the given steps according to the mapping, everywhere a step IRI is
    referenced. Steps and references that the mapping does not cover keep the IRI they have

    A step IRI is referenced in three places, and forgetting any of them leaves a workflow model
    that still points at the steps it was built from
    """
    for workflow_model_step in workflow_model_steps:
        workflow_model_step.iri = step_iri_mapping.get(workflow_model_step.iri, workflow_model_step.iri)
        workflow_model_step.next_steps = [step_iri_mapping.get(next_step_iri, next_step_iri)
                                          for next_step_iri in workflow_model_step.next_steps]

        if workflow_model_step.repetition is not None and workflow_model_step.repetition.repeats_until:
            workflow_model_step.repetition.repeats_until = step_iri_mapping.get(workflow_model_step.repetition.repeats_until,
                                                                                workflow_model_step.repetition.repeats_until)


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

        # Giving the steps new IRIs is not enough. Everything that refers to a step has to follow,
        # or the copy keeps pointing at the steps of the model it was copied from
        step_iri_mapping = {step_iri: dw_prefix[generate_unique_identifier()]
                            for step_iri in workflow_model_copy.workflow_model_steps}
        remap_step_iris(workflow_model_copy.workflow_model_steps.values(), step_iri_mapping)

        workflow_model_copy.initial_step_iri = step_iri_mapping.get(workflow_model_copy.initial_step_iri,
                                                                    workflow_model_copy.initial_step_iri)
        workflow_model_copy.workflow_model_steps = {workflow_model_step.iri: workflow_model_step
                                                    for workflow_model_step in workflow_model_copy.workflow_model_steps.values()}

        # A repetition is part of the step it hangs on, so the copy needs its own
        for workflow_model_step in workflow_model_copy.workflow_model_steps.values():
            if workflow_model_step.repetition is not None:
                workflow_model_step.repetition.create_new_iri()

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

            # Repetition bounds. They live in their own node, so that a step without a repetition
            # carries no repetition triples at all
            if step.repetition is not None:
                if not step.repetition.iri:
                    step.repetition.create_new_iri()

                g.add((step.iri, URIRef(workflow_model_step_config_key_to_iri["repetition"]), step.repetition.iri))
                g.add((step.repetition.iri, rdf_prefix.type, dw_prefix.Repetition))
                g.add((step.repetition.iri,
                       URIRef(workflow_model_step_repetition_config_key_to_iri["min_repetitions"]),
                       Literal(step.repetition.min_repetitions, datatype=XSD.integer)))
                g.add((step.repetition.iri,
                       URIRef(workflow_model_step_repetition_config_key_to_iri["max_repetitions"]),
                       Literal(step.repetition.max_repetitions, datatype=XSD.integer)))

                # Absent when only the step the repetition hangs on is repeated
                if step.repetition.repeats_until:
                    g.add((step.repetition.iri,
                           URIRef(workflow_model_step_repetition_config_key_to_iri["repeats_until"]),
                           step.repetition.repeats_until))

                # User-defined metadata
                for (p, objs) in step.repetition.provenance_records.items():
                    for o in objs:
                        g.add((step.repetition.iri, URIRef(p), o))

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

    return queries