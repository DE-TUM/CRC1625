"""
Rewrites a workflow model into an equivalent one without control flow operators, right before it is
validated.

The validation itself only understands plain sequences of steps. Operators are therefore written
down in the workflow model, and removed again before every validation run. Nothing of this is
stored, and the models the caller passes in are left untouched.

Repetition is the only operator implemented so far. A step marked with a `Repetition` is cloned as
often as the workflow instance asks for, and the clones are chained behind the original.
"""
from copy import deepcopy

from rdflib import URIRef

from workflows_validation.workflow_instance import WorkflowInstance
from workflows_validation.workflow_model import WorkflowModel, WorkflowModelStep


class RepetitionError(ValueError):
    """
    Raised when the repetitions of a workflow model and the repetition counts of a workflow instance
    do not fit together. The message is meant to be shown to the end user
    """


def _as_times(count: int) -> str:
    """
    Wording helper for the error messages, so that they do not read "1 times"
    """
    return "once" if count == 1 else f"{count} times"


def check_repetitions(workflow_model: WorkflowModel,
                      workflow_instance: WorkflowInstance) -> None:
    """
    Checks that every repetition of the workflow model is used correctly by the workflow instance,
    and raises a `RepetitionError` if it is not

    This runs before the model is rewritten. Catching the problem here gives the user one clear
    message, instead of a validation trace over a model that was built from wrong numbers
    """
    for step in workflow_model.workflow_model_steps.values():
        step_assignment = workflow_instance.step_assignments.get(step.iri)

        if step.repetition is None:
            # An instance cannot repeat a step that the model does not mark as repeatable
            if step_assignment is not None and step_assignment.repetition_count != 1:
                raise RepetitionError(
                    f"The workflow instance validates step '{step.name}' {_as_times(step_assignment.repetition_count)}, "
                    f"but the workflow model does not allow that step to be repeated")
            continue

        minimum = step.repetition.min_repetitions
        maximum = step.repetition.max_repetitions

        if minimum < 0:
            raise RepetitionError(
                f"Step '{step.name}' allows a negative number of repetitions ({minimum})")

        if minimum > maximum:
            raise RepetitionError(
                f"Step '{step.name}' allows between {minimum} and {maximum} repetitions, which is an empty range")

        if step_assignment is None:
            raise RepetitionError(
                f"Step '{step.name}' is repeatable, but the workflow instance does not assign anything to it")

        repetition_count = step_assignment.repetition_count
        if not minimum <= repetition_count <= maximum:
            raise RepetitionError(
                f"The workflow instance validates step '{step.name}' {_as_times(repetition_count)}, but the workflow "
                f"model allows between {minimum} and {maximum} repetitions")

        # Skipping the only step of a model would leave nothing to validate
        if repetition_count == 0 and step.iri == workflow_model.initial_step_iri and not step.next_steps:
            raise RepetitionError(
                f"Step '{step.name}' is the only step of the workflow model and is skipped by the workflow "
                f"instance, so there would be nothing left to validate")


def unroll_repetitions(workflow_model: WorkflowModel,
                       workflow_instance: WorkflowInstance) -> tuple[WorkflowModel, WorkflowInstance]:
    """
    Yields a copy of the workflow model and the workflow instance in which every repetition has been
    written out as a chain of ordinary steps

    A step that is repeated `n` times is followed by `n - 1` clones of itself. A step that is
    repeated 0 times is skipped. Clones carry `original_step_iri` and `occurrence`, so that their
    validation results can be traced back to the step the user wrote

    Both arguments are copied first. The web UI keeps working with the objects it passed in
    """
    check_repetitions(workflow_model, workflow_instance)

    workflow_model = deepcopy(workflow_model)
    workflow_instance = deepcopy(workflow_instance)

    # The step IRIs are collected before the loop, because it inserts clones into the same dict.
    # Clones never carry a repetition themselves, so the order in which the steps are handled does
    # not matter
    for step_iri in list(workflow_model.workflow_model_steps.keys()):
        step = workflow_model.workflow_model_steps[step_iri]
        if step.repetition is None:
            continue

        repetition_count = workflow_instance.step_assignments[step_iri].repetition_count
        if repetition_count == 0:
            _skip_step(workflow_model, workflow_instance, step)
        else:
            _repeat_step(workflow_model, workflow_instance, step, repetition_count)

    return workflow_model, workflow_instance


def get_original_step(workflow_model_step: WorkflowModelStep,
                      original_workflow_model: WorkflowModel) -> WorkflowModelStep:
    """
    Yields the step a clone was made from, or the step itself if it is not a clone

    `original_workflow_model` has to be the model as it was before `unroll_repetitions` ran
    """
    if workflow_model_step.original_step_iri is None:
        return workflow_model_step

    return original_workflow_model.workflow_model_steps.get(workflow_model_step.original_step_iri, workflow_model_step)


def get_step_display_name(workflow_model_step: WorkflowModelStep,
                          original_workflow_model: WorkflowModel) -> str:
    """
    Yields a readable name for a step, for use in validation reports

    Clones are named after the step they were made from, plus which repetition they are. The clone
    names themselves are built for machines and are not meant to be shown
    """
    if workflow_model_step.original_step_iri is None:
        return workflow_model_step.name

    original_step = get_original_step(workflow_model_step, original_workflow_model)

    return f"{original_step.name} (repetition {workflow_model_step.occurrence + 1})"


def _repeat_step(workflow_model: WorkflowModel,
                 workflow_instance: WorkflowInstance,
                 step: WorkflowModelStep,
                 repetition_count: int) -> None:
    """
    Chains `repetition_count - 1` clones of the given step behind it, so that the step is validated
    `repetition_count` times in a row

    The step itself keeps its IRI and becomes the first occurrence. Its predecessors therefore do
    not have to be touched, and a step that was not repeated still reports under the IRI it has in
    the stored model
    """
    step_assignment = workflow_instance.step_assignments[step.iri]

    # Whatever followed the step has to follow the last clone instead
    successors_of_the_last_occurrence = list(step.next_steps)

    previous_occurrence = step
    for occurrence in range(1, repetition_count):
        clone = deepcopy(step)
        clone.iri = URIRef(f"{step.iri}#rep{occurrence}")
        clone.name = f"{step.name}#rep{occurrence}"
        clone.next_steps = []
        # The clone is the result of resolving the repetition, so it does not repeat again
        clone.repetition = None
        clone.original_step_iri = step.iri
        clone.occurrence = occurrence

        # Without an assignment of its own, a clone would be skipped by the validation, and it would
        # additionally reset its successor back to the start of the entity
        clone_assignment = deepcopy(step_assignment)
        clone_assignment.iri = URIRef(f"{step_assignment.iri}#rep{occurrence}")
        clone_assignment.workflow_step_iri = clone.iri
        clone_assignment.repetition_count = 1

        workflow_model.workflow_model_steps[clone.iri] = clone
        workflow_instance.step_assignments[clone.iri] = clone_assignment

        previous_occurrence.next_steps = [clone.iri]
        previous_occurrence = clone

    previous_occurrence.next_steps = successors_of_the_last_occurrence

    # A step that runs a single time is not marked as an occurrence. Reports about it then look
    # exactly like reports about a step that was never repeatable in the first place
    if repetition_count > 1:
        step.original_step_iri = step.iri
        step.occurrence = 0

    step.repetition = None
    step_assignment.repetition_count = 1


def _skip_step(workflow_model: WorkflowModel,
               workflow_instance: WorkflowInstance,
               step: WorkflowModelStep) -> None:
    """
    Removes the given step, so that the workflow continues with its successors

    The initial step is handled differently. It cannot be removed, because a model names exactly one
    entry point and the validation starts its traversal there. Emptying its assignment has the same
    effect: the step produces no validation job, and its successors then start at the entity itself,
    which is exactly where they would have started anyway
    """
    if step.iri == workflow_model.initial_step_iri:
        workflow_instance.step_assignments[step.iri].assigned_entities = []
        workflow_instance.step_assignments[step.iri].repetition_count = 1
        step.repetition = None
        return

    for other_step in workflow_model.workflow_model_steps.values():
        if step.iri not in other_step.next_steps:
            continue

        # The skipped step is replaced by everything that came after it. A successor that the
        # predecessor already had is not added twice, or the validation would visit it twice
        rewired_next_steps = []
        for next_step_iri in other_step.next_steps:
            replacements = step.next_steps if next_step_iri == step.iri else [next_step_iri]
            for replacement in replacements:
                if replacement not in rewired_next_steps:
                    rewired_next_steps.append(replacement)

        other_step.next_steps = rewired_next_steps

    del workflow_model.workflow_model_steps[step.iri]
    workflow_instance.step_assignments.pop(step.iri, None)
