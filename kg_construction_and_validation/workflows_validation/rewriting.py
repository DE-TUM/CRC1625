"""
Rewrites a workflow model into an equivalent one without control flow operators, right before it is
validated.

The validation itself only understands plain sequences of steps. Operators are therefore written
down in the workflow model, and removed again before every validation run. Nothing of this is
stored, and the models the caller passes in are left untouched.

Repetition is the only operator implemented so far. It covers a repeated region, which is a linear
sequence of steps from the step that carries the `Repetition` up to the step it ends at. A single
repeated step is the case where the sequence is one step long.
"""
from copy import deepcopy
from itertools import combinations

from rdflib import URIRef

from workflows_validation.workflow_instance import WorkflowInstance
from workflows_validation.workflow_model import WorkflowModel, WorkflowModelStep, remap_step_iris


class RepetitionError(ValueError):
    """
    Raised when the repetitions of a workflow model and the repetition counts of a workflow instance
    do not fit together. The message is meant to be shown to the end user
    """


def get_region_steps(workflow_model: WorkflowModel,
                     first_step: WorkflowModelStep) -> list[WorkflowModelStep]:
    """
    Yields the steps that the repetition of `first_step` covers, in the order they are validated

    A repetition without an end covers only the step it hangs on. Otherwise the region runs along
    the successors until it reaches that end. The region has to be linear, because everything after
    the last repetition continues from a single step. A step may only branch once the region is over
    """
    if first_step.repetition is None or not first_step.repetition.repeats_until:
        return [first_step]

    last_step_iri = first_step.repetition.repeats_until
    if last_step_iri not in workflow_model.workflow_model_steps:
        raise RepetitionError(
            f"The repeated sequence starting at step '{first_step.name}' ends at a step that is not "
            f"part of the workflow model")

    region_steps = [first_step]
    visited_step_iris = {first_step.iri}

    while region_steps[-1].iri != last_step_iri:
        current_step = region_steps[-1]

        if len(current_step.next_steps) != 1:
            raise RepetitionError(
                f"Step '{current_step.name}' lies inside the repeated sequence starting at "
                f"'{first_step.name}' and has {len(current_step.next_steps)} successors. A repeated "
                f"sequence has to be linear, so only its last step may branch")

        next_step_iri = current_step.next_steps[0]
        if next_step_iri in visited_step_iris:
            raise RepetitionError(
                f"The repeated sequence starting at step '{first_step.name}' runs in a circle")

        region_steps.append(workflow_model.workflow_model_steps[next_step_iri])
        visited_step_iris.add(next_step_iri)

    return region_steps


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


def check_repetition_regions(workflow_model: WorkflowModel,
                             workflow_instance: WorkflowInstance) -> None:
    """
    Checks that every repeated region of the workflow model is a linear sequence, and that any two
    regions are either nested or completely separate. Raises a `RepetitionError` if they are not

    Two regions that merely overlap cannot be written out, because there is no order in which both
    would come out whole. An inner region has to close before the region around it does
    """
    region_step_iris_by_first_step: dict[URIRef, tuple[str, set[URIRef]]] = {}

    for step in workflow_model.workflow_model_steps.values():
        if step.repetition is None:
            continue

        region_steps = get_region_steps(workflow_model, step)
        region_step_iris_by_first_step[step.iri] = (step.name, {region_step.iri for region_step in region_steps})

        # Skipping a region that starts at the entry point and runs to the end empties the model
        is_skipped = workflow_instance.step_assignments[step.iri].repetition_count == 0
        if is_skipped and step.iri == workflow_model.initial_step_iri and not region_steps[-1].next_steps:
            raise RepetitionError(
                f"The repeated sequence starting at step '{step.name}' covers the whole workflow model and is "
                f"skipped by the workflow instance, so there would be nothing left to validate")

    for (first_name, first_region), (second_name, second_region) in combinations(region_step_iris_by_first_step.values(), 2):
        regions_are_separate = not (first_region & second_region)
        regions_are_nested = first_region <= second_region or second_region <= first_region

        if not regions_are_separate and not regions_are_nested:
            raise RepetitionError(
                f"The repeated sequences starting at steps '{first_name}' and '{second_name}' overlap. "
                f"One of them has to end before the other one does, or they have to be separate")


def unroll_repetitions(workflow_model: WorkflowModel,
                       workflow_instance: WorkflowInstance) -> tuple[WorkflowModel, WorkflowInstance]:
    """
    Yields a copy of the workflow model and the workflow instance in which every repetition has been
    written out as a chain of ordinary steps

    A region that is repeated `n` times is followed by `n - 1` copies of itself. A region that is
    repeated 0 times is skipped. Copied steps carry `original_step_iri` and `occurrences`, so that
    their validation results can be traced back to the steps the user wrote

    Regions are resolved from the outside in. Writing out an inner region first could push its
    copies past the end of the region around it, which would leave that one without an end

    Both arguments are copied first. The web UI keeps working with the objects it passed in
    """
    check_repetitions(workflow_model, workflow_instance)
    check_repetition_regions(workflow_model, workflow_instance)

    workflow_model = deepcopy(workflow_model)
    workflow_instance = deepcopy(workflow_instance)

    # Writing out a region removes its repetition and leaves the repetitions nested inside it once
    # per occurrence. Those are picked up by the following rounds, until none are left
    while True:
        first_step = _find_an_outermost_repeatable_step(workflow_model)
        if first_step is None:
            break

        region_steps = get_region_steps(workflow_model, first_step)
        repetition_count = workflow_instance.step_assignments[first_step.iri].repetition_count

        if repetition_count == 0:
            _skip_region(workflow_model, workflow_instance, region_steps)
        else:
            _repeat_region(workflow_model, workflow_instance, region_steps, repetition_count)

    return workflow_model, workflow_instance


def get_original_step(workflow_model_step: WorkflowModelStep,
                      original_workflow_model: WorkflowModel) -> WorkflowModelStep:
    """
    Yields the step a copy was made from, or the step itself if it is not a copy

    `original_workflow_model` has to be the model as it was before `unroll_repetitions` ran
    """
    if workflow_model_step.original_step_iri is None:
        return workflow_model_step

    return original_workflow_model.workflow_model_steps.get(workflow_model_step.original_step_iri, workflow_model_step)


def get_step_display_name(workflow_model_step: WorkflowModelStep,
                          original_workflow_model: WorkflowModel) -> str:
    """
    Yields a readable name for a step, for use in validation reports

    A copy is named after the step it was made from, plus which repetition it belongs to. Steps in a
    repeated sequence inside another one get one number per sequence, outermost first. The step
    names themselves are built for machines and are not meant to be shown
    """
    if not workflow_model_step.occurrences:
        return workflow_model_step.name

    original_step = get_original_step(workflow_model_step, original_workflow_model)
    repetition_numbers = ".".join(str(occurrence + 1) for occurrence in workflow_model_step.occurrences)

    return f"{original_step.name} (repetition {repetition_numbers})"


def _as_times(count: int) -> str:
    """
    Wording helper for the error messages, so that they do not read "1 times"
    """
    return "once" if count == 1 else f"{count} times"


def _get_occurrence_suffix(occurrences: list[int]) -> str:
    """
    Yields the whole chain of repetitions a copy belongs to, as one string
    """
    return ".".join(str(occurrence) for occurrence in occurrences)


def _get_occurrence_iri(original_step_iri: URIRef, occurrences: list[int]) -> URIRef:
    """
    Yields the IRI of one occurrence of a step

    The whole chain goes into the IRI, not just the last number. The same step can be copied once
    by a repeated sequence and again by a repetition nested inside it, and both copies would
    otherwise ask for the same IRI
    """
    return URIRef(f"{original_step_iri}_rep{_get_occurrence_suffix(occurrences)}")


def _get_base_name(workflow_model: WorkflowModel,
                   workflow_model_step: WorkflowModelStep) -> str:
    """
    Yields the name the given step has in the stored model

    A copy carries a suffix saying which occurrence it is. Copying a copy has to start from the
    stored name again, or the suffixes pile up
    """
    if workflow_model_step.original_step_iri is None:
        return workflow_model_step.name

    original_step = workflow_model.workflow_model_steps.get(workflow_model_step.original_step_iri)

    return original_step.name if original_step is not None else workflow_model_step.name


def _find_an_outermost_repeatable_step(workflow_model: WorkflowModel) -> WorkflowModelStep | None:
    """
    Yields the first step of a region that no other remaining region contains, or `None` when there
    is no repetition left to write out

    Regions are either nested or separate at this point, so the largest one is never contained in
    another and is therefore always a valid choice
    """
    repeatable_steps = [step for step in workflow_model.workflow_model_steps.values() if step.repetition is not None]
    if not repeatable_steps:
        return None

    return max(repeatable_steps, key=lambda step: len(get_region_steps(workflow_model, step)))


def _repeat_region(workflow_model: WorkflowModel,
                   workflow_instance: WorkflowInstance,
                   region_steps: list[WorkflowModelStep],
                   repetition_count: int) -> None:
    """
    Chains `repetition_count - 1` copies of the given region behind it, so that the whole sequence
    is validated `repetition_count` times in a row

    The region itself keeps its IRIs and becomes the first occurrence. Its predecessors therefore do
    not have to be touched, and a step that was not repeated still reports under the IRI it has in
    the stored model
    """
    first_step = region_steps[0]

    # Whatever followed the region has to follow the last copy instead
    successors_of_the_last_occurrence = list(region_steps[-1].next_steps)

    last_step_of_the_previous_occurrence = region_steps[-1]
    for occurrence in range(1, repetition_count):
        occurrences_by_step_iri = {region_step.iri: region_step.occurrences + [occurrence] for region_step in region_steps}
        step_iri_mapping = {region_step.iri: _get_occurrence_iri(region_step.original_step_iri or region_step.iri,
                                                                 occurrences_by_step_iri[region_step.iri])
                            for region_step in region_steps}

        copied_steps = [deepcopy(region_step) for region_step in region_steps]
        remap_step_iris(copied_steps, step_iri_mapping)

        for region_step, copied_step in zip(region_steps, copied_steps):
            occurrences = occurrences_by_step_iri[region_step.iri]

            copied_step.name = f"{_get_base_name(workflow_model, region_step)}_rep{_get_occurrence_suffix(occurrences)}"
            # A region inside another one is copied along and written out in a later round
            copied_step.original_step_iri = region_step.original_step_iri or region_step.iri
            copied_step.occurrences = occurrences

            workflow_model.workflow_model_steps[copied_step.iri] = copied_step
            _copy_step_assignment(workflow_instance, region_step, copied_step, occurrences)

        # Only the repetition of this region is resolved here. Regions nested inside it travel
        # along with their own counts and are written out in a later round
        copied_steps[0].repetition = None
        copied_first_assignment = workflow_instance.step_assignments.get(copied_steps[0].iri)
        if copied_first_assignment is not None:
            copied_first_assignment.repetition_count = 1

        last_step_of_the_previous_occurrence.next_steps = [copied_steps[0].iri]
        last_step_of_the_previous_occurrence = copied_steps[-1]

    last_step_of_the_previous_occurrence.next_steps = successors_of_the_last_occurrence

    # The region runs a single time, so it looks exactly like a region that was never repeatable
    if repetition_count > 1:
        for region_step in region_steps:
            region_step.original_step_iri = region_step.original_step_iri or region_step.iri
            region_step.occurrences = region_step.occurrences + [0]

    first_step.repetition = None
    workflow_instance.step_assignments[first_step.iri].repetition_count = 1


def _copy_step_assignment(workflow_instance: WorkflowInstance,
                          region_step: WorkflowModelStep,
                          copied_step: WorkflowModelStep,
                          occurrences: list[int]) -> None:
    """
    Gives a copied step an assignment of its own

    Without one, the validation would skip the copy, and it would additionally reset its successor
    back to the start of the entity
    """
    step_assignment = workflow_instance.step_assignments.get(region_step.iri)
    if step_assignment is None:
        return

    copied_assignment = deepcopy(step_assignment)
    copied_assignment.iri = URIRef(f"{step_assignment.iri}_rep{_get_occurrence_suffix(occurrences)}")
    copied_assignment.workflow_step_iri = copied_step.iri

    workflow_instance.step_assignments[copied_step.iri] = copied_assignment


def _skip_region(workflow_model: WorkflowModel,
                 workflow_instance: WorkflowInstance,
                 region_steps: list[WorkflowModelStep]) -> None:
    """
    Removes the given region, so that the workflow continues with whatever followed it

    The initial step is handled differently. It cannot be removed, because a model names exactly one
    entry point and the validation starts its traversal there. Emptying its assignment has the same
    effect: the step produces no validation job, and the step after it then starts at the entity
    itself, which is exactly where it would have started anyway
    """
    first_step = region_steps[0]
    successors_of_the_region = list(region_steps[-1].next_steps)
    region_step_iris = {region_step.iri for region_step in region_steps}

    steps_to_remove = region_steps
    if first_step.iri == workflow_model.initial_step_iri:
        workflow_instance.step_assignments[first_step.iri].assigned_entities = []
        workflow_instance.step_assignments[first_step.iri].repetition_count = 1
        first_step.repetition = None
        first_step.next_steps = successors_of_the_region

        steps_to_remove = region_steps[1:]
        region_step_iris.remove(first_step.iri)
    else:
        for other_step in workflow_model.workflow_model_steps.values():
            if other_step.iri in region_step_iris or first_step.iri not in other_step.next_steps:
                continue

            # The region is replaced by everything that came after it. A successor that the
            # predecessor already had is not added twice, or the validation would visit it twice
            rewired_next_steps = []
            for next_step_iri in other_step.next_steps:
                replacements = successors_of_the_region if next_step_iri == first_step.iri else [next_step_iri]
                for replacement in replacements:
                    if replacement not in rewired_next_steps:
                        rewired_next_steps.append(replacement)

            other_step.next_steps = rewired_next_steps

    for step_to_remove in steps_to_remove:
        del workflow_model.workflow_model_steps[step_to_remove.iri]
        workflow_instance.step_assignments.pop(step_to_remove.iri, None)
