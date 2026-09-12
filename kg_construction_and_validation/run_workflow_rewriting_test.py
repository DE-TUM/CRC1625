"""
Tests for the rewriting that resolves control flow operators before a validation run

These tests work on workflow models and instances in memory. They need neither a triple store nor a
running validation, so they are quick to run and can be used while developing an operator.
"""
import logging
import sys

from rdflib import URIRef

from workflows_validation.common import dw_prefix
from workflows_validation.rewriting import RepetitionError, get_step_display_name, unroll_repetitions
from workflows_validation.workflow_instance import StepAssignment, WorkflowInstance
from workflows_validation.workflow_model import Repetition, WorkflowModel, WorkflowModelStep

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

entity_iri = dw_prefix["handover_group_0"]


def build_workflow(successors_by_step_name: dict[str, list[str]],
                   initial_step_name: str) -> tuple[WorkflowModel, WorkflowInstance]:
    """
    Builds a workflow model with one step per entry of `successors_by_step_name`, and a workflow
    instance that assigns the same entity to every one of them

    The steps carry no SHACL shape, because these tests only look at the shape of the graph
    """
    workflow_model = WorkflowModel()
    workflow_model.create_new_iri()
    workflow_model.initial_step_iri = dw_prefix[initial_step_name]

    workflow_instance = WorkflowInstance()
    workflow_instance.create_new_iri()
    workflow_instance.workflow_model_iri = workflow_model.iri

    for step_name, successor_names in successors_by_step_name.items():
        step = WorkflowModelStep()
        step.iri = dw_prefix[step_name]
        step.name = step_name
        step.next_steps = [dw_prefix[successor_name] for successor_name in successor_names]
        workflow_model.workflow_model_steps[step.iri] = step

        step_assignment = StepAssignment()
        step_assignment.iri = dw_prefix[f"assignment_of_{step_name}"]
        step_assignment.workflow_step_iri = step.iri
        step_assignment.assigned_entities = [entity_iri]
        step_assignment.property_to_follow = dw_prefix.nextStep
        workflow_instance.step_assignments[step.iri] = step_assignment

    return workflow_model, workflow_instance


def set_repetition(workflow_model: WorkflowModel,
                   workflow_instance: WorkflowInstance,
                   step_name: str,
                   minimum: int,
                   maximum: int,
                   repetition_count: int,
                   until_step_name: str | None = None) -> None:
    """
    Marks the named step as repeatable in the model, and sets how often the instance repeats it

    `until_step_name` extends the repetition to a whole sequence ending at that step. Without it,
    only the named step is repeated
    """
    step_iri = dw_prefix[step_name]
    workflow_model.workflow_model_steps[step_iri].repetition = Repetition(
        min_repetitions=minimum,
        max_repetitions=maximum,
        repeats_until=dw_prefix[until_step_name] if until_step_name else None)
    workflow_instance.step_assignments[step_iri].repetition_count = repetition_count


def get_step_names_in_order(workflow_model: WorkflowModel) -> list[str]:
    """
    Yields the names of the steps along the only path of a linear workflow model, starting at its
    initial step. Raises if the model branches, so that a test cannot pass by accident
    """
    step_names = []
    current_step_iri = workflow_model.initial_step_iri

    while current_step_iri is not None:
        step = workflow_model.workflow_model_steps[current_step_iri]
        step_names.append(step.name)

        if len(step.next_steps) > 1:
            raise ValueError(f"Step '{step.name}' branches, so the model is not linear")
        current_step_iri = step.next_steps[0] if step.next_steps else None

    return step_names


def expect_repetition_error(description: str, workflow_model: WorkflowModel, workflow_instance: WorkflowInstance):
    """
    The whole rewriting is run, not just one of the checks, so that the test also covers the case
    where a problem is caught somewhere other than where it is expected
    """
    try:
        unroll_repetitions(workflow_model, workflow_instance)
    except RepetitionError as error:
        logging.info(f"Rejected as expected ({description}): {error}")
        return

    raise ValueError(f"The check should have rejected this workflow ({description})")


def test_a_step_without_a_repetition_is_left_alone():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": []}, "a")

    rewritten_model, rewritten_instance = unroll_repetitions(workflow_model, workflow_instance)

    assert get_step_names_in_order(rewritten_model) == ["a", "b", "c"]
    assert set(rewritten_instance.step_assignments.keys()) == set(rewritten_model.workflow_model_steps.keys())
    logging.info("Workflow without repetitions passed")


def test_a_repeated_step_is_cloned():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=1, maximum=5, repetition_count=3)

    rewritten_model, rewritten_instance = unroll_repetitions(workflow_model, workflow_instance)

    assert get_step_names_in_order(rewritten_model) == ["a", "b", "b_rep1", "b_rep2", "c"]

    # Every clone needs an assignment of its own, or the validation would skip it
    for step_iri in rewritten_model.workflow_model_steps:
        assert step_iri in rewritten_instance.step_assignments
        assert rewritten_instance.step_assignments[step_iri].assigned_entities == [entity_iri]

    # All three occurrences point back at the step the user wrote
    occurrences = [step for step in rewritten_model.workflow_model_steps.values()
                   if step.original_step_iri == dw_prefix["b"]]
    assert sorted(step.occurrences for step in occurrences) == [[0], [1], [2]]

    # The original step keeps its IRI, so a report about it needs no translation
    assert dw_prefix["b"] in rewritten_model.workflow_model_steps
    logging.info("Repeated step test passed")


def test_the_repeated_step_can_be_the_last_one():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=1, maximum=3, repetition_count=2)

    rewritten_model, _ = unroll_repetitions(workflow_model, workflow_instance)

    assert get_step_names_in_order(rewritten_model) == ["a", "b", "b_rep1"]
    logging.info("Repetition of the last step passed")


def test_several_steps_can_be_repeated():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=1, maximum=3, repetition_count=2)
    set_repetition(workflow_model, workflow_instance, "c", minimum=1, maximum=3, repetition_count=3)

    rewritten_model, _ = unroll_repetitions(workflow_model, workflow_instance)

    assert get_step_names_in_order(rewritten_model) == ["a", "a_rep1", "b", "c", "c_rep1", "c_rep2"]
    logging.info("Several repeated steps passed")


def test_a_step_repeated_once_stays_as_it_is():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=1, maximum=3, repetition_count=1)

    rewritten_model, _ = unroll_repetitions(workflow_model, workflow_instance)

    assert get_step_names_in_order(rewritten_model) == ["a", "b"]
    # A single run is not an occurrence of anything, so reports look as if the step never repeated
    assert rewritten_model.workflow_model_steps[dw_prefix["a"]].original_step_iri is None
    logging.info("Repetition count of one passed")


def test_a_skipped_step_is_removed():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=0, maximum=3, repetition_count=0)

    rewritten_model, rewritten_instance = unroll_repetitions(workflow_model, workflow_instance)

    assert get_step_names_in_order(rewritten_model) == ["a", "c"]
    assert dw_prefix["b"] not in rewritten_model.workflow_model_steps
    assert dw_prefix["b"] not in rewritten_instance.step_assignments
    logging.info("Skipped step test passed")


def test_skipping_a_branching_step_moves_the_branch_up():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c", "d"], "c": [], "d": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=0, maximum=1, repetition_count=0)

    rewritten_model, _ = unroll_repetitions(workflow_model, workflow_instance)

    assert rewritten_model.workflow_model_steps[dw_prefix["a"]].next_steps == [dw_prefix["c"], dw_prefix["d"]]
    assert dw_prefix["b"] not in rewritten_model.workflow_model_steps
    logging.info("Skipping a branching step passed")


def test_skipping_does_not_duplicate_a_successor_the_predecessor_already_had():
    # "a" reaches "c" directly and through "b", so removing "b" must not give "a" two edges to "c"
    workflow_model, workflow_instance = build_workflow({"a": ["b", "c"], "b": ["c"], "c": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=0, maximum=1, repetition_count=0)

    rewritten_model, _ = unroll_repetitions(workflow_model, workflow_instance)

    assert rewritten_model.workflow_model_steps[dw_prefix["a"]].next_steps == [dw_prefix["c"]]
    logging.info("Skipping without duplicated successors passed")


def test_the_skipped_initial_step_is_kept_but_emptied():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=0, maximum=1, repetition_count=0)

    rewritten_model, rewritten_instance = unroll_repetitions(workflow_model, workflow_instance)

    # The model names exactly one entry point, so the step has to stay where it is
    assert rewritten_model.initial_step_iri == dw_prefix["a"]
    assert dw_prefix["a"] in rewritten_model.workflow_model_steps

    # Without assigned entities the step produces no validation job, and "b" starts at the entity
    assert rewritten_instance.step_assignments[dw_prefix["a"]].assigned_entities == []
    assert rewritten_instance.step_assignments[dw_prefix["b"]].assigned_entities == [entity_iri]
    logging.info("Skipped initial step test passed")


def test_a_repetition_of_a_clone_is_never_resolved_again():
    workflow_model, workflow_instance = build_workflow({"a": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=1, maximum=4, repetition_count=4)

    rewritten_model, rewritten_instance = unroll_repetitions(workflow_model, workflow_instance)

    assert len(rewritten_model.workflow_model_steps) == 4
    for step in rewritten_model.workflow_model_steps.values():
        assert step.repetition is None
    for step_assignment in rewritten_instance.step_assignments.values():
        assert step_assignment.repetition_count == 1

    # Rewriting an already rewritten workflow changes nothing
    rewritten_twice, _ = unroll_repetitions(rewritten_model, rewritten_instance)
    assert get_step_names_in_order(rewritten_twice) == get_step_names_in_order(rewritten_model)
    logging.info("Rewriting is idempotent, test passed")


def test_the_originals_are_left_untouched():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=1, maximum=3, repetition_count=3)

    unroll_repetitions(workflow_model, workflow_instance)

    # The web UI keeps working with the objects it passed in
    assert len(workflow_model.workflow_model_steps) == 2
    assert len(workflow_instance.step_assignments) == 2
    assert workflow_model.workflow_model_steps[dw_prefix["a"]].repetition is not None
    assert workflow_instance.step_assignments[dw_prefix["a"]].repetition_count == 3
    logging.info("Originals left untouched, test passed")


def test_clones_get_a_readable_name_for_reports():
    workflow_model, workflow_instance = build_workflow({"a": []}, "a")
    workflow_model.workflow_model_steps[dw_prefix["a"]].name = "Characterization"
    set_repetition(workflow_model, workflow_instance, "a", minimum=1, maximum=3, repetition_count=2)

    rewritten_model, _ = unroll_repetitions(workflow_model, workflow_instance)

    # All occurrences of a repeated step are numbered, including the first one
    original = rewritten_model.workflow_model_steps[dw_prefix["a"]]
    assert get_step_display_name(original, workflow_model) == "Characterization (repetition 1)"

    clone = rewritten_model.workflow_model_steps[URIRef(f"{dw_prefix['a']}_rep1")]
    assert get_step_display_name(clone, workflow_model) == "Characterization (repetition 2)"

    # A step that was never repeatable keeps its plain name
    workflow_model, workflow_instance = build_workflow({"a": []}, "a")
    workflow_model.workflow_model_steps[dw_prefix["a"]].name = "Characterization"
    rewritten_model, _ = unroll_repetitions(workflow_model, workflow_instance)

    assert get_step_display_name(rewritten_model.workflow_model_steps[dw_prefix["a"]], workflow_model) == "Characterization"
    logging.info("Report names test passed")


def test_wrong_repetitions_are_rejected():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=1, maximum=3, repetition_count=4)
    expect_repetition_error("count above the maximum", workflow_model, workflow_instance)

    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=2, maximum=3, repetition_count=1)
    expect_repetition_error("count below the minimum", workflow_model, workflow_instance)

    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=3, maximum=2, repetition_count=3)
    expect_repetition_error("empty range", workflow_model, workflow_instance)

    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": []}, "a")
    workflow_instance.step_assignments[dw_prefix["a"]].repetition_count = 2
    expect_repetition_error("repeated without the model allowing it", workflow_model, workflow_instance)

    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=1, maximum=3, repetition_count=2)
    del workflow_instance.step_assignments[dw_prefix["a"]]
    expect_repetition_error("repeatable step without an assignment", workflow_model, workflow_instance)

    workflow_model, workflow_instance = build_workflow({"a": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=0, maximum=1, repetition_count=0)
    expect_repetition_error("the only step of the model is skipped", workflow_model, workflow_instance)

    logging.info("Rejection tests passed")


def test_a_repeated_sequence_is_copied_as_a_whole():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": ["d"], "d": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=1, maximum=4, repetition_count=3, until_step_name="c")

    rewritten_model, rewritten_instance = unroll_repetitions(workflow_model, workflow_instance)

    assert get_step_names_in_order(rewritten_model) == ["a", "b", "c", "b_rep1", "c_rep1", "b_rep2", "c_rep2", "d"]

    for step_iri in rewritten_model.workflow_model_steps:
        assert step_iri in rewritten_instance.step_assignments
    logging.info("Repeated sequence test passed")


def test_a_repeated_sequence_can_end_the_model():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=1, maximum=3, repetition_count=2, until_step_name="c")

    rewritten_model, _ = unroll_repetitions(workflow_model, workflow_instance)

    assert get_step_names_in_order(rewritten_model) == ["a", "b", "c", "b_rep1", "c_rep1"]
    logging.info("Repeated sequence at the end of the model passed")


def test_a_sequence_inside_a_sequence_runs_in_every_outer_occurrence():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": ["d"], "d": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=1, maximum=3, repetition_count=2, until_step_name="c")
    set_repetition(workflow_model, workflow_instance, "c", minimum=1, maximum=3, repetition_count=2)

    rewritten_model, _ = unroll_repetitions(workflow_model, workflow_instance)

    # The inner step runs twice within each of the two outer occurrences. The whole chain of
    # repetitions goes into a copied name, so the two copies of "c" cannot collide
    assert get_step_names_in_order(rewritten_model) == ["a",
                                                        "b", "c", "c_rep0.1",
                                                        "b_rep1", "c_rep1", "c_rep1.1",
                                                        "d"]
    logging.info("Nested sequences test passed")


def test_nested_sequences_may_end_at_the_same_step():
    # This is the case that forces the outer sequence to be written out first. Doing the inner one
    # first would push its copies past "d", which is where the outer one ends
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": ["d"], "d": ["e"], "e": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=1, maximum=3, repetition_count=2, until_step_name="d")
    set_repetition(workflow_model, workflow_instance, "c", minimum=1, maximum=3, repetition_count=2, until_step_name="d")

    rewritten_model, _ = unroll_repetitions(workflow_model, workflow_instance)

    assert get_step_names_in_order(rewritten_model) == ["a",
                                                        "b", "c", "d", "c_rep0.1", "d_rep0.1",
                                                        "b_rep1", "c_rep1", "d_rep1", "c_rep1.1", "d_rep1.1",
                                                        "e"]
    logging.info("Nested sequences with a shared last step passed")


def test_occurrences_are_numbered_from_the_outside_in():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": []}, "a")
    workflow_model.workflow_model_steps[dw_prefix["b"]].name = "Synthesis"
    workflow_model.workflow_model_steps[dw_prefix["c"]].name = "Characterization"
    set_repetition(workflow_model, workflow_instance, "b", minimum=1, maximum=3, repetition_count=2, until_step_name="c")
    set_repetition(workflow_model, workflow_instance, "c", minimum=1, maximum=3, repetition_count=2)

    rewritten_model, _ = unroll_repetitions(workflow_model, workflow_instance)

    display_names = [get_step_display_name(rewritten_model.workflow_model_steps[step_iri], workflow_model)
                     for step_iri in [dw_prefix["a"],
                                      dw_prefix["b"],
                                      dw_prefix["c"],
                                      URIRef(f"{dw_prefix['c']}_rep0.1"),
                                      URIRef(f"{dw_prefix['b']}_rep1"),
                                      URIRef(f"{dw_prefix['c']}_rep1"),
                                      URIRef(f"{dw_prefix['c']}_rep1.1")]]

    # A step that sits in two repeated sequences gets one number per sequence, outermost first
    assert display_names == ["a",
                             "Synthesis (repetition 1)",
                             "Characterization (repetition 1.1)",
                             "Characterization (repetition 1.2)",
                             "Synthesis (repetition 2)",
                             "Characterization (repetition 2.1)",
                             "Characterization (repetition 2.2)"]
    logging.info("Nested occurrence numbering passed")


def test_a_skipped_sequence_is_removed():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": ["d"], "d": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=0, maximum=3, repetition_count=0, until_step_name="c")

    rewritten_model, rewritten_instance = unroll_repetitions(workflow_model, workflow_instance)

    assert get_step_names_in_order(rewritten_model) == ["a", "d"]
    for step_name in ["b", "c"]:
        assert dw_prefix[step_name] not in rewritten_model.workflow_model_steps
        assert dw_prefix[step_name] not in rewritten_instance.step_assignments
    logging.info("Skipped sequence test passed")


def test_a_skipped_sequence_at_the_start_keeps_only_the_entry_point():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=0, maximum=3, repetition_count=0, until_step_name="b")

    rewritten_model, rewritten_instance = unroll_repetitions(workflow_model, workflow_instance)

    # The model names exactly one entry point, so the first step stays and is emptied instead
    assert rewritten_model.initial_step_iri == dw_prefix["a"]
    assert get_step_names_in_order(rewritten_model) == ["a", "c"]
    assert dw_prefix["b"] not in rewritten_model.workflow_model_steps
    assert rewritten_instance.step_assignments[dw_prefix["a"]].assigned_entities == []
    assert rewritten_instance.step_assignments[dw_prefix["c"]].assigned_entities == [entity_iri]
    logging.info("Skipped sequence at the entry point passed")


def test_broken_sequences_are_rejected():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c", "d"], "c": ["e"], "d": [], "e": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=1, maximum=3, repetition_count=2, until_step_name="e")
    expect_repetition_error("a step inside the sequence branches", workflow_model, workflow_instance)

    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=1, maximum=3, repetition_count=2, until_step_name="b")
    workflow_model.workflow_model_steps[dw_prefix["a"]].repetition.repeats_until = dw_prefix["not_a_step"]
    expect_repetition_error("the sequence ends outside the model", workflow_model, workflow_instance)

    # [b, c] and [c, d] share c without either containing the other
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": ["d"], "d": []}, "a")
    set_repetition(workflow_model, workflow_instance, "b", minimum=1, maximum=3, repetition_count=2, until_step_name="c")
    set_repetition(workflow_model, workflow_instance, "c", minimum=1, maximum=3, repetition_count=2, until_step_name="d")
    expect_repetition_error("two sequences overlap", workflow_model, workflow_instance)

    logging.info("Broken sequence rejection tests passed")


def test_a_copied_workflow_model_is_independent():
    workflow_model, workflow_instance = build_workflow({"a": ["b"], "b": ["c"], "c": []}, "a")
    set_repetition(workflow_model, workflow_instance, "a", minimum=1, maximum=3, repetition_count=2, until_step_name="b")

    workflow_model_copy = workflow_model.create_copy()

    original_step_iris = set(workflow_model.workflow_model_steps.keys())
    copied_step_iris = set(workflow_model_copy.workflow_model_steps.keys())

    # Nothing in the copy may still point at a step of the model it was copied from
    assert not (original_step_iris & copied_step_iris)
    assert workflow_model_copy.initial_step_iri in copied_step_iris

    for copied_step in workflow_model_copy.workflow_model_steps.values():
        assert copied_step.iri in copied_step_iris
        for next_step_iri in copied_step.next_steps:
            assert next_step_iri in copied_step_iris
        if copied_step.repetition is not None:
            assert copied_step.repetition.repeats_until in copied_step_iris

    copied_repetition = workflow_model_copy.workflow_model_steps[workflow_model_copy.initial_step_iri].repetition
    assert copied_repetition.iri != workflow_model.workflow_model_steps[dw_prefix["a"]].repetition.iri

    # The copy has to stay readable, which it is not if a reference was missed
    assert get_step_names_in_order(workflow_model_copy) == ["a", "b", "c"]
    logging.info("Independent copy test passed")


test_a_step_without_a_repetition_is_left_alone()
test_a_repeated_step_is_cloned()
test_the_repeated_step_can_be_the_last_one()
test_several_steps_can_be_repeated()
test_a_step_repeated_once_stays_as_it_is()
test_a_skipped_step_is_removed()
test_skipping_a_branching_step_moves_the_branch_up()
test_skipping_does_not_duplicate_a_successor_the_predecessor_already_had()
test_the_skipped_initial_step_is_kept_but_emptied()
test_a_repetition_of_a_clone_is_never_resolved_again()
test_the_originals_are_left_untouched()
test_clones_get_a_readable_name_for_reports()
test_wrong_repetitions_are_rejected()
test_a_repeated_sequence_is_copied_as_a_whole()
test_a_repeated_sequence_can_end_the_model()
test_a_sequence_inside_a_sequence_runs_in_every_outer_occurrence()
test_nested_sequences_may_end_at_the_same_step()
test_occurrences_are_numbered_from_the_outside_in()
test_a_skipped_sequence_is_removed()
test_a_skipped_sequence_at_the_start_keeps_only_the_entry_point()
test_broken_sequences_are_rejected()
test_a_copied_workflow_model_is_independent()

logging.info("All rewriting tests passed")
