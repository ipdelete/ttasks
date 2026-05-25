"""Tests for Task validation and state-machine behavior."""

from datetime import datetime
from typing import Any

import pytest

from ttasks import Task, TaskResult, TaskStatus


def test_type_must_be_task_type() -> None:
    """Tasks reject non-TaskType type values at construction time."""
    task_type: Any = "bash"

    with pytest.raises(TypeError, match="type must be a TaskType"):
        Task(title="Example", payload="echo hi", type=task_type)


def test_timeout_must_be_positive() -> None:
    """Tasks reject zero and negative timeout values at construction time."""
    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        Task.bash("echo hi", title="Example", timeout=0)

    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        Task.bash("echo hi", title="Example", timeout=-1)


def test_repr_includes_identity_title_and_status() -> None:
    """The task repr includes the useful debugging fields."""
    task = Task.bash("echo hi", title="Example")

    assert repr(task) == (
        f"Task(id={task.id!r}, title='Example', status={TaskStatus.PENDING.value})"
    )


def test_timeout_defaults_to_no_automatic_timeout() -> None:
    """Omitting timeout intentionally means no automatic timeout is applied."""
    task = Task.bash("echo hi", title="No timeout")

    assert task.timeout is None


def test_timeout_accepts_positive_values() -> None:
    """Tasks accept positive timeout values for bounded execution."""
    task = Task.bash("echo hi", title="Timeout", timeout=1.5)

    assert task.timeout == 1.5


def test_id_is_read_only() -> None:
    """External callers cannot mutate task identity after construction."""
    task = Task.bash("echo hi", title="Example")
    original_id = task.id

    # Use dynamic setattr so the type checker accepts this runtime guard test.
    attr = "id"
    with pytest.raises(AttributeError):
        setattr(task, attr, "new-id")

    assert task.id == original_id


def test_status_is_read_only() -> None:
    """External callers cannot bypass the state machine via status assignment."""
    task = Task.bash("echo hi", title="Example")

    # Use dynamic setattr so the type checker accepts this runtime guard test.
    attr = "status"
    with pytest.raises(AttributeError):
        setattr(task, attr, TaskStatus.SUCCEEDED)

    assert task.status == TaskStatus.PENDING


def test_can_transition_to_rejects_non_task_status() -> None:
    """can_transition_to reports a clean TypeError for invalid status values."""
    task = Task.bash("echo hi", title="Example")
    status: Any = "done"

    with pytest.raises(TypeError, match="status must be a TaskStatus"):
        task.can_transition_to(status)


def test_transition_to_rejects_non_task_status() -> None:
    """transition_to reports a clean TypeError for invalid status values."""
    task = Task.bash("echo hi", title="Example")
    status: Any = "done"

    with pytest.raises(TypeError, match="status must be a TaskStatus"):
        task.transition_to(status)

    assert task.status == TaskStatus.PENDING


def test_status_changes_through_transition_to() -> None:
    """Valid status transitions are applied through transition_to()."""
    task = Task.bash("echo hi", title="Example")

    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.SUCCEEDED)

    assert task.status == TaskStatus.SUCCEEDED
    assert task.error is None


def test_cancel_changes_status_through_state_machine() -> None:
    """cancel() is a domain helper around the CANCELLED transition."""
    task = Task.bash("echo hi", title="Example")

    task.cancel()

    assert task.status == TaskStatus.CANCELLED


def test_cancel_is_idempotent() -> None:
    """Calling cancel repeatedly leaves the task cancelled without error."""
    task = Task.bash("echo hi", title="Example")

    task.cancel()
    task.cancel()

    assert task.status == TaskStatus.CANCELLED


def test_cancel_preserves_previous_error() -> None:
    """Cancelling a failed task keeps the failure reason for inspection."""
    task = Task.bash("echo hi", title="Example")
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.FAILED, error="boom")

    task.cancel()
    task.cancel()

    assert task.status == TaskStatus.CANCELLED
    assert task.error == "boom"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("title", "Changed"),
        ("description", "Changed"),
        ("payload", "echo changed"),
        ("timeout", 1.0),
        ("error", "changed"),
    ],
)
def test_done_tasks_reject_public_field_mutation(field: str, value: object) -> None:
    """SUCCEEDED tasks are immutable to normal public attribute assignment."""
    task = Task.bash("echo hi", title="Example")
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.SUCCEEDED)

    with pytest.raises(AttributeError, match="SUCCEEDED tasks are immutable"):
        setattr(task, field, value)


def test_invalid_transition_preserves_error() -> None:
    """A rejected transition does not mutate status or error."""
    task = Task.bash("echo hi", title="Example")
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.FAILED, error="boom")

    with pytest.raises(ValueError):
        task.transition_to(TaskStatus.SUCCEEDED)

    assert task.status == TaskStatus.FAILED
    assert task.error == "boom"


def test_failed_tasks_remain_mutable_for_retry() -> None:
    """FAILED tasks remain editable so callers can repair and retry them."""
    task = Task.bash("exit 1", title="Example")
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.FAILED, error="boom")

    task.payload = "echo recovered"
    task.error = None

    assert task.payload == "echo recovered"
    assert task.error is None


@pytest.mark.parametrize(
    ("initial_status", "next_status"),
    [
        (TaskStatus.PENDING, TaskStatus.RUNNING),
        (TaskStatus.PENDING, TaskStatus.CANCELLED),
        (TaskStatus.PENDING, TaskStatus.BLOCKED),
        (TaskStatus.PENDING, TaskStatus.FAILED),
        (TaskStatus.RUNNING, TaskStatus.SUCCEEDED),
        (TaskStatus.RUNNING, TaskStatus.FAILED),
        (TaskStatus.RUNNING, TaskStatus.CANCELLED),
        (TaskStatus.FAILED, TaskStatus.RUNNING),
        (TaskStatus.FAILED, TaskStatus.CANCELLED),
        (TaskStatus.BLOCKED, TaskStatus.RUNNING),
        (TaskStatus.BLOCKED, TaskStatus.CANCELLED),
    ],
)
def test_allowed_transitions(
    initial_status: TaskStatus, next_status: TaskStatus
) -> None:
    """Every transition in the state-machine table is accepted."""
    task = task_with_status(initial_status)

    task.transition_to(next_status)

    assert task.status == next_status


@pytest.mark.parametrize(
    ("initial_status", "next_status"),
    [
        (initial_status, next_status)
        for initial_status in TaskStatus
        for next_status in TaskStatus
        if (initial_status, next_status)
        not in {
            (TaskStatus.PENDING, TaskStatus.RUNNING),
            (TaskStatus.PENDING, TaskStatus.CANCELLED),
            (TaskStatus.PENDING, TaskStatus.BLOCKED),
            (TaskStatus.PENDING, TaskStatus.FAILED),
            (TaskStatus.RUNNING, TaskStatus.SUCCEEDED),
            (TaskStatus.RUNNING, TaskStatus.FAILED),
            (TaskStatus.RUNNING, TaskStatus.CANCELLED),
            (TaskStatus.FAILED, TaskStatus.RUNNING),
            (TaskStatus.FAILED, TaskStatus.CANCELLED),
            (TaskStatus.BLOCKED, TaskStatus.RUNNING),
            (TaskStatus.BLOCKED, TaskStatus.CANCELLED),
        }
    ],
)
def test_disallowed_transitions_are_rejected(
    initial_status: TaskStatus, next_status: TaskStatus
) -> None:
    """Every transition outside the state-machine table is rejected."""
    task = task_with_status(initial_status)

    with pytest.raises(
        ValueError,
        match=(
            f"Cannot transition task from {initial_status.value!r} "
            f"to {next_status.value!r}"
        ),
    ):
        task.transition_to(next_status)

    assert task.status == initial_status


def task_with_status(status: TaskStatus) -> Task:
    """Build a task and put it into status using valid public transitions."""
    task = Task.bash("echo hi", title="Example")

    match status:
        case TaskStatus.PENDING:
            return task
        case TaskStatus.RUNNING:
            task.transition_to(TaskStatus.RUNNING)
        case TaskStatus.SUCCEEDED:
            task.transition_to(TaskStatus.RUNNING)
            task.transition_to(TaskStatus.SUCCEEDED)
        case TaskStatus.FAILED:
            task.transition_to(TaskStatus.RUNNING)
            task.transition_to(TaskStatus.FAILED, error="boom")
        case TaskStatus.CANCELLED:
            task.cancel()
        case TaskStatus.BLOCKED:
            task.transition_to(TaskStatus.BLOCKED)

    return task


@pytest.mark.parametrize(
    ("status", "predicate", "expected"),
    [
        (TaskStatus.PENDING, "is_pending", True),
        (TaskStatus.PENDING, "is_running", False),
        (TaskStatus.PENDING, "is_succeeded", False),
        (TaskStatus.PENDING, "is_failed", False),
        (TaskStatus.PENDING, "is_cancelled", False),
        (TaskStatus.PENDING, "is_terminal", False),
        (TaskStatus.RUNNING, "is_pending", False),
        (TaskStatus.RUNNING, "is_running", True),
        (TaskStatus.RUNNING, "is_succeeded", False),
        (TaskStatus.RUNNING, "is_terminal", False),
        (TaskStatus.SUCCEEDED, "is_succeeded", True),
        (TaskStatus.SUCCEEDED, "is_running", False),
        (TaskStatus.SUCCEEDED, "is_terminal", True),
        (TaskStatus.FAILED, "is_failed", True),
        (TaskStatus.FAILED, "is_succeeded", False),
        (TaskStatus.FAILED, "is_terminal", True),
        (TaskStatus.CANCELLED, "is_cancelled", True),
        (TaskStatus.CANCELLED, "is_pending", False),
        (TaskStatus.CANCELLED, "is_terminal", True),
    ],
)
def test_status_predicates(
    status: TaskStatus, predicate: str, expected: bool
) -> None:
    """Status predicate properties reflect the current TaskStatus."""
    task = task_with_status(status)

    assert getattr(task, predicate) is expected


class TestTaskHashingAndEquality:
    """Tasks are identity-by-id so they work as set / dict keys."""

    def test_task_equality_is_by_id(self) -> None:
        t1 = Task.bash("echo a", title="A")
        t2 = Task(title="B", payload="echo b", type=t1.type, _id=t1.id)
        assert t1 == t2

    def test_task_hash_matches_id_hash(self) -> None:
        t = Task.bash("echo a", title="A")
        assert hash(t) == hash(t.id)

    def test_task_set_and_dict_membership(self) -> None:
        t1 = Task.bash("echo a", title="A")
        t2 = Task(title="A-dup", payload="echo a", type=t1.type, _id=t1.id)
        t3 = Task.bash("echo b", title="B")
        assert {t1, t2, t3} == {t1, t3}
        d = {t1: "first"}
        assert d[t2] == "first"

    def test_distinct_ids_are_not_equal(self) -> None:
        t1 = Task.bash("echo a", title="A")
        t2 = Task.bash("echo a", title="A")
        assert t1 != t2

    def test_task_not_equal_to_non_task(self) -> None:
        t = Task.bash("echo a", title="A")
        assert t != "not-a-task"
        assert t != None  # noqa: E711

    def test_id_is_immutable_after_construction(self) -> None:
        t = Task.bash("echo a", title="A")
        with pytest.raises(AttributeError, match="immutable"):
            t._id = "other"

    def test_same_id_different_status_still_equal(self) -> None:
        t1 = Task.bash("echo a", title="A")
        t2 = Task(title="A", payload="echo a", type=t1.type, _id=t1.id)
        t2.transition_to(TaskStatus.RUNNING)
        assert t1 == t2
        assert hash(t1) == hash(t2)


class TestTaskStatusSucceeded:
    """SUCCEEDED has been renamed to SUCCEEDED."""

    def test_taskstatus_succeeded_value_is_succeeded(self) -> None:
        assert TaskStatus.SUCCEEDED.value == "succeeded"

    def test_task_is_succeeded_after_success(self) -> None:
        task = Task.bash("echo a", title="A")
        task.transition_to(TaskStatus.RUNNING)
        task.transition_to(TaskStatus.SUCCEEDED)
        assert task.is_succeeded is True
        assert task.status == TaskStatus.SUCCEEDED

    def test_old_done_name_is_gone(self) -> None:
        assert not hasattr(TaskStatus, "DONE")


class TestBlockedStatus:
    """BLOCKED is a terminal status that supports retry (BLOCKED→RUNNING)."""

    def test_blocked_value_is_blocked(self) -> None:
        assert TaskStatus.BLOCKED.value == "blocked"

    def test_is_blocked_predicate(self) -> None:
        task = task_with_status(TaskStatus.BLOCKED)
        assert task.is_blocked is True
        assert task.is_terminal is True

    def test_pending_can_transition_to_blocked(self) -> None:
        task = Task.bash("a", title="A")
        assert task.can_transition_to(TaskStatus.BLOCKED)
        task.transition_to(TaskStatus.BLOCKED)
        assert task.status == TaskStatus.BLOCKED

    def test_blocked_can_transition_back_to_running(self) -> None:
        task = task_with_status(TaskStatus.BLOCKED)
        assert task.can_transition_to(TaskStatus.RUNNING)
        task.transition_to(TaskStatus.RUNNING)
        assert task.status == TaskStatus.RUNNING


class TestBlockedBy:
    """blocked_by records the direct upstream parent that triggered the block."""

    def test_blocked_by_default_none(self) -> None:
        task = Task.bash("a", title="A")
        assert task.blocked_by is None

    def test_public_write_rejected(self) -> None:
        task = Task.bash("a", title="A")
        with pytest.raises(AttributeError):
            task.blocked_by = "parent"  # ty: ignore[invalid-assignment]

    def test_set_via_private_setter(self) -> None:
        task = Task.bash("a", title="A")
        task._set_blocked_by("parent-id")
        assert task.blocked_by == "parent-id"

    def test_result_public_write_rejected(self) -> None:
        task = Task.bash("a", title="A")
        with pytest.raises(AttributeError):
            task.result = None  # ty: ignore[invalid-assignment]

    def test_result_set_via_private_setter(self) -> None:
        task = Task.bash("a", title="A")
        r = TaskResult(
            task_id=task.id,
            status=TaskStatus.SUCCEEDED,
            started_at=datetime.now(),
            finished_at=datetime.now(),
            duration=0.01,
        )
        task._set_result(r)
        assert task.result is r


class TestRunningEntryResetsCarryover:
    """Entering RUNNING clears any prior run's result and blocked_by."""

    def test_failed_to_running_clears_result(self) -> None:
        task = task_with_status(TaskStatus.FAILED)
        r = TaskResult(
            task_id=task.id,
            status=TaskStatus.FAILED,
            started_at=datetime.now(),
            finished_at=datetime.now(),
            duration=0.01,
        )
        task._set_result(r)

        task.transition_to(TaskStatus.RUNNING)

        assert task.result is None

    def test_blocked_to_running_clears_blocked_by(self) -> None:
        task = task_with_status(TaskStatus.BLOCKED)
        task._set_blocked_by("parent-id")

        task.transition_to(TaskStatus.RUNNING)

        assert task.blocked_by is None


# ---- TaskStatus enum predicates (step sm-1) ---------------------------------


class TestTaskStatusPredicates:
    """TaskStatus carries its own predicates so callsites don't dup set literals."""

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (TaskStatus.PENDING, False),
            (TaskStatus.RUNNING, False),
            (TaskStatus.SUCCEEDED, True),
            (TaskStatus.FAILED, False),
            (TaskStatus.CANCELLED, True),
            (TaskStatus.BLOCKED, False),
        ],
    )
    def test_is_sink_truth_table(
        self, status: TaskStatus, expected: bool
    ) -> None:
        """is_sink: states with no outgoing transitions in the SM."""
        assert status.is_sink is expected

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (TaskStatus.PENDING, False),
            (TaskStatus.RUNNING, False),
            (TaskStatus.SUCCEEDED, False),
            (TaskStatus.FAILED, True),
            (TaskStatus.CANCELLED, True),
            (TaskStatus.BLOCKED, True),
        ],
    )
    def test_is_bad_truth_table(
        self, status: TaskStatus, expected: bool
    ) -> None:
        """is_bad: an upstream parent in this state blocks ready descendants."""
        assert status.is_bad is expected

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (TaskStatus.PENDING, True),
            (TaskStatus.RUNNING, True),
            (TaskStatus.SUCCEEDED, False),
            (TaskStatus.FAILED, False),
            (TaskStatus.CANCELLED, False),
            (TaskStatus.BLOCKED, False),
        ],
    )
    def test_is_active_truth_table(
        self, status: TaskStatus, expected: bool
    ) -> None:
        """is_active: task may still progress without intervention."""
        assert status.is_active is expected

    def test_is_sink_matches_allowed_transitions(self) -> None:
        """is_sink must agree with the canonical _ALLOWED_TRANSITIONS table.

        This is the drift guard: if a future change adds an outgoing edge
        from a sink state (or removes one from a non-sink state), this
        test catches it before the predicate goes out of sync.
        """
        from ttasks._task import _ALLOWED_TRANSITIONS

        for status in TaskStatus:
            assert status.is_sink == (_ALLOWED_TRANSITIONS[status] == set()), (
                f"is_sink/_ALLOWED_TRANSITIONS disagree for {status}"
            )

    def test_active_and_sink_are_disjoint(self) -> None:
        """A status cannot be both able-to-progress and unable-to-move."""
        for status in TaskStatus:
            assert not (status.is_active and status.is_sink)

    def test_active_and_bad_are_disjoint(self) -> None:
        """A status cannot be both able-to-progress and blocking-descendants."""
        for status in TaskStatus:
            assert not (status.is_active and status.is_bad)

    def test_predicates_cover_all_statuses(self) -> None:
        """Every status falls under is_active, is_bad, or is_sink."""
        for status in TaskStatus:
            assert status.is_active or status.is_bad or status.is_sink
