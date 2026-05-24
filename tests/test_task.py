"""Tests for Task validation and state-machine behavior."""

from typing import Any

import pytest

from ttasks.task import Task, TaskStatus, TaskType


def test_type_must_be_task_type() -> None:
    """Tasks reject non-TaskType type values at construction time."""
    task_type: Any = "bash"

    with pytest.raises(TypeError, match="type must be a TaskType"):
        Task(title="Example", payload="echo hi", type=task_type)


def test_timeout_must_be_positive() -> None:
    """Tasks reject zero and negative timeout values at construction time."""
    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        Task(title="Example", payload="echo hi", type=TaskType.BASH, timeout=0)

    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        Task(title="Example", payload="echo hi", type=TaskType.BASH, timeout=-1)


def test_repr_includes_identity_title_and_status() -> None:
    """The task repr includes the useful debugging fields."""
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    assert repr(task) == (
        f"Task(id={task.id!r}, title='Example', status={TaskStatus.PENDING.value})"
    )


def test_timeout_defaults_to_no_automatic_timeout() -> None:
    """Omitting timeout intentionally means no automatic timeout is applied."""
    task = Task(title="No timeout", payload="echo hi", type=TaskType.BASH)

    assert task.timeout is None


def test_timeout_accepts_positive_values() -> None:
    """Tasks accept positive timeout values for bounded execution."""
    task = Task(
        title="Timeout",
        payload="echo hi",
        type=TaskType.BASH,
        timeout=1.5,
    )

    assert task.timeout == 1.5


def test_id_is_read_only() -> None:
    """External callers cannot mutate task identity after construction."""
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    original_id = task.id

    # Use dynamic setattr so the type checker accepts this runtime guard test.
    attr = "id"
    with pytest.raises(AttributeError):
        setattr(task, attr, "new-id")

    assert task.id == original_id


def test_status_is_read_only() -> None:
    """External callers cannot bypass the state machine via status assignment."""
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    # Use dynamic setattr so the type checker accepts this runtime guard test.
    attr = "status"
    with pytest.raises(AttributeError):
        setattr(task, attr, TaskStatus.DONE)

    assert task.status == TaskStatus.PENDING


def test_can_transition_to_rejects_non_task_status() -> None:
    """can_transition_to reports a clean TypeError for invalid status values."""
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    status: Any = "done"

    with pytest.raises(TypeError, match="status must be a TaskStatus"):
        task.can_transition_to(status)


def test_transition_to_rejects_non_task_status() -> None:
    """transition_to reports a clean TypeError for invalid status values."""
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    status: Any = "done"

    with pytest.raises(TypeError, match="status must be a TaskStatus"):
        task.transition_to(status)

    assert task.status == TaskStatus.PENDING


def test_status_changes_through_transition_to() -> None:
    """Valid status transitions are applied through transition_to()."""
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.DONE)

    assert task.status == TaskStatus.DONE
    assert task.error is None


def test_cancel_changes_status_through_state_machine() -> None:
    """cancel() is a domain helper around the CANCELLED transition."""
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    task.cancel()

    assert task.status == TaskStatus.CANCELLED


def test_cancel_is_idempotent() -> None:
    """Calling cancel repeatedly leaves the task cancelled without error."""
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    task.cancel()
    task.cancel()

    assert task.status == TaskStatus.CANCELLED


def test_cancel_preserves_previous_error() -> None:
    """Cancelling a failed task keeps the failure reason for inspection."""
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.FAILED, error="boom")

    task.cancel()
    task.cancel()

    assert task.status == TaskStatus.CANCELLED
    assert task.error == "boom"


def test_invalid_transition_preserves_error() -> None:
    """A rejected transition does not mutate status or error."""
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.FAILED, error="boom")

    with pytest.raises(ValueError):
        task.transition_to(TaskStatus.DONE)

    assert task.status == TaskStatus.FAILED
    assert task.error == "boom"


@pytest.mark.parametrize(
    ("initial_status", "next_status"),
    [
        (TaskStatus.PENDING, TaskStatus.RUNNING),
        (TaskStatus.PENDING, TaskStatus.CANCELLED),
        (TaskStatus.RUNNING, TaskStatus.DONE),
        (TaskStatus.RUNNING, TaskStatus.FAILED),
        (TaskStatus.RUNNING, TaskStatus.CANCELLED),
        (TaskStatus.FAILED, TaskStatus.RUNNING),
        (TaskStatus.FAILED, TaskStatus.CANCELLED),
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
            (TaskStatus.RUNNING, TaskStatus.DONE),
            (TaskStatus.RUNNING, TaskStatus.FAILED),
            (TaskStatus.RUNNING, TaskStatus.CANCELLED),
            (TaskStatus.FAILED, TaskStatus.RUNNING),
            (TaskStatus.FAILED, TaskStatus.CANCELLED),
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
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    match status:
        case TaskStatus.PENDING:
            return task
        case TaskStatus.RUNNING:
            task.transition_to(TaskStatus.RUNNING)
        case TaskStatus.DONE:
            task.transition_to(TaskStatus.RUNNING)
            task.transition_to(TaskStatus.DONE)
        case TaskStatus.FAILED:
            task.transition_to(TaskStatus.RUNNING)
            task.transition_to(TaskStatus.FAILED, error="boom")
        case TaskStatus.CANCELLED:
            task.cancel()

    return task
