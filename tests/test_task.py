"""Tests for Task validation and state-machine behavior."""

import pytest

from task import Task, TaskStatus, TaskType


def test_timeout_must_be_positive() -> None:
    """Tasks reject zero and negative timeout values at construction time."""
    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        Task(title="Example", payload="echo hi", type=TaskType.BASH, timeout=0)

    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        Task(title="Example", payload="echo hi", type=TaskType.BASH, timeout=-1)


def test_status_is_read_only() -> None:
    """External callers cannot bypass the state machine via status assignment."""
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    # Use dynamic setattr so the type checker accepts this runtime guard test.
    attr = "status"
    with pytest.raises(AttributeError):
        setattr(task, attr, TaskStatus.DONE)

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
