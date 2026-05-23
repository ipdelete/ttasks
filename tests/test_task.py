import pytest

from task import Task, TaskStatus, TaskType


def test_timeout_must_be_positive() -> None:
    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        Task(title="Example", payload="echo hi", type=TaskType.BASH, timeout=0)

    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        Task(title="Example", payload="echo hi", type=TaskType.BASH, timeout=-1)


def test_status_is_read_only() -> None:
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    attr = "status"
    with pytest.raises(AttributeError):
        setattr(task, attr, TaskStatus.DONE)

    assert task.status == TaskStatus.PENDING


def test_status_changes_through_transition_to() -> None:
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.DONE)

    assert task.status == TaskStatus.DONE
    assert task.error is None


def test_cancel_changes_status_through_state_machine() -> None:
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    task.cancel()

    assert task.status == TaskStatus.CANCELLED


def test_invalid_transition_is_rejected() -> None:
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    task.transition_to(TaskStatus.CANCELLED)

    with pytest.raises(
        ValueError, match="Cannot transition task from 'cancelled' to 'done'"
    ):
        task.transition_to(TaskStatus.DONE)

    assert task.status == TaskStatus.CANCELLED
