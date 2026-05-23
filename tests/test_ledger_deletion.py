"""Tests for TaskLedger deletion and cancellation semantics."""

import pytest

from ttasks.ledger import TaskLedger
from ttasks.task import Task, TaskStatus, TaskType


def test_del_removes_task_from_ledger() -> None:
    """Deleting from the ledger removes the task entirely."""
    ledger = TaskLedger()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    ledger[task.id] = task

    del ledger[task.id]

    assert task.id not in ledger
    assert len(ledger) == 0


def test_delete_missing_task_raises_key_error() -> None:
    """Deleting a missing task preserves normal dictionary KeyError behavior."""
    ledger = TaskLedger()

    with pytest.raises(KeyError):
        del ledger["missing"]


def test_cancel_missing_task_raises_key_error() -> None:
    """Cancelling a missing task raises KeyError."""
    ledger = TaskLedger()

    with pytest.raises(KeyError):
        ledger.cancel("missing")


def test_cancel_marks_task_cancelled_and_keeps_it_in_ledger() -> None:
    """Cancelling preserves the task record while changing its status."""
    ledger = TaskLedger()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    ledger[task.id] = task

    ledger.cancel(task.id)

    assert task.id in ledger
    assert len(ledger) == 1
    assert ledger[task.id] is task
    assert ledger[task.id].status == TaskStatus.CANCELLED
