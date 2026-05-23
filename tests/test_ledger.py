"""Tests for TaskLedger ID consistency."""

from typing import Any

import pytest

from ledger import TaskLedger
from task import Task, TaskType


def test_ledger_rejects_non_task_values() -> None:
    """Only Task instances can be stored in the ledger."""
    ledger = TaskLedger()
    not_a_task: Any = "not a task"

    with pytest.raises(TypeError, match="Expected Task, got str"):
        ledger["id"] = not_a_task

    assert "id" not in ledger


def test_ledger_rejects_task_id_mismatch() -> None:
    """A task cannot be stored under an ID that differs from task.id."""
    ledger = TaskLedger()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    with pytest.raises(ValueError, match="task_id must match task.id"):
        ledger["wrong-id"] = task

    assert "wrong-id" not in ledger
    assert task.id not in ledger


def test_iterates_over_tasks_in_insertion_order() -> None:
    """Iterating a ledger yields the stored task objects in insertion order."""
    ledger = TaskLedger()
    first = Task(title="First", payload="echo 1", type=TaskType.BASH)
    second = Task(title="Second", payload="echo 2", type=TaskType.BASH)

    ledger[first.id] = first
    ledger[second.id] = second

    assert list(ledger) == [first, second]


def test_repr_includes_task_count() -> None:
    """The ledger repr summarizes the current number of stored tasks."""
    ledger = TaskLedger()

    assert repr(ledger) == "TaskLedger(0 tasks)"


def test_get_missing_task_raises_key_error() -> None:
    """Reading a missing task preserves normal dictionary KeyError behavior."""
    ledger = TaskLedger()

    with pytest.raises(KeyError):
        ledger["missing"]


def test_ledger_accepts_task_under_its_own_id() -> None:
    """A task stored under task.id is retrievable by that same ID."""
    ledger = TaskLedger()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    ledger[task.id] = task

    assert task.id in ledger
    assert ledger[task.id] is task
