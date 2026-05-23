"""Tests for TaskLedger ID consistency."""

import pytest

from ledger import TaskLedger
from task import Task, TaskType


def test_ledger_rejects_task_id_mismatch() -> None:
    """A task cannot be stored under an ID that differs from task.id."""
    ledger = TaskLedger()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    with pytest.raises(ValueError, match="task_id must match task.id"):
        ledger["wrong-id"] = task

    assert "wrong-id" not in ledger
    assert task.id not in ledger


def test_ledger_accepts_task_under_its_own_id() -> None:
    """A task stored under task.id is retrievable by that same ID."""
    ledger = TaskLedger()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    ledger[task.id] = task

    assert task.id in ledger
    assert ledger[task.id] is task
