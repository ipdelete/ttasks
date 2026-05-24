"""Tests for SQLiteTaskLedger durable task persistence."""

from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from ttasks.storage.sqlite import SQLiteTaskLedger
from ttasks.task import Task, TaskResult, TaskStatus, TaskType


def make_ledger(tmp_path: Path) -> SQLiteTaskLedger:
    """Return a ledger backed by a temporary database path."""
    return SQLiteTaskLedger(tmp_path / "ttasks.db")


def test_saves_and_loads_pending_task(tmp_path: Path) -> None:
    """Assigning a task to the ledger durably saves its current snapshot."""
    ledger = make_ledger(tmp_path)
    task = Task(
        title="Example",
        description="Demo task",
        payload="echo hi",
        type=TaskType.BASH,
        timeout=2.5,
    )

    ledger[task.id] = task
    restored = ledger[task.id]

    assert restored is not task
    assert restored.id == task.id
    assert restored.title == "Example"
    assert restored.description == "Demo task"
    assert restored.payload == "echo hi"
    assert restored.type == TaskType.BASH
    assert restored.timeout == 2.5
    assert restored.created_at == task.created_at
    assert restored.status == TaskStatus.PENDING
    assert restored.error is None
    assert restored.result is None


def test_persists_tasks_across_ledger_instances(tmp_path: Path) -> None:
    """Tasks saved by one ledger instance can be loaded by another."""
    path = tmp_path / "ttasks.db"
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    SQLiteTaskLedger(path)[task.id] = task

    restored = SQLiteTaskLedger(path)[task.id]
    assert restored.id == task.id
    assert restored.title == "Example"


def test_save_alias_persists_updated_task_snapshot(tmp_path: Path) -> None:
    """save(task) is an explicit alias for assigning task by its own ID."""
    ledger = make_ledger(tmp_path)
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    ledger[task.id] = task

    task.description = "changed"
    ledger.save(task)

    assert ledger[task.id].description == "changed"


def test_mutating_task_after_assignment_does_not_write_through(tmp_path: Path) -> None:
    """Saved tasks are snapshots; later object mutation requires another save."""
    ledger = make_ledger(tmp_path)
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    ledger[task.id] = task
    task.description = "not saved"

    assert ledger[task.id].description == ""


def test_rejects_non_task_values(tmp_path: Path) -> None:
    """Only Task instances can be stored in the ledger."""
    ledger = make_ledger(tmp_path)
    not_a_task: Any = "not a task"

    with pytest.raises(TypeError, match="Expected Task, got str"):
        ledger["id"] = not_a_task

    assert "id" not in ledger


def test_rejects_task_id_mismatch(tmp_path: Path) -> None:
    """A task cannot be stored under an ID that differs from task.id."""
    ledger = make_ledger(tmp_path)
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    with pytest.raises(ValueError, match="task_id must match task.id"):
        ledger["wrong-id"] = task

    assert "wrong-id" not in ledger
    assert task.id not in ledger


def test_missing_task_operations_raise_key_error(tmp_path: Path) -> None:
    """Missing reads and deletes preserve dictionary KeyError behavior."""
    ledger = make_ledger(tmp_path)

    with pytest.raises(KeyError):
        ledger["missing"]

    with pytest.raises(KeyError):
        del ledger["missing"]


def test_contains_len_iter_and_repr(tmp_path: Path) -> None:
    """The SQLite ledger preserves the in-memory ledger mapping conveniences."""
    ledger = make_ledger(tmp_path)
    first = Task(title="First", payload="echo 1", type=TaskType.BASH)
    second = Task(title="Second", payload="echo 2", type=TaskType.BASH)

    ledger[first.id] = first
    ledger[second.id] = second

    assert first.id in ledger
    assert second.id in ledger
    assert "missing" not in ledger
    assert len(ledger) == 2
    assert [task.id for task in ledger] == [first.id, second.id]
    assert repr(ledger) == "SQLiteTaskLedger(2 tasks)"


def test_delete_removes_task_and_result(tmp_path: Path) -> None:
    """Deleting a task removes its persisted result as well."""
    ledger = make_ledger(tmp_path)
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    started_at = datetime.now()
    finished_at = datetime.now()
    result = TaskResult(
        task_id=task.id,
        status=TaskStatus.DONE,
        started_at=started_at,
        finished_at=finished_at,
        duration=0.1,
        output="hi\n",
        raw="hi\n",
    )
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.DONE)
    object.__setattr__(task, "result", result)
    ledger[task.id] = task

    del ledger[task.id]

    assert task.id not in ledger
    assert len(ledger) == 0
    with pytest.raises(KeyError):
        ledger[task.id]


def test_saves_and_loads_terminal_task_result_without_raw(tmp_path: Path) -> None:
    """TaskResult fields are persisted except for arbitrary raw objects."""
    ledger = make_ledger(tmp_path)
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    started_at = datetime.now()
    finished_at = datetime.now()
    raw = object()
    result = TaskResult(
        task_id=task.id,
        status=TaskStatus.DONE,
        started_at=started_at,
        finished_at=finished_at,
        duration=0.25,
        output="hi\n",
        error=None,
        returncode=0,
        raw=raw,
    )
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.DONE)
    object.__setattr__(task, "result", result)

    ledger[task.id] = task
    restored = ledger[task.id]

    assert restored.status == TaskStatus.DONE
    assert restored.result is not None
    assert restored.result.task_id == task.id
    assert restored.result.status == TaskStatus.DONE
    assert restored.result.started_at == started_at
    assert restored.result.finished_at == finished_at
    assert restored.result.duration == 0.25
    assert restored.result.output == "hi\n"
    assert restored.result.error is None
    assert restored.result.returncode == 0
    assert restored.result.raw is None


def test_saving_task_without_result_removes_stale_result(tmp_path: Path) -> None:
    """Saving a task with result=None clears any previously saved result row."""
    ledger = make_ledger(tmp_path)
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    result = TaskResult(
        task_id=task.id,
        status=TaskStatus.FAILED,
        started_at=datetime.now(),
        finished_at=datetime.now(),
        duration=0.1,
        error="boom",
        returncode=1,
    )
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.FAILED, error="boom")
    task.result = result
    ledger[task.id] = task

    object.__setattr__(task, "result", None)
    ledger[task.id] = task

    assert ledger[task.id].result is None


def test_cancel_persists_cancelled_state(tmp_path: Path) -> None:
    """cancel(task_id) updates and saves the requested task."""
    ledger = make_ledger(tmp_path)
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    ledger[task.id] = task

    ledger.cancel(task.id)

    assert ledger[task.id].status == TaskStatus.CANCELLED
