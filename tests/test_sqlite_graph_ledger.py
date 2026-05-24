"""Tests for SQLiteGraphLedger durable graph persistence."""

from pathlib import Path
from typing import Any

import pytest

from ttasks import TaskGraph
from ttasks.storage.sqlite import SQLiteGraphLedger, SQLiteTaskLedger
from ttasks.task import Task, TaskType


def make_ledgers(tmp_path: Path) -> tuple[SQLiteTaskLedger, SQLiteGraphLedger]:
    """Return task and graph ledgers backed by one temporary database."""
    path = tmp_path / "ttasks.db"
    tasks = SQLiteTaskLedger(path)
    graphs = SQLiteGraphLedger(path, tasks=tasks)
    return tasks, graphs


def test_saves_and_loads_empty_graph(tmp_path: Path) -> None:
    """Assigning a graph to the ledger durably saves its metadata."""
    tasks, graphs = make_ledgers(tmp_path)
    graph = TaskGraph(ledger=tasks, title="empty")

    graphs[graph.id] = graph
    restored = graphs[graph.id]

    assert restored is not graph
    assert restored.id == graph.id
    assert restored.title == "empty"
    assert restored.created_at == graph.created_at
    assert restored.ledger is tasks
    assert len(restored) == 0


def test_saves_and_loads_graph_tasks_and_dependencies(tmp_path: Path) -> None:
    """Graph task membership and dependency edges round-trip through SQLite."""
    tasks, graphs = make_ledgers(tmp_path)
    build = Task(title="Build", payload="echo build", type=TaskType.BASH)
    test = Task(title="Test", payload="echo test", type=TaskType.BASH)
    package = Task(title="Package", payload="echo package", type=TaskType.BASH)
    graph = TaskGraph(ledger=tasks, title="pipeline")
    graph[build] = []
    graph[test] = [build]
    graph[package] = [build, test]

    graphs[graph.id] = graph
    restored = graphs[graph.id]
    restored_tasks = list(restored)
    restored_by_id = {task.id: task for task in restored_tasks}

    assert [task.id for task in restored_tasks] == [build.id, test.id, package.id]
    assert restored[restored_by_id[build.id]] == []
    assert [task.id for task in restored[restored_by_id[test.id]]] == [build.id]
    assert [task.id for task in restored[restored_by_id[package.id]]] == [
        build.id,
        test.id,
    ]


def test_saving_graph_persists_member_tasks(tmp_path: Path) -> None:
    """Saving a graph also saves every task referenced by the graph."""
    tasks, graphs = make_ledgers(tmp_path)
    task = Task(title="Build", payload="echo build", type=TaskType.BASH)
    graph = TaskGraph(ledger=tasks, title="pipeline")
    graph[task] = []

    graphs[graph.id] = graph

    assert task.id in tasks
    assert tasks[task.id].title == "Build"


def test_persists_graphs_across_ledger_instances(tmp_path: Path) -> None:
    """Graphs saved by one ledger instance can be loaded by another."""
    path = tmp_path / "ttasks.db"
    tasks = SQLiteTaskLedger(path)
    graphs = SQLiteGraphLedger(path, tasks=tasks)
    task = Task(title="Build", payload="echo build", type=TaskType.BASH)
    graph = TaskGraph(ledger=tasks, title="pipeline")
    graph[task] = []

    graphs[graph.id] = graph

    restored_tasks = SQLiteTaskLedger(path)
    restored_graphs = SQLiteGraphLedger(path, tasks=restored_tasks)
    restored = restored_graphs[graph.id]
    assert restored.id == graph.id
    assert [task.title for task in restored] == ["Build"]


def test_save_alias_persists_updated_graph_snapshot(tmp_path: Path) -> None:
    """save(graph) is an explicit alias for assigning graph by its own ID."""
    _, graphs = make_ledgers(tmp_path)
    graph = TaskGraph(title="old")
    graphs[graph.id] = graph

    graph.title = "new"
    graphs.save(graph)

    assert graphs[graph.id].title == "new"


def test_rejects_non_graph_values(tmp_path: Path) -> None:
    """Only TaskGraph instances can be stored in the ledger."""
    _, graphs = make_ledgers(tmp_path)
    not_a_graph: Any = "not a graph"

    with pytest.raises(TypeError, match="Expected TaskGraph, got str"):
        graphs["id"] = not_a_graph

    assert "id" not in graphs


def test_rejects_graph_id_mismatch(tmp_path: Path) -> None:
    """A graph cannot be stored under an ID that differs from graph.id."""
    _, graphs = make_ledgers(tmp_path)
    graph = TaskGraph(title="pipeline")

    with pytest.raises(ValueError, match="graph_id must match graph.id"):
        graphs["wrong-id"] = graph

    assert "wrong-id" not in graphs
    assert graph.id not in graphs


def test_missing_graph_operations_raise_key_error(tmp_path: Path) -> None:
    """Missing reads and deletes preserve dictionary KeyError behavior."""
    _, graphs = make_ledgers(tmp_path)

    with pytest.raises(KeyError):
        graphs["missing"]

    with pytest.raises(KeyError):
        del graphs["missing"]


def test_contains_len_iter_and_repr(tmp_path: Path) -> None:
    """The SQLite graph ledger preserves mapping conveniences."""
    _, graphs = make_ledgers(tmp_path)
    first = TaskGraph(title="first")
    second = TaskGraph(title="second")

    graphs[first.id] = first
    graphs[second.id] = second

    assert first.id in graphs
    assert second.id in graphs
    assert "missing" not in graphs
    assert len(graphs) == 2
    assert [graph.id for graph in graphs] == [first.id, second.id]
    assert repr(graphs) == "SQLiteGraphLedger(2 graphs)"


def test_delete_graph_keeps_shared_tasks(tmp_path: Path) -> None:
    """Deleting a graph removes graph metadata without deleting its tasks."""
    tasks, graphs = make_ledgers(tmp_path)
    task = Task(title="Build", payload="echo build", type=TaskType.BASH)
    graph = TaskGraph(ledger=tasks, title="pipeline")
    graph[task] = []
    graphs[graph.id] = graph

    del graphs[graph.id]

    assert graph.id not in graphs
    assert task.id in tasks
    with pytest.raises(KeyError):
        graphs[graph.id]


def test_finally_and_optional_task_metadata_round_trips(tmp_path: Path) -> None:
    """Finally-task flags and optional-task flags are persisted."""
    tasks, graphs = make_ledgers(tmp_path)
    build = Task(title="Build", payload="echo build", type=TaskType.BASH)
    report = Task(title="Report", payload="echo report", type=TaskType.BASH)
    graph = TaskGraph(ledger=tasks, title="pipeline")
    graph[build] = []
    graph.add_finally(report, after=[build], required=False)

    graphs[graph.id] = graph
    restored = graphs[graph.id]

    assert restored._finally == {report.id}
    assert restored._optional == {report.id}
