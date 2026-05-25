"""Tests for the SQLite-backed Store."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import _bash, _opaque

from ttasks.executor import TaskExecutor
from ttasks.storage.sqlite import SQLiteStore
from ttasks.task import TaskResult, TaskStatus, TaskType
from ttasks.workflow import TaskGraph


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    """Return a fresh SQLite database path inside ``tmp_path``."""
    return tmp_path / "store.db"


@pytest.fixture()
def store(store_path: Path) -> SQLiteStore:
    """Return a fresh :class:`SQLiteStore` rooted at ``store_path``."""
    return SQLiteStore(store_path)


# ---- tasks collection -------------------------------------------------------


class TestSQLiteTaskCollection:
    """Round-trips and snapshot semantics for SQLite-backed tasks."""

    def test_save_and_load_roundtrip(self, store: SQLiteStore) -> None:
        task = _bash("Hello", "echo hi")
        store.tasks.save(task)
        loaded = store.tasks[task.id]
        assert loaded.id == task.id
        assert loaded.title == task.title
        assert loaded.payload == task.payload
        assert loaded.type == TaskType.BASH
        assert loaded.status == TaskStatus.PENDING

    def test_load_returns_detached_snapshot(self, store: SQLiteStore) -> None:
        """Mutating the loaded task does not write back to the store."""
        task = _bash()
        store.tasks.save(task)
        loaded = store.tasks[task.id]
        assert loaded is not task
        loaded.title = "different"
        again = store.tasks[task.id]
        assert again.title == task.title

    def test_task_result_roundtrips(self, store: SQLiteStore) -> None:
        from datetime import datetime

        task = _bash()
        task.transition_to(TaskStatus.RUNNING)
        task.result = TaskResult(
            task_id=task.id,
            status=TaskStatus.DONE,
            started_at=datetime(2024, 1, 1, 12, 0),
            finished_at=datetime(2024, 1, 1, 12, 0, 5),
            duration=5.0,
            output="ok",
            returncode=0,
        )
        task.transition_to(TaskStatus.DONE)
        store.tasks.save(task)
        loaded = store.tasks[task.id]
        assert loaded.status == TaskStatus.DONE
        assert loaded.result is not None
        assert loaded.result.output == "ok"
        assert loaded.result.returncode == 0

    def test_missing_task_raises_key_error(self, store: SQLiteStore) -> None:
        with pytest.raises(KeyError):
            store.tasks["missing"]

    def test_setitem_rejects_id_mismatch(self, store: SQLiteStore) -> None:
        with pytest.raises(ValueError):
            store.tasks["other-id"] = _bash()

    def test_setitem_rejects_non_task(self, store: SQLiteStore) -> None:
        bogus = _opaque("not a task")
        with pytest.raises(TypeError):
            store.tasks["x"] = bogus  # ty: ignore[invalid-assignment]

    def test_iter_yields_persisted_ids(self, store: SQLiteStore) -> None:
        a, b = _bash("A"), _bash("B")
        store.tasks.save(a)
        store.tasks.save(b)
        assert set(store.tasks) == {a.id, b.id}

    def test_contains_supports_id_and_task(self, store: SQLiteStore) -> None:
        task = _bash()
        store.tasks.save(task)
        assert task.id in store.tasks
        assert task in store.tasks
        assert "missing" not in store.tasks

    def test_delitem_removes_task(self, store: SQLiteStore) -> None:
        task = _bash()
        store.tasks.save(task)
        del store.tasks[task.id]
        assert task.id not in store.tasks

    def test_persists_across_store_instances(self, store_path: Path) -> None:
        task = _bash()
        SQLiteStore(store_path).tasks.save(task)
        assert SQLiteStore(store_path).tasks[task.id].title == task.title

    def test_delitem_missing_task_raises_key_error(self, store: SQLiteStore) -> None:
        with pytest.raises(KeyError):
            del store.tasks["missing"]

    def test_len_and_non_string_contains(self, store: SQLiteStore) -> None:
        assert len(store.tasks) == 0
        store.tasks.save(_bash("A"))
        store.tasks.save(_bash("B"))
        assert len(store.tasks) == 2
        # Non-Task, non-str keys cannot match a primary key.
        assert 123 not in store.tasks

    def test_cancel_persists_cancelled_status(self, store: SQLiteStore) -> None:
        task = _bash()
        store.tasks.save(task)
        store.tasks.cancel(task.id)
        assert store.tasks[task.id].status == TaskStatus.CANCELLED


# ---- graphs collection ------------------------------------------------------


class TestSQLiteGraphCollection:
    """Topology, finally metadata, and atomic save behavior for graphs."""

    def test_save_persists_graph_and_member_tasks_atomically(
        self, store: SQLiteStore
    ) -> None:
        a, b = _bash("A"), _bash("B")
        graph = TaskGraph(title="pipeline")
        graph[a] = []
        graph[b] = [a]
        store.graphs.save(graph)

        # Member tasks were persisted as part of the graph save.
        assert a.id in store.tasks
        assert b.id in store.tasks
        assert graph.id in store.graphs

    def test_graph_topology_roundtrips(self, store: SQLiteStore) -> None:
        a, b, c = _bash("A"), _bash("B"), _bash("C")
        graph = TaskGraph(title="diamond")
        graph[a] = []
        graph[b] = [a]
        graph[c] = [a, b]
        store.graphs.save(graph)

        loaded = store.graphs[graph.id]
        loaded_by_id = {t.id: t for t in loaded}
        assert set(loaded_by_id) == {a.id, b.id, c.id}
        assert [d.id for d in loaded.dependencies(loaded_by_id[c.id])] == [a.id, b.id]
        assert loaded.dependencies(loaded_by_id[a.id]) == []

    def test_finally_metadata_roundtrips(self, store: SQLiteStore) -> None:
        main = _bash("main")
        required_cleanup = _bash("required-cleanup")
        optional_cleanup = _bash("optional-cleanup")
        graph = TaskGraph()
        graph[main] = []
        graph.add(required_cleanup, after=[main], finally_=True)
        graph.add(optional_cleanup, after=[main], finally_=True, required=False)
        store.graphs.save(graph)

        loaded = store.graphs[graph.id]
        by_id = {t.id: t for t in loaded}
        assert not loaded.is_finally(by_id[main.id])
        assert loaded.is_finally(by_id[required_cleanup.id])
        assert loaded.is_finally(by_id[optional_cleanup.id])
        assert not loaded.is_optional(by_id[required_cleanup.id])
        assert loaded.is_optional(by_id[optional_cleanup.id])

    def test_loaded_graph_holds_detached_tasks(self, store: SQLiteStore) -> None:
        a = _bash("A")
        graph = TaskGraph()
        graph[a] = []
        store.graphs.save(graph)

        loaded = store.graphs[graph.id]
        (loaded_task,) = list(loaded)
        assert loaded_task is not a
        assert loaded_task.id == a.id

    def test_delete_graph_keeps_member_tasks(self, store: SQLiteStore) -> None:
        a = _bash("A")
        graph = TaskGraph()
        graph[a] = []
        store.graphs.save(graph)
        del store.graphs[graph.id]
        assert graph.id not in store.graphs
        # Member task survives so other graphs/queries can still reach it.
        assert a.id in store.tasks

    def test_setitem_rejects_id_mismatch(self, store: SQLiteStore) -> None:
        with pytest.raises(ValueError):
            store.graphs["other-id"] = TaskGraph()

    def test_setitem_rejects_non_graph(self, store: SQLiteStore) -> None:
        bogus = _opaque("not a graph")
        with pytest.raises(TypeError):
            store.graphs["x"] = bogus  # ty: ignore[invalid-assignment]

    def test_missing_graph_raises_key_error(self, store: SQLiteStore) -> None:
        with pytest.raises(KeyError):
            store.graphs["missing"]

    def test_persists_across_store_instances(self, store_path: Path) -> None:
        a = _bash("A")
        graph = TaskGraph(title="persist")
        graph[a] = []
        SQLiteStore(store_path).graphs.save(graph)

        loaded = SQLiteStore(store_path).graphs[graph.id]
        assert loaded.title == "persist"
        assert list(loaded)[0].id == a.id

    def test_delitem_missing_graph_raises_key_error(self, store: SQLiteStore) -> None:
        with pytest.raises(KeyError):
            del store.graphs["missing"]

    def test_iter_len_and_contains_variants(self, store: SQLiteStore) -> None:
        assert len(store.graphs) == 0
        assert list(iter(store.graphs)) == []
        g1, g2 = TaskGraph(title="one"), TaskGraph(title="two")
        store.graphs.save(g1)
        store.graphs.save(g2)
        assert set(iter(store.graphs)) == {g1.id, g2.id}
        assert len(store.graphs) == 2
        # Membership accepts a TaskGraph instance and rejects non-str keys.
        assert g1 in store.graphs
        assert 123 not in store.graphs


def test_sqlite_store_repr_includes_database_path(store_path: Path) -> None:
    """repr is a quick way to surface the underlying database file."""
    store = SQLiteStore(store_path)
    assert repr(store) == f"SQLiteStore({store_path!s})"


# ---- end-to-end with TaskGraph.run + auto-save ------------------------------


class TestSQLiteStoreUnderTaskGraphRun:
    """Concurrent saves from TaskGraph.run via auto-persistence."""

    def test_wide_dag_persists_all_tasks_via_executor_auto_save(
        self, store: SQLiteStore
    ) -> None:
        """A wide DAG run with auto-persist writes every task durably."""
        root = _bash("root", "echo root")
        leaves = [_bash(f"leaf{i}", f"echo leaf{i}") for i in range(8)]
        graph = TaskGraph(title="wide")
        graph[root] = []
        for leaf in leaves:
            graph[leaf] = [root]
        store.graphs.save(graph)

        executor = TaskExecutor(store=store)
        graph.run(executor, max_workers=4)

        assert graph.ok
        assert not executor.persistence_errors
        # Every task's terminal status is queryable from the store.
        assert store.tasks[root.id].status == TaskStatus.DONE
        for leaf in leaves:
            loaded = store.tasks[leaf.id]
            assert loaded.status == TaskStatus.DONE
            assert loaded.result is not None
            assert loaded.result.output.strip() == leaf.title
