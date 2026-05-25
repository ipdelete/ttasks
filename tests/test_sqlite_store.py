"""Tests for the SQLite-backed Store."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from conftest import _bash, _opaque

from ttasks import (
    SQLiteStore,
    TaskExecutor,
    TaskGraph,
    TaskResult,
    TaskStatus,
    TaskType,
)


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
        task._set_result(TaskResult(
            task_id=task.id,
            status=TaskStatus.SUCCEEDED,
            started_at=datetime(2024, 1, 1, 12, 0),
            finished_at=datetime(2024, 1, 1, 12, 0, 5),
            duration=5.0,
            output="ok",
            returncode=0,
        ))
        task.transition_to(TaskStatus.SUCCEEDED)
        store.tasks.save(task)
        loaded = store.tasks[task.id]
        assert loaded.status == TaskStatus.SUCCEEDED
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

    def test_memory_store_reuses_schema_across_operations(self) -> None:
        """SQLiteStore(':memory:') works across the collection's connections."""
        store = SQLiteStore(":memory:")
        task = _bash("Memory", "echo memory")

        store.tasks.save(task)

        assert len(store.tasks) == 1
        assert store.tasks[task.id].title == "Memory"

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


class TestSchemaVersionEnforcement:
    """Schema-version safety: refuse silent destructive migrations."""

    def test_schema_mismatch_raises_without_opt_in(
        self, store_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        SQLiteStore(store_path).tasks.save(_bash("doomed", "echo gone"))

        monkeypatch.setattr("ttasks._sqlite._SCHEMA_VERSION", "999")
        with pytest.raises(RuntimeError, match="schema_version"):
            SQLiteStore(store_path)

    def test_schema_mismatch_rebuilds_with_opt_in_and_warns(
        self,
        store_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        recwarn: pytest.WarningsRecorder,
    ) -> None:
        SQLiteStore(store_path).tasks.save(_bash("doomed", "echo gone"))

        monkeypatch.setattr("ttasks._sqlite._SCHEMA_VERSION", "999")
        rebuilt = SQLiteStore(store_path, allow_destructive_migration=True)

        assert len(rebuilt.tasks) == 0
        rebuilt.tasks.save(_bash("fresh", "echo new"))
        assert len(rebuilt.tasks) == 1
        assert any(
            issubclass(w.category, UserWarning)
            and "schema_version" in str(w.message)
            for w in recwarn.list
        )

    def test_schema_match_preserves_data(self, store_path: Path) -> None:
        task = _bash("kept", "echo keep")
        SQLiteStore(store_path).tasks.save(task)
        # Same version on re-open → no rebuild, prior row still there.
        assert SQLiteStore(store_path).tasks[task.id].title == "kept"

    def test_schema_version_row_updated_after_rebuild(
        self, store_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sqlite3

        SQLiteStore(store_path)
        monkeypatch.setattr("ttasks._sqlite._SCHEMA_VERSION", "999")
        SQLiteStore(store_path, allow_destructive_migration=True)
        with sqlite3.connect(store_path) as conn:
            row = conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
        assert row[0] == "999"

    def test_populated_database_without_metadata_row_raises(
        self, store_path: Path
    ) -> None:
        """A non-empty DB missing the schema_version row is treated as foreign."""
        import sqlite3

        with sqlite3.connect(store_path) as conn:
            conn.execute(
                "CREATE TABLE tasks (id TEXT PRIMARY KEY, payload TEXT)"
            )
            conn.execute(
                "INSERT INTO tasks (id, payload) VALUES ('x', 'y')"
            )

        with pytest.raises(RuntimeError, match="schema_version"):
            SQLiteStore(store_path)

    def test_fresh_empty_database_is_accepted(self, store_path: Path) -> None:
        """Brand-new path opens cleanly and stamps the current version."""
        store = SQLiteStore(store_path)
        assert len(store.tasks) == 0
        store.tasks.save(_bash("ok", "echo ok"))
        assert len(store.tasks) == 1


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
        assert store.tasks[root.id].status == TaskStatus.SUCCEEDED
        for leaf in leaves:
            loaded = store.tasks[leaf.id]
            assert loaded.status == TaskStatus.SUCCEEDED
            assert loaded.result is not None
            assert loaded.result.output.strip() == leaf.title


class TestGraphRunAutoSave:
    """TaskGraph.run(executor) auto-saves the graph when executor has a store."""

    def test_run_persists_graph_at_start_before_handlers_execute(
        self, store: SQLiteStore
    ) -> None:
        seen_at_handler: list[bool] = []

        def handler(_ctx: object) -> str:
            seen_at_handler.append(graph.id in store.graphs)
            return "ok"

        executor = TaskExecutor.empty(store=store)
        executor.register(TaskType.BASH, handler)
        task = _bash("probe", "echo probe")
        graph = TaskGraph(title="autosave-start")
        graph[task] = []

        assert graph.id not in store.graphs
        graph.run(executor)
        assert seen_at_handler == [True]
        assert graph.id in store.graphs

    def test_run_persists_graph_at_end_reflects_final_statuses(
        self, store: SQLiteStore
    ) -> None:
        a = _bash("a", "echo a")
        b = _bash("b", "echo b")
        graph = TaskGraph(title="autosave-end")
        graph[a] = []
        graph[b] = [a]

        graph.run(TaskExecutor(store=store))
        reloaded = store.graphs[graph.id]
        assert {t.status for t in reloaded} == {TaskStatus.SUCCEEDED}

    def test_run_without_store_is_noop_for_persistence(self) -> None:
        # No store, no crash. Smoke check.
        task = _bash("nostore", "echo x")
        graph = TaskGraph(title="nostore")
        graph[task] = []
        graph.run(TaskExecutor())
        assert task.status == TaskStatus.SUCCEEDED

    def test_explicit_save_then_run_is_idempotent(
        self, store: SQLiteStore
    ) -> None:
        task = _bash("idempotent", "echo idem")
        graph = TaskGraph(title="idem")
        graph[task] = []
        store.graphs.save(graph)
        graph.run(TaskExecutor(store=store))
        # Still exactly one graph row, still all tasks SUCCEEDED.
        assert len(store.graphs) == 1
        assert store.graphs[graph.id].title == "idem"
        assert store.tasks[task.id].status == TaskStatus.SUCCEEDED

    def test_run_does_not_persist_invalid_graph(
        self, store: SQLiteStore
    ) -> None:
        # Build a graph that fails validation: task with dep on unknown id.
        task = _bash("orphan", "echo o")
        graph = TaskGraph(title="invalid")
        # Insert raw to bypass __setitem__ checks, simulating a corrupted graph.
        # Simpler: introduce a cycle via __setitem__.
        a = _bash("a", "echo a")
        b = _bash("b", "echo b")
        graph[a] = [b]
        graph[b] = [a]

        with pytest.raises(ValueError, match="cycle"):
            graph.run(TaskExecutor(store=store))
        assert graph.id not in store.graphs
        # The probe task we set up doesn't end up in the store either.
        assert task.id not in store.tasks


class TestTerminationReasonRoundtrip:
    """termination_reason persists through SQLite."""

    @pytest.mark.parametrize(
        "reason",
        [None, "exit_code", "timeout", "cancelled", "handler"],
    )
    def test_termination_reason_roundtrips(
        self, store: SQLiteStore, reason: Any
    ) -> None:
        task = _bash("t", "echo t")
        if reason is not None:
            task.transition_to(TaskStatus.RUNNING)
            task.transition_to(TaskStatus.FAILED, error="x")
        else:
            task.transition_to(TaskStatus.RUNNING)
            task.transition_to(TaskStatus.SUCCEEDED)
        result = TaskResult(
            task_id=task.id,
            status=TaskStatus.SUCCEEDED if reason is None else TaskStatus.FAILED,
            started_at=datetime.now(),
            finished_at=datetime.now(),
            duration=0.01,
            termination_reason=reason,
        )
        task._set_result(result)
        store.tasks.save(task)
        loaded = store.tasks[task.id]
        assert loaded.result is not None
        assert loaded.result.termination_reason == reason
