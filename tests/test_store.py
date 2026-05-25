"""Tests for the unified Store and its in-memory backend."""

import pytest
from conftest import _bash, _opaque

from ttasks.store import (
    InMemoryGraphCollection,
    InMemoryStore,
    InMemoryTaskCollection,
    Store,
)
from ttasks.task import TaskStatus
from ttasks.workflow import TaskGraph

# ---- InMemoryTaskCollection -------------------------------------------------


class TestInMemoryTaskCollection:
    """Mapping semantics, validation, and save() for the in-memory tasks collection."""

    def test_setitem_then_getitem_returns_same_object(self) -> None:
        tasks = InMemoryTaskCollection()
        task = _bash()
        tasks[task.id] = task
        assert tasks[task.id] is task

    def test_save_persists_under_task_id(self) -> None:
        tasks = InMemoryTaskCollection()
        task = _bash()
        tasks.save(task)
        assert tasks[task.id] is task

    def test_iter_yields_task_ids_in_insertion_order(self) -> None:
        tasks = InMemoryTaskCollection()
        a, b = _bash("A"), _bash("B")
        tasks.save(a)
        tasks.save(b)
        assert list(tasks) == [a.id, b.id]

    def test_values_yields_task_objects(self) -> None:
        tasks = InMemoryTaskCollection()
        a, b = _bash("A"), _bash("B")
        tasks.save(a)
        tasks.save(b)
        assert list(tasks.values()) == [a, b]

    def test_contains_supports_id_and_task(self) -> None:
        tasks = InMemoryTaskCollection()
        task = _bash()
        tasks.save(task)
        assert task.id in tasks
        assert task in tasks
        assert "missing" not in tasks

    def test_len_reflects_stored_tasks(self) -> None:
        tasks = InMemoryTaskCollection()
        assert len(tasks) == 0
        tasks.save(_bash("A"))
        tasks.save(_bash("B"))
        assert len(tasks) == 2

    def test_missing_key_raises_key_error(self) -> None:
        with pytest.raises(KeyError):
            InMemoryTaskCollection()["missing"]

    def test_setitem_rejects_non_task(self) -> None:
        bogus = _opaque("not a task")
        with pytest.raises(TypeError):
            InMemoryTaskCollection()["x"] = bogus  # ty: ignore[invalid-assignment]

    def test_setitem_rejects_id_mismatch(self) -> None:
        tasks = InMemoryTaskCollection()
        task = _bash()
        with pytest.raises(ValueError):
            tasks["other-id"] = task

    def test_delitem_removes_task(self) -> None:
        tasks = InMemoryTaskCollection()
        task = _bash()
        tasks.save(task)
        del tasks[task.id]
        assert task.id not in tasks
        with pytest.raises(KeyError):
            del tasks[task.id]

    def test_cancel_updates_task_status_in_place(self) -> None:
        tasks = InMemoryTaskCollection()
        task = _bash()
        tasks.save(task)
        tasks.cancel(task.id)
        assert task.status == TaskStatus.CANCELLED
        assert tasks[task.id] is task


# ---- InMemoryGraphCollection ------------------------------------------------


class TestInMemoryGraphCollection:
    """Mapping semantics, validation, and save() for in-memory graphs."""

    def test_setitem_then_getitem_returns_same_graph(self) -> None:
        graphs = InMemoryGraphCollection()
        graph = TaskGraph(title="g")
        graphs[graph.id] = graph
        assert graphs[graph.id] is graph

    def test_save_persists_under_graph_id(self) -> None:
        graphs = InMemoryGraphCollection()
        graph = TaskGraph()
        graphs.save(graph)
        assert graph in graphs

    def test_setitem_rejects_non_graph(self) -> None:
        bogus = _opaque("not a graph")
        with pytest.raises(TypeError):
            InMemoryGraphCollection()["x"] = bogus  # ty: ignore[invalid-assignment]

    def test_setitem_rejects_id_mismatch(self) -> None:
        graphs = InMemoryGraphCollection()
        graph = TaskGraph()
        with pytest.raises(ValueError):
            graphs["other-id"] = graph

    def test_delitem_removes_graph(self) -> None:
        graphs = InMemoryGraphCollection()
        graph = TaskGraph()
        graphs.save(graph)
        del graphs[graph.id]
        assert graph.id not in graphs


# ---- InMemoryStore ----------------------------------------------------------


class TestInMemoryStore:
    """The store exposes tasks and graphs collections that interoperate."""

    def test_exposes_tasks_and_graphs(self) -> None:
        store = InMemoryStore()
        assert isinstance(store.tasks, InMemoryTaskCollection)
        assert isinstance(store.graphs, InMemoryGraphCollection)

    def test_tasks_and_graphs_are_independent(self) -> None:
        store = InMemoryStore()
        task = _bash()
        graph = TaskGraph()
        store.tasks.save(task)
        store.graphs.save(graph)
        assert task in store.tasks
        assert graph in store.graphs
        assert task.id not in store.graphs
        assert graph.id not in store.tasks

    def test_satisfies_store_protocol(self) -> None:
        """``InMemoryStore`` is a runtime-checkable :class:`Store`."""
        store = InMemoryStore()
        assert isinstance(store, Store)

    def test_collections_satisfy_protocols_structurally(self) -> None:
        """In-memory collections satisfy their protocol surfaces."""
        store = InMemoryStore()
        # Protocols here are not @runtime_checkable; check method presence.
        for name in ("save", "__getitem__", "__setitem__", "__iter__", "__len__"):
            assert callable(getattr(store.tasks, name))
            assert callable(getattr(store.graphs, name))
