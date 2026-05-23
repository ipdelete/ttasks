"""Tests for TaskGraph: mapping protocol, validation, execution, failure."""

import time

import pytest

from executor import default_executor
from ledger import TaskLedger
from task import Task, TaskStatus, TaskType
from workflow import TaskGraph


def _bash(title: str, payload: str) -> Task:
    """Shorthand for a bash task."""
    return Task(title=title, type=TaskType.BASH, payload=payload)


# ---- Mapping protocol --------------------------------------------------------


def test_setitem_registers_task_in_ledger() -> None:
    """Assigning a task into the graph also registers it in the ledger."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []
    assert a.id in graph.ledger
    assert graph.ledger[a.id] is a


def test_getitem_returns_dep_tasks() -> None:
    """graph[task] returns the list of upstream Task objects."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    assert graph[b] == [a]
    assert graph[a] == []


def test_contains_accepts_task_only() -> None:
    """`in` returns True only for registered Task instances."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []
    assert a in graph
    assert "not a task" not in graph
    assert 42 not in graph


def test_iter_yields_all_tasks() -> None:
    """Iterating the graph yields every registered Task."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    assert list(graph) == [a, b]


def test_len_counts_tasks() -> None:
    """len() reports the number of registered tasks."""
    graph = TaskGraph()
    assert len(graph) == 0
    a = _bash("A", "echo a")
    graph[a] = []
    assert len(graph) == 1


def test_repr_includes_edges() -> None:
    """repr() summarises node count and edges."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    rep = repr(graph)
    assert "TaskGraph(2 tasks" in rep
    assert "A->B" in rep


# ---- Ledger composition ------------------------------------------------------


def test_default_constructor_creates_own_ledger() -> None:
    """A graph constructed without a ledger gets a fresh empty one."""
    graph = TaskGraph()
    assert isinstance(graph.ledger, TaskLedger)
    assert len(graph.ledger) == 0


def test_constructor_uses_provided_ledger() -> None:
    """A ledger passed in is held by identity, not copied."""
    ledger = TaskLedger()
    graph = TaskGraph(ledger=ledger)
    assert graph.ledger is ledger


def test_ledger_can_be_pre_populated() -> None:
    """Tasks already in the ledger are not in the graph until assigned."""
    ledger = TaskLedger()
    a = _bash("A", "echo a")
    ledger[a.id] = a
    graph = TaskGraph(ledger=ledger)
    assert a not in graph
    assert len(graph) == 0
    assert a.id in ledger


# ---- Validation --------------------------------------------------------------


def test_run_raises_on_unregistered_dep() -> None:
    """Referencing a Task that was never assigned to the graph is a ValueError."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[b] = [a]  # a never registered
    with pytest.raises(ValueError, match="depends on unregistered"):
        graph.run(default_executor())


def test_run_raises_on_self_loop() -> None:
    """A self-loop is a cycle."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = [a]
    with pytest.raises(ValueError, match="cycle"):
        graph.run(default_executor())


def test_run_raises_on_two_node_cycle() -> None:
    """A → B → A is rejected."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = [b]
    graph[b] = [a]
    with pytest.raises(ValueError, match="cycle"):
        graph.run(default_executor())


def test_run_raises_on_larger_cycle() -> None:
    """A → C → B → A is rejected."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    graph = TaskGraph()
    graph[a] = [c]
    graph[b] = [a]
    graph[c] = [b]
    with pytest.raises(ValueError, match="cycle"):
        graph.run(default_executor())


# ---- Execution ---------------------------------------------------------------


def test_empty_graph_runs_without_hanging() -> None:
    """An empty graph returns immediately with no futures."""
    graph = TaskGraph()
    futures = graph.run(default_executor())
    assert futures == {}


def test_single_node_runs() -> None:
    """A graph with one root task executes it and reports success."""
    a = _bash("A", "echo hello")
    graph = TaskGraph()
    graph[a] = []
    futures = graph.run(default_executor())
    assert futures[a.id].result().output.strip() == "hello"
    assert a.status == TaskStatus.DONE


def test_linear_chain_runs_in_order() -> None:
    """A → B → C all complete successfully."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph[c] = [b]
    futures = graph.run(default_executor())
    for task in (a, b, c):
        assert futures[task.id].exception() is None
        assert task.status == TaskStatus.DONE


def test_diamond_runs_with_parallelism() -> None:
    """B and C must run in parallel: wall time well under serial."""
    a = _bash("A", "sleep 0.3")
    b = _bash("B", "sleep 0.3")
    c = _bash("C", "sleep 0.3")
    d = _bash("D", "sleep 0.3")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph[c] = [a]
    graph[d] = [b, c]
    start = time.monotonic()
    futures = graph.run(default_executor())
    elapsed = time.monotonic() - start
    # Serial would be ~1.2s; diamond parallel is ~0.9s. Allow generous slack.
    assert elapsed < 1.15, f"diamond took {elapsed:.2f}s — looks serial"
    assert all(futures[t.id].exception() is None for t in (a, b, c, d))


# ---- Failure policy ----------------------------------------------------------


def test_failure_blocks_descendants() -> None:
    """If A fails, B and C (downstream of A) never run."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph[c] = [b]
    futures = graph.run(default_executor())
    assert futures[a.id].exception() is not None
    assert b.id not in futures
    assert c.id not in futures
    assert a.status == TaskStatus.FAILED


def test_failure_does_not_affect_independent_branch() -> None:
    """A failing root does not block a parallel branch with no shared deps."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = []
    graph[c] = [b]
    futures = graph.run(default_executor())
    assert futures[a.id].exception() is not None
    assert futures[b.id].exception() is None
    assert futures[c.id].exception() is None


def test_failure_in_diamond_blocks_only_downstream() -> None:
    """A fails → B, C, D all blocked (every other node depends on A)."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    d = _bash("D", "echo d")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph[c] = [a]
    graph[d] = [b, c]
    futures = graph.run(default_executor())
    assert futures[a.id].exception() is not None
    assert b.id not in futures
    assert c.id not in futures
    assert d.id not in futures


def test_failure_terminates_run_without_hanging() -> None:
    """A failed task with descendants must not deadlock the run."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    start = time.monotonic()
    graph.run(default_executor())
    elapsed = time.monotonic() - start
    # Should finish almost immediately; allow for executor overhead.
    assert elapsed < 2.0, f"run hung for {elapsed:.2f}s after failure"
