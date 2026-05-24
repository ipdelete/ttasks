"""Tests for TaskGraph: mapping protocol, validation, execution, failure."""

import time

import pytest

from ttasks.executor import make_default_executor
from ttasks.ledger import TaskLedger
from ttasks.task import Task, TaskStatus, TaskType
from ttasks.workflow import TaskGraph


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
        graph.run(make_default_executor())


def test_run_raises_on_self_loop() -> None:
    """A self-loop is a cycle."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = [a]
    with pytest.raises(ValueError, match="cycle"):
        graph.run(make_default_executor())


def test_run_raises_on_two_node_cycle() -> None:
    """A → B → A is rejected."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = [b]
    graph[b] = [a]
    with pytest.raises(ValueError, match="cycle"):
        graph.run(make_default_executor())


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
        graph.run(make_default_executor())


# ---- Execution ---------------------------------------------------------------


def test_empty_graph_runs_without_hanging() -> None:
    """An empty graph completes immediately without deadlocking."""
    graph = TaskGraph()
    assert graph.run(make_default_executor()) is graph
    assert graph.ok


def test_single_node_runs() -> None:
    """A graph with one root task executes it and reports success."""
    a = _bash("A", "echo hello")
    graph = TaskGraph()
    graph[a] = []
    graph.run(make_default_executor())
    assert a.status == TaskStatus.DONE
    assert a.result is not None
    assert a.result.output.strip() == "hello"


def test_linear_chain_runs_in_order() -> None:
    """A → B → C all complete successfully."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph[c] = [b]
    graph.run(make_default_executor())
    assert graph.ok
    for task in (a, b, c):
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
    graph.run(make_default_executor())
    elapsed = time.monotonic() - start
    # Serial would be ~1.2s; diamond parallel is ~0.9s. Allow generous slack.
    assert elapsed < 1.15, f"diamond took {elapsed:.2f}s — looks serial"
    assert graph.ok


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
    graph.run(make_default_executor())
    assert a.status == TaskStatus.FAILED
    assert graph.failed == [a]
    assert {t.id for t in graph.blocked} == {b.id, c.id}
    assert b.status == TaskStatus.PENDING
    assert c.status == TaskStatus.PENDING


def test_failure_does_not_affect_independent_branch() -> None:
    """A failing root does not block a parallel branch with no shared deps."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = []
    graph[c] = [b]
    graph.run(make_default_executor())
    assert graph.failed == [a]
    assert {t.id for t in graph.succeeded} == {b.id, c.id}
    assert graph.blocked == []


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
    graph.run(make_default_executor())
    assert graph.failed == [a]
    assert {t.id for t in graph.blocked} == {b.id, c.id, d.id}


def test_failure_terminates_run_without_hanging() -> None:
    """A failed task with descendants must not deadlock the run."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    start = time.monotonic()
    graph.run(make_default_executor())
    elapsed = time.monotonic() - start
    # Should finish almost immediately; allow for executor overhead.
    assert elapsed < 2.0, f"run hung for {elapsed:.2f}s after failure"


# ---- ledger as post-run view -------------------------------------------------


def test_ledger_carries_results_after_run() -> None:
    """After graph.run(), every executed task in the ledger has task.result set."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(make_default_executor())

    for task in graph.ledger:
        assert task.result is not None
        assert task.result.status == TaskStatus.DONE
        assert task.result.output.strip() == task.title.lower()


def test_blocked_task_has_no_result_after_run() -> None:
    """A task whose deps failed never runs, so task.result stays None."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(make_default_executor())

    assert a.result is not None
    assert a.result.status == TaskStatus.FAILED
    assert b.result is None  # never executed
    assert b.status == TaskStatus.PENDING


# ---- run() returns self (chaining) -------------------------------------------


def test_run_returns_self() -> None:
    """graph.run() returns the graph itself so callers can chain."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []
    assert graph.run(make_default_executor()) is graph


def test_run_returns_self_for_empty_graph() -> None:
    """Empty graph still returns self (not None, not {})."""
    graph = TaskGraph()
    assert graph.run(make_default_executor()) is graph


# ---- status views: succeeded / failed / blocked / ok -------------------------


def test_succeeded_lists_done_tasks_in_graph() -> None:
    """After a clean run every task in the graph is in succeeded."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(make_default_executor())
    assert {t.id for t in graph.succeeded} == {a.id, b.id}


def test_succeeded_empty_before_run() -> None:
    """Before any run, no task has DONE status, so succeeded is empty."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []
    assert graph.succeeded == []


def test_succeeded_only_lists_graph_tasks_not_whole_ledger() -> None:
    """A task in the shared ledger but not in this graph is not in succeeded."""
    ledger = TaskLedger()
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    g1 = TaskGraph(ledger=ledger)
    g2 = TaskGraph(ledger=ledger)
    g1[a] = []
    g2[b] = []
    g1.run(make_default_executor())
    g2.run(make_default_executor())
    assert g1.succeeded == [a]
    assert g2.succeeded == [b]


def test_failed_lists_failed_tasks() -> None:
    """A task that raised lands in graph.failed."""
    a = _bash("A", "exit 1")
    graph = TaskGraph()
    graph[a] = []
    graph.run(make_default_executor())
    assert graph.failed == [a]


def test_failed_empty_when_all_succeed() -> None:
    """A clean run leaves failed empty."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []
    graph.run(make_default_executor())
    assert graph.failed == []


def test_blocked_lists_skipped_descendants() -> None:
    """Tasks skipped because their upstream failed land in graph.blocked."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph[c] = [b]
    graph.run(make_default_executor())
    assert {t.id for t in graph.blocked} == {b.id, c.id}


def test_blocked_empty_before_run() -> None:
    """PENDING tasks before any run are NOT counted as blocked."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []
    assert graph.blocked == []


def test_blocked_empty_when_no_failures() -> None:
    """A run with no failures leaves blocked empty."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(make_default_executor())
    assert graph.blocked == []


def test_blocked_resets_at_start_of_run() -> None:
    """Calling run() again clears blocked state from the previous run."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(make_default_executor())
    assert b in graph.blocked
    # Second run: validation fails before execution because tasks are
    # already DONE/FAILED, but blocked should reset at entry. We force
    # the reset path by calling run() on an empty graph reusing this one's
    # internal state surrogate — simplest: just call run() again and check
    # blocked is recomputed (still contains b, since a still failed).
    graph.run(make_default_executor())
    # The point: blocked is a function of the most recent run, not
    # accumulated across runs.
    assert {t.id for t in graph.blocked} == {b.id}


def test_ok_true_after_clean_run() -> None:
    """All tasks DONE, none failed, none blocked -> ok is True."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(make_default_executor())
    assert graph.ok


def test_ok_false_after_failure() -> None:
    """Any failure -> ok is False."""
    a = _bash("A", "exit 1")
    graph = TaskGraph()
    graph[a] = []
    graph.run(make_default_executor())
    assert not graph.ok


def test_ok_false_when_tasks_blocked() -> None:
    """A blocked task means the graph did not complete -> not ok."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(make_default_executor())
    assert not graph.ok


def test_ok_false_before_run() -> None:
    """A graph with PENDING tasks has not succeeded -> not ok."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []
    assert not graph.ok


def test_ok_true_for_empty_graph() -> None:
    """Vacuously: no tasks, nothing to fail."""
    graph = TaskGraph()
    graph.run(make_default_executor())
    assert graph.ok


# ---- topology views: roots / leaves ------------------------------------------


def test_roots_returns_tasks_with_no_deps() -> None:
    """Roots are tasks whose deps list is empty."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = []
    graph[c] = [a, b]
    assert {t.id for t in graph.roots()} == {a.id, b.id}


def test_roots_empty_for_empty_graph() -> None:
    """No tasks -> no roots."""
    assert TaskGraph().roots() == []


def test_roots_all_when_no_edges() -> None:
    """Every task without deps is a root."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = []
    assert {t.id for t in graph.roots()} == {a.id, b.id}


def test_leaves_returns_tasks_with_no_dependents() -> None:
    """Leaves are tasks that nothing else depends on."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph[c] = [a]
    assert {t.id for t in graph.leaves()} == {b.id, c.id}


def test_leaves_empty_for_empty_graph() -> None:
    """No tasks -> no leaves."""
    assert TaskGraph().leaves() == []


def test_diamond_roots_and_leaves() -> None:
    """Diamond: A is the only root, D is the only leaf."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    d = _bash("D", "echo d")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph[c] = [a]
    graph[d] = [b, c]
    assert graph.roots() == [a]
    assert graph.leaves() == [d]
