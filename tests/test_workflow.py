"""Tests for TaskGraph: mapping protocol, validation, execution, failure."""

import time
from datetime import datetime
from typing import Any

import pytest

from ttasks.executor import TaskExecutor, make_default_executor
from ttasks.ledger import TaskLedger
from ttasks.task import Task, TaskStatus, TaskType
from ttasks.workflow import TaskGraph


def _bash(title: str, payload: str) -> Task:
    """Shorthand for a bash task."""
    return Task(title=title, type=TaskType.BASH, payload=payload)


# ---- Graph identity ----------------------------------------------------------


def test_graph_has_read_only_id() -> None:
    """TaskGraph has an immutable identity."""
    graph = TaskGraph()
    graph_id = graph.id

    attr = "id"
    with pytest.raises(AttributeError):
        setattr(graph, attr, "new-id")

    assert graph.id == graph_id


def test_graph_accepts_title() -> None:
    """TaskGraph stores display title metadata."""
    graph = TaskGraph(title="Build")

    assert graph.title == "Build"


def test_graph_rejects_non_string_title() -> None:
    """TaskGraph title must be a string."""
    title: Any = 42

    with pytest.raises(TypeError, match="title must be a str"):
        TaskGraph(title=title)


def test_graph_created_at_defaults_to_now() -> None:
    """TaskGraph records when it was created."""
    before = datetime.now()
    graph = TaskGraph()
    after = datetime.now()

    assert before <= graph.created_at <= after


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


def test_constructor_accepts_positional_ledger() -> None:
    """The existing positional ledger constructor form still works."""
    ledger = TaskLedger()
    graph = TaskGraph(ledger)

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


def test_run_rejects_non_positive_max_workers() -> None:
    """max_workers must be positive before run state is reset or scheduled."""
    graph = TaskGraph()

    with pytest.raises(ValueError, match="max_workers must be greater than 0"):
        graph.run(make_default_executor(), max_workers=0)


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


def test_graph_passes_direct_upstream_task_refs() -> None:
    """Handlers receive direct upstream tasks from the graph ledger."""
    a = _bash("A", "")
    b = _bash("B", "")
    executor = TaskExecutor()

    def handler(context) -> str:
        """Return output using direct upstream task refs."""
        if context.id == a.id:
            assert context.upstream == {}
            return "a"
        assert context.upstream[a.id] is a
        assert context.upstream[a.id] is graph.ledger[a.id]
        assert context.upstream[a.id].result is not None
        return context.upstream[a.id].result.output.upper()

    executor.register(TaskType.BASH, handler)
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]

    graph.run(executor)

    assert b.result is not None
    assert b.result.output == "A"


def test_graph_passes_only_direct_upstream_task_refs() -> None:
    """Handlers see direct dependencies, not every transitive ancestor."""
    a = _bash("A", "")
    b = _bash("B", "")
    c = _bash("C", "")
    executor = TaskExecutor()

    def handler(context) -> str:
        """Assert upstream visibility follows direct graph edges."""
        if context.id == a.id:
            return "a"
        if context.id == b.id:
            assert set(context.upstream) == {a.id}
            return "b"
        assert set(context.upstream) == {b.id}
        assert a.id not in context.upstream
        return "c"

    executor.register(TaskType.BASH, handler)
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph[c] = [b]

    graph.run(executor)

    assert graph.ok


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


def test_graph_records_executor_errors() -> None:
    """Executor setup errors are exposed even when task state stays pending."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []

    graph.run(TaskExecutor())

    assert a.id in graph.errors
    assert isinstance(graph.errors[a.id], ValueError)
    assert "No handler registered" in str(graph.errors[a.id])
    assert a.status == TaskStatus.PENDING
    assert graph.failed == []
    assert graph.blocked == []
    assert not graph.ok


def test_executor_error_blocks_descendants() -> None:
    """A task that cannot execute blocks downstream tasks and records its error."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]

    graph.run(TaskExecutor())

    assert a.id in graph.errors
    assert graph.blocked == [b]
    assert b.status == TaskStatus.PENDING
    assert not graph.ok


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


# ---- Finally tasks -----------------------------------------------------------


def test_add_finally_runs_after_failed_and_blocked_tasks() -> None:
    """A finally task runs after deps are inactive, even if they did not pass."""
    a = _bash("A", "")
    b = _bash("B", "")
    report = _bash("Report", "")
    executor = TaskExecutor()

    def handler(context) -> str:
        """Fail A; report should still see both upstream refs."""
        if context.id == a.id:
            raise RuntimeError("boom")
        assert context.id == report.id
        assert set(context.upstream) == {a.id, b.id}
        assert context.upstream[a.id].status == TaskStatus.FAILED
        assert context.upstream[b.id].status == TaskStatus.PENDING
        return "report"

    executor.register(TaskType.BASH, handler)
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.add_finally(report, after=[a, b])

    graph.run(executor)

    assert a.status == TaskStatus.FAILED
    assert b in graph.blocked
    assert report.status == TaskStatus.DONE
    assert report.result is not None
    assert report.result.output == "report"
    assert not graph.ok


def test_optional_finally_failure_does_not_make_graph_not_ok() -> None:
    """Optional reporting task failures are visible but ignored by ok."""
    a = _bash("A", "")
    report = _bash("Report", "")
    executor = TaskExecutor()

    def handler(context) -> str:
        """Let A pass but fail the optional finalizer."""
        if context.id == a.id:
            return "a"
        raise RuntimeError("copilot unavailable")

    executor.register(TaskType.BASH, handler)
    graph = TaskGraph()
    graph[a] = []
    graph.add_finally(report, after=[a], required=False)

    graph.run(executor)

    assert a.status == TaskStatus.DONE
    assert report.status == TaskStatus.FAILED
    assert report in graph.failed
    assert report.id in graph.errors
    assert graph.ok


def test_required_finally_failure_makes_graph_not_ok() -> None:
    """Required finally tasks participate in graph.ok like normal tasks."""
    a = _bash("A", "")
    report = _bash("Report", "")
    executor = TaskExecutor()

    def handler(context) -> str:
        """Let A pass but fail the required finalizer."""
        if context.id == a.id:
            return "a"
        raise RuntimeError("report failed")

    executor.register(TaskType.BASH, handler)
    graph = TaskGraph()
    graph[a] = []
    graph.add_finally(report, after=[a])

    graph.run(executor)

    assert report.status == TaskStatus.FAILED
    assert not graph.ok


def test_add_finally_rejects_non_bool_required() -> None:
    """required is intentionally strict to avoid truthy policy surprises."""
    graph = TaskGraph()
    report = _bash("Report", "")
    required: Any = "no"

    with pytest.raises(TypeError, match="required must be a bool"):
        graph.add_finally(report, after=[], required=required)


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


def test_cancelled_lists_cancelled_tasks() -> None:
    """Cancelled graph tasks are available as a status view."""
    a = _bash("A", "echo a")
    a.cancel()
    graph = TaskGraph()
    graph[a] = []

    graph.run(make_default_executor())

    assert graph.cancelled == [a]
    assert graph.blocked == [a]
    assert not graph.ok


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

    graph.run(make_default_executor())

    # The point: blocked is a function of the most recent run, not
    # accumulated across runs.
    assert {t.id for t in graph.blocked} == {b.id}


def test_clean_graph_can_be_run_again_without_blocking_done_dependencies() -> None:
    """A rerun treats already-DONE tasks as satisfied, not failed futures."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]

    graph.run(make_default_executor())
    graph.run(make_default_executor())

    assert graph.ok
    assert graph.blocked == []
    assert graph.failed == []
    assert a.status == TaskStatus.DONE
    assert b.status == TaskStatus.DONE


def test_done_dependency_allows_pending_descendant_to_run() -> None:
    """A task added after its dependency completed can still run."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph.run(make_default_executor())

    graph[b] = [a]
    graph.run(make_default_executor())

    assert graph.ok
    assert graph.blocked == []
    assert b.status == TaskStatus.DONE


def test_cancelled_root_is_blocked_instead_of_hanging() -> None:
    """A non-runnable root task terminates the run instead of deadlocking."""
    a = _bash("A", "echo a")
    a.cancel()
    graph = TaskGraph()
    graph[a] = []

    graph.run(make_default_executor())

    assert graph.blocked == [a]
    assert not graph.ok


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
