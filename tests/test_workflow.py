"""Tests for TaskGraph: mapping protocol, validation, execution, failure."""

import time
from datetime import datetime
from typing import Any

import pytest
from conftest import _bash

from ttasks import TaskExecutor, TaskGraph, TaskStatus, TaskType

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


def test_setitem_registers_task_in_graph() -> None:
    """Assigning a task into the graph adds it as a member."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []
    assert a in graph
    assert list(graph) == [a]


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


# ---- Public introspection ----------------------------------------------------


def test_dependencies_returns_direct_upstream_tasks() -> None:
    """``graph.dependencies(task)`` returns the direct upstream Tasks."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    assert graph.dependencies(a) == []
    assert graph.dependencies(b) == [a]


def test_items_yields_task_dep_pairs_in_insertion_order() -> None:
    """``graph.items()`` yields ``(task, deps)`` pairs in insertion order."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    assert list(graph.items()) == [(a, []), (b, [a])]


def test_is_finally_distinguishes_normal_and_finally_tasks() -> None:
    """``is_finally`` reflects whether the task was added with finally_=True."""
    a = _bash("A", "echo a")
    report = _bash("Report", "echo report")
    graph = TaskGraph()
    graph[a] = []
    graph.add(report, after=[a], finally_=True)
    assert not graph.is_finally(a)
    assert graph.is_finally(report)


def test_is_optional_reflects_required_flag_on_finally_tasks() -> None:
    """``is_optional`` is True only for finally tasks added with required=False."""
    a = _bash("A", "echo a")
    optional = _bash("Optional", "echo opt")
    required = _bash("Required", "echo req")
    graph = TaskGraph()
    graph[a] = []
    graph.add(optional, after=[a], finally_=True, required=False)
    graph.add(required, after=[a], finally_=True)
    assert not graph.is_optional(a)
    assert graph.is_optional(optional)
    assert not graph.is_optional(required)


def test_finally_optional_and_required_task_views_are_ordered_snapshots() -> None:
    """Task metadata views follow graph order without exposing internal sets."""
    setup = _bash("Setup", "echo setup")
    optional_cleanup = _bash("Optional", "echo optional")
    build = _bash("Build", "echo build")
    required_cleanup = _bash("Required", "echo required")
    publish = _bash("Publish", "echo publish")
    graph = TaskGraph()

    graph.add(setup)
    graph.add(optional_cleanup, after=[setup], finally_=True, required=False)
    graph.add(build, after=[setup])
    graph.add(required_cleanup, after=[build], finally_=True)
    graph.add(publish, after=[build])

    assert graph.finally_tasks == [optional_cleanup, required_cleanup]
    assert graph.optional_tasks == [optional_cleanup]
    assert graph.required_tasks == [setup, build, required_cleanup, publish]

    returned = graph.optional_tasks
    returned.clear()
    assert graph.optional_tasks == [optional_cleanup]


def test_finally_optional_and_required_views_follow_readded_task_metadata() -> None:
    """Re-adding a task updates the public optional/finally task views."""
    setup = _bash("Setup", "echo setup")
    cleanup = _bash("Cleanup", "echo cleanup")
    graph = TaskGraph()
    graph.add(setup)

    graph.add(cleanup, after=[setup], finally_=True, required=False)
    assert graph.finally_tasks == [cleanup]
    assert graph.optional_tasks == [cleanup]
    assert graph.required_tasks == [setup]

    graph.add(cleanup, after=[setup], finally_=True)
    assert graph.finally_tasks == [cleanup]
    assert graph.optional_tasks == []
    assert graph.required_tasks == [setup, cleanup]

    graph.add(cleanup, after=[setup])
    assert graph.finally_tasks == []
    assert graph.optional_tasks == []
    assert graph.required_tasks == [setup, cleanup]


# ---- Validation --------------------------------------------------------------


def test_run_rejects_non_positive_max_workers() -> None:
    """max_workers must be positive before run state is reset or scheduled."""
    graph = TaskGraph()

    with pytest.raises(ValueError, match="max_workers must be greater than 0"):
        graph.run(TaskExecutor(), max_workers=0)


def test_run_raises_on_unregistered_dep() -> None:
    """Referencing a Task that was never assigned to the graph is a ValueError."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[b] = [a]  # a never registered
    with pytest.raises(ValueError, match="depends on unregistered"):
        graph.run(TaskExecutor())


def test_run_raises_on_self_loop() -> None:
    """A self-loop is a cycle."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = [a]
    with pytest.raises(ValueError, match="cycle"):
        graph.run(TaskExecutor())


def test_run_raises_on_two_node_cycle() -> None:
    """A → B → A is rejected."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = [b]
    graph[b] = [a]
    with pytest.raises(ValueError, match="cycle"):
        graph.run(TaskExecutor())


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
        graph.run(TaskExecutor())


def test_run_rejects_stale_running_task() -> None:
    """A pre-existing RUNNING task is rejected at run() instead of deadlocking."""
    a = _bash("A", "echo a")
    a.transition_to(TaskStatus.RUNNING)
    graph = TaskGraph()
    graph[a] = []

    with pytest.raises(ValueError, match="RUNNING"):
        graph.run(TaskExecutor())


def test_run_no_progress_guard_raises_runtime_error() -> None:
    """Scheduler deadlock raises RuntimeError from run() instead of hanging."""
    # Synthesize a stuck state by skipping validation and putting a task in
    # RUNNING. _run_inner cannot transition RUNNING → RUNNING and cannot
    # treat it as finished, so the no-progress guard must fire.
    a = _bash("A", "echo a")
    a.transition_to(TaskStatus.RUNNING)
    graph = TaskGraph()
    graph[a] = []

    with pytest.raises(RuntimeError, match="no progress"):
        graph._run_inner(TaskExecutor(), max_workers=1)


# ---- Execution ---------------------------------------------------------------


def test_graph_passes_direct_upstream_task_refs() -> None:
    """Handlers receive direct upstream task refs from the graph."""
    a = _bash("A", "")
    b = _bash("B", "")
    executor = TaskExecutor()

    def handler(context) -> str:
        """Return output using direct upstream task refs."""
        if context.id == a.id:
            assert context.upstream == {}
            return "a"
        assert context.upstream[a.id] is a
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
    assert graph.run(TaskExecutor()) is graph
    assert graph.ok


def test_single_node_runs() -> None:
    """A graph with one root task executes it and reports success."""
    a = _bash("A", "echo hello")
    graph = TaskGraph()
    graph[a] = []
    graph.run(TaskExecutor())
    assert a.status == TaskStatus.SUCCEEDED
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
    graph.run(TaskExecutor())
    assert graph.ok
    for task in (a, b, c):
        assert task.status == TaskStatus.SUCCEEDED


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
    graph.run(TaskExecutor())
    elapsed = time.monotonic() - start
    # Serial would be ~1.2s; diamond parallel is ~0.9s. Allow generous slack.
    assert elapsed < 1.15, f"diamond took {elapsed:.2f}s — looks serial"
    assert graph.ok


# ---- Failure policy ----------------------------------------------------------


def test_required_executor_error_makes_graph_not_ok_even_if_status_succeeded() -> None:
    """A required task future error makes graph.ok false even with success status."""

    class BrokenExecutor(TaskExecutor):
        def execute(self, task, upstream=None):
            task.transition_to(TaskStatus.RUNNING)
            task.transition_to(TaskStatus.SUCCEEDED)
            raise RuntimeError("executor post-processing failed")

    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []

    graph.run(BrokenExecutor())

    assert a.status == TaskStatus.SUCCEEDED
    assert a.id in graph.errors
    assert not graph.ok


def test_graph_records_executor_errors() -> None:
    """Pre-start handler errors terminalize the task as FAILED with the error."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []

    graph.run(TaskExecutor.empty())

    assert a.id in graph.errors
    assert isinstance(graph.errors[a.id], ValueError)
    assert "No handler registered" in str(graph.errors[a.id])
    assert a.status == TaskStatus.FAILED
    assert a.result is not None
    assert a.result.termination_reason == "handler"
    assert graph.failed == [a]
    assert graph.blocked == []
    assert not graph.ok


def test_executor_error_blocks_descendants() -> None:
    """Pre-start failure terminalizes parent FAILED and blocks descendants on it."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]

    graph.run(TaskExecutor.empty())

    assert a.id in graph.errors
    assert a.status == TaskStatus.FAILED
    assert graph.failed == [a]
    assert graph.blocked == [b]
    assert b.status == TaskStatus.BLOCKED
    assert b.blocked_by == a.id
    # Descendant blocks on a FAILED parent, not a still-PENDING one.
    assert b.blocked_by is not None
    assert graph._tasks[b.blocked_by].status == TaskStatus.FAILED
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
    graph.run(TaskExecutor())
    assert a.status == TaskStatus.FAILED
    assert graph.failed == [a]
    assert {t.id for t in graph.blocked} == {b.id, c.id}
    assert b.status == TaskStatus.BLOCKED
    assert c.status == TaskStatus.BLOCKED
    assert b.blocked_by == a.id
    assert c.blocked_by == b.id


def test_failure_does_not_affect_independent_branch() -> None:
    """A failing root does not block a parallel branch with no shared deps."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = []
    graph[c] = [b]
    graph.run(TaskExecutor())
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
    graph.run(TaskExecutor())
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
    graph.run(TaskExecutor())
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
        assert context.upstream[b.id].status == TaskStatus.BLOCKED
        return "report"

    executor.register(TaskType.BASH, handler)
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.add(report, after=[a, b], finally_=True)

    graph.run(executor)

    assert a.status == TaskStatus.FAILED
    assert b in graph.blocked
    assert report.status == TaskStatus.SUCCEEDED
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
    graph.add(report, after=[a], finally_=True, required=False)

    graph.run(executor)

    assert a.status == TaskStatus.SUCCEEDED
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
    graph.add(report, after=[a], finally_=True)

    graph.run(executor)

    assert report.status == TaskStatus.FAILED
    assert not graph.ok


def test_optional_required_failure_and_blocked_views_split_statuses() -> None:
    """Post-run status views classify optional and required task outcomes."""
    root = _bash("Root", "")
    required_fail = _bash("Required fail", "")
    optional_fail = _bash("Optional fail", "")
    blocked = _bash("Blocked", "")
    executor = TaskExecutor()

    def handler(context) -> str:
        """Fail the two report targets and let the root succeed."""
        if context.id == root.id:
            return "root"
        raise RuntimeError(context.title)

    executor.register(TaskType.BASH, handler)
    graph = TaskGraph()
    graph.add(root)
    graph.add(required_fail, after=[root])
    graph.add(optional_fail, after=[root], finally_=True, required=False)
    graph.add(blocked, after=[required_fail])

    graph.run(executor)

    assert graph.optional_failed == [optional_fail]
    assert graph.required_failed == [required_fail]
    assert graph.required_blocked == [blocked]
    assert not graph.ok


# ---- graph as post-run view -------------------------------------------------


def test_graph_tasks_carry_results_after_run() -> None:
    """After graph.run(), every executed task in the graph has task.result set."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(TaskExecutor())

    for task in graph:
        assert task.result is not None
        assert task.result.status == TaskStatus.SUCCEEDED
        assert task.result.output.strip() == task.title.lower()


def test_blocked_task_has_no_result_after_run() -> None:
    """A task whose deps failed never runs, so task.result stays None."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(TaskExecutor())

    assert a.result is not None
    assert a.result.status == TaskStatus.FAILED
    assert b.result is None  # never executed
    assert b.status == TaskStatus.BLOCKED
    assert b.blocked_by == a.id


# ---- run() returns self (chaining) -------------------------------------------


def test_run_returns_self() -> None:
    """graph.run() returns the graph itself so callers can chain."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []
    assert graph.run(TaskExecutor()) is graph


def test_run_returns_self_for_empty_graph() -> None:
    """Empty graph still returns self (not None, not {})."""
    graph = TaskGraph()
    assert graph.run(TaskExecutor()) is graph


# ---- status views: succeeded / failed / blocked / ok -------------------------


def test_succeeded_empty_before_run() -> None:
    """Before any run, no task has SUCCEEDED status, so succeeded is empty."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []
    assert graph.succeeded == []


def test_succeeded_only_lists_graph_tasks_not_other_graphs() -> None:
    """Two independent graphs do not pollute each other's succeeded view."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    g1 = TaskGraph()
    g2 = TaskGraph()
    g1[a] = []
    g2[b] = []
    g1.run(TaskExecutor())
    g2.run(TaskExecutor())
    assert g1.succeeded == [a]
    assert g2.succeeded == [b]


def test_cancelled_lists_cancelled_tasks() -> None:
    """Cancelled graph tasks are available as a status view."""
    a = _bash("A", "echo a")
    a.cancel()
    graph = TaskGraph()
    graph[a] = []

    graph.run(TaskExecutor())

    assert graph.cancelled == [a]
    assert graph.blocked == []
    assert not graph.ok


def test_failed_lists_failed_tasks() -> None:
    """A task that raised lands in graph.failed."""
    a = _bash("A", "exit 1")
    graph = TaskGraph()
    graph[a] = []
    graph.run(TaskExecutor())
    assert graph.failed == [a]


def test_failed_empty_when_all_succeed() -> None:
    """A clean run leaves failed empty."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []
    graph.run(TaskExecutor())
    assert graph.failed == []


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
    graph.run(TaskExecutor())
    assert graph.blocked == []


def test_blocked_resets_at_start_of_run() -> None:
    """Calling run() again clears blocked state from the previous run."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(TaskExecutor())
    assert b in graph.blocked

    graph.run(TaskExecutor())

    # The point: blocked is a function of the most recent run, not
    # accumulated across runs.
    assert {t.id for t in graph.blocked} == {b.id}


def test_clean_graph_can_be_run_again_without_blocking_done_dependencies() -> None:
    """A rerun treats already-SUCCEEDED tasks as satisfied, not failed futures."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]

    graph.run(TaskExecutor())
    graph.run(TaskExecutor())

    assert graph.ok
    assert graph.blocked == []
    assert graph.failed == []
    assert a.status == TaskStatus.SUCCEEDED
    assert b.status == TaskStatus.SUCCEEDED


def test_done_dependency_allows_pending_descendant_to_run() -> None:
    """A task added after its dependency completed can still run."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph.run(TaskExecutor())

    graph[b] = [a]
    graph.run(TaskExecutor())

    assert graph.ok
    assert graph.blocked == []
    assert b.status == TaskStatus.SUCCEEDED


def test_cancelled_root_is_blocked_instead_of_hanging() -> None:
    """A non-runnable root task terminates the run instead of deadlocking."""
    a = _bash("A", "echo a")
    a.cancel()
    graph = TaskGraph()
    graph[a] = []

    graph.run(TaskExecutor())

    assert graph.cancelled == [a]
    assert graph.blocked == []
    assert not graph.ok


def test_ok_true_after_clean_run() -> None:
    """All tasks SUCCEEDED, none failed, none blocked -> ok is True."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(TaskExecutor())
    assert graph.ok


def test_ok_false_after_failure() -> None:
    """Any failure -> ok is False."""
    a = _bash("A", "exit 1")
    graph = TaskGraph()
    graph[a] = []
    graph.run(TaskExecutor())
    assert not graph.ok


def test_ok_false_when_tasks_blocked() -> None:
    """A blocked task means the graph did not complete -> not ok."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(TaskExecutor())
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
    graph.run(TaskExecutor())
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


# ---- graph.add() ------------------------------------------------------------


def test_add_with_no_deps_registers_task() -> None:
    """graph.add(task) registers a task with no upstream dependencies."""
    a = _bash("A", "echo a")
    graph = TaskGraph()

    graph.add(a)

    assert a in graph
    assert graph.dependencies(a) == []
    assert not graph.is_finally(a)
    assert not graph.is_optional(a)


def test_add_with_after_records_upstream_deps() -> None:
    """graph.add(task, after=[...]) records the upstream task list."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    c = _bash("C", "echo c")
    graph = TaskGraph()
    graph.add(a)
    graph.add(b)

    graph.add(c, after=[a, b])

    assert graph.dependencies(c) == [a, b]


def test_add_after_accepts_arbitrary_iterables() -> None:
    """The after parameter accepts any iterable, including generators."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph.add(a)

    graph.add(b, after=(t for t in [a]))

    assert graph.dependencies(b) == [a]


def test_add_deduplicates_repeated_dependencies() -> None:
    """Repeated upstream tasks are one edge, not a false cycle."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph.add(a)

    graph.add(b, after=[a, a])

    assert graph.dependencies(b) == [a]
    graph.run(TaskExecutor())
    assert graph.ok


def test_add_finally_marks_task_as_finally_required_by_default() -> None:
    """finally_=True without required= keeps the task required."""
    a = _bash("A", "echo a")
    cleanup = _bash("C", "echo c")
    graph = TaskGraph()
    graph.add(a)

    graph.add(cleanup, after=[a], finally_=True)

    assert graph.is_finally(cleanup)
    assert not graph.is_optional(cleanup)


def test_add_finally_optional_marks_task_as_optional() -> None:
    """finally_=True with required=False marks the task optional."""
    a = _bash("A", "echo a")
    cleanup = _bash("C", "echo c")
    graph = TaskGraph()
    graph.add(a)

    graph.add(cleanup, after=[a], finally_=True, required=False)

    assert graph.is_finally(cleanup)
    assert graph.is_optional(cleanup)


def test_add_required_false_without_finally_raises() -> None:
    """required=False is only meaningful with finally_=True."""
    a = _bash("A", "echo a")
    graph = TaskGraph()

    msg = "required=False is only valid with finally_=True"
    with pytest.raises(ValueError, match=msg):
        graph.add(a, required=False)


def test_add_rejects_non_task_and_non_bool_finally() -> None:
    """add() validates the task type and the finally_ flag's type."""
    graph = TaskGraph()
    bogus: Any = "not a task"
    bad_finally: Any = "yes"

    with pytest.raises(TypeError, match="Expected Task"):
        graph.add(bogus)
    with pytest.raises(TypeError, match="finally_ must be a bool"):
        graph.add(_bash("A", "echo a"), finally_=bad_finally)


def test_add_runs_like_setitem_form() -> None:
    """A graph built with .add() executes to the same end state as the mapping form."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph.add(a)
    graph.add(b, after=[a])

    graph.run(TaskExecutor())

    assert graph.ok
    assert graph.succeeded == [a, b]


def test_setitem_registers_task_and_dependencies() -> None:
    """``graph[task] = deps`` registers task with the given dependencies."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]

    assert graph.dependencies(b) == [a]


def test_add_rejects_non_bool_required() -> None:
    """add() validates the required flag's type."""
    a = _bash("A", "echo a")
    graph = TaskGraph()
    bad_required: Any = "no"
    with pytest.raises(TypeError, match="required must be a bool"):
        graph.add(a, finally_=True, required=bad_required)


# ---- Step 12: carryover-BLOCKED retry ----------------------------------------


def test_failed_parent_added_after_child_retries_before_child_is_blocked() -> None:
    """A retryable failed parent can recover even when visited after its child."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    a.transition_to(TaskStatus.RUNNING)
    a.transition_to(TaskStatus.FAILED, error="previous failure")
    graph = TaskGraph()
    graph[b] = [a]
    graph[a] = []

    graph.run(TaskExecutor())

    assert graph.ok
    assert graph.blocked == []
    assert a.status == TaskStatus.SUCCEEDED
    assert b.status == TaskStatus.SUCCEEDED


def test_carryover_blocked_with_succeeded_parent_recovers() -> None:
    """A BLOCKED task entering run() with all parents SUCCEEDED runs and succeeds."""
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]

    # First run succeeds A but then we manually mark B as BLOCKED on a stale id
    # to simulate carryover from a hypothetical prior failed run.
    graph.run(TaskExecutor())
    assert a.status == TaskStatus.SUCCEEDED
    assert b.status == TaskStatus.SUCCEEDED

    # Force B back into a carryover-BLOCKED state pointing at A.
    b._set_blocked_by(a.id)
    object.__setattr__(b, "_status", TaskStatus.BLOCKED)

    graph.run(TaskExecutor())

    assert b.status == TaskStatus.SUCCEEDED
    assert b.blocked_by is None


def test_carryover_blocked_with_failed_parent_stays_blocked() -> None:
    """Carryover BLOCKED whose parent is still FAILED stays BLOCKED, no loop."""
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]

    graph.run(TaskExecutor())
    assert a.status == TaskStatus.FAILED
    assert b.status == TaskStatus.BLOCKED
    # Second run with the same broken parent: B stays BLOCKED, no infinite loop.
    graph.run(TaskExecutor())
    assert a.status == TaskStatus.FAILED
    assert b.status == TaskStatus.BLOCKED


def test_within_run_blocked_is_not_retried_same_run() -> None:
    """A task BLOCKED during this run is terminal-for-the-run, not retried."""
    attempts: dict[str, int] = {"B": 0}
    a = _bash("A", "exit 1")
    b = _bash("B", "echo b")
    executor = TaskExecutor()
    original = executor._handlers[TaskType.BASH]

    def counting(context: Any) -> Any:
        if context.title == "B":
            attempts["B"] += 1
        return original(context)

    executor.register(TaskType.BASH, counting)

    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.run(executor)

    assert a.status == TaskStatus.FAILED
    assert b.status == TaskStatus.BLOCKED
    # B was never submitted because it became BLOCKED in this run.
    assert attempts["B"] == 0


def test_finally_waits_for_retryable_failed_dependency_added_later() -> None:
    """A finally task waits for a retryable failed dependency to rerun first."""
    parent = _bash("parent", "")
    parent.transition_to(TaskStatus.RUNNING)
    parent.transition_to(TaskStatus.FAILED, error="old failure")
    cleanup = _bash("cleanup", "")
    seen: list[str] = []

    executor = TaskExecutor.empty()

    def handler(context: Any) -> str:
        seen.append(context.title)
        return "ok"

    executor.register(TaskType.BASH, handler)
    graph = TaskGraph()
    graph.add(cleanup, after=[parent], finally_=True)
    graph[parent] = []

    graph.run(executor, max_workers=1)

    assert seen == ["parent", "cleanup"]
    assert parent.status == TaskStatus.SUCCEEDED
    assert cleanup.status == TaskStatus.SUCCEEDED


def test_finally_runs_after_carryover_blocked_recovers() -> None:
    """A finally task fires after a carryover-BLOCKED task recovers to SUCCEEDED."""
    finally_ran: list[str] = []
    a = _bash("A", "echo a")
    b = _bash("B", "echo b")

    def finally_handler(context: Any) -> str:
        finally_ran.append(context.title)
        return "ok"

    finally_task = _bash("FIN", "echo fin")

    executor = TaskExecutor()
    original = executor._handlers[TaskType.BASH]

    def dispatch(context: Any) -> Any:
        if context.title == "FIN":
            return finally_handler(context)
        return original(context)

    executor.register(TaskType.BASH, dispatch)

    graph = TaskGraph()
    graph[a] = []
    graph[b] = [a]
    graph.add(finally_task, after=[b], finally_=True)
    graph.run(executor)

    assert a.status == TaskStatus.SUCCEEDED
    assert b.status == TaskStatus.SUCCEEDED

    # Force B back into carryover-BLOCKED to test recovery + finally semantics.
    b._set_blocked_by(a.id)
    object.__setattr__(b, "_status", TaskStatus.BLOCKED)
    object.__setattr__(finally_task, "_status", TaskStatus.PENDING)
    finally_task._set_result(None)
    finally_ran.clear()

    graph.run(executor)

    assert b.status == TaskStatus.SUCCEEDED
    assert finally_task.status == TaskStatus.SUCCEEDED
    assert finally_ran == ["FIN"]


# ---- Step 14: graph autosave failure policy ---------------------------------


def test_graph_persistence_errors_initially_empty() -> None:
    """A fresh executor exposes an empty graph_persistence_errors list."""
    executor = TaskExecutor()
    assert executor.graph_persistence_errors == []


def test_graph_save_failure_does_not_break_run_and_records_error() -> None:
    """A failing graph save records on graph_persistence_errors and warns."""
    import warnings

    class _BrokenGraphs:
        def save(self, graph: Any) -> None:
            raise RuntimeError("disk full")

    class _BrokenStore:
        @property
        def tasks(self) -> Any:
            return None

        @property
        def graphs(self) -> Any:
            return _BrokenGraphs()

    a = _bash("A", "echo a")
    graph = TaskGraph()
    graph[a] = []

    executor = TaskExecutor(store=_BrokenStore())
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        graph.run(executor)

    assert a.status == TaskStatus.SUCCEEDED
    assert executor.graph_persistence_errors
    assert all(gid == graph.id for gid, _ in executor.graph_persistence_errors)
    assert any("graph persistence failed" in str(w.message) for w in captured)


def test_persist_graph_no_store_is_noop() -> None:
    """Without a configured store, _persist_graph silently does nothing."""
    executor = TaskExecutor()
    graph = TaskGraph()
    executor._persist_graph(graph)
    assert executor.graph_persistence_errors == []
