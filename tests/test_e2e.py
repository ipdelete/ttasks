"""Live end-to-end tests for ttasks.

These tests build real ``TaskGraph`` instances and run them through a real
``TaskExecutor`` against actual subprocesses (and, for the mixed-type test,
the Copilot SDK). They are slower and less hermetic than the unit suite, so
they are gated behind the ``live`` pytest marker and excluded by default:

    uv run pytest -m live           # run only the live e2e suite
    uv run pytest -m ''             # run everything

The module-level ``pytestmark`` applies the marker to every test below.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterable
from pathlib import Path

import pytest

from ttasks import (
    SQLiteStore,
    Task,
    TaskEvent,
    TaskEventType,
    TaskExecutor,
    TaskGraph,
    TaskStatus,
    TaskType,
)

pytestmark = pytest.mark.live


# --- helpers ----------------------------------------------------------------


def _collect_events(executor: TaskExecutor) -> list[TaskEvent]:
    """Subscribe to ``executor.events`` and return a list that accumulates events."""
    captured: list[TaskEvent] = []
    executor.events.subscribe(captured.append)
    return captured


def _terminals_by_task(events: Iterable[TaskEvent]) -> dict[str, TaskEvent]:
    """Map task_id -> the single terminal event (SUCCEEDED/FAILED/CANCELLED/BLOCKED)."""
    terminal_types = {
        TaskEventType.SUCCEEDED,
        TaskEventType.FAILED,
        TaskEventType.CANCELLED,
        TaskEventType.BLOCKED,
    }
    out: dict[str, TaskEvent] = {}
    for ev in events:
        if ev.type in terminal_types:
            assert ev.task_id not in out, (
                f"task {ev.task_id} got two terminal events: "
                f"{out[ev.task_id].type} and {ev.type}"
            )
            out[ev.task_id] = ev
    return out


# --- Test 1: diamond + finally cleanup --------------------------------------


def test_diamond_with_finally_cleanup(tmp_path: Path) -> None:
    """Fan-out/fan-in with a required-cleanup finally task.

    mkdir → [write_a, write_b, write_c in parallel] → concat → validate;
    finally: rm -rf the workdir (optional). Asserts every node succeeded,
    the cleanup actually ran, and the graph roundtrips through SQLite.
    """
    work = tmp_path / "work"
    out = tmp_path / "out.txt"
    db = tmp_path / "diamond.db"

    setup = Task.bash(f"mkdir -p {work}", title="setup")
    write_a = Task.bash(f"echo a > {work}/a.txt", title="write_a")
    write_b = Task.bash(f"echo b > {work}/b.txt", title="write_b")
    write_c = Task.bash(f"echo c > {work}/c.txt", title="write_c")
    concat = Task.bash(
        f"cat {work}/a.txt {work}/b.txt {work}/c.txt > {out}", title="concat",
    )
    validate = Task.bash(
        f"test \"$(wc -l < {out})\" = \"3\"", title="validate",
    )
    cleanup = Task.bash(f"rm -rf {work}", title="cleanup")

    store = SQLiteStore(db)
    executor = TaskExecutor(store=store)
    events = _collect_events(executor)

    def bash_with_progress(context):
        """Emit progress from one real bash graph node, then run it normally."""
        if context.id == concat.id:
            context.emit_progress(50, "concatenating")
        return executor._run_bash(context)

    executor.register(TaskType.BASH, bash_with_progress)

    graph = TaskGraph(title="diamond")
    graph.add(setup)
    graph.add(write_a, after=[setup])
    graph.add(write_b, after=[setup])
    graph.add(write_c, after=[setup])
    graph.add(concat, after=[write_a, write_b, write_c])
    graph.add(validate, after=[concat])
    graph.add(cleanup, after=[validate], finally_=True, required=False)
    store.graphs.save(graph)

    graph.run(executor)

    # Outcome.
    assert graph.ok
    for task in [setup, write_a, write_b, write_c, concat, validate, cleanup]:
        assert task.status == TaskStatus.SUCCEEDED, f"{task.title} not SUCCEEDED"
    assert out.read_text() == "a\nb\nc\n"
    assert not work.exists(), "finally cleanup did not remove workdir"

    # Each task got exactly one terminal event, all SUCCEEDED.
    terminals = _terminals_by_task(events)
    assert set(terminals) == {t.id for t in graph}
    assert all(ev.type is TaskEventType.SUCCEEDED for ev in terminals.values())

    progress_events = [ev for ev in events if ev.type is TaskEventType.PROGRESS]
    assert len(progress_events) == 1
    progress = progress_events[0]
    assert progress.task is concat
    assert progress.previous_status is None
    assert progress.status == TaskStatus.RUNNING
    assert progress.progress_percent == 50.0
    assert progress.progress_message == "concatenating"

    # Cleanup ran after validate (timestamp ordering).
    assert terminals[cleanup.id].timestamp >= terminals[validate.id].timestamp

    # SQLite roundtrip: reopen and verify every task + the graph survives.
    reopened = SQLiteStore(db)
    assert len(reopened.tasks) == len(graph)
    for task in graph:
        persisted = reopened.tasks[task.id]
        assert persisted.status == task.status
        assert persisted.title == task.title
    persisted_graph = reopened.graphs[graph.id]
    assert set(persisted_graph) == set(graph)


# --- Test 2: failure cascade with optional finally --------------------------


def test_failure_cascade_with_optional_finally(tmp_path: Path) -> None:
    """One mid-layer task fails; publish becomes BLOCKED; optional finally runs."""
    sentinel = tmp_path / "cleanup-ran"

    setup = Task.bash("true", title="setup")
    step_ok = Task.bash("true", title="step_ok")
    step_fails = Task.bash("exit 1", title="step_fails")
    step_ok2 = Task.bash("true", title="step_ok2")
    publish = Task.bash("true", title="publish")
    cleanup = Task.bash(f"touch {sentinel}", title="cleanup")

    executor = TaskExecutor()
    events = _collect_events(executor)

    graph = TaskGraph(title="failure-cascade")
    graph.add(setup)
    graph.add(step_ok, after=[setup])
    graph.add(step_fails, after=[setup])
    graph.add(step_ok2, after=[setup])
    graph.add(publish, after=[step_ok, step_fails, step_ok2])
    graph.add(cleanup, after=[publish], finally_=True, required=False)

    graph.run(executor)

    # Per-task status.
    expected = {
        setup.id: TaskStatus.SUCCEEDED,
        step_ok.id: TaskStatus.SUCCEEDED,
        step_ok2.id: TaskStatus.SUCCEEDED,
        step_fails.id: TaskStatus.FAILED,
        publish.id: TaskStatus.BLOCKED,
        cleanup.id: TaskStatus.SUCCEEDED,
    }
    actual = {t.id: t.status for t in graph}
    assert actual == expected

    assert not graph.ok
    assert graph.failed == [step_fails]
    assert graph.blocked == [publish]
    assert publish.blocked_by == step_fails.id
    assert sentinel.exists(), "optional finally cleanup did not run"

    # Exactly one FAILED event, for step_fails.
    failed_events = [ev for ev in events if ev.type is TaskEventType.FAILED]
    assert len(failed_events) == 1
    assert failed_events[0].task_id == step_fails.id

    # publish (blocked) never STARTED.
    started_ids = {ev.task_id for ev in events if ev.type is TaskEventType.STARTED}
    assert publish.id not in started_ids
    # Every non-blocked task did start.
    assert started_ids == {setup.id, step_ok.id, step_fails.id, step_ok2.id, cleanup.id}


# --- Test 4: transition zoo + persistence sibling ---------------------------


def _build_transition_zoo() -> tuple[TaskGraph, dict[str, Task]]:
    """Build a ~20-node graph that exercises every executor state path.

    Returns (graph, tasks_by_label) so tests can assert by label.
    """
    t: dict[str, Task] = {}

    # L0 roots (parallel, all succeed).
    for name in ("R1", "R2", "R3"):
        t[name] = Task.bash("true", title=name)

    # L1: A,B,C,D,E (B fails via non-zero exit).
    t["A"] = Task.bash("true", title="A")
    t["B"] = Task.bash("exit 1", title="B")  # exit-code FAILED
    t["C"] = Task.bash("true", title="C")
    t["D"] = Task.bash("true", title="D")
    t["E"] = Task.bash("true", title="E")

    # L2.
    t["H"] = Task.bash("true", title="H")
    t["I"] = Task.bash("true", title="I")
    t["J"] = Task.bash("true", title="J")  # blocked by B
    t["K"] = Task.bash("true", title="K")
    t["L"] = Task.bash("sleep 5", title="L", timeout=0.1)  # timeout FAILED
    t["M"] = Task.bash("true", title="M")

    # L3 leaves.
    t["P"] = Task.bash("true", title="P")
    t["Q"] = Task.bash("true", title="Q")  # blocked transitively (B → J → Q)
    t["S"] = Task.bash("true", title="S")  # blocked transitively via L (timeout)

    # Finally layer.
    t["F1"] = Task.bash("true", title="F1")
    t["F2"] = Task.bash("exit 1", title="F2")  # optional finally that fails
    t["F3"] = Task.bash("true", title="F3")
    t["F4"] = Task.bash("true", title="F4")
    t["F5"] = Task.bash("true", title="F5")

    g = TaskGraph(title="transition-zoo")

    g.add(t["R1"])
    g.add(t["R2"])
    g.add(t["R3"])

    g.add(t["A"], after=[t["R1"]])
    g.add(t["B"], after=[t["R1"], t["R2"]])
    g.add(t["C"], after=[t["R2"]])
    g.add(t["D"], after=[t["R2"], t["R3"]])
    g.add(t["E"], after=[t["R3"]])

    g.add(t["H"], after=[t["A"]])
    g.add(t["I"], after=[t["C"]])
    g.add(t["J"], after=[t["A"], t["B"]])      # multi-parent, B FAILED → BLOCKED
    g.add(t["K"], after=[t["D"]])
    g.add(t["L"], after=[t["D"], t["E"]])      # will FAIL via timeout
    g.add(t["M"], after=[t["E"]])

    g.add(t["P"], after=[t["H"], t["K"]])      # both parents succeed
    g.add(t["Q"], after=[t["J"]])              # parent BLOCKED → BLOCKED
    g.add(t["S"], after=[t["L"]])              # parent FAILED (timeout) → BLOCKED

    g.add(t["F1"], after=[t["P"]],            finally_=True, required=True)
    g.add(t["F2"], after=[t["Q"]],            finally_=True, required=False)
    g.add(t["F3"], after=[t["F1"]],           finally_=True, required=True)
    g.add(t["F4"], after=[],                  finally_=True, required=True)
    g.add(t["F5"], after=[t["S"], t["L"]],    finally_=True, required=False)

    return g, t


EXPECTED_ZOO_STATUS: dict[str, TaskStatus] = {
    "R1": TaskStatus.SUCCEEDED, "R2": TaskStatus.SUCCEEDED, "R3": TaskStatus.SUCCEEDED,
    "A": TaskStatus.SUCCEEDED, "B": TaskStatus.FAILED, "C": TaskStatus.SUCCEEDED,
    "D": TaskStatus.SUCCEEDED, "E": TaskStatus.SUCCEEDED,
    "H": TaskStatus.SUCCEEDED, "I": TaskStatus.SUCCEEDED,
    "J": TaskStatus.BLOCKED,    # blocked by B
    "K": TaskStatus.SUCCEEDED,
    "L": TaskStatus.FAILED,     # timeout
    "M": TaskStatus.SUCCEEDED,
    "P": TaskStatus.SUCCEEDED,
    "Q": TaskStatus.BLOCKED,    # blocked transitively via J
    "S": TaskStatus.BLOCKED,    # blocked transitively via L
    "F1": TaskStatus.SUCCEEDED,
    "F2": TaskStatus.FAILED,    # optional finally that itself fails
    "F3": TaskStatus.SUCCEEDED,      # chained finally-on-finally
    "F4": TaskStatus.SUCCEEDED,      # finally with no upstream
    "F5": TaskStatus.SUCCEEDED,      # finally on blocked + failed deps still runs
}

EXPECTED_BLOCKED_LABELS = {"J", "Q", "S"}


def test_transition_zoo() -> None:
    """One graph that exercises every executor state path; literal assertions."""
    graph, t = _build_transition_zoo()
    executor = TaskExecutor()
    events = _collect_events(executor)

    graph.run(executor)

    # 1. Exact per-task status.
    actual_status = {label: t[label].status for label in EXPECTED_ZOO_STATUS}
    assert actual_status == EXPECTED_ZOO_STATUS

    # 2. graph.ok is False (B and L are required failures; F2 is optional).
    assert graph.ok is False

    # 3. Exact node identity for the three buckets (compare by id).
    expected_done_ids = {
        t[label].id for label, st in EXPECTED_ZOO_STATUS.items()
        if st is TaskStatus.SUCCEEDED
    }
    expected_failed_ids = {
        t[label].id for label, st in EXPECTED_ZOO_STATUS.items()
        if st is TaskStatus.FAILED
    }
    expected_blocked_ids = {t[label].id for label in EXPECTED_BLOCKED_LABELS}
    assert {tk.id for tk in graph.succeeded} == expected_done_ids
    assert {tk.id for tk in graph.failed} == expected_failed_ids
    assert {tk.id for tk in graph.blocked} == expected_blocked_ids
    assert graph.optional_failed == [t["F2"]]
    assert graph.required_failed == [t["B"], t["L"]]
    assert graph.required_blocked == [t["J"], t["Q"], t["S"]]

    # 4. Blocked tasks never STARTED and have no result.
    started_ids = {ev.task_id for ev in events if ev.type is TaskEventType.STARTED}
    for label in EXPECTED_BLOCKED_LABELS:
        assert t[label].id not in started_ids
        assert t[label].result is None

    # 5. Every task got exactly one terminal event (including BLOCKED).
    terminals = _terminals_by_task(events)
    assert set(terminals) == {t[label].id for label in EXPECTED_ZOO_STATUS}

    # 5b. BLOCKED tasks record the upstream parent that blocked them.
    assert t["J"].blocked_by == t["B"].id
    assert t["Q"].blocked_by == t["J"].id
    assert t["S"].blocked_by == t["L"].id

    # 6. Event ordering: for every dependency edge u → v where both terminated
    #    and v is not BLOCKED, u's terminal event precedes v's terminal event.
    #    BLOCKED is emitted as soon as one parent fails, so the other parents
    #    of a blocked node may legitimately terminate after it.
    for v in graph:
        if v.id not in terminals:
            continue
        if v.status == TaskStatus.BLOCKED:
            continue
        for u in graph.dependencies(v):
            if u.id not in terminals:
                continue
            assert terminals[u.id].timestamp <= terminals[v.id].timestamp, (
                f"terminal ordering violated: {u.title} after {v.title}"
            )

    # 7. termination_reason distinguishes B (non-zero exit) from L (timeout).
    assert t["B"].result is not None
    assert t["B"].result.termination_reason == "exit_code"
    assert t["L"].result is not None
    assert t["L"].result.termination_reason == "timeout"
    # SUCCEEDED tasks carry no termination_reason.
    assert t["A"].result is not None
    assert t["A"].result.termination_reason is None


def test_transition_zoo_persists(tmp_path: Path) -> None:
    """Same graph, persisted through SQLite; statuses + results survive reopen."""
    db = tmp_path / "zoo.db"
    store = SQLiteStore(db)
    executor = TaskExecutor(store=store)

    graph, t = _build_transition_zoo()
    store.graphs.save(graph)
    graph.run(executor)

    reopened = SQLiteStore(db)
    persisted_graph = reopened.graphs[graph.id]
    assert {tk.id for tk in persisted_graph} == {tk.id for tk in graph}

    for label, expected_status in EXPECTED_ZOO_STATUS.items():
        task = t[label]
        persisted = reopened.tasks[task.id]
        assert persisted.status == expected_status, f"{label} status differs"
        # Result presence parity (blocked tasks have no result).
        assert (persisted.result is None) == (task.result is None), (
            f"{label} result-presence differs"
        )
        if task.result is not None and persisted.result is not None:
            assert persisted.result.output == task.result.output
            assert persisted.result.error == task.result.error


# --- Test 3: mixed-type Copilot workflow (network) --------------------------


def _copilot_available() -> bool:
    """True if the Copilot SDK CLI shim is on PATH."""
    return shutil.which("copilot") is not None


@pytest.mark.skipif(
    not _copilot_available(),
    reason="Copilot SDK CLI not on PATH; skipping live mixed-type test",
)
def test_mixed_type_copilot_workflow(tmp_path: Path) -> None:
    """BASH → PROMPT → AGENT → BASH; persists through SQLite and roundtrips."""
    db = tmp_path / "mixed.db"
    artifact = tmp_path / "agent-output.txt"

    bash_collect = Task.bash(
        f"echo 'context line' > {artifact}", title="bash_collect",
    )
    prompt_summarize = Task.prompt(
        "Reply with exactly this text and no punctuation: mixed prompt ok",
        title="prompt_summarize",
        timeout=60,
    )
    agent_act = Task.agent(
        f"Append the single word SUCCEEDED to the file at {artifact}.",
        title="agent_act",
        timeout=120,
    )
    bash_verify = Task.bash(
        f"grep -q SUCCEEDED {artifact}", title="bash_verify",
    )

    store = SQLiteStore(db)
    executor = TaskExecutor(store=store)
    # PROMPT and AGENT handlers are auto-registered by default with the
    # Copilot SDK; no manual wiring needed.

    graph = TaskGraph(title="mixed-type")
    graph.add(bash_collect)
    graph.add(prompt_summarize, after=[bash_collect])
    graph.add(agent_act, after=[prompt_summarize])
    graph.add(bash_verify, after=[agent_act])
    store.graphs.save(graph)

    graph.run(executor)

    assert graph.ok, (
        f"graph not ok: statuses="
        f"{ {t.title: t.status.value for t in graph} }"
    )
    for task in graph:
        assert task.status == TaskStatus.SUCCEEDED
    assert prompt_summarize.result is not None
    assert "mixed prompt ok" in (prompt_summarize.result.output or "")

    # Persistence roundtrip.
    reopened = SQLiteStore(db)
    for task in graph:
        persisted = reopened.tasks[task.id]
        assert persisted.status == TaskStatus.SUCCEEDED
        assert persisted.type == task.type
        if task.result is not None:
            assert persisted.result is not None
            assert persisted.result.output == task.result.output
