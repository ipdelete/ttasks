r"""Executable demo: one shared ledger, two DAGs, graph-level views.

Two graphs share a ledger but each has its own view of run state. The
graph itself answers post-run questions (ok? succeeded? failed?
blocked?) and topology questions (roots, leaves) without forcing the
caller to manage Futures or do set-differences against the ledger.

This file is a smoke test of the public API surface from a consumer's
point of view: every name imported below comes from `ttasks`, the
flat re-export surface, not from submodules.
"""

import time
from pathlib import Path

from ttasks import Task, TaskEvent, TaskGraph, TaskType, make_default_executor
from ttasks.storage.sqlite import SQLiteGraphLedger, SQLiteTaskLedger


def _bash(title: str, payload: str) -> Task:
    """Shorthand for the demo: create a bash one-liner task."""
    return Task(title=title, type=TaskType.BASH, payload=payload)


def _prompt(title: str, payload: str) -> Task:
    """Shorthand for the demo: create a Copilot prompt task."""
    return Task(title=title, type=TaskType.PROMPT, payload=payload, timeout=30)


def _agent(title: str, payload: str) -> Task:
    """Shorthand for the demo: create a Copilot agent task."""
    return Task(title=title, type=TaskType.AGENT, payload=payload, timeout=60)


def _print_graph(graph: TaskGraph) -> None:
    """Show topology + outcome for one graph."""
    roots = ", ".join(t.title for t in graph.roots()) or "(none)"
    leaves = ", ".join(t.title for t in graph.leaves()) or "(none)"
    print(
        f"\n  {graph.title}: {len(graph)} tasks  "
        f"roots=[{roots}]  leaves=[{leaves}]"
    )
    print(f"     ok={graph.ok}  "
          f"succeeded={len(graph.succeeded)}  "
          f"failed={len(graph.failed)}  "
          f"blocked={len(graph.blocked)}")
    for task in graph:
        output = task.result.output.strip() if task.result else "-"
        print(f"     {task.title}  status={task.status.value:<9} output={output!r}")


def _print_event(event: TaskEvent) -> None:
    """Print one lifecycle event from the executor event stream."""
    previous = event.previous_status.value if event.previous_status else "none"
    error = f" error={event.error!r}" if event.error else ""
    print(
        f"event: {event.type.value:<9} task={event.task.title:<2} "
        f"{previous}->{event.status.value}{error}"
    )


def main() -> None:
    """Build two DAGs against a shared ledger and inspect them via the graph."""
    ledger_path = Path("ttasks-demo.db")
    ledger_path.unlink(missing_ok=True)
    ledger = SQLiteTaskLedger(ledger_path)
    graphs = SQLiteGraphLedger(ledger_path, tasks=ledger)
    executor = make_default_executor()
    unsubscribe_print = executor.events.subscribe(_print_event)
    unsubscribe_save = executor.events.subscribe(lambda event: ledger.save(event.task))

    # Graph alpha: X -> Y -> Z (linear; Z is a tool-capable agent task).
    x = _bash("X", "echo x")
    y = _bash("Y", "echo y")
    z = _agent(
        "Z",
        "Read README.md in the current directory and summarize the project "
        "in one concise sentence.",
    )
    alpha = TaskGraph(ledger=ledger, title="alpha")
    alpha[x] = []
    alpha[y] = [x]
    alpha[z] = [y]
    graphs[alpha.id] = alpha

    # Graph beta: P -> {Q, R, S} (fan-out; S is a no-tools prompt task).
    p = _bash("P", "echo p")
    q = _bash("Q", "echo q")
    r = _bash("R", "echo r")
    s = _prompt(
        "S",
        "Reply with exactly this text and no punctuation: ttasks prompt ok",
    )
    beta = TaskGraph(ledger=ledger, title="beta")
    beta[p] = []
    beta[q] = [p]
    beta[r] = [p]
    beta[s] = [p]
    graphs[beta.id] = beta

    # Graph gamma: F fails, G is blocked. Demonstrates the blocked view.
    f = _bash("F", "exit 1")
    g = _bash("G", "echo g")
    gamma = TaskGraph(ledger=ledger, title="gamma")
    gamma[f] = []
    gamma[g] = [f]
    graphs[gamma.id] = gamma

    # run() returns the graph itself, so calls are chainable.
    start = time.monotonic()
    try:
        alpha.run(executor)
        beta.run(executor)
        gamma.run(executor)
    finally:
        unsubscribe_print()
        unsubscribe_save()
    elapsed = time.monotonic() - start

    for graph in [alpha, beta, gamma]:
        _print_graph(graph)

    # The shared ledger is the union: every task across every graph.
    print(f"\n  shared ledger holds {len(ledger)} tasks "
          f"across {len(graphs)} graphs")
    print(f"\nwall time: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
