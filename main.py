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

from ttasks import (
    GraphLedger,
    Task,
    TaskEvent,
    TaskGraph,
    TaskLedger,
    TaskType,
    make_default_executor,
)


def _bash(title: str, payload: str) -> Task:
    """Shorthand for the demo: every task is a bash one-liner."""
    return Task(title=title, type=TaskType.BASH, payload=payload)


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
    ledger = TaskLedger()
    graphs = GraphLedger()
    executor = make_default_executor()
    unsubscribe = executor.events.subscribe(_print_event)

    # Graph alpha: X -> Y (linear, both succeed).
    x = _bash("X", "echo x")
    y = _bash("Y", "echo y")
    alpha = TaskGraph(ledger=ledger, title="alpha")
    alpha[x] = []
    alpha[y] = [x]
    graphs[alpha.id] = alpha

    # Graph beta: P -> {Q, R} (diamond top half, all succeed).
    p = _bash("P", "echo p")
    q = _bash("Q", "echo q")
    r = _bash("R", "echo r")
    beta = TaskGraph(ledger=ledger, title="beta")
    beta[p] = []
    beta[q] = [p]
    beta[r] = [p]
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
        unsubscribe()
    elapsed = time.monotonic() - start

    for graph in graphs:
        _print_graph(graph)

    # The shared ledger is the union: every task across every graph.
    print(f"\n  shared ledger holds {len(ledger)} tasks "
          f"across {len(graphs)} graphs")
    print(f"\nwall time: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
