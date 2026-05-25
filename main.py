r"""Executable demo: one shared store, three DAGs, graph-level views.

The executor is wired to a :class:`SQLiteStore`, so every task is durably
persisted on each lifecycle transition without any event-subscriber
plumbing. Graphs are persisted explicitly with ``store.graphs[g.id] = g``.

The graph itself answers post-run questions (ok? succeeded? failed?
blocked?) and topology questions (roots, leaves) without forcing the
caller to manage Futures.

This file is a smoke test of the public API surface from a consumer's
point of view: every name imported below comes from ``ttasks``, the
flat re-export surface, not from submodules.
"""

import time
from pathlib import Path

from ttasks import Task, TaskEvent, TaskGraph, make_default_executor
from ttasks.storage.sqlite import SQLiteStore


def _bash(title: str, payload: str) -> Task:
    """Shorthand for the demo: create a bash one-liner task."""
    return Task.bash(payload, title=title)


def _prompt(title: str, payload: str) -> Task:
    """Shorthand for the demo: create a Copilot prompt task."""
    return Task.prompt(payload, title=title, timeout=30)


def _agent(title: str, payload: str) -> Task:
    """Shorthand for the demo: create a Copilot agent task."""
    return Task.agent(payload, title=title, timeout=60)


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
    """Build three DAGs against a shared SQLite store and inspect them."""
    store_path = Path("ttasks-demo.db")
    store_path.unlink(missing_ok=True)
    store = SQLiteStore(store_path)

    # The executor auto-persists every task transition to store.tasks.
    executor = make_default_executor(store=store)
    unsubscribe_print = executor.events.subscribe(_print_event)

    # Graph alpha: X -> Y -> Z (linear; Z is a tool-capable agent task).
    x = _bash("X", "echo x")
    y = _bash("Y", "echo y")
    z = _agent(
        "Z",
        "Read README.md in the current directory and summarize the project "
        "in one concise sentence.",
    )
    alpha = TaskGraph(title="alpha")
    alpha[x] = []
    alpha[y] = [x]
    alpha[z] = [y]
    store.graphs[alpha.id] = alpha

    # Graph beta: P -> {Q, R, S} (fan-out; S is a no-tools prompt task).
    p = _bash("P", "echo p")
    q = _bash("Q", "echo q")
    r = _bash("R", "echo r")
    s = _prompt(
        "S",
        "Reply with exactly this text and no punctuation: ttasks prompt ok",
    )
    beta = TaskGraph(title="beta")
    beta[p] = []
    beta[q] = [p]
    beta[r] = [p]
    beta[s] = [p]
    store.graphs[beta.id] = beta

    # Graph gamma: F fails, G is blocked. Demonstrates the blocked view.
    f = _bash("F", "exit 1")
    g = _bash("G", "echo g")
    gamma = TaskGraph(title="gamma")
    gamma[f] = []
    gamma[g] = [f]
    store.graphs[gamma.id] = gamma

    # run() returns the graph itself, so calls are chainable.
    start = time.monotonic()
    try:
        alpha.run(executor)
        beta.run(executor)
        gamma.run(executor)
    finally:
        unsubscribe_print()
    elapsed = time.monotonic() - start

    for graph in [alpha, beta, gamma]:
        _print_graph(graph)

    print(f"\n  store holds {len(store.tasks)} tasks "
          f"across {len(store.graphs)} graphs")
    if executor.persistence_errors:
        print(f"  persistence errors: {len(executor.persistence_errors)}")
    print(f"\nwall time: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
