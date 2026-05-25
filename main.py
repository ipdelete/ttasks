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

from ttasks import Task, TaskEvent, TaskExecutor, TaskGraph
from ttasks.storage.sqlite import SQLiteStore


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
    executor = TaskExecutor(store=store)
    unsubscribe_print = executor.events.subscribe(_print_event)

    # Graph alpha: X -> Y -> Z (linear; Z is a tool-capable agent task).
    x = Task.bash("echo x", title="X")
    y = Task.bash("echo y", title="Y")
    z = Task.agent(
        "Read README.md in the current directory and summarize the project "
        "in one concise sentence.",
        title="Z",
        timeout=60,
    )
    alpha = TaskGraph(title="alpha")
    alpha.add(x)
    alpha.add(y, after=[x])
    alpha.add(z, after=[y])
    store.graphs.save(alpha)

    # Graph beta: P -> {Q, R, S} (fan-out; S is a no-tools prompt task).
    p = Task.bash("echo p", title="P")
    q = Task.bash("echo q", title="Q")
    r = Task.bash("echo r", title="R")
    s = Task.prompt(
        "Reply with exactly this text and no punctuation: ttasks prompt ok",
        title="S",
        timeout=30,
    )
    beta = TaskGraph(title="beta")
    beta.add(p)
    beta.add(q, after=[p])
    beta.add(r, after=[p])
    beta.add(s, after=[p])
    store.graphs.save(beta)

    # Graph gamma: F fails, G is blocked. Demonstrates the blocked view.
    f = Task.bash("exit 1", title="F")
    g = Task.bash("echo g", title="G")
    gamma = TaskGraph(title="gamma")
    gamma.add(f)
    gamma.add(g, after=[f])
    store.graphs.save(gamma)

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
