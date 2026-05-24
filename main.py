r"""Executable demo: one shared ledger, two DAGs, results on the ledger.

After running both graphs, every task carries its own TaskResult. The
ledger is the single post-run view — no futures, no dict-by-id, just
walk the tasks.
"""

import time

from ttasks import Task, TaskGraph, TaskLedger, TaskType, default_executor


def main() -> None:
    """Build two DAGs against a shared ledger, run them, walk the ledger."""
    ledger = TaskLedger()

    # Graph alpha: X -> Y
    x = Task(title="X", type=TaskType.BASH, payload="echo x")
    y = Task(title="Y", type=TaskType.BASH, payload="echo y")
    alpha = TaskGraph(ledger=ledger)
    alpha[x] = []
    alpha[y] = [x]

    # Graph beta: P -> {Q, R}
    p = Task(title="P", type=TaskType.BASH, payload="echo p")
    q = Task(title="Q", type=TaskType.BASH, payload="echo q")
    r = Task(title="R", type=TaskType.BASH, payload="echo r")
    beta = TaskGraph(ledger=ledger)
    beta[p] = []
    beta[q] = [p]
    beta[r] = [p]

    executor = default_executor()
    start = time.monotonic()
    alpha.run(executor)
    beta.run(executor)
    elapsed = time.monotonic() - start

    # Single post-run view: walk the ledger, read each task's result.
    for task in ledger:
        output = task.result.output.strip() if task.result else "-"
        print(f"  {task.title}  status={task.status.value:<9} output={output!r}")

    print(f"\nwall time: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
