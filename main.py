r"""Executable demo: one shared ledger, two independent DAGs.

Graph alpha:           Graph beta:
        X                     P
        |                    / \
        Y                   Q   R

After running both graphs, the single shared TaskLedger contains all
five tasks. The graphs themselves are independent — each runs on its
own pool, neither knows about the other.
"""

import time

from executor import default_executor
from ledger import TaskLedger
from task import Task, TaskType
from workflow import TaskGraph


def main() -> None:
    """Run two graphs against one shared ledger; print the merged registry."""
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

    print(f"alpha: {alpha}")
    print(f"beta:  {beta}")
    print(f"ledger before run: {len(ledger)} task(s)")

    executor = default_executor()
    start = time.monotonic()
    alpha.run(executor)
    beta.run(executor)
    elapsed = time.monotonic() - start

    print(f"\nledger after run: {len(ledger)} task(s)")
    for task in ledger:
        print(f"  {task.title}: status={task.status.value}")
    print(f"\nwall time: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
