r"""Executable demo: run a diamond DAG of tasks on top of ThreadPoolExecutor.

Graph:

        A
       / \
      B   C       (B and C run in parallel once A finishes)
       \ /
        D        (D runs once both B and C finish)

No new SDK abstractions. The DAG lives entirely in the consumer:
  * ThreadPoolExecutor provides bounded concurrency.
  * Future.add_done_callback handles "submit dependents when ready".
  * A small lock guards the shared futures dict and the readiness check.
"""

import time
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event, RLock

from executor import default_executor
from ledger import TaskLedger
from task import Task, TaskType


def main() -> None:
    """Build a diamond DAG, run it, print arrival times."""
    ledger = TaskLedger()
    executor = default_executor()

    # One-second sleeps make the parallelism obvious in the timestamps.
    a = Task(title="A", type=TaskType.BASH, payload="sleep 1; echo A done")
    b = Task(title="B", type=TaskType.BASH, payload="sleep 1; echo B done")
    c = Task(title="C", type=TaskType.BASH, payload="sleep 1; echo C done")
    d = Task(title="D", type=TaskType.BASH, payload="sleep 1; echo D done")
    tasks: dict[str, Task] = {t.id: t for t in (a, b, c, d)}
    for t in tasks.values():
        ledger[t.id] = t

    # The DAG itself: task_id -> list of upstream task_ids it depends on.
    deps: dict[str, list[str]] = {
        a.id: [],
        b.id: [a.id],
        c.id: [a.id],
        d.id: [b.id, c.id],
    }

    futures: dict[str, Future] = {}
    lock = RLock()
    done = Event()
    start = time.monotonic()

    def stamp() -> str:
        return f"[{time.monotonic() - start:5.2f}s]"

    def submit(task_id: str, pool: ThreadPoolExecutor) -> None:
        """Schedule one task and register its done-callback."""
        task = tasks[task_id]
        print(f"{stamp()} submit  {task.title}")
        fut = pool.submit(executor.execute, task)
        futures[task_id] = fut
        fut.add_done_callback(lambda f, tid=task_id: on_finish(tid, f, pool))

    def ready(task_id: str) -> bool:
        """Return True if every dep has completed successfully."""
        return all(
            d in futures and futures[d].done() and futures[d].exception() is None
            for d in deps[task_id]
        )

    def on_finish(task_id: str, fut: Future, pool: ThreadPoolExecutor) -> None:
        """Print arrival, submit any newly-unblocked dependents, signal done."""
        title = tasks[task_id].title
        err = fut.exception()
        if err is not None:
            print(f"{stamp()} fail    {title}: {err}")
        else:
            print(f"{stamp()} finish  {title}")

        with lock:
            for dependent_id, dep_list in deps.items():
                if dependent_id in futures:
                    continue
                if dep_list and ready(dependent_id):
                    submit(dependent_id, pool)

            if len(futures) == len(deps) and all(f.done() for f in futures.values()):
                done.set()

    with ThreadPoolExecutor(max_workers=4) as pool:
        with lock:
            for task_id, dep_list in deps.items():
                if not dep_list:
                    submit(task_id, pool)
        done.wait()

    print(f"\nwall time: {time.monotonic() - start:.2f}s")


if __name__ == "__main__":
    main()
