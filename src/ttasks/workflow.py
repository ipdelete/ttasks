"""TaskGraph: a DAG runner that composes TaskLedger + TaskExecutor.

This module is a *consumer* of the SDK, not part of it. The DAG lives in
TaskGraph; the tasks themselves live in a TaskLedger; execution goes
through a TaskExecutor on a ThreadPoolExecutor. Nothing about Task,
TaskLedger, or TaskExecutor needs to change to support DAGs.
"""

import uuid
from collections.abc import Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from threading import Event, RLock

from .executor import TaskExecutor
from .ledger import TaskLedger
from .task import Task, TaskStatus


class TaskGraph:
    """A directed acyclic graph of Tasks.

    Tasks are stored in an associated TaskLedger; the edges (dependencies)
    are stored on the graph. The two are kept in sync: assigning a task to
    the graph registers it in the ledger.
    """

    def __init__(
        self,
        ledger: TaskLedger | None = None,
        *,
        title: str = "",
    ) -> None:
        """Create a graph backed by ledger, or by a fresh TaskLedger."""
        if not isinstance(title, str):
            raise TypeError("title must be a str")
        self._id = str(uuid.uuid4())
        self.title = title
        self.created_at = datetime.now()
        self._ledger = ledger if ledger is not None else TaskLedger()
        self._deps: dict[str, list[str]] = {}
        # Tasks skipped during the most recent run() because an upstream task
        # failed/cancelled or because the task itself could not be submitted.
        # Cleared at the start of each run().
        self._blocked: set[str] = set()
        # Exceptions raised by submitted task futures during the most recent
        # run, keyed by task id. Cleared at the start of each run().
        self._errors: dict[str, BaseException] = {}
        # Finally tasks run after their dependencies finish, fail, cancel, or
        # become blocked. Optional tasks report failures without making ok false.
        self._finally: set[str] = set()
        self._optional: set[str] = set()

    # ---- mapping protocol ---------------------------------------------------

    def __setitem__(self, task: Task, deps: Iterable[Task]) -> None:
        """Register `task` in the ledger and record its upstream dependencies."""
        self._ledger[task.id] = task
        self._deps[task.id] = [d.id for d in deps]
        self._finally.discard(task.id)
        self._optional.discard(task.id)

    def add_finally(
        self,
        task: Task,
        after: Iterable[Task],
        *,
        required: bool = True,
    ) -> None:
        """Register a task that runs after dependencies are no longer active.

        Unlike normal graph dependencies, ``after`` tasks do not need to
        succeed. The finally task becomes ready once every listed task has
        succeeded, failed, been cancelled, been blocked, or raised an executor
        error. If ``required`` is false, failures from this task are visible in
        graph views but do not make :attr:`ok` false.
        """
        if not isinstance(required, bool):
            raise TypeError("required must be a bool")
        self._ledger[task.id] = task
        self._deps[task.id] = [d.id for d in after]
        self._finally.add(task.id)
        if required:
            self._optional.discard(task.id)
        else:
            self._optional.add(task.id)

    def __getitem__(self, task: Task) -> list[Task]:
        """Return the upstream Task objects that `task` depends on."""
        return [self._ledger[d] for d in self._deps[task.id]]

    def __contains__(self, task: object) -> bool:
        """Return whether task is a Task registered in this graph."""
        return isinstance(task, Task) and task.id in self._deps

    def __iter__(self) -> Iterator[Task]:
        """Iterate over graph tasks in insertion order."""
        return (self._ledger[tid] for tid in self._deps)

    def __len__(self) -> int:
        """Return the number of tasks registered in this graph."""
        return len(self._deps)

    def __repr__(self) -> str:
        """Return a concise representation including dependency edges."""
        edges = ", ".join(
            f"{self._ledger[d].title}->{self._ledger[t].title}"
            for t, ds in self._deps.items()
            for d in ds
        )
        return f"TaskGraph({len(self)} tasks, edges=[{edges}])"

    # ---- accessors ----------------------------------------------------------

    @property
    def id(self) -> str:
        """Return the immutable graph identity."""
        return self._id

    @property
    def ledger(self) -> TaskLedger:
        """The TaskLedger backing this graph."""
        return self._ledger

    # ---- status views (post-run) --------------------------------------------

    @property
    def succeeded(self) -> list[Task]:
        """Tasks in this graph whose status is DONE."""
        return [t for t in self if t.status == TaskStatus.DONE]

    @property
    def failed(self) -> list[Task]:
        """Tasks in this graph whose status is FAILED."""
        return [t for t in self if t.status == TaskStatus.FAILED]

    @property
    def cancelled(self) -> list[Task]:
        """Tasks in this graph whose status is CANCELLED."""
        return [t for t in self if t.status == TaskStatus.CANCELLED]

    @property
    def blocked(self) -> list[Task]:
        """Tasks skipped during the most recent run().

        A task is blocked when an upstream task failed/cancelled, or when the
        task itself could not be submitted because its lifecycle state cannot
        move to RUNNING. Distinct from "PENDING because run() was never called":
        this list is populated only by run() and reset on each call.
        """
        return [self._ledger[tid] for tid in self._blocked]

    @property
    def errors(self) -> dict[str, BaseException]:
        """Exceptions raised by task futures during the most recent run."""
        return dict(self._errors)

    @property
    def ok(self) -> bool:
        """True iff every required task succeeded without run errors."""
        required = [tid for tid in self._deps if tid not in self._optional]
        return (
            all(self._ledger[tid].status == TaskStatus.DONE for tid in required)
            and not any(tid not in self._optional for tid in self._errors)
            and not any(tid not in self._optional for tid in self._blocked)
        )

    # ---- topology views -----------------------------------------------------

    def roots(self) -> list[Task]:
        """Tasks with no upstream dependencies."""
        return [self._ledger[tid] for tid, ds in self._deps.items() if not ds]

    def leaves(self) -> list[Task]:
        """Tasks that no other task depends on."""
        depended_on: set[str] = set()
        for ds in self._deps.values():
            depended_on.update(ds)
        return [
            self._ledger[tid] for tid in self._deps if tid not in depended_on
        ]

    # ---- validation ---------------------------------------------------------

    def _validate(self) -> None:
        """Raise ValueError on missing deps or cycles. Called from run()."""
        for tid, ds in self._deps.items():
            for d in ds:
                if d not in self._deps:
                    raise ValueError(
                        f"task {self._ledger[tid].title!r} depends on "
                        f"unregistered task id {d!r}"
                    )
        # Kahn's algorithm: count visited nodes vs total.
        indeg = {tid: len(ds) for tid, ds in self._deps.items()}
        queue = [tid for tid, n in indeg.items() if n == 0]
        visited = 0
        while queue:
            cur = queue.pop()
            visited += 1
            for tid, ds in self._deps.items():
                if cur in ds:
                    indeg[tid] -= 1
                    if indeg[tid] == 0:
                        queue.append(tid)
        if visited != len(self._deps):
            raise ValueError("TaskGraph contains a cycle")

    # ---- execution ----------------------------------------------------------

    def run(
        self,
        executor: TaskExecutor,
        max_workers: int = 4,
    ) -> "TaskGraph":
        """Execute the DAG. Blocks until done. Returns self for chaining.

        Failure policy: if a task fails or is cancelled, every descendant is
        marked blocked and never submitted; the run terminates instead of
        hanging. Already-DONE tasks count as satisfied dependencies so a graph
        can be run again or extended after partial completion. Use graph.failed
        and graph.blocked to inspect the outcome.
        """
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        self._validate()
        # Reset run-scoped state from any previous run.
        self._blocked = set()
        self._errors = {}

        # Empty graph: nothing to wait for. Return early to avoid deadlock.
        if not self._deps:
            return self

        futures: dict[str, Future] = {}
        blocked: set[str] = self._blocked
        lock = RLock()
        done = Event()

        with ThreadPoolExecutor(max_workers=max_workers) as pool:

            def succeeded(tid: str) -> bool:
                """Return whether tid is already done or succeeded in this run."""
                task = self._ledger[tid]
                if task.status == TaskStatus.DONE:
                    return True
                return (
                    tid in futures
                    and futures[tid].done()
                    and futures[tid].exception() is None
                )

            def inactive(tid: str) -> bool:
                """Return whether tid can no longer change in this run."""
                task = self._ledger[tid]
                return (
                    task.status in {
                        TaskStatus.DONE,
                        TaskStatus.FAILED,
                        TaskStatus.CANCELLED,
                    }
                    or tid in blocked
                    or tid in self._errors
                    or (tid in futures and futures[tid].done())
                )

            def ready(tid: str) -> bool:
                """Return whether all upstream dependencies are satisfied."""
                if tid in self._finally:
                    return all(inactive(d) for d in self._deps[tid])
                return all(succeeded(d) for d in self._deps[tid])

            def dep_failed_or_blocked(tid: str) -> bool:
                """Return whether any dependency prevents tid from running."""
                return any(
                    d in blocked
                    or d in self._errors
                    or self._ledger[d].status == TaskStatus.CANCELLED
                    or (
                        d in futures
                        and futures[d].done()
                        and futures[d].exception() is not None
                    )
                    for d in self._deps[tid]
                )

            def finished(tid: str) -> bool:
                """Return whether tid no longer needs scheduler attention."""
                return (
                    self._ledger[tid].status == TaskStatus.DONE
                    or tid in blocked
                    or (tid in futures and futures[tid].done())
                )

            def upstream_tasks(tid: str) -> dict[str, Task]:
                """Return direct upstream task refs for tid from the ledger."""
                return {dep_id: self._ledger[dep_id] for dep_id in self._deps[tid]}

            def submit(tid: str) -> None:
                """Submit tid to the thread pool and register its callback."""
                fut = pool.submit(
                    executor.execute,
                    self._ledger[tid],
                    upstream_tasks(tid),
                )
                futures[tid] = fut
                fut.add_done_callback(lambda f, t=tid: on_finish(t, f))

            def schedule() -> None:
                """Advance scheduling until no more tasks can change state."""
                # Propagate blocking transitively and submit tasks whose deps
                # are satisfied. Already-DONE tasks are treated as satisfied so
                # graph reruns and newly-added descendants do not deadlock.
                changed = True
                while changed:
                    changed = False
                    for tid in self._deps:
                        task = self._ledger[tid]
                        if (
                            tid in futures
                            or tid in blocked
                            or task.status == TaskStatus.DONE
                        ):
                            continue
                        if tid not in self._finally and dep_failed_or_blocked(tid):
                            blocked.add(tid)
                            changed = True
                        elif ready(tid):
                            if task.can_transition_to(TaskStatus.RUNNING):
                                submit(tid)
                            else:
                                blocked.add(tid)
                            changed = True

                # Exit when every task is done, has finished this run, or is blocked.
                if all(finished(tid) for tid in self._deps):
                    done.set()

            def on_finish(tid: str, fut: Future) -> None:
                """Resume scheduling after a submitted task future completes."""
                with lock:
                    exception = fut.exception()
                    if exception is not None:
                        self._errors[tid] = exception
                    schedule()

            # Kick off every task whose dependencies are already satisfied.
            with lock:
                schedule()

            done.wait()

        return self
