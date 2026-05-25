"""TaskGraph: a DAG of Tasks executed on a ThreadPoolExecutor.

The graph owns its own task references and dependency edges; it does not
delegate task storage to a separate ledger. Persistence is the job of
``ttasks.store.Store`` (consulted via ``TaskExecutor`` for auto-save).
"""

import uuid
from collections.abc import Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from threading import Event, RLock

from ._executor import TaskExecutor
from ._task import Task, TaskStatus


class TaskGraph:
    """A directed acyclic graph of :class:`Task` objects.

    Tasks and edges are stored on the graph itself. ``graph.run(executor)``
    submits ready tasks to a thread pool; if the executor was constructed
    with a ``store``, every lifecycle transition is auto-persisted there.
    """

    def __init__(self, *, title: str = "") -> None:
        """Create a graph with display ``title``."""
        if not isinstance(title, str):
            raise TypeError("title must be a str")
        self._id = str(uuid.uuid4())
        self.title = title
        self.created_at = datetime.now()
        # task_id -> Task; insertion order preserved for stable iteration.
        self._tasks: dict[str, Task] = {}
        # task_id -> list of upstream task_ids.
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

    def add(
        self,
        task: Task,
        *,
        after: Iterable[Task] = (),
        finally_: bool = False,
        required: bool = True,
    ) -> None:
        """Register ``task`` in the graph.

        ``after`` lists upstream tasks that must complete before ``task`` runs.
        ``finally_=True`` registers a finally task: it becomes ready once every
        listed upstream task is no longer active, regardless of success.
        ``required=False`` is only meaningful with ``finally_=True`` and marks
        the task as optional so its failure does not make :attr:`ok` false.
        """
        if not isinstance(task, Task):
            raise TypeError(f"Expected Task, got {type(task).__name__}")
        if not isinstance(finally_, bool):
            raise TypeError("finally_ must be a bool")
        if not isinstance(required, bool):
            raise TypeError("required must be a bool")
        if not required and not finally_:
            raise ValueError("required=False is only valid with finally_=True")

        self._tasks[task.id] = task
        self._deps[task.id] = [d.id for d in after]
        if finally_:
            self._finally.add(task.id)
            if required:
                self._optional.discard(task.id)
            else:
                self._optional.add(task.id)
        else:
            self._finally.discard(task.id)
            self._optional.discard(task.id)

    def __setitem__(self, task: Task, deps: Iterable[Task]) -> None:
        """Register ``task`` in the graph and record its upstream dependencies.

        Protocol-level alternative to :meth:`add` for callers using mapping
        syntax. Prefer :meth:`add` in new code.
        """
        self.add(task, after=deps)

    def add_finally(
        self,
        task: Task,
        after: Iterable[Task],
        *,
        required: bool = True,
    ) -> None:
        """Register a finally task. Prefer :meth:`add` with ``finally_=True``."""
        self.add(task, after=after, finally_=True, required=required)

    def __getitem__(self, task: Task) -> list[Task]:
        """Return the upstream :class:`Task` objects ``task`` depends on."""
        return self.dependencies(task)

    def __contains__(self, task: object) -> bool:
        """Return whether ``task`` is a Task registered in this graph."""
        return isinstance(task, Task) and task.id in self._deps

    def __iter__(self) -> Iterator[Task]:
        """Iterate over graph tasks in insertion order."""
        return (self._tasks[tid] for tid in self._deps)

    def __len__(self) -> int:
        """Return the number of tasks registered in this graph."""
        return len(self._deps)

    def __repr__(self) -> str:
        """Return a concise representation including dependency edges."""
        edges = ", ".join(
            f"{self._tasks[d].title}->{self._tasks[t].title}"
            for t, ds in self._deps.items()
            for d in ds
        )
        return f"TaskGraph({len(self)} tasks, edges=[{edges}])"

    # ---- public introspection (used by persistence backends) ---------------

    @property
    def id(self) -> str:
        """Return the immutable graph identity."""
        return self._id

    def dependencies(self, task: Task) -> list[Task]:
        """Return the direct upstream tasks of ``task``."""
        return [self._tasks[d] for d in self._deps[task.id]]

    def is_finally(self, task: Task) -> bool:
        """Return whether ``task`` was registered via :meth:`add_finally`."""
        return task.id in self._finally

    def is_optional(self, task: Task) -> bool:
        """Return whether ``task`` is a finally task with ``required=False``."""
        return task.id in self._optional

    def items(self) -> Iterator[tuple[Task, list[Task]]]:
        """Yield ``(task, deps)`` pairs in insertion order."""
        for tid in self._deps:
            task = self._tasks[tid]
            yield task, [self._tasks[d] for d in self._deps[tid]]

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
        """Tasks skipped during the most recent :meth:`run`.

        A task is blocked when an upstream task failed/cancelled, or when the
        task itself could not be submitted because its lifecycle state cannot
        move to RUNNING. Distinct from "PENDING because run() was never called":
        this list is populated only by :meth:`run` and reset on each call.
        """
        return [self._tasks[tid] for tid in self._blocked]

    @property
    def errors(self) -> dict[str, BaseException]:
        """Exceptions raised by task futures during the most recent run."""
        return dict(self._errors)

    @property
    def ok(self) -> bool:
        """True iff every required task succeeded without run errors."""
        return all(
            self._tasks[tid].status == TaskStatus.DONE
            for tid in self._deps
            if tid not in self._optional
        )

    # ---- topology views -----------------------------------------------------

    def roots(self) -> list[Task]:
        """Tasks with no upstream dependencies."""
        return [self._tasks[tid] for tid, ds in self._deps.items() if not ds]

    def leaves(self) -> list[Task]:
        """Tasks that no other task depends on."""
        depended_on: set[str] = set()
        for ds in self._deps.values():
            depended_on.update(ds)
        return [
            self._tasks[tid] for tid in self._deps if tid not in depended_on
        ]

    # ---- validation ---------------------------------------------------------

    def _validate(self) -> None:
        """Raise ValueError on missing deps or cycles. Called from :meth:`run`."""
        for tid, ds in self._deps.items():
            for d in ds:
                if d not in self._deps:
                    raise ValueError(
                        f"task {self._tasks[tid].title!r} depends on "
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
        """Execute the DAG. Blocks until done. Returns ``self`` for chaining.

        Failure policy: if a task fails or is cancelled, every descendant is
        marked blocked and never submitted; the run terminates instead of
        hanging. Already-DONE tasks count as satisfied dependencies so a graph
        can be run again or extended after partial completion. Use
        :attr:`failed` and :attr:`blocked` to inspect the outcome.
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
                task = self._tasks[tid]
                if task.status == TaskStatus.DONE:
                    return True
                return (
                    tid in futures
                    and futures[tid].done()
                    and futures[tid].exception() is None
                )

            def inactive(tid: str) -> bool:
                """Return whether tid can no longer change in this run."""
                task = self._tasks[tid]
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
                    or self._tasks[d].status == TaskStatus.CANCELLED
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
                    self._tasks[tid].status == TaskStatus.DONE
                    or tid in blocked
                    or (tid in futures and futures[tid].done())
                )

            def upstream_tasks(tid: str) -> dict[str, Task]:
                """Return direct upstream task refs for tid from the graph."""
                return {dep_id: self._tasks[dep_id] for dep_id in self._deps[tid]}

            def submit(tid: str) -> None:
                """Submit tid to the thread pool and register its callback."""
                fut = pool.submit(
                    executor.execute,
                    self._tasks[tid],
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
                        task = self._tasks[tid]
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
