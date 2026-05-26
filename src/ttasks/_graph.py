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
        deps: list[str] = []
        for dep in after:
            if dep.id not in deps:
                deps.append(dep.id)
        self._deps[task.id] = deps
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

        Mapping-syntax sugar for :meth:`add` (without ``finally_``).
        """
        self.add(task, after=deps)

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
        """Return whether ``task`` was registered with ``finally_=True``."""
        return task.id in self._finally

    def is_optional(self, task: Task) -> bool:
        """Return whether ``task`` is a finally task with ``required=False``."""
        return task.id in self._optional

    @property
    def finally_tasks(self) -> list[Task]:
        """Tasks registered with ``finally_=True``, in graph insertion order."""
        return [t for t in self if t.id in self._finally]

    @property
    def optional_tasks(self) -> list[Task]:
        """Tasks registered with ``finally_=True, required=False``."""
        return [t for t in self if t.id in self._optional]

    @property
    def required_tasks(self) -> list[Task]:
        """Tasks whose failure contributes to :attr:`ok`, in insertion order."""
        return [t for t in self if t.id not in self._optional]

    def items(self) -> Iterator[tuple[Task, list[Task]]]:
        """Yield ``(task, deps)`` pairs in insertion order."""
        for tid in self._deps:
            task = self._tasks[tid]
            yield task, [self._tasks[d] for d in self._deps[tid]]

    # ---- status views (post-run) --------------------------------------------

    @property
    def succeeded(self) -> list[Task]:
        """Tasks in this graph whose status is SUCCEEDED."""
        return [t for t in self if t.status == TaskStatus.SUCCEEDED]

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
        """Tasks in this graph whose status is BLOCKED."""
        return [t for t in self if t.status == TaskStatus.BLOCKED]

    @property
    def optional_failed(self) -> list[Task]:
        """Optional tasks in FAILED status.

        This is a status-specific view for reporting. Use :attr:`ok` as the
        authoritative graph success predicate.
        """
        return [t for t in self.failed if t.id in self._optional]

    @property
    def required_failed(self) -> list[Task]:
        """Required tasks in FAILED status.

        This is a status-specific view for reporting. Use :attr:`ok` as the
        authoritative graph success predicate.
        """
        return [t for t in self.failed if t.id not in self._optional]

    @property
    def required_blocked(self) -> list[Task]:
        """Required tasks in BLOCKED status.

        This is a status-specific view for reporting. Use :attr:`ok` as the
        authoritative graph success predicate.
        """
        return [t for t in self.blocked if t.id not in self._optional]

    @property
    def errors(self) -> dict[str, BaseException]:
        """Exceptions raised by task futures during the most recent run."""
        return dict(self._errors)

    @property
    def ok(self) -> bool:
        """True iff every required task succeeded without run errors."""
        return all(
            self._tasks[tid].status == TaskStatus.SUCCEEDED
            and tid not in self._errors
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
        """Raise ValueError on missing deps, cycles, or stale RUNNING state.

        Called from :meth:`run`. A task already in RUNNING cannot transition
        again and would deadlock the scheduler, so we surface it eagerly
        rather than time out.
        """
        for task in self._tasks.values():
            if task.status == TaskStatus.RUNNING:
                raise ValueError(
                    f"task {task.title!r} is RUNNING; reset before run()"
                )
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
        hanging. Already-SUCCEEDED tasks count as satisfied dependencies so a graph
        can be run again or extended after partial completion. Use
        :attr:`failed` and :attr:`blocked` to inspect the outcome.
        """
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        self._validate()
        # Auto-save after validation so an invalid graph leaves no trace.
        executor._persist_graph(self)

        try:
            return self._run_inner(executor, max_workers)
        finally:
            executor._persist_graph(self)

    def _run_inner(
        self,
        executor: TaskExecutor,
        max_workers: int,
    ) -> "TaskGraph":
        """Inner scheduler loop, split out so :meth:`run` can wrap save logic."""
        # Reset run-scoped state from any previous run.
        self._errors = {}

        # Empty graph: nothing to wait for. Return early to avoid deadlock.
        if not self._deps:
            return self

        # Snapshot tasks that entered this run already BLOCKED. Only these
        # are eligible for in-run retry; tasks that get BLOCKED during this
        # run stay terminal so finally readiness and inactive() remain
        # consistent within a single invocation.
        entering_blocked = {
            tid for tid, t in self._tasks.items() if t.status == TaskStatus.BLOCKED
        }

        futures: dict[str, Future] = {}
        lock = RLock()
        done = Event()
        # ``scheduler_error`` is the single-slot escape hatch for surfacing
        # exceptions raised inside ThreadPoolExecutor callbacks (e.g., the
        # no-progress guard) so they propagate out of run() instead of being
        # silently swallowed in a callback thread.
        scheduler_error: list[BaseException] = []

        with ThreadPoolExecutor(max_workers=max_workers) as pool:

            def succeeded(tid: str) -> bool:
                """Return whether tid is already done or succeeded in this run."""
                return self._tasks[tid].status == TaskStatus.SUCCEEDED

            def retryable_this_run(tid: str) -> bool:
                """Return whether a bad-status task can still recover this run."""
                task = self._tasks[tid]
                return (
                    tid not in futures
                    and task.can_transition_to(TaskStatus.RUNNING)
                    and (
                        task.status != TaskStatus.BLOCKED
                        or tid in entering_blocked
                    )
                )

            def inactive(tid: str) -> bool:
                """Return whether tid can no longer change in this run."""
                if tid in futures:
                    return futures[tid].done()
                task = self._tasks[tid]
                return (
                    (task.is_terminal and not retryable_this_run(tid))
                    or tid in self._errors
                )

            def ready(tid: str) -> bool:
                """Return whether all upstream dependencies are satisfied."""
                if tid in self._finally:
                    return all(inactive(d) for d in self._deps[tid])
                return all(succeeded(d) for d in self._deps[tid])

            def first_bad_parent(tid: str) -> str | None:
                """Return the first dep (in declaration order) blocking ``tid``.

                A parent "blocks" when its status is FAILED, CANCELLED, or
                BLOCKED and it cannot still be retried during this run.
                Returns ``None`` if every bad parent is still recoverable.
                Pre-start handler errors terminalize the parent to FAILED
                before raising, so status plus retry eligibility is
                authoritative.
                """
                for d in self._deps[tid]:
                    if d in futures and not futures[d].done():
                        continue
                    if self._tasks[d].status.is_bad and not retryable_this_run(d):
                        return d
                return None

            def finished(tid: str) -> bool:
                """Return whether tid no longer needs scheduler attention."""
                task = self._tasks[tid]
                return task.is_terminal or (
                    tid in futures and futures[tid].done()
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
                changed = True
                while changed:
                    changed = False
                    for tid in self._deps:
                        task = self._tasks[tid]
                        if tid in futures:
                            continue
                        # SUCCEEDED/CANCELLED are absolute terminal states
                        # SUCCEEDED and CANCELLED are absolute SM sinks:
                        # never retry-eligible. BLOCKED tasks that entered
                        # this run blocked are eligible for retry (carryover);
                        # BLOCKED tasks that became blocked during this run
                        # stay blocked so finally readiness is unambiguous.
                        if task.status.is_sink:
                            continue
                        if (
                            task.status == TaskStatus.BLOCKED
                            and tid not in entering_blocked
                        ):
                            continue
                        if tid not in self._finally:
                            bad = first_bad_parent(tid)
                            if bad is not None:
                                if task.status == TaskStatus.PENDING:
                                    executor.mark_blocked(task, bad)
                                    changed = True
                                # Carryover BLOCKED whose parents are still
                                # bad: stays BLOCKED until parents recover.
                                continue
                        if ready(tid) and task.can_transition_to(TaskStatus.RUNNING):
                            submit(tid)
                            changed = True

                if all(finished(tid) for tid in self._deps):
                    done.set()
                    return
                # No live work to wait on and not finished: deadlocked.
                live = any(not f.done() for f in futures.values())
                if not live:
                    stuck = [
                        self._tasks[tid].title
                        for tid in self._deps
                        if not finished(tid)
                    ]
                    scheduler_error.append(
                        RuntimeError(
                            f"scheduler made no progress; stuck={stuck!r}"
                        )
                    )
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

        if scheduler_error:
            raise scheduler_error[0]
        return self
