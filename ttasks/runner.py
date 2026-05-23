"""High-level task runner: ledger + executor + worker pool + event bus.

TaskRunner is the front door of the SDK for consumers that want submit-and-forget
semantics and live status updates. It composes the lower-level pieces and adds:

  * a bounded worker pool that runs submitted tasks off the caller's thread
  * a subscription API that notifies listeners when a task transitions

The runner intentionally speaks only in domain terms (Task, TaskStatus,
TaskEvent). It knows nothing about HTTP, JSON, or any specific UI.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime

from ttasks.executor import TaskExecutor, TaskResult, default_executor
from ttasks.ledger import TaskLedger
from ttasks.task import Task, TaskStatus


@dataclass(frozen=True)
class TaskEvent:
    """A domain event emitted when a task transitions between statuses."""

    task_id: str
    old_status: TaskStatus
    new_status: TaskStatus
    at: datetime
    error: str | None = None


EventListener = Callable[[TaskEvent], None]


class TaskRunner:
    """Submit tasks, observe their lifecycle, cancel them.

    Consumers hold one TaskRunner and never touch the underlying ledger or
    executor directly. This keeps lifecycle ownership in one place and makes
    the event stream the single source of truth for "something changed".
    """

    def __init__(
        self,
        ledger: TaskLedger | None = None,
        executor: TaskExecutor | None = None,
        max_workers: int = 4,
    ) -> None:
        """Create a runner with optional pre-built ledger and executor."""
        self._ledger = ledger if ledger is not None else TaskLedger()
        self._executor = executor if executor is not None else default_executor()
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._listeners: list[EventListener] = []
        self._lock = threading.Lock()
        self._futures: dict[str, Future[TaskResult]] = {}

    # -- ledger access ----------------------------------------------------

    @property
    def ledger(self) -> TaskLedger:
        """Return the underlying ledger for read-only inspection."""
        return self._ledger

    def add(self, task: Task) -> Task:
        """Register a task with the runner without starting it."""
        self._ledger[task.id] = task
        return task

    def remove(self, task_id: str) -> None:
        """Remove a task from the ledger if it is not currently running."""
        task = self._ledger[task_id]
        if task.status == TaskStatus.RUNNING:
            raise ValueError("Cannot remove a running task; cancel it first.")
        del self._ledger[task_id]

    # -- submission -------------------------------------------------------

    def submit(self, task_id: str) -> Future[TaskResult]:
        """Schedule the task identified by task_id to run on the worker pool."""
        task = self._ledger[task_id]
        if not task.can_transition_to(TaskStatus.RUNNING):
            raise ValueError(
                f"Cannot submit task with status {task.status.value!r}"
            )

        previous_status = task.status
        future = self._pool.submit(self._run, task, previous_status)
        with self._lock:
            self._futures[task.id] = future
        future.add_done_callback(lambda _f, tid=task.id: self._forget(tid))
        return future

    def cancel(self, task_id: str) -> None:
        """Cancel the task and stop its subprocess if one is in flight."""
        task = self._ledger[task_id]
        previous_status = task.status
        self._executor.cancel(task)
        if previous_status != task.status:
            self._emit(task, previous_status)

    # -- subscriptions ----------------------------------------------------

    def subscribe(self, listener: EventListener) -> Callable[[], None]:
        """Register listener to be called for every task transition.

        Returns an unsubscribe callable so subscribers do not need to retain
        the original function reference to detach.
        """
        with self._lock:
            self._listeners.append(listener)

        def unsubscribe() -> None:
            with self._lock:
                if listener in self._listeners:
                    self._listeners.remove(listener)

        return unsubscribe

    # -- shutdown ---------------------------------------------------------

    def shutdown(self, wait: bool = True) -> None:
        """Stop accepting new work and tear down the worker pool."""
        self._pool.shutdown(wait=wait)

    # -- internals --------------------------------------------------------

    def _run(self, task: Task, previous_status: TaskStatus) -> TaskResult:
        """Execute task and emit the resulting transition.

        v1 emits a single event per submission: previous_status -> final_status.
        Fine-grained intermediate transitions (e.g. PENDING -> RUNNING -> DONE
        as two events) can be added later by hooking Task.transition_to.
        """
        try:
            return self._executor.execute(task)
        finally:
            self._emit(task, previous_status)

    def _emit(self, task: Task, previous_status: TaskStatus) -> None:
        """Notify listeners of a transition if the status actually changed."""
        if task.status == previous_status:
            return
        event = TaskEvent(
            task_id=task.id,
            old_status=previous_status,
            new_status=task.status,
            at=datetime.now(),
            error=task.error,
        )
        with self._lock:
            listeners = list(self._listeners)
        for listener in listeners:
            with contextlib.suppress(Exception):
                listener(event)
                # Listener failures must not break task execution.

    def _forget(self, task_id: str) -> None:
        """Drop the stored Future once a task has finished."""
        with self._lock:
            self._futures.pop(task_id, None)
