"""Task execution and process-management helpers."""

from __future__ import annotations

import asyncio
import math
import os
import signal
import subprocess
import time
import warnings
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from threading import RLock, Thread, current_thread
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, TextIO, cast

from ._events import EventBus, OutputStream, TaskEvent, TaskEventType
from ._exceptions import TaskCancelled, TaskExecutionError, TaskTimeoutError
from ._task import Task, TaskResult, TaskStatus, TaskType, TerminationReason

if TYPE_CHECKING:
    from ._store import Store


@dataclass(frozen=True, init=False)
class TaskContext:
    """Read-only execution view passed to task handlers.

    The executor owns lifecycle transitions. Handlers receive this context so
    they can inspect task data, cancellation state, and direct upstream task
    refs without being given the public Task state-machine mutation API for the
    current task.
    """

    _task: Task
    _upstream: Mapping[str, Task]
    _progress_emitter: Callable[[float | None, str | None], None] | None

    def __init__(
        self,
        task: Task,
        upstream: Mapping[str, Task] | None = None,
        *,
        progress_emitter: Callable[[float | None, str | None], None] | None = None,
    ) -> None:
        """Create a context for task with read-only upstream task refs."""
        object.__setattr__(self, "_task", task)
        object.__setattr__(self, "_upstream", MappingProxyType(dict(upstream or {})))
        object.__setattr__(self, "_progress_emitter", progress_emitter)

    @property
    def id(self) -> str:
        """Return the task identity."""
        return self._task.id

    @property
    def title(self) -> str:
        """Return the task title."""
        return self._task.title

    @property
    def description(self) -> str:
        """Return the task description."""
        return self._task.description

    @property
    def payload(self) -> str:
        """Return the task payload."""
        return self._task.payload

    @property
    def type(self) -> TaskType:
        """Return the task type."""
        return self._task.type

    @property
    def timeout(self) -> float | None:
        """Return the task timeout."""
        return self._task.timeout

    @property
    def status(self) -> TaskStatus:
        """Return the task's current live status."""
        return self._task.status

    @property
    def cancelled(self) -> bool:
        """Return whether cancellation has been requested for the task."""
        return self.status == TaskStatus.CANCELLED

    @property
    def upstream(self) -> Mapping[str, Task]:
        """Return direct upstream task refs keyed by task ID."""
        return self._upstream

    def raise_if_cancelled(self) -> None:
        """Raise TaskCancelled if cancellation has been requested."""
        if self.cancelled:
            raise TaskCancelled(f"Task {self.id!r} was cancelled")

    def emit_progress(
        self,
        percent: float | None = None,
        message: str | None = None,
    ) -> None:
        """Emit a progress event for the running task.

        At least one of ``percent`` or ``message`` must be provided. Percent is
        an optional finite value from 0 through 100; callers are not required to
        emit monotonically increasing percentages.
        """
        if percent is None and message is None:
            raise ValueError("percent or message is required")
        if percent is not None:
            if isinstance(percent, bool) or not isinstance(percent, (int, float)):
                raise TypeError("percent must be a number")
            if not math.isfinite(percent) or not 0 <= percent <= 100:
                raise ValueError("percent must be between 0 and 100")
            percent = float(percent)
        if message is not None and not isinstance(message, str):
            raise TypeError("message must be a str")

        self.raise_if_cancelled()
        if self._progress_emitter is None:
            raise RuntimeError("progress cannot be emitted without an executor")
        self._progress_emitter(percent, message)


# Handler contract: returning any value means success and the value is
# normalized into TaskResult. Raising TaskCancelled means cancelled. Raising any
# other exception means failed. Handlers that run subprocesses should raise
# TaskExecutionError or TaskTimeoutError to preserve structured process output.
TaskHandler = Callable[[TaskContext], Any]


@dataclass(frozen=True)
class RetryPolicy:
    """Single-task retry configuration."""

    max_attempts: int = 1
    backoff: float = 0.0

    def __post_init__(self) -> None:
        """Validate retry configuration."""
        if isinstance(self.max_attempts, bool) or not isinstance(
            self.max_attempts, int,
        ):
            raise TypeError("max_attempts must be an int")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if isinstance(self.backoff, bool) or not isinstance(
            self.backoff, (int, float),
        ):
            raise TypeError("backoff must be a number")
        if not math.isfinite(self.backoff) or self.backoff < 0:
            raise ValueError("backoff must be a finite non-negative number")
        object.__setattr__(self, "backoff", float(self.backoff))


class TaskExecutor:
    """Dispatch tasks to registered handlers and manage task state transitions.

    When constructed with ``store``, the executor auto-persists each task to
    ``store.tasks`` on every lifecycle transition (RUNNING, SUCCEEDED, FAILED,
    CANCELLED). Persistence runs *before* the corresponding lifecycle event is
    emitted so subscribers can read a consistent store. Persistence failures
    do not propagate as task failures; instead they are recorded on
    :attr:`persistence_errors` and emitted as
    :attr:`TaskEventType.PERSISTENCE_FAILED` events.
    """

    def __init__(self, store: Store | None = None, *, _register_defaults: bool = True):
        """Create an executor optionally backed by ``store`` for auto-persist.

        Built-in BASH, POWERSHELL, PROMPT, and AGENT handlers are registered
        automatically. Use :meth:`empty` to construct an executor without them.
        """
        self._handlers: dict[TaskType, TaskHandler] = {}
        self._running_processes: dict[str, subprocess.Popen[str]] = {}
        self.events = EventBus()
        self.store = store
        self.persistence_errors: list[tuple[str, BaseException]] = []
        self.graph_persistence_errors: list[tuple[str, BaseException]] = []
        self._pool: ThreadPoolExecutor | None = None
        self._pool_lock = RLock()
        self._closed = False
        if _register_defaults:
            self.register(TaskType.BASH, self._run_bash)
            self.register(TaskType.POWERSHELL, self._run_powershell)
            self.register(TaskType.PROMPT, make_copilot_prompt_handler())
            self.register(TaskType.AGENT, make_copilot_agent_handler())

    @classmethod
    def empty(cls, store: Store | None = None) -> TaskExecutor:
        """Construct an executor with no handlers pre-registered."""
        return cls(store=store, _register_defaults=False)

    def register(self, task_type: TaskType, handler: TaskHandler) -> None:
        """Register callable handler as the executor for task_type."""
        if not isinstance(task_type, TaskType):
            raise TypeError("task_type must be a TaskType")
        if not callable(handler):
            raise TypeError("handler must be callable")
        self._handlers[task_type] = handler

    def is_registered(self, task_type: TaskType) -> bool:
        """Return whether a handler is registered for ``task_type``."""
        if not isinstance(task_type, TaskType):
            raise TypeError("task_type must be a TaskType")
        return task_type in self._handlers

    def is_running(self, task_id: str) -> bool:
        """Return whether task_id currently has a live subprocess."""
        process = self._running_processes.get(task_id)
        return process is not None and process.poll() is None

    @property
    def is_shutdown(self) -> bool:
        """Return whether async submission has been shut down."""
        return self._closed

    def submit(
        self,
        task: Task,
        upstream: Mapping[str, Task] | None = None,
        *,
        retry_policy: RetryPolicy | None = None,
    ) -> Future[TaskResult]:
        """Submit ``task`` for asynchronous execution and return its future.

        The task runs through :meth:`execute`, so events, persistence, results,
        and cancellation behavior match synchronous execution. Calling
        :meth:`Future.cancel` only cancels work that has not started yet; cancel
        running tasks through :meth:`cancel`.
        """
        policy = self._resolve_retry_policy(retry_policy)
        # Shallow-copy the mapping so caller mutation cannot race the worker;
        # Task refs themselves intentionally remain shared.
        upstream_snapshot = dict(upstream or {})
        with self._pool_lock:
            if self._closed:
                raise RuntimeError("executor is shut down")
            if self._pool is None:
                self._pool = ThreadPoolExecutor(thread_name_prefix="ttasks")
            future = self._pool.submit(
                self._execute_submitted,
                task,
                upstream_snapshot,
                policy,
            )
            future.add_done_callback(
                lambda submitted: self.cancel(task) if submitted.cancelled() else None
            )
            return future

    def _execute_submitted(
        self,
        task: Task,
        upstream: Mapping[str, Task],
        retry_policy: RetryPolicy,
    ) -> TaskResult:
        """Execute submitted work, preserving queued cancellation semantics."""
        if task.status == TaskStatus.CANCELLED:
            raise TaskCancelled(f"Task {task.id!r} was cancelled")
        return self.execute(task, upstream, retry_policy=retry_policy)

    def shutdown(self) -> None:
        """Shut down async submission, waiting for submitted work to finish.

        Shutdown is idempotent. It prevents new :meth:`submit` calls and waits
        for already-submitted tasks to finish, including queued tasks that have
        not started yet. It does not cancel running or queued tasks.
        """
        with self._pool_lock:
            if self._closed:
                return
            self._closed = True
            pool = self._pool
            self._pool = None
        if pool is not None:
            current = current_thread()
            pool_threads = list(getattr(pool, "_threads", ()))
            if current in pool_threads:
                pool.shutdown(wait=False)
                for thread in pool_threads:
                    if thread is not current:
                        thread.join()
            else:
                pool.shutdown(wait=True)

    def close(self) -> None:
        """Alias for :meth:`shutdown` for resource-cleanup contexts."""
        self.shutdown()

    def __enter__(self) -> TaskExecutor:
        """Return this executor for context-manager use."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        """Close asynchronous execution resources on context exit."""
        self.close()

    def mark_blocked(self, task: Task, parent_id: str | None) -> None:
        """Transition ``task`` to BLOCKED, recording the parent that caused it.

        Public seam used by :class:`TaskGraph` (and any custom scheduler) to
        signal that ``task`` cannot proceed because an upstream dependency
        failed the readiness contract (it failed, was cancelled, or is itself
        blocked). Records ``parent_id`` on the task via
        :meth:`Task._set_blocked_by`, drives the lifecycle transition, and
        emits the BLOCKED event so observers and the store see the outcome.
        """
        previous_status = task.status
        task.transition_to(TaskStatus.BLOCKED)
        task._set_blocked_by(parent_id)
        self._emit(task, TaskEventType.BLOCKED, previous_status)

    def _terminalize(
        self,
        task: Task,
        result: TaskResult,
        status: TaskStatus,
        *,
        previous: TaskStatus,
        event_type: TaskEventType,
        error: str | None = None,
    ) -> None:
        """Drive a single terminal write: result → transition → emit.

        ``result`` is always attached so race orderings still leave a
        TaskResult in place. If ``task`` is already in ``status`` (e.g. an
        external ``cancel()`` raced ahead mid-execute) the transition is
        skipped because the state-machine rejects self-transitions on
        terminal states, but the event still fires so the executor remains
        the single source of terminal events for its own ``execute()`` call.
        """
        task._set_result(result)
        if task.status != status:
            task.transition_to(status, error=error)
        self._emit(task, event_type, previous, error)

    def _emit(
        self,
        task: Task,
        event_type: TaskEventType,
        previous_status: TaskStatus | None,
        error: str | None = None,
    ) -> None:
        """Persist ``task`` if a store is configured, then emit a lifecycle event."""
        self._persist(task)
        self.events.emit(
            TaskEvent(
                type=event_type,
                task_id=task.id,
                task=task,
                timestamp=datetime.now(),
                previous_status=previous_status,
                status=task.status,
                error=error,
            )
        )

    def _emit_progress(
        self,
        task: Task,
        percent: float | None,
        message: str | None,
    ) -> None:
        """Emit a non-persistent progress event for ``task``."""
        self.events.emit(
            TaskEvent(
                type=TaskEventType.PROGRESS,
                task_id=task.id,
                task=task,
                timestamp=datetime.now(),
                previous_status=None,
                status=task.status,
                progress_percent=percent,
                progress_message=message,
            )
        )

    def _emit_output(self, task: Task, stream: OutputStream, chunk: str) -> None:
        """Emit a non-persistent subprocess output event for ``task``."""
        self.events.emit(
            TaskEvent(
                type=TaskEventType.OUTPUT,
                task_id=task.id,
                task=task,
                timestamp=datetime.now(),
                previous_status=None,
                status=task.status,
                output_stream=stream,
                output_chunk=chunk,
            )
        )

    def _persist(self, task: Task) -> None:
        """Auto-save ``task`` to the configured store.

        Failures are recorded on :attr:`persistence_errors` and emitted as
        ``PERSISTENCE_FAILED`` events; they never propagate to the caller.
        """
        if self.store is None:
            return
        try:
            self.store.tasks.save(task)
        except BaseException as error:
            self.persistence_errors.append((task.id, error))
            self.events.emit(
                TaskEvent(
                    type=TaskEventType.PERSISTENCE_FAILED,
                    task_id=task.id,
                    task=task,
                    timestamp=datetime.now(),
                    previous_status=None,
                    status=task.status,
                    error=str(error),
                )
            )

    def _persist_graph(self, graph: Any) -> None:
        """Auto-save ``graph`` to the configured store.

        Failures are recorded on :attr:`graph_persistence_errors` and surfaced
        via :func:`warnings.warn`; they never propagate to the caller. Graph
        persistence has no event type because :class:`TaskEvent` is
        task-centric; the list is the discovery channel.
        """
        if self.store is None:
            return
        try:
            self.store.graphs.save(graph)
        except BaseException as error:
            self.graph_persistence_errors.append((graph.id, error))
            with suppress(Warning):
                warnings.warn(
                    f"graph persistence failed for graph {graph.id!r}: {error}",
                    stacklevel=2,
                )

    def cancel(self, task: Task) -> None:
        """Cancel a task and terminate its subprocess if one is active.

        SUCCEEDED is an irreversible sink: cancel() is a silent no-op rather
        than raising, so callers don't need to know which states accept
        transitions. For tasks that were not actively executing (PENDING /
        FAILED / BLOCKED) this emits a CANCELLED event and attaches a
        CANCELLED ``TaskResult`` so observers and the store see the outcome.
        Cancelling a RUNNING task does **not** emit here: the active
        ``execute()`` loop owns the terminal event for that task and
        will emit CANCELLED when its handler unwinds via
        :class:`TaskCancelled`. Cancelling an already-CANCELLED task is
        a no-op on Task state but still reaps any lingering subprocess
        so duplicate requests stay harmless yet complete.
        """
        if task.status == TaskStatus.SUCCEEDED:
            return

        previous = task.status
        task.cancel()

        process = self._running_processes.get(task.id)
        if process is not None and process.poll() is None:
            self._terminate_process(process)

        if previous in {TaskStatus.PENDING, TaskStatus.FAILED, TaskStatus.BLOCKED}:
            now = datetime.now()
            result = TaskResult(
                task_id=task.id,
                status=TaskStatus.CANCELLED,
                started_at=now,
                finished_at=now,
                duration=0.0,
                error="cancelled",
                termination_reason="cancelled",
            )
            self._terminalize(
                task,
                result,
                TaskStatus.CANCELLED,
                previous=previous,
                event_type=TaskEventType.CANCELLED,
                error="cancelled",
            )

    def execute(
        self,
        task: Task,
        upstream: Mapping[str, Task] | None = None,
        *,
        retry_policy: RetryPolicy | None = None,
    ) -> TaskResult:
        """Execute task with its registered handler.

        upstream contains direct dependency task refs keyed by task ID. Single
        task execution normally leaves it empty; TaskGraph populates it from
        the graph ledger before submitting each non-root task.

        Execution always moves through RUNNING first. Returning from a handler
        means success and moves the task to SUCCEEDED; raising from a handler means
        failure unless cancellation happened while the handler was in flight.
        Handlers should signal cooperative cancellation by raising TaskCancelled
        rather than mutating task state directly; the executor performs the
        CANCELLED transition.

        A non-zero subprocess return code is not interpreted here for arbitrary
        custom handlers. Handlers that want subprocess failures represented as
        structured TaskResult data should raise TaskExecutionError or
        TaskTimeoutError.

        ``retry_policy`` retries failed attempts for this single task only.
        Cancellation is never retried.
        """
        policy = self._resolve_retry_policy(retry_policy)
        if policy.max_attempts == 1 or self._handlers.get(task.type) is None:
            return self._execute_once(task, upstream)

        for attempt in range(policy.max_attempts):
            try:
                return self._execute_once(task, upstream)
            except TaskCancelled:
                raise
            except Exception:
                if task.status == TaskStatus.CANCELLED:
                    raise TaskCancelled(
                        f"Task {task.id!r} was cancelled",
                    ) from None
                out_of_attempts = attempt + 1 >= policy.max_attempts
                if out_of_attempts or task.status != TaskStatus.FAILED:
                    raise
                if policy.backoff:
                    self._sleep_retry_backoff(task, policy.backoff)
                if task.status == TaskStatus.CANCELLED:
                    raise TaskCancelled(
                        f"Task {task.id!r} was cancelled",
                    ) from None

        raise AssertionError("unreachable retry loop exit")  # pragma: no cover

    @staticmethod
    def _resolve_retry_policy(retry_policy: RetryPolicy | None) -> RetryPolicy:
        """Return a concrete RetryPolicy, rejecting malformed public input."""
        if retry_policy is None:
            return RetryPolicy()
        if not isinstance(retry_policy, RetryPolicy):
            raise TypeError("retry_policy must be a RetryPolicy")
        return retry_policy

    @staticmethod
    def _sleep_retry_backoff(task: Task, backoff: float) -> None:
        """Sleep between retry attempts while periodically observing cancel()."""
        if backoff <= 0.5:
            time.sleep(backoff)
            return

        deadline = time.monotonic() + backoff
        while task.status != TaskStatus.CANCELLED:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(remaining, 0.05))

    def _execute_once(
        self,
        task: Task,
        upstream: Mapping[str, Task] | None = None,
    ) -> TaskResult:
        """Execute one task attempt with its registered handler."""
        if not task.can_transition_to(TaskStatus.RUNNING):
            raise ValueError(f"Cannot execute task with status {task.status.value!r}")

        handler = self._handlers.get(task.type)
        if handler is None:
            message = f"No handler registered for task type {task.type.value!r}"
            previous_status = task.status
            if not task.can_transition_to(TaskStatus.FAILED):
                task.transition_to(TaskStatus.RUNNING)
            finished_at = datetime.now()
            failed_result = TaskResult(
                task_id=task.id,
                status=TaskStatus.FAILED,
                started_at=finished_at,
                finished_at=finished_at,
                duration=0.0,
                error=message,
                termination_reason="handler",
            )
            self._terminalize(
                task,
                failed_result,
                TaskStatus.FAILED,
                previous=previous_status,
                event_type=TaskEventType.FAILED,
                error=message,
            )
            raise ValueError(message)

        previous_status = task.status
        task.transition_to(TaskStatus.RUNNING)
        self._emit(task, TaskEventType.STARTED, previous_status)
        started_at = datetime.now()
        started_monotonic = time.monotonic()

        def build_result(status: TaskStatus, **extras: Any) -> TaskResult:
            """Build the terminal TaskResult for ``task``."""
            finished_at = datetime.now()
            return TaskResult(
                task_id=task.id,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                duration=time.monotonic() - started_monotonic,
                **extras,
            )

        context = TaskContext(
            task,
            upstream=upstream,
            progress_emitter=lambda percent, message: self._emit_progress(
                task, percent, message,
            ),
        )

        try:
            raw_result = handler(context)
            context.raise_if_cancelled()
            finished_at = datetime.now()
            duration = time.monotonic() - started_monotonic
            result = TaskResult.from_raw(
                task,
                raw_result,
                status=TaskStatus.SUCCEEDED,
                started_at=started_at,
                finished_at=finished_at,
                duration=duration,
            )
            self._terminalize(
                task,
                result,
                TaskStatus.SUCCEEDED,
                previous=TaskStatus.RUNNING,
                event_type=TaskEventType.SUCCEEDED,
            )
            return result
        except TaskCancelled as e:
            cancelled_result = build_result(
                TaskStatus.CANCELLED, error=str(e), termination_reason="cancelled"
            )
            self._terminalize(
                task,
                cancelled_result,
                TaskStatus.CANCELLED,
                previous=TaskStatus.RUNNING,
                event_type=TaskEventType.CANCELLED,
                error=str(e),
            )
            raise
        except Exception as e:
            if task.status == TaskStatus.CANCELLED:
                cancelled = TaskCancelled(f"Task {task.id!r} was cancelled")
                cancelled_result = build_result(
                    TaskStatus.CANCELLED, error=str(e), termination_reason="cancelled"
                )
                self._terminalize(
                    task,
                    cancelled_result,
                    TaskStatus.CANCELLED,
                    previous=TaskStatus.RUNNING,
                    event_type=TaskEventType.CANCELLED,
                    error=str(e),
                )
                raise cancelled from e
            if isinstance(e, TaskExecutionError | TaskTimeoutError):
                completed = e.completed
                if isinstance(e, TaskExecutionError):
                    err_text = completed.stderr or str(e)
                    reason: TerminationReason = "exit_code"
                else:
                    err_text = str(e)
                    reason = "timeout"
                failed_result = build_result(
                    TaskStatus.FAILED,
                    output=completed.stdout or "",
                    error=err_text,
                    returncode=completed.returncode,
                    raw=completed,
                    termination_reason=reason,
                )
            else:
                failed_result = build_result(
                    TaskStatus.FAILED, error=str(e), termination_reason="handler"
                )
            self._terminalize(
                task,
                failed_result,
                TaskStatus.FAILED,
                previous=TaskStatus.RUNNING,
                event_type=TaskEventType.FAILED,
                error=str(e),
            )
            raise

    def _run_command(
        self,
        context: TaskContext,
        args: str | list[str],
        *,
        shell: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        """Run a subprocess for task and enforce cancellation/timeout behavior.

        context.timeout=None follows subprocess semantics: wait indefinitely
        unless another caller cancels the task through TaskExecutor.cancel().
        Non-zero exits raise TaskExecutionError; timeouts raise
        TaskTimeoutError. Both exceptions carry a CompletedProcess so execute()
        can attach stdout, stderr, returncode, and raw process details.
        """
        process = subprocess.Popen(
            args,
            shell=shell,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        self._running_processes[context.id] = process
        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []

        def read_output(
            pipe: TextIO,
            stream: OutputStream,
            chunks: list[str],
        ) -> None:
            """Read one text stream to completion and emit each line."""
            for chunk in iter(pipe.readline, ""):
                chunks.append(chunk)
                self._emit_output(context._task, stream, chunk)

        stdout_thread = Thread(
            target=read_output,
            args=(cast("TextIO", process.stdout), "stdout", stdout_chunks),
            daemon=True,
        )
        stderr_thread = Thread(
            target=read_output,
            args=(cast("TextIO", process.stderr), "stderr", stderr_chunks),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        timed_out = False
        timeout_error: subprocess.TimeoutExpired | None = None
        deadline = (
            None if context.timeout is None else time.monotonic() + context.timeout
        )

        def remaining_timeout() -> float | None:
            """Return remaining wall-clock timeout for process/output draining."""
            if deadline is None:
                return None
            return max(0.0, deadline - time.monotonic())

        if context.cancelled:
            self._terminate_process(process)
        try:
            try:
                process.wait(timeout=context.timeout)
            except subprocess.TimeoutExpired as e:
                self._terminate_process(process)
                timed_out = True
                timeout_error = e
        finally:
            for thread in (stdout_thread, stderr_thread):
                thread.join(remaining_timeout())
            if (
                not timed_out
                and deadline is not None
                and (stdout_thread.is_alive() or stderr_thread.is_alive())
            ):
                self._terminate_process(process)
                timed_out = True
            if timed_out:
                stdout_thread.join()
                stderr_thread.join()
            self._running_processes.pop(context.id, None)

        stdout = "".join(stdout_chunks)
        stderr = "".join(stderr_chunks)
        if timed_out:
            message = f"Task timed out after {context.timeout} seconds"
            completed = subprocess.CompletedProcess(
                args=args,
                returncode=process.returncode,
                stdout=stdout,
                stderr=stderr,
            )
            if timeout_error is not None:
                raise TaskTimeoutError(message, completed) from timeout_error
            raise TaskTimeoutError(message, completed)

        result = subprocess.CompletedProcess(
            args=args,
            returncode=process.returncode,
            stdout=stdout,
            stderr=stderr,
        )
        if result.returncode != 0:
            if context.cancelled:
                raise TaskCancelled(f"Task {context.id!r} was cancelled")
            message = result.stderr or f"exited with code {result.returncode}"
            raise TaskExecutionError(message, result)
        return result

    @staticmethod
    def _terminate_process(process: subprocess.Popen[str]) -> None:
        """Terminate a process group, escalating to SIGKILL if needed."""
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return

        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                return
            process.wait()

    def _run_bash(self, context: TaskContext) -> subprocess.CompletedProcess[str]:
        """Run trusted bash payload text through the system shell."""
        # Intentionally uses shell=True because TaskType.BASH represents trusted
        # shell code, not a shell-free argv command.
        return self._run_command(context, context.payload, shell=True)

    def _run_powershell(self, context: TaskContext) -> subprocess.CompletedProcess[str]:
        """Run trusted PowerShell payload text with pwsh."""
        return self._run_command(context, ["pwsh", "-Command", context.payload])


DEFAULT_COPILOT_PROMPT_MODEL = "gpt-5.4-mini"
DEFAULT_COPILOT_PROMPT_TIMEOUT = 60.0
DEFAULT_COPILOT_AGENT_MODEL = "gpt-5.5"


def _make_copilot_handler(
    *,
    model: str,
    default_timeout: float | None,
    tools_enabled: bool,
) -> TaskHandler:
    """Return a handler that drives one Copilot turn per task execution."""
    if not model:
        raise ValueError("model must not be empty")

    def handler(context: TaskContext) -> str:
        """Run one synchronous Copilot task through the async SDK."""
        return asyncio.run(
            _run_copilot_text(
                context,
                model=model,
                default_timeout=default_timeout,
                tools_enabled=tools_enabled,
            )
        )

    return handler


def make_copilot_prompt_handler(
    *,
    model: str = DEFAULT_COPILOT_PROMPT_MODEL,
    timeout: float = DEFAULT_COPILOT_PROMPT_TIMEOUT,
) -> TaskHandler:
    """Return a PROMPT handler backed by the GitHub Copilot SDK.

    The handler sends context.payload as a single-turn text prompt, disables
    tools with an empty available_tools allowlist, and returns the assistant
    message content as task output. context.timeout overrides timeout per task.
    """
    if timeout <= 0:
        raise ValueError("timeout must be greater than 0")
    return _make_copilot_handler(
        model=model, default_timeout=timeout, tools_enabled=False,
    )


def make_copilot_agent_handler(
    *,
    model: str = DEFAULT_COPILOT_AGENT_MODEL,
) -> TaskHandler:
    """Return an AGENT handler backed by the GitHub Copilot SDK.

    The handler sends context.payload as a single-turn agent instruction,
    leaves Copilot's default tools enabled, approves permission requests, and
    returns the assistant message content as task output. context.timeout is
    used when provided; otherwise no ttasks timeout is applied.
    """
    return _make_copilot_handler(
        model=model, default_timeout=None, tools_enabled=True,
    )


async def _run_copilot_text(
    context: TaskContext,
    *,
    model: str,
    default_timeout: float | None,
    tools_enabled: bool,
) -> str:
    """Send one Copilot turn and return assistant text."""
    from copilot import CopilotClient
    from copilot.generated.session_events import AssistantMessageData
    from copilot.session import PermissionHandler

    context.raise_if_cancelled()
    effective_timeout = (
        context.timeout if context.timeout is not None else default_timeout
    )

    async with CopilotClient() as client:
        context.raise_if_cancelled()
        if tools_enabled:
            session_context = await client.create_session(
                on_permission_request=PermissionHandler.approve_all,
                model=model,
            )
        else:
            session_context = await client.create_session(
                on_permission_request=PermissionHandler.approve_all,
                model=model,
                available_tools=[],
            )
        async with session_context as session:
            context.raise_if_cancelled()
            send_and_wait: Any = session.send_and_wait
            response = await send_and_wait(
                context.payload,
                timeout=effective_timeout,
            )

    context.raise_if_cancelled()
    if response is None or not isinstance(response.data, AssistantMessageData):
        return ""
    return response.data.content or ""
