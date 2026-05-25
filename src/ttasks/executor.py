"""Task execution and process-management helpers."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from .events import EventBus, TaskEvent, TaskEventType
from .task import Task, TaskResult, TaskStatus, TaskType

if TYPE_CHECKING:
    from .store import Store


class TaskCancelled(RuntimeError):
    """Signal that task execution was cancelled.

    Handlers should not mutate Task lifecycle state directly. They may raise
    TaskCancelled to cooperatively abort; TaskExecutor owns the transition to
    CANCELLED and records the terminal TaskResult.
    """


class TaskExecutionError(RuntimeError):
    """Raised when a subprocess exits unsuccessfully.

    completed preserves stdout, stderr, and returncode so TaskExecutor can
    attach structured failure details to Task.result instead of keeping only
    the exception string.
    """

    def __init__(self, message: str, completed: subprocess.CompletedProcess[str]):
        """Create an execution error for completed."""
        super().__init__(message)
        self.completed = completed


class TaskTimeoutError(TimeoutError):
    """Raised when a subprocess exceeds its timeout.

    completed preserves any output collected after terminating the process.
    """

    def __init__(self, message: str, completed: subprocess.CompletedProcess[str]):
        """Create a timeout error for completed."""
        super().__init__(message)
        self.completed = completed


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

    def __init__(
        self,
        task: Task,
        upstream: Mapping[str, Task] | None = None,
    ) -> None:
        """Create a context for task with read-only upstream task refs."""
        object.__setattr__(self, "_task", task)
        object.__setattr__(self, "_upstream", MappingProxyType(dict(upstream or {})))

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


# Handler contract: returning any value means success and the value is
# normalized into TaskResult. Raising TaskCancelled means cancelled. Raising any
# other exception means failed. Handlers that run subprocesses should raise
# TaskExecutionError or TaskTimeoutError to preserve structured process output.
TaskHandler = Callable[[TaskContext], Any]


class TaskExecutor:
    """Dispatch tasks to registered handlers and manage task state transitions.

    When constructed with ``store``, the executor auto-persists each task to
    ``store.tasks`` on every lifecycle transition (RUNNING, DONE, FAILED,
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

    def cancel(self, task: Task) -> None:
        """Cancel a task and terminate its subprocess if one is active."""
        task.cancel()

        process = self._running_processes.get(task.id)
        if process is not None and process.poll() is None:
            self._terminate_process(process)

    def execute(
        self,
        task: Task,
        upstream: Mapping[str, Task] | None = None,
    ) -> TaskResult:
        """Execute task with its registered handler.

        upstream contains direct dependency task refs keyed by task ID. Single
        task execution normally leaves it empty; TaskGraph populates it from
        the graph ledger before submitting each non-root task.

        Execution always moves through RUNNING first. Returning from a handler
        means success and moves the task to DONE; raising from a handler means
        failure unless cancellation happened while the handler was in flight.
        Handlers should signal cooperative cancellation by raising TaskCancelled
        rather than mutating task state directly; the executor performs the
        CANCELLED transition.

        A non-zero subprocess return code is not interpreted here for arbitrary
        custom handlers. Handlers that want subprocess failures represented as
        structured TaskResult data should raise TaskExecutionError or
        TaskTimeoutError.
        """
        if not task.can_transition_to(TaskStatus.RUNNING):
            raise ValueError(f"Cannot execute task with status {task.status.value!r}")

        handler = self._handlers.get(task.type)
        if handler is None:
            raise ValueError(f"No handler registered for task type {task.type.value!r}")

        previous_status = task.status
        task.transition_to(TaskStatus.RUNNING)
        self._emit(task, TaskEventType.STARTED, previous_status)
        started_at = datetime.now()
        started_monotonic = time.monotonic()

        def result_timing() -> tuple[datetime, float]:
            """Return finish time and duration for a terminal TaskResult."""
            return datetime.now(), time.monotonic() - started_monotonic

        context = TaskContext(task, upstream=upstream)

        def finalize(status: TaskStatus, **extras: Any) -> TaskResult:
            """Build, attach, and return the terminal TaskResult for ``task``."""
            finished_at = datetime.now()
            result = TaskResult(
                task_id=task.id,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                duration=time.monotonic() - started_monotonic,
                **extras,
            )
            task.result = result
            return result

        try:
            raw_result = handler(context)
            context.raise_if_cancelled()
            finished_at, duration = result_timing()
            result = TaskResult.from_raw(
                task,
                raw_result,
                status=TaskStatus.DONE,
                started_at=started_at,
                finished_at=finished_at,
                duration=duration,
            )
            task.result = result
            task.transition_to(TaskStatus.DONE)
            self._emit(task, TaskEventType.SUCCEEDED, TaskStatus.RUNNING)
            return result
        except TaskCancelled as e:
            if task.status != TaskStatus.CANCELLED:
                task.cancel()
            finalize(TaskStatus.CANCELLED, error=str(e))
            self._emit(task, TaskEventType.CANCELLED, TaskStatus.RUNNING, str(e))
            raise
        except Exception as e:
            if task.status == TaskStatus.CANCELLED:
                cancelled = TaskCancelled(f"Task {task.id!r} was cancelled")
                finalize(TaskStatus.CANCELLED, error=str(e))
                self._emit(task, TaskEventType.CANCELLED, TaskStatus.RUNNING, str(e))
                raise cancelled from e
            task.transition_to(TaskStatus.FAILED, error=str(e))
            if isinstance(e, TaskExecutionError | TaskTimeoutError):
                completed = e.completed
                if isinstance(e, TaskExecutionError):
                    error = completed.stderr or str(e)
                else:
                    error = str(e)
                finalize(
                    TaskStatus.FAILED,
                    output=completed.stdout or "",
                    error=error,
                    returncode=completed.returncode,
                    raw=completed,
                )
            else:
                finalize(TaskStatus.FAILED, error=str(e))
            self._emit(task, TaskEventType.FAILED, TaskStatus.RUNNING, str(e))
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
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        self._running_processes[context.id] = process
        if context.cancelled:
            self._terminate_process(process)
        try:
            try:
                stdout, stderr = process.communicate(timeout=context.timeout)
            except subprocess.TimeoutExpired as e:
                self._terminate_process(process)
                stdout, stderr = process.communicate()
                message = f"Task timed out after {context.timeout} seconds"
                completed = subprocess.CompletedProcess(
                    args=args,
                    returncode=process.returncode,
                    stdout=stdout,
                    stderr=stderr,
                )
                raise TaskTimeoutError(message, completed) from e
        finally:
            self._running_processes.pop(context.id, None)

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
    if not model:
        raise ValueError("model must not be empty")
    if timeout <= 0:
        raise ValueError("timeout must be greater than 0")

    def handler(context: TaskContext) -> str:
        """Run one synchronous prompt task through the async Copilot SDK."""
        return asyncio.run(
            _run_copilot_text(
                context,
                model=model,
                default_timeout=timeout,
                tools_enabled=False,
            )
        )

    return handler


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
    if not model:
        raise ValueError("model must not be empty")

    def handler(context: TaskContext) -> str:
        """Run one synchronous agent task through the async Copilot SDK."""
        return asyncio.run(
            _run_copilot_text(
                context,
                model=model,
                default_timeout=None,
                tools_enabled=True,
            )
        )

    return handler


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


