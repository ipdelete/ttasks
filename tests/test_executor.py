"""Tests for task execution, retries, timeout, and cancellation."""

import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import pytest

from ttasks.events import TaskEvent, TaskEventType
from ttasks.executor import (
    TaskCancelled,
    TaskContext,
    TaskExecutionError,
    TaskExecutor,
    TaskResult,
    TaskTimeoutError,
    make_copilot_agent_handler,
    make_copilot_prompt_handler,
)
from ttasks.task import Task, TaskStatus, TaskType


def assert_result_timing(result: TaskResult, before: datetime, after: datetime) -> None:
    """Assert result timing is populated and bounded by before/after."""
    assert before <= result.started_at <= result.finished_at <= after
    assert result.duration >= 0


def test_task_context_exposes_read_only_task_view() -> None:
    """TaskContext exposes task data without exposing lifecycle mutators."""
    task = Task(
        title="Example",
        description="Demo task",
        payload="echo hi",
        type=TaskType.BASH,
        timeout=1.5,
    )
    context = TaskContext(task)

    assert context.id == task.id
    assert context.title == "Example"
    assert context.description == "Demo task"
    assert context.payload == "echo hi"
    assert context.type == TaskType.BASH
    assert context.timeout == 1.5
    assert context.status == TaskStatus.PENDING
    assert context.cancelled is False
    assert context.upstream == {}
    context.raise_if_cancelled()


def test_task_context_exposes_read_only_upstream_task_refs() -> None:
    """TaskContext exposes upstream task refs through a read-only mapping."""
    parent = Task(title="Parent", payload="echo parent", type=TaskType.BASH)
    child = Task(title="Child", payload="echo child", type=TaskType.BASH)
    context = TaskContext(child, upstream={parent.id: parent})
    upstream: Any = context.upstream

    assert context.upstream[parent.id] is parent
    with pytest.raises(TypeError):
        upstream["other"] = child


def test_register_rejects_non_task_type() -> None:
    """Handler registration rejects keys that are not TaskType values."""
    executor = TaskExecutor()
    task_type: Any = "bash"

    with pytest.raises(TypeError, match="task_type must be a TaskType"):
        executor.register(task_type, lambda context: "ok")


def test_register_rejects_non_callable_handler() -> None:
    """Handler registration rejects values that cannot be called."""
    executor = TaskExecutor()
    handler: Any = "not callable"

    with pytest.raises(TypeError, match="handler must be callable"):
        executor.register(TaskType.BASH, handler)


def test_execute_success_emits_started_and_succeeded_events() -> None:
    """Successful execution emits lifecycle events in order."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)
    events: list[TaskEvent] = []

    def handler(context: TaskContext) -> str:
        """Return a successful handler result."""
        assert context.status == TaskStatus.RUNNING
        return "ok"

    executor.register(TaskType.BASH, handler)
    executor.events.subscribe(events.append)

    executor.execute(task)

    assert [event.type for event in events] == [
        TaskEventType.STARTED,
        TaskEventType.SUCCEEDED,
    ]
    assert [event.previous_status for event in events] == [
        TaskStatus.PENDING,
        TaskStatus.RUNNING,
    ]
    assert [event.status for event in events] == [TaskStatus.RUNNING, TaskStatus.DONE]
    assert all(event.task is task for event in events)
    assert events[1].task.result is task.result


def test_execute_failure_emits_started_and_failed_events() -> None:
    """Failed execution emits lifecycle events with error details."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)
    events: list[TaskEvent] = []

    def handler(context: TaskContext) -> None:
        """Raise a representative handler failure."""
        raise RuntimeError("boom")

    executor.register(TaskType.BASH, handler)
    executor.events.subscribe(events.append)

    with pytest.raises(RuntimeError, match="boom"):
        executor.execute(task)

    assert [event.type for event in events] == [
        TaskEventType.STARTED,
        TaskEventType.FAILED,
    ]
    assert events[1].previous_status == TaskStatus.RUNNING
    assert events[1].status == TaskStatus.FAILED
    assert events[1].error == "boom"
    assert events[1].task.result is task.result


def test_execute_cancellation_emits_started_and_cancelled_events() -> None:
    """Cancelled execution emits lifecycle events with cancellation details."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)
    events: list[TaskEvent] = []

    def handler(context: TaskContext) -> None:
        """Signal cooperative cancellation."""
        raise TaskCancelled("stop")

    executor.register(TaskType.BASH, handler)
    executor.events.subscribe(events.append)

    with pytest.raises(TaskCancelled, match="stop"):
        executor.execute(task)

    assert [event.type for event in events] == [
        TaskEventType.STARTED,
        TaskEventType.CANCELLED,
    ]
    assert events[1].previous_status == TaskStatus.RUNNING
    assert events[1].status == TaskStatus.CANCELLED
    assert events[1].error == "stop"
    assert events[1].task.result is task.result


def test_retry_after_failure_emits_started_event_from_failed_status() -> None:
    """Retry events preserve the FAILED -> RUNNING transition."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)
    events: list[TaskEvent] = []
    attempts = 0

    def handler(context: TaskContext) -> str:
        """Fail once and then recover."""
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("boom")
        return "ok"

    executor.register(TaskType.BASH, handler)
    executor.events.subscribe(events.append)

    with pytest.raises(RuntimeError, match="boom"):
        executor.execute(task)
    executor.execute(task)

    started_events = [event for event in events if event.type == TaskEventType.STARTED]
    assert [event.previous_status for event in started_events] == [
        TaskStatus.PENDING,
        TaskStatus.FAILED,
    ]


def test_execute_passes_upstream_task_refs_to_handler() -> None:
    """execute() passes upstream task refs into the handler context."""
    executor = TaskExecutor()
    parent = Task(title="Parent", payload="", type=TaskType.BASH)
    child = Task(title="Child", payload="", type=TaskType.BASH)

    def handler(context: TaskContext) -> str:
        """Assert the handler sees the provided upstream task."""
        assert context.upstream[parent.id] is parent
        return "ok"

    executor.register(TaskType.BASH, handler)

    result = executor.execute(child, upstream={parent.id: parent})

    assert result.output == "ok"
    assert child.status == TaskStatus.DONE


def test_execute_moves_task_through_running_to_done() -> None:
    """execute() marks a task RUNNING before the handler sees it, then DONE."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)

    def handler(context: TaskContext) -> str:
        """Assert the executor transitions to RUNNING before dispatch."""
        assert context.status == TaskStatus.RUNNING
        return "ok"

    executor.register(TaskType.BASH, handler)

    result = executor.execute(task)

    assert result.task_id == task.id
    assert result.status == TaskStatus.DONE
    assert result.output == "ok"
    assert result.raw == "ok"
    assert result.started_at <= result.finished_at
    assert result.duration >= 0
    assert task.status == TaskStatus.DONE


def test_task_result_wraps_non_string_raw_values() -> None:
    """Arbitrary handler return values are preserved on TaskResult.raw."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)
    raw = {"answer": 42}

    def handler(context: TaskContext) -> dict[str, int]:
        """Return a non-string object to exercise raw result wrapping."""
        return raw

    executor.register(TaskType.BASH, handler)

    result = executor.execute(task)

    assert result.task_id == task.id
    assert result.status == TaskStatus.DONE
    assert result.raw == raw
    assert result.started_at <= result.finished_at
    assert result.duration >= 0


def test_execute_rejects_task_without_registered_handler() -> None:
    """Tasks without handlers are rejected before they start running."""
    executor = TaskExecutor.empty()
    task = Task(title="Example", payload="", type=TaskType.BASH)

    with pytest.raises(ValueError, match="No handler registered for task type 'bash'"):
        executor.execute(task)

    assert task.status == TaskStatus.PENDING


def test_handler_failure_marks_task_failed_and_stores_error() -> None:
    """Handler exceptions move the task to FAILED and store the error text."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)

    def handler(context: TaskContext) -> None:
        """Raise a representative handler failure."""
        raise RuntimeError("boom")

    executor.register(TaskType.BASH, handler)

    with pytest.raises(RuntimeError, match="boom"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "boom"


def test_execute_rejects_cancelled_task_without_calling_handler() -> None:
    """Cancelled tasks are rejected before any handler side effects occur."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)
    called = False

    def handler(context: TaskContext) -> None:
        """Record if this unexpectedly gets called."""
        nonlocal called
        called = True

    executor.register(TaskType.BASH, handler)
    task.cancel()

    with pytest.raises(ValueError, match="Cannot execute task with status 'cancelled'"):
        executor.execute(task)

    assert called is False
    assert task.status == TaskStatus.CANCELLED


def test_executor_clears_previous_error_on_successful_retry() -> None:
    """A successful retry clears the stale error from a previous failure."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)
    attempts = 0

    def handler(context: TaskContext) -> str:
        """Fail once, then succeed on retry."""
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("boom")
        return "ok"

    executor.register(TaskType.BASH, handler)

    with pytest.raises(RuntimeError, match="boom"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "boom"

    result = executor.execute(task)

    assert result.output == "ok"
    assert result.raw == "ok"
    assert result.status == TaskStatus.DONE
    assert task.status == TaskStatus.DONE
    assert task.error is None


def test_successful_execute_sets_task_result_timing() -> None:
    """Successful execution records start, finish, and duration timing."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    before = datetime.now()
    result = executor.execute(task)
    after = datetime.now()

    assert_result_timing(result, before, after)
    assert task.result is result


def test_default_executor_can_execute_bash() -> None:
    """The default executor includes a working BASH handler."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)

    result = executor.execute(task)

    assert result.task_id == task.id
    assert result.status == TaskStatus.DONE
    assert result.output == "hi\n"
    assert result.error is None
    assert result.returncode == 0
    assert isinstance(result.raw, subprocess.CompletedProcess)
    assert task.status == TaskStatus.DONE
    assert not executor.is_running(task.id)


def test_bash_task_supports_shell_syntax() -> None:
    """BASH tasks intentionally execute shell syntax such as pipes."""
    executor = TaskExecutor()
    task = Task(
        title="Shell syntax",
        payload="printf 'hello\\n' | grep hello",
        type=TaskType.BASH,
    )

    result = executor.execute(task)

    assert result.output == "hello\n"
    assert result.returncode == 0
    assert task.status == TaskStatus.DONE


def test_bash_nonzero_exit_marks_task_failed() -> None:
    """A shell command with a non-zero return code fails the task."""
    executor = TaskExecutor()
    task = Task(title="Failing command", payload="exit 7", type=TaskType.BASH)

    with pytest.raises(TaskExecutionError, match="exited with code 7"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "exited with code 7"
    assert not executor.is_running(task.id)


def test_bash_failure_uses_stderr_as_error() -> None:
    """Subprocess stderr is preferred over the generic exit-code message."""
    executor = TaskExecutor()
    task = Task(
        title="Failing command",
        payload="echo boom >&2; exit 1",
        type=TaskType.BASH,
    )

    with pytest.raises(TaskExecutionError, match="boom"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "boom\n"
    assert not executor.is_running(task.id)


def test_failed_subprocess_result_preserves_output_error_and_returncode() -> None:
    """Failed subprocesses still attach structured process details."""
    executor = TaskExecutor()
    task = Task(
        title="Structured failure",
        payload="echo before; echo boom >&2; exit 7",
        type=TaskType.BASH,
    )

    with pytest.raises(TaskExecutionError, match="boom"):
        executor.execute(task)

    assert task.result is not None
    assert task.result.status == TaskStatus.FAILED
    assert task.result.output == "before\n"
    assert task.result.error == "boom\n"
    assert task.result.returncode == 7
    assert isinstance(task.result.raw, subprocess.CompletedProcess)


def test_running_process_registry_is_cleaned_after_failure() -> None:
    """Failed subprocesses are removed from the running-process registry."""
    executor = TaskExecutor()
    task = Task(title="Fail", payload="exit 1", type=TaskType.BASH)

    with pytest.raises(RuntimeError):
        executor.execute(task)

    assert not executor.is_running(task.id)


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="pwsh is not installed")
def test_powershell_task_executes() -> None:
    """PowerShell tasks execute when pwsh is available on the host."""
    executor = TaskExecutor()
    task = Task(title="PowerShell", payload="'hello'", type=TaskType.POWERSHELL)

    result = executor.execute(task)

    assert "hello" in result.output
    assert result.returncode == 0
    assert task.status == TaskStatus.DONE
    assert not executor.is_running(task.id)


def test_bash_task_without_timeout_waits_for_completion() -> None:
    """timeout=None means the subprocess is allowed to run until it exits."""
    executor = TaskExecutor()
    task = Task(title="No timeout", payload="sleep 0.1; echo done", type=TaskType.BASH)

    result = executor.execute(task)

    assert result.output == "done\n"
    assert result.returncode == 0
    assert task.status == TaskStatus.DONE
    assert task.timeout is None


def test_bash_task_times_out() -> None:
    """A subprocess exceeding task.timeout is terminated and marked FAILED."""
    executor = TaskExecutor()
    task = Task(
        title="Slow",
        payload="sleep 30",
        type=TaskType.BASH,
        timeout=0.1,
    )

    with pytest.raises(TaskTimeoutError, match="Task timed out after 0.1 seconds"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "Task timed out after 0.1 seconds"
    assert not executor.is_running(task.id)


def test_timed_out_subprocess_result_preserves_partial_output() -> None:
    """Timeout results retain output captured before termination."""
    executor = TaskExecutor()
    task = Task(
        title="Partial timeout",
        payload="echo before; echo warn >&2; sleep 30",
        type=TaskType.BASH,
        timeout=0.1,
    )

    with pytest.raises(TaskTimeoutError, match="Task timed out after 0.1 seconds"):
        executor.execute(task)

    assert task.result is not None
    assert task.result.status == TaskStatus.FAILED
    assert task.result.output == "before\n"
    assert task.result.error == "Task timed out after 0.1 seconds"
    assert isinstance(task.result.raw, subprocess.CompletedProcess)
    assert task.result.raw.stderr == "warn\n"


def test_handler_cancellation_after_return_raises_task_cancelled() -> None:
    """If a handler leaves the task CANCELLED, execute() raises TaskCancelled."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)

    def handler(context: TaskContext) -> str:
        """Cancel the task before returning a result."""
        task.cancel()
        return "ignored"

    executor.register(TaskType.BASH, handler)

    with pytest.raises(TaskCancelled, match=f"Task {task.id!r} was cancelled"):
        executor.execute(task)

    assert task.status == TaskStatus.CANCELLED


def test_handler_task_cancelled_exception_marks_task_cancelled() -> None:
    """A handler raising TaskCancelled directly leaves the task terminal."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)

    def handler(context: TaskContext) -> None:
        """Raise cancellation without mutating the task directly."""
        raise TaskCancelled("worker cancelled")

    executor.register(TaskType.BASH, handler)

    with pytest.raises(TaskCancelled, match="worker cancelled"):
        executor.execute(task)

    assert task.status == TaskStatus.CANCELLED
    assert task.result is not None
    assert task.result.status == TaskStatus.CANCELLED
    assert task.result.error == "worker cancelled"


def test_handler_error_after_cancellation_raises_task_cancelled() -> None:
    """Handler errors are reported as cancellation if task was cancelled first."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)

    def handler(context: TaskContext) -> None:
        """Cancel the task, then raise like a terminated worker might."""
        task.cancel()
        raise RuntimeError("worker stopped")

    executor.register(TaskType.BASH, handler)

    with pytest.raises(TaskCancelled, match=f"Task {task.id!r} was cancelled"):
        executor.execute(task)

    assert task.status == TaskStatus.CANCELLED
    assert task.error is None


def test_cancel_without_running_process_only_cancels_task() -> None:
    """Executor cancellation works even when no subprocess is registered."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)

    executor.cancel(task)
    executor.cancel(task)

    assert task.status == TaskStatus.CANCELLED
    assert not executor.is_running(task.id)


def test_run_command_terminates_if_task_cancelled_during_process_start() -> None:
    """A cancellation between Popen and process registration is still honored."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)
    task.transition_to(TaskStatus.RUNNING)
    process = Mock(spec=subprocess.Popen)
    process.pid = 12345
    process.returncode = -signal.SIGTERM
    process.communicate.return_value = ("", "")

    def fake_popen(*args: object, **kwargs: object) -> Mock:
        """Cancel after process creation but before _run_command can register it."""
        task.cancel()
        return process

    with (
        patch("ttasks.executor.subprocess.Popen", side_effect=fake_popen),
        patch.object(executor, "_terminate_process") as terminate,
        pytest.raises(TaskCancelled, match=f"Task {task.id!r} was cancelled"),
    ):
        executor._run_command(TaskContext(task), "ignored", shell=True)

    terminate.assert_called_once_with(process)
    assert not executor.is_running(task.id)


def test_run_command_reports_cancelled_nonzero_process_as_task_cancelled() -> None:
    """A cancelled task with a non-zero process exit raises TaskCancelled."""
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)
    task.transition_to(TaskStatus.RUNNING)
    task.cancel()

    with pytest.raises(TaskCancelled, match=f"Task {task.id!r} was cancelled"):
        executor._run_command(TaskContext(task), "exit 1", shell=True)

    assert not executor.is_running(task.id)


def test_terminate_process_ignores_already_exited_process() -> None:
    """A missing process group is harmless during termination."""
    process = Mock(spec=subprocess.Popen)
    process.pid = 12345

    with patch("ttasks.executor.os.killpg", side_effect=ProcessLookupError):
        TaskExecutor._terminate_process(process)

    process.wait.assert_not_called()


def test_terminate_process_escalates_to_sigkill() -> None:
    """Processes that ignore SIGTERM are killed with SIGKILL."""
    process = Mock(spec=subprocess.Popen)
    process.pid = 12345
    process.wait.side_effect = [subprocess.TimeoutExpired(cmd="cmd", timeout=5), 0]

    with patch("ttasks.executor.os.killpg") as killpg:
        TaskExecutor._terminate_process(process)

    assert killpg.call_args_list == [
        ((12345, signal.SIGTERM),),
        ((12345, signal.SIGKILL),),
    ]
    assert process.wait.call_count == 2


def test_terminate_process_ignores_missing_group_during_sigkill() -> None:
    """A process group disappearing before SIGKILL is harmless."""
    process = Mock(spec=subprocess.Popen)
    process.pid = 12345
    process.wait.side_effect = subprocess.TimeoutExpired(cmd="cmd", timeout=5)

    with patch(
        "ttasks.executor.os.killpg",
        side_effect=[None, ProcessLookupError],
    ) as killpg:
        TaskExecutor._terminate_process(process)

    assert killpg.call_args_list == [
        ((12345, signal.SIGTERM),),
        ((12345, signal.SIGKILL),),
    ]


def install_fake_copilot(
    monkeypatch: pytest.MonkeyPatch,
    *,
    content: str | None = "response",
    data: object | None = None,
    error: BaseException | None = None,
) -> dict[str, Any]:
    """Install fake copilot modules and return call recording state."""
    recorded: dict[str, Any] = {}

    class AssistantMessageData:
        """Fake Copilot assistant message data."""

        def __init__(self, content: str | None) -> None:
            """Store assistant message content."""
            self.content = content

    class FakeSession:
        """Fake Copilot session async context manager."""

        async def __aenter__(self) -> "FakeSession":
            """Enter the fake session context."""
            return self

        async def __aexit__(self, *args: object) -> None:
            """Record fake session context exit."""
            recorded["session_exited"] = True

        async def send_and_wait(self, prompt: str, *, timeout: float) -> object | None:
            """Record the prompt call and return configured fake response."""
            recorded["prompt"] = prompt
            recorded["timeout"] = timeout
            if error is not None:
                raise error
            if data is not None:
                return SimpleNamespace(data=data)
            if content is None:
                return None
            return SimpleNamespace(data=AssistantMessageData(content))

    class FakeClient:
        """Fake Copilot client async context manager."""

        async def __aenter__(self) -> "FakeClient":
            """Enter the fake client context."""
            recorded["client_entered"] = True
            return self

        async def __aexit__(self, *args: object) -> None:
            """Record fake client context exit."""
            recorded["client_exited"] = True

        async def create_session(self, **kwargs: object) -> FakeSession:
            """Record session options and return a fake session."""
            recorded["create_session"] = kwargs
            return FakeSession()

    class FakePermissionHandler:
        """Fake Copilot permission handler namespace."""

        @staticmethod
        def approve_all(*args: object) -> object:
            """Return a placeholder approval result."""
            return object()

    copilot: Any = ModuleType("copilot")
    copilot.__path__ = []
    copilot.CopilotClient = FakeClient
    generated: Any = ModuleType("copilot.generated")
    generated.__path__ = []
    session_events: Any = ModuleType("copilot.generated.session_events")
    session_events.AssistantMessageData = AssistantMessageData
    session_module: Any = ModuleType("copilot.session")
    session_module.PermissionHandler = FakePermissionHandler

    monkeypatch.setitem(sys.modules, "copilot", copilot)
    monkeypatch.setitem(sys.modules, "copilot.generated", generated)
    monkeypatch.setitem(sys.modules, "copilot.generated.session_events", session_events)
    monkeypatch.setitem(sys.modules, "copilot.session", session_module)
    return recorded


def test_make_copilot_prompt_handler_rejects_empty_model() -> None:
    """Copilot prompt handlers require a non-empty model name."""
    with pytest.raises(ValueError, match="model must not be empty"):
        make_copilot_prompt_handler(model="")


def test_make_copilot_prompt_handler_rejects_non_positive_timeout() -> None:
    """Copilot prompt handlers require a positive default timeout."""
    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        make_copilot_prompt_handler(timeout=0)


def test_default_prompt_handler_uses_copilot_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default PROMPT handler sends one no-tools Copilot prompt."""
    recorded = install_fake_copilot(monkeypatch, content="hello back")
    executor = TaskExecutor()
    task = Task(title="Prompt", payload="hello", type=TaskType.PROMPT)

    result = executor.execute(task)

    assert result.output == "hello back"
    assert task.status == TaskStatus.DONE
    assert recorded["prompt"] == "hello"
    assert recorded["timeout"] == 60.0
    create_session = recorded["create_session"]
    assert callable(create_session["on_permission_request"])
    assert create_session["model"] == "gpt-5.4-mini"
    assert create_session["available_tools"] == []
    assert recorded["client_entered"] is True
    assert recorded["client_exited"] is True
    assert recorded["session_exited"] is True


def test_copilot_prompt_handler_uses_task_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task timeout overrides the Copilot prompt handler default timeout."""
    recorded = install_fake_copilot(monkeypatch, content="done")
    executor = TaskExecutor()
    task = Task(title="Prompt", payload="hello", type=TaskType.PROMPT, timeout=2.5)

    executor.execute(task)

    assert recorded["timeout"] == 2.5


def test_copilot_prompt_handler_allows_model_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Callers can register a Copilot prompt handler with a different model."""
    recorded = install_fake_copilot(monkeypatch, content="done")
    executor = TaskExecutor()
    executor.register(
        TaskType.PROMPT,
        make_copilot_prompt_handler(model="gpt-custom", timeout=12),
    )
    task = Task(title="Prompt", payload="hello", type=TaskType.PROMPT)

    executor.execute(task)

    assert recorded["timeout"] == 12
    create_session = recorded["create_session"]
    assert callable(create_session["on_permission_request"])
    assert create_session["model"] == "gpt-custom"
    assert create_session["available_tools"] == []


def test_copilot_prompt_handler_none_response_returns_empty_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A prompt with no assistant message normalizes to empty output."""
    install_fake_copilot(monkeypatch, content=None)
    executor = TaskExecutor()
    task = Task(title="Prompt", payload="hello", type=TaskType.PROMPT)

    result = executor.execute(task)

    assert result.output == ""


def test_copilot_prompt_handler_unknown_response_data_returns_empty_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unexpected Copilot response data normalizes to empty output."""
    install_fake_copilot(monkeypatch, data=object())
    executor = TaskExecutor()
    task = Task(title="Prompt", payload="hello", type=TaskType.PROMPT)

    result = executor.execute(task)

    assert result.output == ""


def test_copilot_prompt_handler_sdk_error_marks_task_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Copilot SDK errors follow normal task failure handling."""
    install_fake_copilot(monkeypatch, error=RuntimeError("sdk boom"))
    executor = TaskExecutor()
    task = Task(title="Prompt", payload="hello", type=TaskType.PROMPT)

    with pytest.raises(RuntimeError, match="sdk boom"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "sdk boom"


def test_make_copilot_agent_handler_rejects_empty_model() -> None:
    """Copilot agent handlers require a non-empty model name."""
    with pytest.raises(ValueError, match="model must not be empty"):
        make_copilot_agent_handler(model="")


def test_default_agent_handler_uses_copilot_sdk_with_tools_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default AGENT handler sends one tool-capable Copilot instruction."""
    recorded = install_fake_copilot(monkeypatch, content="agent done")
    executor = TaskExecutor()
    task = Task(title="Agent", payload="inspect repo", type=TaskType.AGENT)

    result = executor.execute(task)

    assert result.output == "agent done"
    assert task.status == TaskStatus.DONE
    assert recorded["prompt"] == "inspect repo"
    assert recorded["timeout"] is None
    create_session = recorded["create_session"]
    assert callable(create_session["on_permission_request"])
    assert create_session["model"] == "gpt-5.5"
    assert "available_tools" not in create_session


def test_copilot_agent_handler_uses_task_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task timeout overrides the Copilot agent handler no-timeout default."""
    recorded = install_fake_copilot(monkeypatch, content="done")
    executor = TaskExecutor()
    task = Task(title="Agent", payload="hello", type=TaskType.AGENT, timeout=3.5)

    executor.execute(task)

    assert recorded["timeout"] == 3.5


def test_copilot_agent_handler_allows_model_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Callers can register a Copilot agent handler with a different model."""
    recorded = install_fake_copilot(monkeypatch, content="done")
    executor = TaskExecutor()
    executor.register(TaskType.AGENT, make_copilot_agent_handler(model="agent-custom"))
    task = Task(title="Agent", payload="hello", type=TaskType.AGENT)

    executor.execute(task)

    create_session = recorded["create_session"]
    assert create_session["model"] == "agent-custom"
    assert "available_tools" not in create_session


def test_copilot_agent_handler_sdk_error_marks_task_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Copilot agent SDK errors follow normal task failure handling."""
    install_fake_copilot(monkeypatch, error=RuntimeError("agent boom"))
    executor = TaskExecutor()
    task = Task(title="Agent", payload="hello", type=TaskType.AGENT)

    with pytest.raises(RuntimeError, match="agent boom"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "agent boom"


def test_cancel_stops_in_flight_bash_task() -> None:
    """Cancelling a running bash task terminates its subprocess."""
    executor = TaskExecutor()
    task = Task(title="Long running", payload="sleep 30", type=TaskType.BASH)
    errors: list[BaseException] = []

    def run_task() -> None:
        """Run task in a background thread so the test can cancel it."""
        try:
            executor.execute(task)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run_task)
    thread.start()

    # Wait until both the task state and subprocess registry show it is running.
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if task.status == TaskStatus.RUNNING and executor.is_running(task.id):
            break
        time.sleep(0.01)
    else:
        pytest.fail("task did not start running")

    executor.cancel(task)
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert task.status == TaskStatus.CANCELLED
    assert len(errors) == 1
    assert isinstance(errors[0], TaskCancelled)
    assert not executor.is_running(task.id)


# ---- task.result is set on every terminal path ------------------------------


def test_task_result_is_none_before_execution() -> None:
    """A freshly-constructed task has no result yet."""
    task = Task(title="X", payload="echo hi", type=TaskType.BASH)
    assert task.result is None


def test_successful_execute_sets_task_result() -> None:
    """A task that completes successfully carries its TaskResult on the task."""
    task = Task(title="X", payload="echo hi", type=TaskType.BASH)
    executor = TaskExecutor()
    returned = executor.execute(task)

    assert task.result is returned
    assert task.result.status == TaskStatus.DONE
    assert task.result.output.strip() == "hi"
    assert task.result.returncode == 0


def test_failed_execute_sets_task_result_with_failed_status() -> None:
    """A task that fails still produces a TaskResult attached to the task."""
    task = Task(title="X", payload="exit 1", type=TaskType.BASH)
    executor = TaskExecutor()

    before = datetime.now()
    with pytest.raises(RuntimeError):
        executor.execute(task)
    after = datetime.now()

    assert task.result is not None
    assert task.result.status == TaskStatus.FAILED
    assert task.result.error  # exception text captured
    assert_result_timing(task.result, before, after)
    assert task.status == TaskStatus.FAILED


def test_cancelled_execute_sets_task_result_with_cancelled_status() -> None:
    """A task cancelled mid-run still produces a TaskResult on the task."""
    task = Task(title="X", payload="sleep 5", type=TaskType.BASH)
    executor = TaskExecutor()
    errors: list[BaseException] = []

    def run() -> None:
        """Execute the task in a background thread for cancellation."""
        try:
            executor.execute(task)
        except BaseException as e:
            errors.append(e)

    before = datetime.now()
    thread = threading.Thread(target=run)
    thread.start()

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if executor.is_running(task.id):
            break
        time.sleep(0.01)
    else:
        pytest.fail("task did not start running")

    executor.cancel(task)
    thread.join(timeout=2)

    assert isinstance(errors[0], TaskCancelled)
    after = datetime.now()

    assert task.status == TaskStatus.CANCELLED
    assert task.result is not None
    assert task.result.status == TaskStatus.CANCELLED
    assert_result_timing(task.result, before, after)


def test_retry_after_failure_replaces_task_result() -> None:
    """Re-running a failed task overwrites task.result, doesn't keep the old one."""
    task = Task(title="X", payload="exit 1", type=TaskType.BASH)
    executor = TaskExecutor()

    with pytest.raises(RuntimeError):
        executor.execute(task)
    first_result = task.result
    assert first_result is not None
    assert first_result.status == TaskStatus.FAILED

    # Repair: replace payload so the retry succeeds.
    task.payload = "echo recovered"
    executor.execute(task)

    assert task.result is not None
    assert task.result is not first_result
    assert task.result.status == TaskStatus.DONE
    assert task.result.output.strip() == "recovered"


# ---- store-backed auto-persistence ------------------------------------------


def _bash_task(payload: str = "echo ok") -> Task:
    """Return a fresh bash task used by the auto-persist tests."""
    return Task.bash(payload, title="t")


def test_executor_without_store_does_not_record_persistence() -> None:
    """When no store is configured the executor never touches persistence."""
    from ttasks.store import InMemoryStore  # noqa: F401  (import keeps API alive)

    executor = TaskExecutor()
    task = _bash_task()
    executor.execute(task)
    assert executor.store is None
    assert executor.persistence_errors == []


def test_executor_auto_persists_each_lifecycle_transition() -> None:
    """Both STARTED and SUCCEEDED transitions write the task to the store."""
    from ttasks.store import InMemoryStore

    store = InMemoryStore()

    saved_statuses: list[TaskStatus] = []

    class _RecordingTasks:
        """Tasks collection that records the status at each save call."""

        def __init__(self, inner):
            self._inner = inner

        def save(self, task):
            saved_statuses.append(task.status)
            self._inner.save(task)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    class _RecordingStore:
        def __init__(self, inner):
            self._inner = inner
            self._tasks = _RecordingTasks(inner.tasks)

        @property
        def tasks(self):
            return self._tasks

        @property
        def graphs(self):
            return self._inner.graphs

    executor = TaskExecutor(store=_RecordingStore(store))
    task = _bash_task()
    executor.execute(task)

    assert TaskStatus.RUNNING in saved_statuses
    assert TaskStatus.DONE in saved_statuses
    assert store.tasks[task.id].status == TaskStatus.DONE


def test_executor_saves_before_emitting_lifecycle_event() -> None:
    """Subscribers reading the store on event see the new task state."""
    from ttasks.store import InMemoryStore

    store = InMemoryStore()
    executor = TaskExecutor(store=store)
    observed: list[TaskStatus] = []

    def on_event(event: TaskEvent) -> None:
        snapshot = store.tasks.get(event.task_id)
        if snapshot is not None:
            observed.append(snapshot.status)

    executor.events.subscribe(on_event)
    task = _bash_task()
    executor.execute(task)

    assert TaskStatus.RUNNING in observed
    assert TaskStatus.DONE in observed


def test_persistence_failure_is_recorded_and_emitted_not_raised() -> None:
    """A store that raises on save records an error and emits PERSISTENCE_FAILED."""

    class _BrokenTasks:
        def save(self, task):
            raise RuntimeError("disk full")

    class _BrokenStore:
        @property
        def tasks(self):
            return _BrokenTasks()

        @property
        def graphs(self):
            return None

    events: list[TaskEvent] = []
    executor = TaskExecutor(store=_BrokenStore())
    executor.events.subscribe(events.append)

    task = _bash_task()
    result = executor.execute(task)

    assert result.status == TaskStatus.DONE
    assert task.status == TaskStatus.DONE
    # Persistence errors did not poison execution but were recorded.
    assert executor.persistence_errors
    assert all(tid == task.id for tid, _ in executor.persistence_errors)
    assert any(
        event.type == TaskEventType.PERSISTENCE_FAILED for event in events
    )


def test_default_executor_pre_registers_all_task_types() -> None:
    """TaskExecutor() pre-registers BASH, POWERSHELL, PROMPT, and AGENT."""
    executor = TaskExecutor()

    assert executor.is_registered(TaskType.BASH)
    assert executor.is_registered(TaskType.POWERSHELL)
    assert executor.is_registered(TaskType.PROMPT)
    assert executor.is_registered(TaskType.AGENT)


def test_empty_executor_has_no_handlers_registered() -> None:
    """TaskExecutor.empty() returns an executor with no handlers."""
    executor = TaskExecutor.empty()

    assert not executor.is_registered(TaskType.BASH)
    assert not executor.is_registered(TaskType.POWERSHELL)
    assert not executor.is_registered(TaskType.PROMPT)
    assert not executor.is_registered(TaskType.AGENT)


def test_empty_executor_forwards_store_for_auto_persist() -> None:
    """TaskExecutor.empty(store=...) still attaches the store for auto-persist."""
    from ttasks.store import InMemoryStore

    store = InMemoryStore()
    executor = TaskExecutor.empty(store=store)

    assert executor.store is store


def test_is_registered_rejects_non_task_type() -> None:
    """is_registered validates its argument is a TaskType."""
    executor = TaskExecutor.empty()
    bogus: Any = "bash"

    with pytest.raises(TypeError, match="task_type must be a TaskType"):
        executor.is_registered(bogus)
