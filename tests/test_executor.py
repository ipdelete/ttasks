"""Tests for task execution, retries, timeout, and cancellation."""

import shutil
import signal
import subprocess
import threading
import time
from unittest.mock import Mock, patch

import pytest

from executor import (
    TaskCancelled,
    TaskContext,
    TaskExecutor,
    TaskResult,
    default_executor,
)
from task import Task, TaskStatus, TaskType


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
    context.raise_if_cancelled()


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

    assert result == TaskResult(
        task_id=task.id,
        status=TaskStatus.DONE,
        output="ok",
        raw="ok",
    )
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

    assert result == TaskResult(task_id=task.id, status=TaskStatus.DONE, raw=raw)


def test_execute_rejects_task_without_registered_handler() -> None:
    """Tasks without handlers are rejected before they start running."""
    executor = TaskExecutor()
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


def test_default_executor_can_execute_bash() -> None:
    """The default executor includes a working BASH handler."""
    executor = default_executor()
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
    executor = default_executor()
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
    executor = default_executor()
    task = Task(title="Failing command", payload="exit 7", type=TaskType.BASH)

    with pytest.raises(RuntimeError, match="exited with code 7"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "exited with code 7"
    assert not executor.is_running(task.id)


def test_bash_failure_uses_stderr_as_error() -> None:
    """Subprocess stderr is preferred over the generic exit-code message."""
    executor = default_executor()
    task = Task(
        title="Failing command",
        payload="echo boom >&2; exit 1",
        type=TaskType.BASH,
    )

    with pytest.raises(RuntimeError, match="boom"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "boom\n"
    assert not executor.is_running(task.id)


def test_running_process_registry_is_cleaned_after_failure() -> None:
    """Failed subprocesses are removed from the running-process registry."""
    executor = default_executor()
    task = Task(title="Fail", payload="exit 1", type=TaskType.BASH)

    with pytest.raises(RuntimeError):
        executor.execute(task)

    assert not executor.is_running(task.id)


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="pwsh is not installed")
def test_powershell_task_executes() -> None:
    """PowerShell tasks execute when pwsh is available on the host."""
    executor = default_executor()
    task = Task(title="PowerShell", payload="'hello'", type=TaskType.POWERSHELL)

    result = executor.execute(task)

    assert "hello" in result.output
    assert result.returncode == 0
    assert task.status == TaskStatus.DONE
    assert not executor.is_running(task.id)


def test_bash_task_without_timeout_waits_for_completion() -> None:
    """timeout=None means the subprocess is allowed to run until it exits."""
    executor = default_executor()
    task = Task(title="No timeout", payload="sleep 0.1; echo done", type=TaskType.BASH)

    result = executor.execute(task)

    assert result.output == "done\n"
    assert result.returncode == 0
    assert task.status == TaskStatus.DONE
    assert task.timeout is None


def test_bash_task_times_out() -> None:
    """A subprocess exceeding task.timeout is terminated and marked FAILED."""
    executor = default_executor()
    task = Task(
        title="Slow",
        payload="sleep 30",
        type=TaskType.BASH,
        timeout=0.1,
    )

    with pytest.raises(TimeoutError, match="Task timed out after 0.1 seconds"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "Task timed out after 0.1 seconds"
    assert not executor.is_running(task.id)


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
        patch("executor.subprocess.Popen", side_effect=fake_popen),
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

    with patch("executor.os.killpg", side_effect=ProcessLookupError):
        TaskExecutor._terminate_process(process)

    process.wait.assert_not_called()


def test_terminate_process_escalates_to_sigkill() -> None:
    """Processes that ignore SIGTERM are killed with SIGKILL."""
    process = Mock(spec=subprocess.Popen)
    process.pid = 12345
    process.wait.side_effect = [subprocess.TimeoutExpired(cmd="cmd", timeout=5), 0]

    with patch("executor.os.killpg") as killpg:
        TaskExecutor._terminate_process(process)

    assert killpg.call_args_list == [
        ((12345, signal.SIGTERM),),
        ((12345, signal.SIGKILL),),
    ]
    assert process.wait.call_count == 2


def test_prompt_handler_is_not_configured() -> None:
    """The default PROMPT handler is an explicit placeholder."""
    executor = default_executor()
    task = Task(title="Prompt", payload="hello", type=TaskType.PROMPT)

    with pytest.raises(NotImplementedError, match="Prompt handler not configured"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "Prompt handler not configured"


def test_agent_handler_is_not_configured() -> None:
    """The default AGENT handler is an explicit placeholder."""
    executor = default_executor()
    task = Task(title="Agent", payload="hello", type=TaskType.AGENT)

    with pytest.raises(NotImplementedError, match="Agent handler not configured"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "Agent handler not configured"


def test_cancel_stops_in_flight_bash_task() -> None:
    """Cancelling a running bash task terminates its subprocess."""
    executor = default_executor()
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
