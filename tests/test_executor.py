"""Tests for task execution, retries, timeout, and cancellation."""

import io
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
from conftest import _bash

from ttasks import (
    Task,
    TaskCancelled,
    TaskContext,
    TaskEvent,
    TaskEventType,
    TaskExecutionError,
    TaskExecutor,
    TaskResult,
    TaskStatus,
    TaskTimeoutError,
    TaskType,
    make_copilot_agent_handler,
    make_copilot_prompt_handler,
)


def assert_result_timing(result: TaskResult, before: datetime, after: datetime) -> None:
    """Assert result timing is populated and bounded by before/after."""
    assert before <= result.started_at <= result.finished_at <= after
    assert result.duration >= 0


def test_task_context_exposes_read_only_task_view() -> None:
    """TaskContext exposes task data without exposing lifecycle mutators."""
    task = Task.bash("echo hi", title="Example", description="Demo task", timeout=1.5)
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
    parent = Task.bash("echo parent", title="Parent")
    child = Task.bash("echo child", title="Child")
    context = TaskContext(child, upstream={parent.id: parent})
    upstream: Any = context.upstream

    assert context.upstream[parent.id] is parent
    with pytest.raises(TypeError):
        upstream["other"] = child


def test_task_context_emit_progress_uses_injected_emitter() -> None:
    """TaskContext can emit validated progress through its injected callback."""
    task = Task.bash("", title="Example")
    seen: list[tuple[float | None, str | None]] = []
    context = TaskContext(task, progress_emitter=lambda percent, message: seen.append(
        (percent, message)
    ))

    context.emit_progress(12, "warming up")
    context.emit_progress(message="still working")

    assert seen == [(12.0, "warming up"), (None, "still working")]


def test_task_context_emit_progress_requires_executor_emitter() -> None:
    """Manually built contexts fail clearly when no executor owns progress."""
    task = Task.bash("", title="Example")
    context = TaskContext(task)

    with pytest.raises(RuntimeError, match="without an executor"):
        context.emit_progress(message="starting")


def test_task_context_emit_progress_rejects_empty_event() -> None:
    """Progress events must carry either a percentage or a message."""
    task = Task.bash("", title="Example")
    context = TaskContext(task)

    with pytest.raises(ValueError, match="percent or message is required"):
        context.emit_progress()


@pytest.mark.parametrize("percent", [True, "50"])
def test_task_context_emit_progress_rejects_non_numeric_percent(
    percent: Any,
) -> None:
    """Progress percent must be numeric but not bool."""
    task = Task.bash("", title="Example")
    context = TaskContext(task)

    with pytest.raises(TypeError, match="percent must be a number"):
        context.emit_progress(percent=percent)


@pytest.mark.parametrize("percent", [-1, 101, float("nan"), float("inf")])
def test_task_context_emit_progress_rejects_out_of_range_percent(
    percent: float,
) -> None:
    """Progress percent must be finite and between 0 and 100."""
    task = Task.bash("", title="Example")
    context = TaskContext(task)

    with pytest.raises(ValueError, match="percent must be between 0 and 100"):
        context.emit_progress(percent=percent)


def test_task_context_emit_progress_rejects_non_string_message() -> None:
    """Progress messages must be strings when provided."""
    task = Task.bash("", title="Example")
    context = TaskContext(task)
    message: Any = 42

    with pytest.raises(TypeError, match="message must be a str"):
        context.emit_progress(message=message)


def test_task_context_emit_progress_raises_when_cancelled() -> None:
    """Emitting progress observes cooperative cancellation."""
    task = Task.bash("", title="Example")
    task.cancel()
    context = TaskContext(task, progress_emitter=lambda _percent, _message: None)

    with pytest.raises(TaskCancelled, match="was cancelled"):
        context.emit_progress(message="too late")


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
    task = Task.bash("", title="Example")
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
    assert [event.status for event in events] == [
        TaskStatus.RUNNING,
        TaskStatus.SUCCEEDED,
    ]
    assert all(event.task is task for event in events)
    assert events[1].task.result is task.result


def test_execute_handler_can_emit_progress_event() -> None:
    """Handlers can report progress through the executor event bus."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    events: list[TaskEvent] = []

    def handler(context: TaskContext) -> str:
        """Emit a representative progress update."""
        context.emit_progress(25, "warming up")
        return "ok"

    executor.register(TaskType.BASH, handler)
    executor.events.subscribe(events.append)

    executor.execute(task)

    assert [event.type for event in events] == [
        TaskEventType.STARTED,
        TaskEventType.PROGRESS,
        TaskEventType.SUCCEEDED,
    ]
    progress = events[1]
    assert progress.task is task
    assert progress.previous_status is None
    assert progress.status == TaskStatus.RUNNING
    assert progress.progress_percent == 25.0
    assert progress.progress_message == "warming up"


def test_progress_subscriber_errors_do_not_fail_task_execution() -> None:
    """Progress observers are isolated like lifecycle observers."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    seen: list[TaskEvent] = []

    def handler(context: TaskContext) -> str:
        """Emit progress and then complete successfully."""
        context.emit_progress(message="halfway")
        return "ok"

    def broken(event: TaskEvent) -> None:
        """Fail only while observing the progress event."""
        if event.type is TaskEventType.PROGRESS:
            raise RuntimeError("observer failed")

    executor.register(TaskType.BASH, handler)
    executor.events.subscribe(broken)
    executor.events.subscribe(seen.append)

    result = executor.execute(task)

    assert result.status == TaskStatus.SUCCEEDED
    assert [event.type for event in seen] == [
        TaskEventType.STARTED,
        TaskEventType.PROGRESS,
        TaskEventType.SUCCEEDED,
    ]
    assert len(executor.events.errors) == 1
    assert str(executor.events.errors[0]) == "observer failed"


def test_execute_failure_emits_started_and_failed_events() -> None:
    """Failed execution emits lifecycle events with error details."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
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
    task = Task.bash("", title="Example")
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
    task = Task.bash("", title="Example")
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
    parent = Task.bash("", title="Parent")
    child = Task.bash("", title="Child")

    def handler(context: TaskContext) -> str:
        """Assert the handler sees the provided upstream task."""
        assert context.upstream[parent.id] is parent
        return "ok"

    executor.register(TaskType.BASH, handler)

    result = executor.execute(child, upstream={parent.id: parent})

    assert result.output == "ok"
    assert child.status == TaskStatus.SUCCEEDED


def test_submit_returns_future_that_completes_with_task_result() -> None:
    """submit() asynchronously executes through the normal executor path."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    events: list[TaskEvent] = []

    def handler(context: TaskContext) -> str:
        """Return a successful handler result."""
        assert context.status == TaskStatus.RUNNING
        return "async-ok"

    executor.register(TaskType.BASH, handler)
    executor.events.subscribe(events.append)

    future = executor.submit(task)
    result = future.result(timeout=1)
    executor.close()

    assert result is task.result
    assert result.status == TaskStatus.SUCCEEDED
    assert result.output == "async-ok"
    assert [event.type for event in events] == [
        TaskEventType.STARTED,
        TaskEventType.SUCCEEDED,
    ]


def test_submit_auto_persists_task_lifecycle() -> None:
    """submit() preserves execute() auto-persistence behavior."""
    from ttasks import InMemoryStore

    store = InMemoryStore()
    executor = TaskExecutor(store=store)
    task = Task.bash("", title="Example")

    def handler(context: TaskContext) -> str:
        """Return a successful handler result."""
        return "ok"

    executor.register(TaskType.BASH, handler)

    result = executor.submit(task).result(timeout=1)
    executor.close()

    assert result.status == TaskStatus.SUCCEEDED
    assert store.tasks[task.id].status == TaskStatus.SUCCEEDED


def test_submit_copies_upstream_mapping_before_worker_runs() -> None:
    """submit() isolates the upstream mapping from caller mutation races."""
    executor = TaskExecutor()
    parent = Task.bash("", title="Parent")
    child = Task.bash("", title="Child")
    release = threading.Event()
    upstream = {parent.id: parent}

    def handler(context: TaskContext) -> str:
        """Wait until the caller mutates its mapping, then inspect context."""
        assert release.wait(timeout=1)
        assert context.upstream[parent.id] is parent
        return "ok"

    executor.register(TaskType.BASH, handler)

    future = executor.submit(child, upstream=upstream)
    upstream.clear()
    release.set()
    result = future.result(timeout=1)
    executor.close()

    assert result.status == TaskStatus.SUCCEEDED


def test_submit_runs_multiple_tasks_concurrently() -> None:
    """submit() supports concurrent execution through the lazy worker pool."""
    executor = TaskExecutor()
    tasks = [Task.bash("", title=f"Task {index}") for index in range(5)]
    release = threading.Event()
    started = 0
    started_lock = threading.Lock()
    all_started = threading.Event()
    events: list[TaskEvent] = []

    def handler(context: TaskContext) -> str:
        """Wait until every submitted task has started before completing."""
        nonlocal started
        with started_lock:
            started += 1
            if started == len(tasks):
                all_started.set()
        assert release.wait(timeout=1)
        return context.title

    executor.register(TaskType.BASH, handler)
    executor.events.subscribe(events.append)

    futures = [executor.submit(task) for task in tasks]
    assert all_started.wait(timeout=1)
    release.set()
    results = [future.result(timeout=1) for future in futures]
    executor.close()

    assert {result.output for result in results} == {task.title for task in tasks}
    assert all(task.status == TaskStatus.SUCCEEDED for task in tasks)
    assert len([event for event in events if event.type is TaskEventType.STARTED]) == 5
    succeeded_events = [
        event for event in events if event.type is TaskEventType.SUCCEEDED
    ]
    assert len(succeeded_events) == 5


def test_submit_missing_handler_future_raises_after_failed_event() -> None:
    """A submitted task without a handler terminalizes before its future raises."""
    executor = TaskExecutor.empty()
    task = Task.bash("", title="Example")
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    future = executor.submit(task)

    with pytest.raises(ValueError, match="No handler registered"):
        future.result(timeout=1)
    executor.close()

    assert task.status == TaskStatus.FAILED
    assert task.result is not None
    assert task.result.termination_reason == "handler"
    assert [event.type for event in events] == [TaskEventType.FAILED]


def test_future_cancel_does_not_cancel_running_task() -> None:
    """Running submitted tasks are cancelled through executor.cancel(), not Future."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    started = threading.Event()

    def handler(context: TaskContext) -> None:
        """Run until cooperative cancellation is requested."""
        started.set()
        while not context.cancelled:
            time.sleep(0.01)
        context.raise_if_cancelled()

    executor.register(TaskType.BASH, handler)

    future = executor.submit(task)
    assert started.wait(timeout=1)
    assert future.cancel() is False
    executor.cancel(task)

    with pytest.raises(TaskCancelled):
        future.result(timeout=1)
    executor.close()
    assert task.status == TaskStatus.CANCELLED


def test_close_is_idempotent_and_rejects_later_submit() -> None:
    """close() can run repeatedly and prevents new async submissions."""
    executor = TaskExecutor()

    executor.close()
    executor.close()

    with pytest.raises(RuntimeError, match="executor is closed"):
        executor.submit(Task.bash("", title="Example"))


def test_close_waits_for_submitted_work_to_finish() -> None:
    """close() drains already-submitted tasks instead of cancelling them."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    started = threading.Event()
    release = threading.Event()

    def handler(context: TaskContext) -> str:
        """Block until close() is waiting, then complete normally."""
        started.set()
        assert release.wait(timeout=1)
        return "done"

    executor.register(TaskType.BASH, handler)
    future = executor.submit(task)
    assert started.wait(timeout=1)
    release.set()
    executor.close()

    assert future.result(timeout=1).status == TaskStatus.SUCCEEDED
    assert task.status == TaskStatus.SUCCEEDED


def test_context_manager_closes_executor() -> None:
    """Leaving a TaskExecutor context closes async execution resources."""
    with TaskExecutor() as executor:
        task = Task.bash("", title="Example")
        executor.register(TaskType.BASH, lambda _context: "ok")
        assert executor.submit(task).result(timeout=1).status == TaskStatus.SUCCEEDED

    with pytest.raises(RuntimeError, match="executor is closed"):
        executor.submit(Task.bash("", title="Later"))


def test_task_result_wraps_non_string_raw_values() -> None:
    """Arbitrary handler return values are preserved on TaskResult.raw."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    raw = {"answer": 42}

    def handler(context: TaskContext) -> dict[str, int]:
        """Return a non-string object to exercise raw result wrapping."""
        return raw

    executor.register(TaskType.BASH, handler)

    result = executor.execute(task)

    assert result.task_id == task.id
    assert result.status == TaskStatus.SUCCEEDED
    assert result.raw == raw
    assert result.started_at <= result.finished_at
    assert result.duration >= 0


def test_execute_terminalizes_task_without_registered_handler() -> None:
    """Missing handler terminalizes the task as FAILED with handler reason."""
    executor = TaskExecutor.empty()
    task = Task.bash("", title="Example")
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    with pytest.raises(ValueError, match="No handler registered for task type 'bash'"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.result is not None
    assert task.result.termination_reason == "handler"
    assert task.result.status == TaskStatus.FAILED
    # No STARTED event is emitted because the task never transitioned to RUNNING.
    assert [e.type for e in events] == [TaskEventType.FAILED]
    assert events[0].previous_status == TaskStatus.PENDING


def test_execute_without_handler_terminalizes_blocked_retry_cleanly() -> None:
    """A retryable BLOCKED task without a handler fails with the handler error."""
    executor = TaskExecutor.empty()
    task = Task.bash("", title="Example")
    task.transition_to(TaskStatus.BLOCKED)
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    with pytest.raises(ValueError, match="No handler registered"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.result is not None
    assert task.result.termination_reason == "handler"
    assert task.result.error == "No handler registered for task type 'bash'"
    assert [e.type for e in events] == [TaskEventType.FAILED]
    assert events[0].previous_status == TaskStatus.BLOCKED


def test_execute_rejects_cancelled_task_without_calling_handler() -> None:
    """Cancelled tasks are rejected before any handler side effects occur."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
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


def test_failed_event_subscriber_sees_attached_result() -> None:
    """Subscribers of FAILED see task.result already attached (no race)."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")

    def handler(context: TaskContext) -> None:
        raise RuntimeError("boom")

    executor.register(TaskType.BASH, handler)

    seen_results: list[TaskResult | None] = []

    def on_event(event: TaskEvent) -> None:
        if event.type == TaskEventType.FAILED:
            seen_results.append(event.task.result)

    executor.events.subscribe(on_event)

    with pytest.raises(RuntimeError):
        executor.execute(task)

    assert len(seen_results) == 1
    assert seen_results[0] is not None
    assert seen_results[0].status == TaskStatus.FAILED


def test_externally_cancelled_running_task_emits_one_cancelled_event() -> None:
    """A task cancelled while RUNNING produces exactly one CANCELLED event."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    def handler(context: TaskContext) -> None:
        # Simulate external cancellation arriving mid-handler.
        executor.cancel(task)
        context.raise_if_cancelled()

    executor.register(TaskType.BASH, handler)

    with pytest.raises(TaskCancelled):
        executor.execute(task)

    cancelled_events = [e for e in events if e.type == TaskEventType.CANCELLED]
    assert len(cancelled_events) == 1
    assert task.status == TaskStatus.CANCELLED


# ---- Step 13: executor.cancel() emits + persists ----------------------------


def test_cancel_pending_task_emits_cancelled_event() -> None:
    """Cancelling a PENDING task emits a single CANCELLED event."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    executor.cancel(task)

    cancelled = [e for e in events if e.type == TaskEventType.CANCELLED]
    assert len(cancelled) == 1
    assert cancelled[0].previous_status == TaskStatus.PENDING
    assert cancelled[0].status == TaskStatus.CANCELLED
    assert task.status == TaskStatus.CANCELLED


def test_cancel_pending_task_attaches_result() -> None:
    """Cancelling a PENDING task attaches a CANCELLED TaskResult."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")

    executor.cancel(task)

    assert task.result is not None
    assert task.result.status == TaskStatus.CANCELLED
    assert task.result.termination_reason == "cancelled"


def test_cancel_pending_task_persists_to_store() -> None:
    """Cancelling a PENDING task triggers a store.tasks.save call."""
    store = Mock()
    store.tasks = Mock()
    executor = TaskExecutor(store=store)
    task = Task.bash("", title="Example")

    executor.cancel(task)

    store.tasks.save.assert_called_once_with(task)


def test_cancel_succeeded_task_is_silent_noop() -> None:
    """Cancelling a SUCCEEDED task is a silent no-op (no raise, no emit, no save).

    SUCCEEDED is an irreversible sink: callers shouldn't need to know which
    states accept transitions, and a successful run must not be retroactively
    rewritten as CANCELLED.
    """
    store = Mock()
    store.tasks = Mock()
    executor = TaskExecutor(store=store)
    task = Task.bash("echo hi", title="Example")
    task.transition_to(TaskStatus.RUNNING)
    succeeded_result = TaskResult(
        task_id=task.id,
        status=TaskStatus.SUCCEEDED,
        started_at=datetime.now(),
        finished_at=datetime.now(),
        duration=0.0,
    )
    task.transition_to(TaskStatus.SUCCEEDED)
    object.__setattr__(task, "_result", succeeded_result)
    store.tasks.save.reset_mock()
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    executor.cancel(task)

    cancelled = [e for e in events if e.type == TaskEventType.CANCELLED]
    assert cancelled == []
    store.tasks.save.assert_not_called()
    assert task.status == TaskStatus.SUCCEEDED
    assert task.result is succeeded_result
    assert task.result.status == TaskStatus.SUCCEEDED


def test_cancel_already_cancelled_with_live_process_still_terminates() -> None:
    """Already-CANCELLED task with a lingering subprocess still gets the process reaped.

    State idempotence on Task must not skip OS-level cleanup: the subprocess
    may still be alive even after the Task state flipped to CANCELLED.
    """
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.CANCELLED)
    process = Mock(spec=subprocess.Popen)
    process.poll.return_value = None
    executor._running_processes[task.id] = process
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    with patch.object(executor, "_terminate_process") as terminate:
        executor.cancel(task)
        terminate.assert_called_once_with(process)

    cancelled = [e for e in events if e.type == TaskEventType.CANCELLED]
    assert cancelled == []
    assert task.status == TaskStatus.CANCELLED


def test_cancel_idempotent_does_not_double_emit() -> None:
    """Repeated cancel() calls only emit one CANCELLED event."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    executor.cancel(task)
    executor.cancel(task)

    cancelled = [e for e in events if e.type == TaskEventType.CANCELLED]
    assert len(cancelled) == 1


def test_cancel_failed_task_emits_cancelled_event() -> None:
    """A FAILED task may be cancelled and the cancel emits a CANCELLED event."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.FAILED, error="boom")
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    executor.cancel(task)

    cancelled = [e for e in events if e.type == TaskEventType.CANCELLED]
    assert len(cancelled) == 1
    assert cancelled[0].previous_status == TaskStatus.FAILED
    assert task.status == TaskStatus.CANCELLED



    """A successful retry clears the stale error from a previous failure."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
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
    assert result.status == TaskStatus.SUCCEEDED
    assert task.status == TaskStatus.SUCCEEDED
    assert task.error is None


def test_default_executor_can_execute_bash() -> None:
    """The default executor includes a working BASH handler."""
    executor = TaskExecutor()
    task = Task.bash("echo hi", title="Example")

    result = executor.execute(task)

    assert result.task_id == task.id
    assert result.status == TaskStatus.SUCCEEDED
    assert result.output == "hi\n"
    assert result.error is None
    assert result.returncode == 0
    assert isinstance(result.raw, subprocess.CompletedProcess)
    assert task.status == TaskStatus.SUCCEEDED
    assert not executor.is_running(task.id)


def test_bash_task_emits_output_events_and_retains_result_output() -> None:
    """Built-in subprocess handlers stream stdout/stderr and retain them."""
    executor = TaskExecutor()
    task = Task.bash(
        "printf 'out1\\nout2\\n'; printf 'err1\\nerr2\\n' >&2",
        title="Streaming",
    )
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    result = executor.execute(task)

    output_events = [event for event in events if event.type is TaskEventType.OUTPUT]
    assert "".join(
        event.output_chunk or ""
        for event in output_events
        if event.output_stream == "stdout"
    ) == result.output
    assert "".join(
        event.output_chunk or ""
        for event in output_events
        if event.output_stream == "stderr"
    ) == result.error
    assert result.output == "out1\nout2\n"
    assert result.error == "err1\nerr2\n"
    assert all(event.previous_status is None for event in output_events)
    assert all(event.status == TaskStatus.RUNNING for event in output_events)
    assert events[-1].type is TaskEventType.SUCCEEDED


def test_failed_bash_task_emits_output_events_before_failed_event() -> None:
    """Streaming output is visible even when the subprocess exits non-zero."""
    executor = TaskExecutor()
    task = Task.bash("echo before; echo boom >&2; exit 7", title="Failing command")
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    with pytest.raises(TaskExecutionError, match="boom"):
        executor.execute(task)

    assert task.result is not None
    output_events = [event for event in events if event.type is TaskEventType.OUTPUT]
    assert "".join(
        event.output_chunk or ""
        for event in output_events
        if event.output_stream == "stdout"
    ) == task.result.output
    assert "".join(
        event.output_chunk or ""
        for event in output_events
        if event.output_stream == "stderr"
    ) == task.result.error
    assert events[-1].type is TaskEventType.FAILED
    assert all(
        events.index(event) < len(events) - 1
        for event in output_events
    )


def test_timed_out_bash_task_emits_partial_output_events() -> None:
    """Timeouts stream output produced before termination."""
    executor = TaskExecutor()
    task = Task.bash(
        "echo before; echo warn >&2; sleep 30",
        title="Partial timeout",
        timeout=0.1,
    )
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    with pytest.raises(TaskTimeoutError, match="Task timed out after 0.1 seconds"):
        executor.execute(task)

    assert task.result is not None
    assert isinstance(task.result.raw, subprocess.CompletedProcess)
    output_events = [event for event in events if event.type is TaskEventType.OUTPUT]
    assert "".join(
        event.output_chunk or ""
        for event in output_events
        if event.output_stream == "stdout"
    ) == task.result.output
    assert "".join(
        event.output_chunk or ""
        for event in output_events
        if event.output_stream == "stderr"
    ) == task.result.raw.stderr
    assert events[-1].type is TaskEventType.FAILED


def test_output_subscriber_errors_do_not_fail_task_execution() -> None:
    """Output observers are isolated like lifecycle observers."""
    executor = TaskExecutor()
    task = Task.bash("echo hi", title="Streaming")
    seen: list[TaskEvent] = []

    def broken(event: TaskEvent) -> None:
        """Fail only while observing output."""
        if event.type is TaskEventType.OUTPUT:
            raise RuntimeError("observer failed")

    executor.events.subscribe(broken)
    executor.events.subscribe(seen.append)

    result = executor.execute(task)

    assert result.status == TaskStatus.SUCCEEDED
    assert any(event.type is TaskEventType.OUTPUT for event in seen)
    assert len(executor.events.errors) == 1
    assert str(executor.events.errors[0]) == "observer failed"


def test_bash_task_supports_shell_syntax() -> None:
    """BASH tasks intentionally execute shell syntax such as pipes."""
    executor = TaskExecutor()
    task = Task.bash("printf 'hello\\n' | grep hello", title="Shell syntax")

    result = executor.execute(task)

    assert result.output == "hello\n"
    assert result.returncode == 0
    assert task.status == TaskStatus.SUCCEEDED


def test_bash_nonzero_exit_marks_task_failed() -> None:
    """A shell command with a non-zero return code fails the task."""
    executor = TaskExecutor()
    task = Task.bash("exit 7", title="Failing command")

    with pytest.raises(TaskExecutionError, match="exited with code 7"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "exited with code 7"
    assert not executor.is_running(task.id)


def test_bash_failure_uses_stderr_as_error() -> None:
    """Subprocess stderr is preferred over the generic exit-code message."""
    executor = TaskExecutor()
    task = Task.bash("echo boom >&2; exit 1", title="Failing command")

    with pytest.raises(TaskExecutionError, match="boom"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "boom\n"
    assert not executor.is_running(task.id)


def test_failed_subprocess_result_preserves_output_error_and_returncode() -> None:
    """Failed subprocesses still attach structured process details."""
    executor = TaskExecutor()
    task = Task.bash("echo before; echo boom >&2; exit 7", title="Structured failure")

    with pytest.raises(TaskExecutionError, match="boom"):
        executor.execute(task)

    assert task.result is not None
    assert task.result.status == TaskStatus.FAILED
    assert task.result.output == "before\n"
    assert task.result.error == "boom\n"
    assert task.result.returncode == 7
    assert isinstance(task.result.raw, subprocess.CompletedProcess)


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="pwsh is not installed")
def test_powershell_task_executes() -> None:
    """PowerShell tasks execute when pwsh is available on the host."""
    executor = TaskExecutor()
    task = Task.powershell("'hello'", title="PowerShell")

    result = executor.execute(task)

    assert "hello" in result.output
    assert result.returncode == 0
    assert task.status == TaskStatus.SUCCEEDED
    assert not executor.is_running(task.id)


def test_bash_task_with_non_utf8_output_succeeds_with_replacement_text() -> None:
    """Successful subprocesses are not failed by undecodable output bytes."""
    executor = TaskExecutor()
    task = Task.bash("python -c 'import sys; sys.stdout.buffer.write(bytes([255]))'")

    result = executor.execute(task)

    assert task.status == TaskStatus.SUCCEEDED
    assert result.output == "�"
    assert result.returncode == 0


def test_bash_task_without_timeout_waits_for_completion() -> None:
    """timeout=None means the subprocess is allowed to run until it exits."""
    executor = TaskExecutor()
    task = Task.bash("sleep 0.1; echo done", title="No timeout")

    result = executor.execute(task)

    assert result.output == "done\n"
    assert result.returncode == 0
    assert task.status == TaskStatus.SUCCEEDED
    assert task.timeout is None


def test_bash_task_times_out() -> None:
    """A subprocess exceeding task.timeout is terminated and marked FAILED."""
    executor = TaskExecutor()
    task = Task.bash("sleep 30", title="Slow", timeout=0.1)

    with pytest.raises(TaskTimeoutError, match="Task timed out after 0.1 seconds"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "Task timed out after 0.1 seconds"
    assert not executor.is_running(task.id)


def test_real_subprocess_timeout_kills_within_wall_budget() -> None:
    """Real `sleep 5` with timeout=0.1 must be killed well under 2s wall time.

    Pins both the termination reason and the practical guarantee that
    timeouts SIGTERM the subprocess rather than waiting for it to exit
    naturally. No mocks: this is the executor against a real `sleep`.
    """
    executor = TaskExecutor()
    task = Task.bash("sleep 5", title="Real timeout", timeout=0.1)

    start = time.monotonic()
    with pytest.raises(TaskTimeoutError):
        executor.execute(task)
    elapsed = time.monotonic() - start

    assert task.status == TaskStatus.FAILED
    assert task.result is not None
    assert task.result.termination_reason == "timeout"
    assert elapsed < 2.0, f"timeout did not kill the subprocess (took {elapsed:.2f}s)"
    assert not executor.is_running(task.id)


def test_timed_out_subprocess_result_preserves_partial_output() -> None:
    """Timeout results retain output captured before termination."""
    executor = TaskExecutor()
    task = Task.bash(
        "echo before; echo warn >&2; sleep 30",
        title="Partial timeout",
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
    task = Task.bash("", title="Example")

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
    task = Task.bash("", title="Example")

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
    task = Task.bash("", title="Example")

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
    task = Task.bash("", title="Example")

    executor.cancel(task)
    executor.cancel(task)

    assert task.status == TaskStatus.CANCELLED
    assert not executor.is_running(task.id)


def test_run_command_terminates_if_task_cancelled_during_process_start() -> None:
    """A cancellation between Popen and process registration is still honored."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    task.transition_to(TaskStatus.RUNNING)
    process = Mock(spec=subprocess.Popen)
    process.pid = 12345
    process.returncode = -signal.SIGTERM
    process.stdout = io.StringIO("")
    process.stderr = io.StringIO("")

    def fake_popen(*args: object, **kwargs: object) -> Mock:
        """Cancel after process creation but before _run_command can register it."""
        task.cancel()
        return process

    with (
        patch("ttasks._executor.subprocess.Popen", side_effect=fake_popen),
        patch.object(executor, "_terminate_process") as terminate,
        pytest.raises(TaskCancelled, match=f"Task {task.id!r} was cancelled"),
    ):
        executor._run_command(TaskContext(task), "ignored", shell=True)

    terminate.assert_called_once_with(process)
    assert not executor.is_running(task.id)


def test_run_command_reports_cancelled_nonzero_process_as_task_cancelled() -> None:
    """A cancelled task with a non-zero process exit raises TaskCancelled."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    task.transition_to(TaskStatus.RUNNING)
    task.cancel()

    with pytest.raises(TaskCancelled, match=f"Task {task.id!r} was cancelled"):
        executor._run_command(TaskContext(task), "exit 1", shell=True)

    assert not executor.is_running(task.id)


def test_terminate_process_ignores_already_exited_process() -> None:
    """A missing process group is harmless during termination."""
    process = Mock(spec=subprocess.Popen)
    process.pid = 12345

    with patch("ttasks._executor.os.killpg", side_effect=ProcessLookupError):
        TaskExecutor._terminate_process(process)

    process.wait.assert_not_called()


def test_terminate_process_escalates_to_sigkill() -> None:
    """Processes that ignore SIGTERM are killed with SIGKILL."""
    process = Mock(spec=subprocess.Popen)
    process.pid = 12345
    process.wait.side_effect = [subprocess.TimeoutExpired(cmd="cmd", timeout=5), 0]

    with patch("ttasks._executor.os.killpg") as killpg:
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
        "ttasks._executor.os.killpg",
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
    task = Task.prompt("hello", title="Prompt")

    result = executor.execute(task)

    assert result.output == "hello back"
    assert task.status == TaskStatus.SUCCEEDED
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
    task = Task.prompt("hello", title="Prompt", timeout=2.5)

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
    task = Task.prompt("hello", title="Prompt")

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
    task = Task.prompt("hello", title="Prompt")

    result = executor.execute(task)

    assert result.output == ""


def test_copilot_prompt_handler_unknown_response_data_returns_empty_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unexpected Copilot response data normalizes to empty output."""
    install_fake_copilot(monkeypatch, data=object())
    executor = TaskExecutor()
    task = Task.prompt("hello", title="Prompt")

    result = executor.execute(task)

    assert result.output == ""


def test_copilot_prompt_handler_sdk_error_marks_task_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Copilot SDK errors follow normal task failure handling."""
    install_fake_copilot(monkeypatch, error=RuntimeError("sdk boom"))
    executor = TaskExecutor()
    task = Task.prompt("hello", title="Prompt")

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
    task = Task.agent("inspect repo", title="Agent")

    result = executor.execute(task)

    assert result.output == "agent done"
    assert task.status == TaskStatus.SUCCEEDED
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
    task = Task.agent("hello", title="Agent", timeout=3.5)

    executor.execute(task)

    assert recorded["timeout"] == 3.5


def test_copilot_agent_handler_allows_model_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Callers can register a Copilot agent handler with a different model."""
    recorded = install_fake_copilot(monkeypatch, content="done")
    executor = TaskExecutor()
    executor.register(TaskType.AGENT, make_copilot_agent_handler(model="agent-custom"))
    task = Task.agent("hello", title="Agent")

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
    task = Task.agent("hello", title="Agent")

    with pytest.raises(RuntimeError, match="agent boom"):
        executor.execute(task)

    assert task.status == TaskStatus.FAILED
    assert task.error == "agent boom"


def test_cancel_stops_in_flight_bash_task() -> None:
    """Cancelling a running bash task terminates its subprocess."""
    executor = TaskExecutor()
    task = Task.bash("sleep 30", title="Long running")
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
    task = Task.bash("echo hi", title="X")
    assert task.result is None


def test_successful_execute_sets_task_result() -> None:
    """A task that completes successfully carries its TaskResult on the task."""
    task = Task.bash("echo hi", title="X")
    executor = TaskExecutor()
    returned = executor.execute(task)

    assert task.result is returned
    assert task.result.status == TaskStatus.SUCCEEDED
    assert task.result.output.strip() == "hi"
    assert task.result.returncode == 0


def test_failed_execute_sets_task_result_with_failed_status() -> None:
    """A task that fails still produces a TaskResult attached to the task."""
    task = Task.bash("exit 1", title="X")
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
    task = Task.bash("sleep 5", title="X")
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
    task = Task.bash("exit 1", title="X")
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
    assert task.result.status == TaskStatus.SUCCEEDED
    assert task.result.output.strip() == "recovered"


# ---- store-backed auto-persistence ------------------------------------------




def test_executor_without_store_does_not_record_persistence() -> None:
    """When no store is configured the executor never touches persistence."""
    executor = TaskExecutor()
    task = _bash()
    executor.execute(task)
    assert executor.store is None
    assert executor.persistence_errors == []


def test_executor_auto_persists_each_lifecycle_transition() -> None:
    """Both STARTED and SUCCEEDED transitions write the task to the store."""
    from ttasks import InMemoryStore

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
    task = _bash()
    executor.execute(task)

    assert TaskStatus.RUNNING in saved_statuses
    assert TaskStatus.SUCCEEDED in saved_statuses
    assert store.tasks[task.id].status == TaskStatus.SUCCEEDED


def test_executor_saves_before_emitting_lifecycle_event() -> None:
    """Subscribers reading the store on event see the new task state."""
    from ttasks import InMemoryStore

    store = InMemoryStore()
    executor = TaskExecutor(store=store)
    observed: list[TaskStatus] = []

    def on_event(event: TaskEvent) -> None:
        snapshot = store.tasks.get(event.task_id)
        if snapshot is not None:
            observed.append(snapshot.status)

    executor.events.subscribe(on_event)
    task = _bash()
    executor.execute(task)

    assert TaskStatus.RUNNING in observed
    assert TaskStatus.SUCCEEDED in observed


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

    task = _bash()
    result = executor.execute(task)

    assert result.status == TaskStatus.SUCCEEDED
    assert task.status == TaskStatus.SUCCEEDED
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
    from ttasks import InMemoryStore

    store = InMemoryStore()
    executor = TaskExecutor.empty(store=store)

    assert executor.store is store


def test_is_registered_rejects_non_task_type() -> None:
    """is_registered validates its argument is a TaskType."""
    executor = TaskExecutor.empty()
    bogus: Any = "bash"

    with pytest.raises(TypeError, match="task_type must be a TaskType"):
        executor.is_registered(bogus)


class TestTerminationReason:
    """TaskResult.termination_reason distinguishes the cause of every terminal."""

    def test_successful_task_has_no_termination_reason(self) -> None:
        task = _bash("ok", "echo ok")
        TaskExecutor().execute(task)
        assert task.result is not None
        assert task.result.termination_reason is None

    def test_exit_code_failure_records_exit_code(self) -> None:
        task = _bash("bad", "exit 1")
        with pytest.raises(TaskExecutionError):
            TaskExecutor().execute(task)
        assert task.result is not None
        assert task.result.termination_reason == "exit_code"

    def test_timeout_failure_records_timeout(self) -> None:
        task = _bash("slow", "sleep 5")
        task.timeout = 0.1
        with pytest.raises(TaskTimeoutError):
            TaskExecutor().execute(task)
        assert task.result is not None
        assert task.result.termination_reason == "timeout"

    def test_cancelled_task_records_cancelled(self) -> None:
        task = _bash("cancelled", "echo c")

        def handler(_ctx: TaskContext) -> str:
            raise TaskCancelled("user")

        executor = TaskExecutor.empty()
        executor.register(TaskType.BASH, handler)
        with pytest.raises(TaskCancelled):
            executor.execute(task)
        assert task.result is not None
        assert task.result.termination_reason == "cancelled"

    def test_handler_exception_records_handler(self) -> None:
        task = _bash("boom", "echo boom")

        def handler(_ctx: TaskContext) -> str:
            raise RuntimeError("kaboom")

        executor = TaskExecutor.empty()
        executor.register(TaskType.BASH, handler)
        with pytest.raises(RuntimeError):
            executor.execute(task)
        assert task.result is not None
        assert task.result.termination_reason == "handler"


# ---- Step 15: public mark_blocked seam --------------------------------------


def test_mark_blocked_rejection_does_not_set_blocked_by() -> None:
    """A failed mark_blocked() call must not leave stale block metadata."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.SUCCEEDED)

    with pytest.raises(ValueError, match="Cannot transition task"):
        executor.mark_blocked(task, "parent-id-123")

    assert task.status == TaskStatus.SUCCEEDED
    assert task.blocked_by is None


def test_mark_blocked_transitions_and_emits_blocked_event() -> None:
    """executor.mark_blocked(task, parent_id) is the public scheduler seam."""
    executor = TaskExecutor()
    task = Task.bash("", title="Example")
    events: list[TaskEvent] = []
    executor.events.subscribe(events.append)

    executor.mark_blocked(task, "parent-id-123")

    assert task.status == TaskStatus.BLOCKED
    assert task.blocked_by == "parent-id-123"
    blocked = [e for e in events if e.type == TaskEventType.BLOCKED]
    assert len(blocked) == 1
    assert blocked[0].previous_status == TaskStatus.PENDING
