import threading
import time

import pytest

from executor import TaskCancelled, TaskExecutor, default_executor
from task import Task, TaskStatus, TaskType


def test_execute_moves_task_through_running_to_done() -> None:
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)

    def handler(task: Task) -> str:
        assert task.status == TaskStatus.RUNNING
        return "ok"

    executor.register(TaskType.BASH, handler)

    assert executor.execute(task) == "ok"
    assert task.status == TaskStatus.DONE


def test_execute_rejects_cancelled_task_without_calling_handler() -> None:
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)
    called = False

    def handler(task: Task) -> None:
        nonlocal called
        called = True

    executor.register(TaskType.BASH, handler)
    task.cancel()

    with pytest.raises(ValueError, match="Cannot execute task with status 'cancelled'"):
        executor.execute(task)

    assert called is False
    assert task.status == TaskStatus.CANCELLED


def test_executor_clears_previous_error_on_successful_retry() -> None:
    executor = TaskExecutor()
    task = Task(title="Example", payload="", type=TaskType.BASH)
    attempts = 0

    def handler(task: Task) -> str:
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

    assert executor.execute(task) == "ok"

    assert task.status == TaskStatus.DONE
    assert task.error is None


def test_bash_task_times_out() -> None:
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


def test_cancel_stops_in_flight_bash_task() -> None:
    executor = default_executor()
    task = Task(title="Long running", payload="sleep 30", type=TaskType.BASH)
    errors: list[BaseException] = []

    def run_task() -> None:
        try:
            executor.execute(task)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run_task)
    thread.start()

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
