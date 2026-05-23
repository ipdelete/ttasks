"""Task execution and process-management helpers."""

import os
import signal
import subprocess
from collections.abc import Callable
from typing import Any

from task import Task, TaskStatus, TaskType


class TaskCancelled(RuntimeError):
    """Raised when execution is stopped because a task was cancelled."""


class TaskExecutor:
    """Dispatch tasks to registered handlers and manage task state transitions."""

    def __init__(self):
        """Create an executor with no handlers and no running subprocesses."""
        self._handlers: dict[TaskType, Callable[[Task], Any]] = {}
        self._running_processes: dict[str, subprocess.Popen[str]] = {}

    def register(self, task_type: TaskType, handler: Callable[[Task], Any]) -> None:
        """Register handler as the executor for task_type."""
        self._handlers[task_type] = handler

    def is_running(self, task_id: str) -> bool:
        """Return whether task_id currently has a live subprocess."""
        process = self._running_processes.get(task_id)
        return process is not None and process.poll() is None

    def cancel(self, task: Task) -> None:
        """Cancel a task and terminate its subprocess if one is active."""
        task.cancel()

        process = self._running_processes.get(task.id)
        if process is not None and process.poll() is None:
            self._terminate_process(process)

    def execute(self, task: Task) -> Any:
        """Execute task with its registered handler.

        Execution always moves through RUNNING first. Successful handlers move
        the task to DONE; failing handlers move it to FAILED unless cancellation
        happened while the handler was in flight.
        """
        if not task.can_transition_to(TaskStatus.RUNNING):
            raise ValueError(f"Cannot execute task with status {task.status.value!r}")

        handler = self._handlers.get(task.type)
        if handler is None:
            raise ValueError(f"No handler registered for task type {task.type.value!r}")

        task.transition_to(TaskStatus.RUNNING)
        try:
            result = handler(task)
            if task.status == TaskStatus.CANCELLED:
                raise TaskCancelled(f"Task {task.id!r} was cancelled")
            task.transition_to(TaskStatus.DONE)
            return result
        except TaskCancelled:
            raise
        except Exception as e:
            if task.status == TaskStatus.CANCELLED:
                raise TaskCancelled(f"Task {task.id!r} was cancelled") from e
            task.transition_to(TaskStatus.FAILED, error=str(e))
            raise

    def _run_command(
        self,
        task: Task,
        args: str | list[str],
        *,
        shell: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        """Run a subprocess for task and enforce timeout/cancellation behavior."""
        process = subprocess.Popen(
            args,
            shell=shell,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        self._running_processes[task.id] = process
        if task.status == TaskStatus.CANCELLED:
            self._terminate_process(process)
        try:
            try:
                stdout, stderr = process.communicate(timeout=task.timeout)
            except subprocess.TimeoutExpired as e:
                self._terminate_process(process)
                process.communicate()
                raise TimeoutError(
                    f"Task timed out after {task.timeout} seconds"
                ) from e
        finally:
            self._running_processes.pop(task.id, None)

        result = subprocess.CompletedProcess(
            args=args,
            returncode=process.returncode,
            stdout=stdout,
            stderr=stderr,
        )
        if result.returncode != 0:
            if task.status == TaskStatus.CANCELLED:
                raise TaskCancelled(f"Task {task.id!r} was cancelled")
            raise RuntimeError(result.stderr or f"exited with code {result.returncode}")
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
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()

    def _run_bash(self, task: Task) -> subprocess.CompletedProcess[str]:
        """Run trusted bash payload text through the system shell."""
        # Intentionally uses shell=True because TaskType.BASH represents trusted
        # shell code, not a shell-free argv command.
        return self._run_command(task, task.payload, shell=True)

    def _run_powershell(self, task: Task) -> subprocess.CompletedProcess[str]:
        """Run trusted PowerShell payload text with pwsh."""
        return self._run_command(task, ["pwsh", "-Command", task.payload])


def _run_prompt(task: Task) -> str:
    """Placeholder for prompt execution until a prompt backend is registered."""
    raise NotImplementedError("Prompt handler not configured")


def _run_agent(task: Task) -> str:
    """Placeholder for agent execution until an agent backend is registered."""
    raise NotImplementedError("Agent handler not configured")


def default_executor() -> TaskExecutor:
    """Return a TaskExecutor configured with the built-in task handlers."""
    executor = TaskExecutor()
    executor.register(TaskType.BASH, executor._run_bash)
    executor.register(TaskType.POWERSHELL, executor._run_powershell)
    executor.register(TaskType.PROMPT, _run_prompt)
    executor.register(TaskType.AGENT, _run_agent)
    return executor
