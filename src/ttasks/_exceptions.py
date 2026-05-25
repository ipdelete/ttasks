"""Exceptions raised by task execution.

Kept in a dedicated module so users can catch the full hierarchy without
importing executor machinery and so the executor module stays focused on
execution logic.
"""

from __future__ import annotations

import subprocess


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
