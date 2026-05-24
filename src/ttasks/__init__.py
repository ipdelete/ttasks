"""ttasks — a small task runner with DAG support.

The public surface is intentionally flat. Submodules
(``ttasks.task``, ``ttasks.executor``, ``ttasks.ledger``, ``ttasks.workflow``)
remain importable for callers who prefer explicit paths.
"""

from .executor import (
    TaskCancelled,
    TaskContext,
    TaskExecutionError,
    TaskExecutor,
    TaskTimeoutError,
    make_default_executor,
)
from .ledger import TaskLedger
from .task import Task, TaskResult, TaskStatus, TaskType
from .workflow import TaskGraph

__all__ = [
    "Task",
    "TaskCancelled",
    "TaskContext",
    "TaskExecutionError",
    "TaskExecutor",
    "TaskGraph",
    "TaskLedger",
    "TaskResult",
    "TaskTimeoutError",
    "TaskStatus",
    "TaskType",
    "make_default_executor",
]
