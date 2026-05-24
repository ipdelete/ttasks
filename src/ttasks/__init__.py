"""ttasks — a small task runner with DAG support.

The public surface is intentionally flat. Submodules
(``ttasks.task``, ``ttasks.executor``, ``ttasks.ledger``, ``ttasks.workflow``)
remain importable for callers who prefer explicit paths.
"""

from .events import EventBus, TaskEvent, TaskEventType
from .executor import (
    TaskCancelled,
    TaskContext,
    TaskExecutionError,
    TaskExecutor,
    TaskTimeoutError,
    make_default_executor,
)
from .ledger import GraphLedger, TaskLedger
from .task import Task, TaskResult, TaskStatus, TaskType
from .workflow import TaskGraph

__all__ = [
    "EventBus",
    "GraphLedger",
    "Task",
    "TaskCancelled",
    "TaskContext",
    "TaskExecutionError",
    "TaskEvent",
    "TaskEventType",
    "TaskExecutor",
    "TaskGraph",
    "TaskLedger",
    "TaskResult",
    "TaskTimeoutError",
    "TaskStatus",
    "TaskType",
    "make_default_executor",
]
