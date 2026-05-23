"""ttasks — a small task ledger and executor SDK.

Public API. Anything not exported here is considered private to the package
and may change without notice.

Consumers (CLI, kanban server, TUI, etc.) should depend only on names re-exported
from this module. The SDK itself does not know about HTTP, JSON, UI columns, or
any other consumer concern.
"""

from ttasks.executor import (
    TaskCancelled,
    TaskContext,
    TaskExecutor,
    TaskResult,
    default_executor,
)
from ttasks.ledger import TaskLedger
from ttasks.runner import TaskEvent, TaskRunner
from ttasks.task import Task, TaskStatus, TaskType

__all__ = [
    "Task",
    "TaskCancelled",
    "TaskContext",
    "TaskEvent",
    "TaskExecutor",
    "TaskLedger",
    "TaskResult",
    "TaskRunner",
    "TaskStatus",
    "TaskType",
    "default_executor",
]
