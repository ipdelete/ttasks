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
    make_copilot_agent_handler,
    make_copilot_prompt_handler,
    make_default_executor,
)
from .ledger import InMemoryGraphLedger, InMemoryTaskLedger
from .task import Task, TaskResult, TaskStatus, TaskType
from .workflow import TaskGraph

__all__ = [
    "EventBus",
    "InMemoryGraphLedger",
    "Task",
    "TaskCancelled",
    "TaskContext",
    "TaskExecutionError",
    "TaskEvent",
    "TaskEventType",
    "TaskExecutor",
    "TaskGraph",
    "InMemoryTaskLedger",
    "TaskResult",
    "TaskTimeoutError",
    "TaskStatus",
    "TaskType",
    "make_copilot_agent_handler",
    "make_copilot_prompt_handler",
    "make_default_executor",
]
