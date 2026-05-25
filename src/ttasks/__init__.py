"""ttasks — a small task runner with DAG support.

The public surface is intentionally flat. Submodules
(``ttasks.task``, ``ttasks.executor``, ``ttasks.store``, ``ttasks.workflow``)
remain importable for callers who prefer explicit paths.
"""

from ._exceptions import TaskCancelled, TaskExecutionError, TaskTimeoutError
from .events import EventBus, TaskEvent, TaskEventType
from .executor import (
    TaskContext,
    TaskExecutor,
    make_copilot_agent_handler,
    make_copilot_prompt_handler,
)
from .store import (
    GraphCollection,
    InMemoryGraphCollection,
    InMemoryStore,
    InMemoryTaskCollection,
    Store,
    TaskCollection,
)
from .task import Task, TaskResult, TaskStatus, TaskType
from .workflow import TaskGraph

__all__ = [
    "EventBus",
    "GraphCollection",
    "InMemoryGraphCollection",
    "InMemoryStore",
    "InMemoryTaskCollection",
    "Store",
    "Task",
    "TaskCancelled",
    "TaskCollection",
    "TaskContext",
    "TaskExecutionError",
    "TaskEvent",
    "TaskEventType",
    "TaskExecutor",
    "TaskGraph",
    "TaskResult",
    "TaskTimeoutError",
    "TaskStatus",
    "TaskType",
    "make_copilot_agent_handler",
    "make_copilot_prompt_handler",
]
