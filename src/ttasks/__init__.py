"""ttasks — a small task runner with DAG support.

The public surface is intentionally flat: ``from ttasks import X`` is the
canonical and only supported import path for every name in ``__all__``.
"""

from ._events import EventBus, TaskEvent, TaskEventType
from ._exceptions import TaskCancelled, TaskExecutionError, TaskTimeoutError
from ._executor import (
    TaskContext,
    TaskExecutor,
    make_copilot_agent_handler,
    make_copilot_prompt_handler,
)
from ._graph import TaskGraph
from ._sqlite import SQLiteStore
from ._store import (
    InMemoryStore,
    Store,
)
from ._task import Task, TaskResult, TaskStatus, TaskType

__all__ = [
    "EventBus",
    "InMemoryStore",
    "SQLiteStore",
    "Store",
    "Task",
    "TaskCancelled",
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

# Rewrite ``__module__`` on every public name so tracebacks and ``repr()`` show
# ``ttasks.TaskTimeoutError`` rather than the internal submodule path. This is
# the same idiom used verbatim by httpx, openai-python, and anthropic-python.
import contextlib as _contextlib
import sys as _sys
import types as _types

_locals = locals()
for _name in __all__:
    with _contextlib.suppress(TypeError, AttributeError):
        _locals[_name].__module__ = "ttasks"
del _locals, _name, _contextlib

# Backward-compatible module aliases for documented storage import paths.
# ``from ttasks.storage.sqlite import SQLiteStore`` appears in the README, so
# register lightweight modules that point back to the canonical public object.
_storage_module = _types.ModuleType("ttasks.storage")
_sqlite_module = _types.ModuleType("ttasks.storage.sqlite")
vars(_storage_module)["SQLiteStore"] = SQLiteStore
vars(_storage_module)["sqlite"] = _sqlite_module
vars(_sqlite_module)["SQLiteStore"] = SQLiteStore
storage = _storage_module
_sys.modules.setdefault("ttasks.storage", _storage_module)
_sys.modules.setdefault("ttasks.storage.sqlite", _sqlite_module)
del _storage_module, _sqlite_module, _sys, _types
