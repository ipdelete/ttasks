"""Public-API contract: every name in ttasks.__all__ must be importable
from the top-level package, and the resolved objects must match what the
submodules export.

This pins the surface that external callers depend on. If a name is
added or removed in ttasks/__init__.py without updating __all__ (or
vice versa), this test fails.
"""

import ttasks
from ttasks import events as events_mod
from ttasks import executor as executor_mod
from ttasks import store as store_mod
from ttasks import task as task_mod
from ttasks import workflow as workflow_mod

EXPECTED_PUBLIC_NAMES = {
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
    "make_default_executor",
}


def test_all_lists_every_public_name() -> None:
    """ttasks.__all__ matches the expected public surface exactly."""
    assert set(ttasks.__all__) == EXPECTED_PUBLIC_NAMES


def test_every_public_name_is_importable_from_top_level() -> None:
    """Each entry in __all__ resolves to an attribute on the ttasks module."""
    for name in ttasks.__all__:
        assert hasattr(ttasks, name), f"ttasks.{name} is missing"


def test_top_level_names_are_the_same_objects_as_submodule_names() -> None:
    """Top-level re-exports point at the canonical submodule objects."""
    assert ttasks.EventBus is events_mod.EventBus
    assert ttasks.InMemoryStore is store_mod.InMemoryStore
    assert ttasks.InMemoryTaskCollection is store_mod.InMemoryTaskCollection
    assert ttasks.InMemoryGraphCollection is store_mod.InMemoryGraphCollection
    assert ttasks.Store is store_mod.Store
    assert ttasks.TaskCollection is store_mod.TaskCollection
    assert ttasks.GraphCollection is store_mod.GraphCollection
    assert ttasks.Task is task_mod.Task
    assert ttasks.TaskStatus is task_mod.TaskStatus
    assert ttasks.TaskType is task_mod.TaskType
    assert ttasks.TaskResult is task_mod.TaskResult
    assert ttasks.TaskTimeoutError is executor_mod.TaskTimeoutError
    assert ttasks.TaskEvent is events_mod.TaskEvent
    assert ttasks.TaskEventType is events_mod.TaskEventType
    assert ttasks.TaskExecutionError is executor_mod.TaskExecutionError
    assert ttasks.TaskExecutor is executor_mod.TaskExecutor
    assert ttasks.TaskContext is executor_mod.TaskContext
    assert ttasks.TaskCancelled is executor_mod.TaskCancelled
    assert (
        ttasks.make_copilot_agent_handler
        is executor_mod.make_copilot_agent_handler
    )
    assert (
        ttasks.make_copilot_prompt_handler
        is executor_mod.make_copilot_prompt_handler
    )
    assert ttasks.make_default_executor is executor_mod.make_default_executor
    assert ttasks.TaskGraph is workflow_mod.TaskGraph

