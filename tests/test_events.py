"""Tests for task execution event publication."""

from datetime import datetime
from typing import Any

import pytest

from ttasks.events import EventBus, TaskEvent, TaskEventType
from ttasks.task import Task, TaskStatus, TaskType


def _event(task: Task, event_type: TaskEventType) -> TaskEvent:
    """Build a representative task event for event-bus tests."""
    return TaskEvent(
        type=event_type,
        task_id=task.id,
        task=task,
        timestamp=datetime.now(),
        previous_status=TaskStatus.PENDING,
        status=task.status,
    )


def test_event_bus_subscribe_receives_emitted_event() -> None:
    """Subscribers are called with emitted events."""
    bus = EventBus()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    event = _event(task, TaskEventType.STARTED)
    seen: list[TaskEvent] = []

    bus.subscribe(seen.append)
    bus.emit(event)

    assert seen == [event]


def test_event_bus_unsubscribe_stops_future_events() -> None:
    """The unsubscribe callback removes the subscriber."""
    bus = EventBus()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    event = _event(task, TaskEventType.STARTED)
    seen: list[TaskEvent] = []

    unsubscribe = bus.subscribe(seen.append)
    unsubscribe()
    bus.emit(event)

    assert seen == []


def test_event_bus_rejects_non_callable_subscribers() -> None:
    """Only callable subscribers can be registered."""
    bus = EventBus()
    handler: Any = "not callable"

    with pytest.raises(TypeError, match="subscriber must be callable"):
        bus.subscribe(handler)


def test_event_bus_records_subscriber_errors_without_stopping_emit() -> None:
    """Subscriber failures are recorded and later subscribers still run."""
    bus = EventBus()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    event = _event(task, TaskEventType.STARTED)
    seen: list[TaskEvent] = []

    def broken(_event: TaskEvent) -> None:
        """Raise like a buggy observer."""
        raise RuntimeError("observer failed")

    bus.subscribe(broken)
    bus.subscribe(seen.append)

    bus.emit(event)

    assert seen == [event]
    assert len(bus.errors) == 1
    assert isinstance(bus.errors[0], RuntimeError)
    assert str(bus.errors[0]) == "observer failed"
    assert bus.errors is not bus.errors


def test_subscribed_context_manager_delivers_events_inside_block() -> None:
    """Events emitted inside `with bus.subscribed(cb):` reach the callback."""
    bus = EventBus()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    event = _event(task, TaskEventType.STARTED)
    seen: list[TaskEvent] = []

    with bus.subscribed(seen.append):
        bus.emit(event)

    assert seen == [event]


def test_subscribed_context_manager_unsubscribes_on_exit() -> None:
    """After the with block exits, further emits do not reach the callback."""
    bus = EventBus()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    seen: list[TaskEvent] = []

    with bus.subscribed(seen.append):
        bus.emit(_event(task, TaskEventType.STARTED))

    bus.emit(_event(task, TaskEventType.SUCCEEDED))

    assert [e.type for e in seen] == [TaskEventType.STARTED]


def test_subscribed_context_manager_unsubscribes_on_exception() -> None:
    """Exceptions raised inside the with block still unsubscribe the callback."""
    bus = EventBus()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    seen: list[TaskEvent] = []

    with pytest.raises(RuntimeError, match="boom"), bus.subscribed(seen.append):
        raise RuntimeError("boom")

    bus.emit(_event(task, TaskEventType.STARTED))

    assert seen == []


def test_subscribed_context_manager_supports_nested_callbacks() -> None:
    """Nested with-blocks both receive events and both unsubscribe on exit."""
    bus = EventBus()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    outer: list[TaskEvent] = []
    inner: list[TaskEvent] = []

    with bus.subscribed(outer.append), bus.subscribed(inner.append):
        bus.emit(_event(task, TaskEventType.STARTED))

    bus.emit(_event(task, TaskEventType.SUCCEEDED))

    assert [e.type for e in outer] == [TaskEventType.STARTED]
    assert [e.type for e in inner] == [TaskEventType.STARTED]
