"""Task event types and publish/subscribe helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from threading import RLock

from .task import Task, TaskStatus


class TaskEventType(Enum):
    """Kinds of task events emitted during execution."""

    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class TaskEvent:
    """A task execution event delivered to event subscribers."""

    type: TaskEventType
    task_id: str
    task: Task
    timestamp: datetime
    previous_status: TaskStatus | None
    status: TaskStatus
    error: str | None = None


TaskEventHandler = Callable[[TaskEvent], None]


class EventBus:
    """Thread-safe publish/subscribe bus for task events."""

    def __init__(self) -> None:
        """Create an event bus with no subscribers or recorded errors."""
        self._subscribers: list[TaskEventHandler] = []
        self._errors: list[BaseException] = []
        self._lock = RLock()

    @property
    def errors(self) -> list[BaseException]:
        """Return subscriber errors recorded while emitting events."""
        with self._lock:
            return list(self._errors)

    def subscribe(self, subscriber: TaskEventHandler) -> Callable[[], None]:
        """Register subscriber and return an idempotent unsubscribe callback."""
        if not callable(subscriber):
            raise TypeError("subscriber must be callable")

        with self._lock:
            self._subscribers.append(subscriber)

        def unsubscribe() -> None:
            """Remove subscriber if it is still registered."""
            with self._lock:
                if subscriber in self._subscribers:
                    self._subscribers.remove(subscriber)

        return unsubscribe

    def emit(self, event: TaskEvent) -> None:
        """Publish event to subscribers without letting observers fail execution."""
        with self._lock:
            subscribers = list(self._subscribers)

        for subscriber in subscribers:
            try:
                subscriber(event)
            except BaseException as error:
                with self._lock:
                    self._errors.append(error)
