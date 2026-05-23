"""Task domain model and state-machine rules."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class TaskStatus(Enum):
    """Lifecycle states a task can move through."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskType(Enum):
    """Kinds of work the executor can dispatch to handlers."""

    BASH = "bash"
    POWERSHELL = "powershell"
    PROMPT = "prompt"
    AGENT = "agent"


# Centralized state-machine definition. All task status changes should flow
# through Task.transition_to() so these rules are enforced consistently.
_ALLOWED_TRANSITIONS = {
    TaskStatus.PENDING: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED},
    TaskStatus.FAILED: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.DONE: set(),
    TaskStatus.CANCELLED: set(),
}


@dataclass
class Task:
    """A unit of work tracked by the ledger and executed by TaskExecutor.

    status is intentionally exposed as a read-only property. Use transition_to()
    or cancel() to mutate it so invalid state transitions cannot be bypassed.
    """

    title: str
    payload: str
    type: TaskType
    description: str = ""
    error: str | None = None
    timeout: float | None = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=datetime.now)
    _status: TaskStatus = field(default=TaskStatus.PENDING, init=False, repr=False)

    def __post_init__(self) -> None:
        """Validate task configuration after dataclass initialization."""
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be greater than 0")

    @property
    def status(self) -> TaskStatus:
        """Return the current lifecycle state without allowing direct writes."""
        return self._status

    def can_transition_to(self, status: TaskStatus) -> bool:
        """Return whether the task may move from its current state to status."""
        return status in _ALLOWED_TRANSITIONS[self._status]

    def transition_to(self, status: TaskStatus, error: str | None = None) -> None:
        """Move the task to a new state if the state-machine allows it.

        error is stored for failed transitions and cleared by successful ones
        because the default value is None.
        """
        if not self.can_transition_to(status):
            message = (
                f"Cannot transition task from {self._status.value!r} "
                f"to {status.value!r}"
            )
            raise ValueError(message)

        self._status = status
        self.error = error

    def cancel(self) -> None:
        """Cancel the task using the state-machine transition rules."""
        self.transition_to(TaskStatus.CANCELLED)

    def __repr__(self):
        """Return a concise representation focused on identity and status."""
        return f"Task(id={self.id!r}, title={self.title!r}, status={self.status.value})"
