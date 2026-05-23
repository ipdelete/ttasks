import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskType(Enum):
    BASH = "bash"
    POWERSHELL = "powershell"
    PROMPT = "prompt"
    AGENT = "agent"


_ALLOWED_TRANSITIONS = {
    TaskStatus.PENDING: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED},
    TaskStatus.FAILED: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.DONE: set(),
    TaskStatus.CANCELLED: set(),
}


@dataclass
class Task:
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
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be greater than 0")

    @property
    def status(self) -> TaskStatus:
        return self._status

    def can_transition_to(self, status: TaskStatus) -> bool:
        return status in _ALLOWED_TRANSITIONS[self._status]

    def transition_to(self, status: TaskStatus, error: str | None = None) -> None:
        if not self.can_transition_to(status):
            message = (
                f"Cannot transition task from {self._status.value!r} "
                f"to {status.value!r}"
            )
            raise ValueError(message)

        self._status = status
        self.error = error

    def cancel(self) -> None:
        self.transition_to(TaskStatus.CANCELLED)

    def __repr__(self):
        return f"Task(id={self.id!r}, title={self.title!r}, status={self.status.value})"
