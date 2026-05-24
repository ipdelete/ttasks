"""Task domain model and state-machine rules."""

from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import cast


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
    Once a task reaches DONE, normal public attribute assignment is rejected so
    completed upstream tasks can be safely shared by reference.

    timeout=None is intentional and means no automatic timeout is applied;
    callers should set a positive timeout for bounded subprocess execution.
    """

    title: str
    payload: str
    type: TaskType
    description: str = ""
    error: str | None = None
    timeout: float | None = None
    _id: str = field(default_factory=lambda: str(uuid.uuid4()), repr=False)
    created_at: datetime = field(default_factory=datetime.now)
    _status: TaskStatus = field(default=TaskStatus.PENDING, init=False, repr=False)
    # The most recent TaskResult attached to this task, set by TaskExecutor on
    # every terminal path (DONE, FAILED, CANCELLED). None until first run.
    result: TaskResult | None = field(default=None, init=False, repr=False)

    def __setattr__(self, name: str, value: object) -> None:
        """Reject normal public mutation after the task reaches DONE."""
        if (
            not name.startswith("_")
            and getattr(self, "_status", None) == TaskStatus.DONE
        ):
            raise AttributeError("DONE tasks are immutable")
        super().__setattr__(name, value)

    def __post_init__(self) -> None:
        """Validate task configuration after dataclass initialization."""
        if not isinstance(self.type, TaskType):
            raise TypeError("type must be a TaskType")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be greater than 0")

    @property
    def id(self) -> str:
        """Return the immutable task identity."""
        return self._id

    @property
    def status(self) -> TaskStatus:
        """Return the current lifecycle state without allowing direct writes."""
        return self._status

    def can_transition_to(self, status: TaskStatus) -> bool:
        """Return whether the task may move from its current state to status."""
        if not isinstance(status, TaskStatus):
            raise TypeError("status must be a TaskStatus")
        return status in _ALLOWED_TRANSITIONS[self._status]

    def transition_to(self, status: TaskStatus, error: str | None = None) -> None:
        """Move the task to a new state if the state-machine allows it.

        error is stored for failed transitions and cleared by successful ones
        because the default value is None.
        """
        if not isinstance(status, TaskStatus):
            raise TypeError("status must be a TaskStatus")
        if not self.can_transition_to(status):
            message = (
                f"Cannot transition task from {self._status.value!r} "
                f"to {status.value!r}"
            )
            raise ValueError(message)

        self.error = error
        self._status = status

    def cancel(self) -> None:
        """Cancel the task without discarding any existing error detail.

        Cancellation is intentionally idempotent so duplicate user/API requests
        are harmless, while transition_to(CANCELLED) remains strict.
        """
        if self.status == TaskStatus.CANCELLED:
            return

        self.transition_to(TaskStatus.CANCELLED, error=self.error)

    def __repr__(self):
        """Return a concise representation focused on identity and status."""
        return f"Task(id={self.id!r}, title={self.title!r}, status={self.status.value})"


@dataclass(frozen=True)
class TaskResult:
    """Normalized record of a single task execution.

    Attached to Task.result by TaskExecutor on every terminal path so the
    Task itself is the canonical post-run view. Frozen so a completed run
    record cannot be mutated after the fact.
    """

    task_id: str
    status: TaskStatus
    started_at: datetime
    finished_at: datetime
    duration: float
    output: str = ""
    error: str | None = None
    returncode: int | None = None
    raw: object | None = None

    @classmethod
    def from_raw(
        cls,
        task: Task,
        raw: object,
        *,
        status: TaskStatus,
        started_at: datetime,
        finished_at: datetime,
        duration: float,
    ) -> TaskResult:
        """Normalize a handler return value into a timed TaskResult."""
        if isinstance(raw, subprocess.CompletedProcess):
            completed = cast("subprocess.CompletedProcess[str]", raw)
            return cls(
                task_id=task.id,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                duration=duration,
                output=completed.stdout or "",
                error=completed.stderr or None,
                returncode=completed.returncode,
                raw=completed,
            )

        if isinstance(raw, str):
            return cls(
                task_id=task.id,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                duration=duration,
                output=raw,
                raw=raw,
            )

        return cls(
            task_id=task.id,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            duration=duration,
            raw=raw,
        )
