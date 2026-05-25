"""Task domain model and state-machine rules."""

from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Literal, cast


class TaskStatus(Enum):
    """Lifecycle states a task can move through."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"


class TaskType(Enum):
    """Kinds of work the executor can dispatch to handlers."""

    BASH = "bash"
    POWERSHELL = "powershell"
    PROMPT = "prompt"
    AGENT = "agent"


# Centralized state-machine definition. All task status changes should flow
# through Task.transition_to() so these rules are enforced consistently.
_ALLOWED_TRANSITIONS = {
    TaskStatus.PENDING: {
        TaskStatus.RUNNING,
        TaskStatus.CANCELLED,
        TaskStatus.BLOCKED,
        TaskStatus.FAILED,
    },
    TaskStatus.RUNNING: {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED},
    TaskStatus.FAILED: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.BLOCKED: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.SUCCEEDED: set(),
    TaskStatus.CANCELLED: set(),
}


@dataclass(eq=False)
class Task:
    """A unit of work tracked by the ledger and executed by TaskExecutor.

    status is intentionally exposed as a read-only property. Use transition_to()
    or cancel() to mutate it so invalid state transitions cannot be bypassed.
    Once a task reaches SUCCEEDED, normal public attribute assignment is rejected so
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
    # every non-BLOCKED terminal path (SUCCEEDED, FAILED, CANCELLED). None until
    # first run. BLOCKED tasks leave this as None — no handler ran.
    _result: TaskResult | None = field(default=None, init=False, repr=False)
    # The id of the direct upstream parent whose state (FAILED/CANCELLED/
    # BLOCKED) caused this task to be marked BLOCKED. None unless blocked.
    _blocked_by: str | None = field(default=None, init=False, repr=False)

    def __setattr__(self, name: str, value: object) -> None:
        """Enforce id immutability and the private-setter discipline.

        ``_id`` is immutable once set so the identity used by
        ``__hash__`` / ``__eq__`` is stable. ``result`` and
        ``blocked_by`` are read-only properties backed by private
        fields — callers must use the executor-internal
        ``_set_result`` / ``_set_blocked_by`` helpers. SUCCEEDED tasks
        reject all remaining public writes so completed upstream tasks
        can be safely shared by reference.
        """
        if name == "_id" and "_id" in self.__dict__:
            raise AttributeError("Task._id is immutable")
        if name in {"result", "blocked_by"}:
            raise AttributeError(
                f"Task.{name} is read-only; use _set_{name}() to mutate"
            )
        if (
            not name.startswith("_")
            and getattr(self, "_status", None) == TaskStatus.SUCCEEDED
        ):
            raise AttributeError("SUCCEEDED tasks are immutable")
        super().__setattr__(name, value)

    def __eq__(self, other: object) -> bool:
        """Tasks are equal iff they share an id (identity-by-id)."""
        if not isinstance(other, Task):
            return NotImplemented
        return self._id == other._id

    def __hash__(self) -> int:
        """Hash by id so tasks work as set / dict keys."""
        return hash(self._id)

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

    @property
    def is_pending(self) -> bool:
        """Return whether the task is in the PENDING state."""
        return self._status == TaskStatus.PENDING

    @property
    def is_running(self) -> bool:
        """Return whether the task is in the RUNNING state."""
        return self._status == TaskStatus.RUNNING

    @property
    def is_succeeded(self) -> bool:
        """Return whether the task has completed successfully."""
        return self._status == TaskStatus.SUCCEEDED

    @property
    def is_failed(self) -> bool:
        """Return whether the task is in the FAILED state."""
        return self._status == TaskStatus.FAILED

    @property
    def is_cancelled(self) -> bool:
        """Return whether the task is in the CANCELLED state."""
        return self._status == TaskStatus.CANCELLED

    @property
    def is_terminal(self) -> bool:
        """Return whether the task is in a terminal state.

        Terminal states: SUCCEEDED, FAILED, CANCELLED, BLOCKED.
        """
        return self._status in {
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.BLOCKED,
        }

    @property
    def is_blocked(self) -> bool:
        """Return whether the task is in the BLOCKED state."""
        return self._status == TaskStatus.BLOCKED

    @property
    def result(self) -> TaskResult | None:
        """Return the latest run's TaskResult, if any."""
        return self._result

    def _set_result(self, result: TaskResult | None) -> None:
        """Attach ``result`` (executor-internal seam)."""
        object.__setattr__(self, "_result", result)

    @property
    def blocked_by(self) -> str | None:
        """ID of the direct upstream parent that triggered the block, if any."""
        return self._blocked_by

    def _set_blocked_by(self, parent_id: str | None) -> None:
        """Attach the blocking parent id (executor-internal seam)."""
        object.__setattr__(self, "_blocked_by", parent_id)

    def can_transition_to(self, status: TaskStatus) -> bool:
        """Return whether the task may move from its current state to status."""
        if not isinstance(status, TaskStatus):
            raise TypeError("status must be a TaskStatus")
        return status in _ALLOWED_TRANSITIONS[self._status]

    def transition_to(self, status: TaskStatus, error: str | None = None) -> None:
        """Move the task to a new state if the state-machine allows it.

        error is stored for failed transitions and cleared by successful ones
        because the default value is None. Entering RUNNING also clears any
        prior run's ``result`` and ``blocked_by`` so a retry starts clean.
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
        if status == TaskStatus.RUNNING:
            object.__setattr__(self, "_result", None)
            object.__setattr__(self, "_blocked_by", None)

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

    @classmethod
    def _make(
        cls,
        task_type: TaskType,
        payload: str,
        *,
        title: str,
        description: str,
        timeout: float | None,
    ) -> Task:
        """Construct a task of ``task_type`` from the shared factory kwargs."""
        return cls(
            title=title,
            payload=payload,
            type=task_type,
            description=description,
            timeout=timeout,
        )

    @classmethod
    def bash(
        cls,
        payload: str,
        *,
        title: str = "",
        description: str = "",
        timeout: float | None = None,
    ) -> Task:
        """Construct a BASH task without requiring callers to import TaskType."""
        return cls._make(
            TaskType.BASH, payload,
            title=title, description=description, timeout=timeout,
        )

    @classmethod
    def powershell(
        cls,
        payload: str,
        *,
        title: str = "",
        description: str = "",
        timeout: float | None = None,
    ) -> Task:
        """Construct a POWERSHELL task without requiring callers to import TaskType."""
        return cls._make(
            TaskType.POWERSHELL, payload,
            title=title, description=description, timeout=timeout,
        )

    @classmethod
    def prompt(
        cls,
        payload: str,
        *,
        title: str = "",
        description: str = "",
        timeout: float | None = None,
    ) -> Task:
        """Construct a PROMPT task without requiring callers to import TaskType."""
        return cls._make(
            TaskType.PROMPT, payload,
            title=title, description=description, timeout=timeout,
        )

    @classmethod
    def agent(
        cls,
        payload: str,
        *,
        title: str = "",
        description: str = "",
        timeout: float | None = None,
    ) -> Task:
        """Construct an AGENT task without requiring callers to import TaskType."""
        return cls._make(
            TaskType.AGENT, payload,
            title=title, description=description, timeout=timeout,
        )


TerminationReason = Literal["exit_code", "timeout", "cancelled", "handler"]


@dataclass(frozen=True)
class TaskResult:
    """Normalized record of a single task execution.

    Attached to Task.result by TaskExecutor on every terminal path so the
    Task itself is the canonical post-run view. Frozen so a completed run
    record cannot be mutated after the fact.

    ``termination_reason`` distinguishes the cause of every terminal
    transition: ``None`` means SUCCEEDED; ``"exit_code"`` means a
    subprocess exited non-zero; ``"timeout"`` means the wall-clock budget
    was exceeded and SIGTERM/SIGKILL fired; ``"cancelled"`` means a
    cooperative cancel signal was honored; ``"handler"`` means the
    handler raised an unstructured exception.
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
    termination_reason: TerminationReason | None = None

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
        base: dict[str, Any] = dict(
            task_id=task.id,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            duration=duration,
        )
        if isinstance(raw, subprocess.CompletedProcess):
            completed = cast("subprocess.CompletedProcess[str]", raw)
            return cls(
                **base,
                output=completed.stdout or "",
                error=completed.stderr or None,
                returncode=completed.returncode,
                raw=completed,
            )

        if isinstance(raw, str):
            return cls(**base, output=raw, raw=raw)

        return cls(**base, raw=raw)
