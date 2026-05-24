"""In-memory task ledger."""

from collections.abc import Iterator

from .task import Task


class TaskLedger:
    """Dictionary-like registry for tasks keyed by their own task IDs."""

    def __init__(self):
        """Create an empty task ledger."""
        self._tasks: dict[str, Task] = {}

    def __setitem__(self, task_id: str, task: Task) -> None:
        """Store a task under its own ID.

        The explicit task_id argument keeps dictionary-style syntax available,
        while the validation prevents the ledger from disagreeing with task.id.
        """
        if not isinstance(task, Task):
            raise TypeError(f"Expected Task, got {type(task).__name__}")
        if task_id != task.id:
            raise ValueError("task_id must match task.id")
        self._tasks[task_id] = task

    def __getitem__(self, task_id: str) -> Task:
        """Return the task for task_id or raise KeyError if it is missing."""
        return self._tasks[task_id]

    def __iter__(self) -> Iterator[Task]:
        """Iterate over stored tasks in insertion order."""
        return iter(self._tasks.values())

    def __len__(self) -> int:
        """Return the number of tasks currently stored."""
        return len(self._tasks)

    def __delitem__(self, task_id: str) -> None:
        """Remove a task from the ledger entirely."""
        del self._tasks[task_id]

    def cancel(self, task_id: str) -> None:
        """Cancel a task while keeping it in the ledger for later inspection."""
        self._tasks[task_id].cancel()

    def __contains__(self, task_id: str) -> bool:
        """Return whether task_id is present in the ledger."""
        return task_id in self._tasks

    def __repr__(self) -> str:
        """Return a concise representation with the number of stored tasks."""
        return f"TaskLedger({len(self._tasks)} tasks)"
