"""In-memory task and graph ledgers."""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Protocol

from .task import Task

if TYPE_CHECKING:
    from .workflow import TaskGraph


class TaskLedgerProtocol(Protocol):
    """Structural protocol for task ledgers consumed by TaskGraph."""

    def __setitem__(self, task_id: str, task: Task) -> None:
        """Store a task under task_id."""

    def __getitem__(self, task_id: str) -> Task:
        """Return the task for task_id."""

    def __iter__(self) -> Iterator[Task]:
        """Iterate over stored tasks."""

    def __contains__(self, task_id: str) -> bool:
        """Return whether task_id is present."""


class InMemoryTaskLedger:
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
        return f"InMemoryTaskLedger({len(self._tasks)} tasks)"


class InMemoryGraphLedger:
    """Dictionary-like registry for graphs keyed by their own graph IDs."""

    def __init__(self):
        """Create an empty graph ledger."""
        self._graphs: dict[str, TaskGraph] = {}

    def __setitem__(self, graph_id: str, graph: TaskGraph) -> None:
        """Store a graph under its own ID.

        The explicit graph_id argument keeps dictionary-style syntax available,
        while validation prevents the ledger from disagreeing with graph.id.
        """
        from .workflow import TaskGraph

        if not isinstance(graph, TaskGraph):
            raise TypeError(f"Expected TaskGraph, got {type(graph).__name__}")
        if graph_id != graph.id:
            raise ValueError("graph_id must match graph.id")
        self._graphs[graph_id] = graph

    def __getitem__(self, graph_id: str) -> TaskGraph:
        """Return the graph for graph_id or raise KeyError if it is missing."""
        return self._graphs[graph_id]

    def __iter__(self) -> Iterator[TaskGraph]:
        """Iterate over stored graphs in insertion order."""
        return iter(self._graphs.values())

    def __len__(self) -> int:
        """Return the number of graphs currently stored."""
        return len(self._graphs)

    def __delitem__(self, graph_id: str) -> None:
        """Remove a graph from the ledger entirely."""
        del self._graphs[graph_id]

    def __contains__(self, graph_id: str) -> bool:
        """Return whether graph_id is present in the ledger."""
        return graph_id in self._graphs

    def __repr__(self) -> str:
        """Return a concise representation with the number of stored graphs."""
        return f"InMemoryGraphLedger({len(self._graphs)} graphs)"
