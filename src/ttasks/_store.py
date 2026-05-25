"""Unified task and graph storage.

A :class:`Store` exposes two collections, :attr:`tasks` and :attr:`graphs`,
each a ``MutableMapping[str, T]`` keyed by the object's own immutable ID.
The in-memory implementation lives here; SQLite-backed storage lives in
``ttasks.storage.sqlite``.

The store is the single seam between the runtime objects (``Task``,
``TaskGraph``) and any durable backend. ``TaskExecutor`` writes to
``store.tasks`` automatically on every lifecycle transition, so callers
do not have to wire event-bus subscribers for normal persistence.
"""

from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from typing import Protocol, runtime_checkable

from ._graph import TaskGraph
from ._task import Task


class TaskCollection(Protocol):
    """Mapping of task ID to :class:`Task` with a ``save(task)`` shortcut.

    Implementations are real :class:`collections.abc.MutableMapping` subclasses;
    this Protocol describes the structural surface callers should rely on.
    """

    def save(self, task: Task) -> None:
        """Persist ``task`` under its own ID."""

    def __getitem__(self, task_id: str) -> Task: ...
    def __setitem__(self, task_id: str, task: Task) -> None: ...
    def __delitem__(self, task_id: str) -> None: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def __contains__(self, key: object) -> bool: ...


class GraphCollection(Protocol):
    """Mapping of graph ID to :class:`TaskGraph` with a ``save(graph)`` shortcut."""

    def save(self, graph: TaskGraph) -> None:
        """Persist ``graph`` under its own ID."""

    def __getitem__(self, graph_id: str) -> TaskGraph: ...
    def __setitem__(self, graph_id: str, graph: TaskGraph) -> None: ...
    def __delitem__(self, graph_id: str) -> None: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def __contains__(self, key: object) -> bool: ...


@runtime_checkable
class Store(Protocol):
    """A unified store exposing :attr:`tasks` and :attr:`graphs` collections."""

    @property
    def tasks(self) -> TaskCollection:
        """Return the task collection."""

    @property
    def graphs(self) -> GraphCollection:
        """Return the graph collection."""


class InMemoryTaskCollection(MutableMapping[str, Task]):
    """Dict-backed :class:`TaskCollection` that holds live task references."""

    def __init__(self) -> None:
        """Create an empty task collection."""
        self._tasks: dict[str, Task] = {}

    def save(self, task: Task) -> None:
        """Persist ``task`` under its own ID."""
        self[task.id] = task

    def __setitem__(self, task_id: str, task: Task) -> None:
        """Store ``task`` under its own ID."""
        if not isinstance(task, Task):
            raise TypeError(f"Expected Task, got {type(task).__name__}")
        if task_id != task.id:
            raise ValueError("task_id must match task.id")
        self._tasks[task_id] = task

    def __getitem__(self, task_id: str) -> Task:
        """Return the task for ``task_id`` or raise ``KeyError``."""
        return self._tasks[task_id]

    def __delitem__(self, task_id: str) -> None:
        """Remove the task identified by ``task_id``."""
        del self._tasks[task_id]

    def __iter__(self) -> Iterator[str]:
        """Iterate over task IDs in insertion order."""
        return iter(self._tasks)

    def __len__(self) -> int:
        """Return the number of stored tasks."""
        return len(self._tasks)

    def __contains__(self, key: object) -> bool:
        """Return whether ``key`` (task or task id) is present."""
        if isinstance(key, Task):
            return key.id in self._tasks
        return key in self._tasks

    def __repr__(self) -> str:
        """Return a concise representation with the stored-task count."""
        return f"InMemoryTaskCollection({len(self._tasks)} tasks)"

    def cancel(self, task_id: str) -> None:
        """Cancel a task in place, keeping it in the collection."""
        self._tasks[task_id].cancel()


class InMemoryGraphCollection(MutableMapping[str, "TaskGraph"]):
    """Dict-backed :class:`GraphCollection` that holds live graph references."""

    def __init__(self) -> None:
        """Create an empty graph collection."""
        self._graphs: dict[str, TaskGraph] = {}

    def save(self, graph: TaskGraph) -> None:
        """Persist ``graph`` under its own ID."""
        self[graph.id] = graph

    def __setitem__(self, graph_id: str, graph: TaskGraph) -> None:
        """Store ``graph`` under its own ID."""
        if not isinstance(graph, TaskGraph):
            raise TypeError(f"Expected TaskGraph, got {type(graph).__name__}")
        if graph_id != graph.id:
            raise ValueError("graph_id must match graph.id")
        self._graphs[graph_id] = graph

    def __getitem__(self, graph_id: str) -> TaskGraph:
        """Return the graph for ``graph_id`` or raise ``KeyError``."""
        return self._graphs[graph_id]

    def __delitem__(self, graph_id: str) -> None:
        """Remove the graph identified by ``graph_id``."""
        del self._graphs[graph_id]

    def __iter__(self) -> Iterator[str]:
        """Iterate over graph IDs in insertion order."""
        return iter(self._graphs)

    def __len__(self) -> int:
        """Return the number of stored graphs."""
        return len(self._graphs)

    def __contains__(self, key: object) -> bool:
        """Return whether ``key`` (graph or graph id) is present."""
        if isinstance(key, TaskGraph):
            return key.id in self._graphs
        return key in self._graphs

    def __repr__(self) -> str:
        """Return a concise representation with the stored-graph count."""
        return f"InMemoryGraphCollection({len(self._graphs)} graphs)"


class InMemoryStore:
    """Live-reference :class:`Store` backed by in-memory dictionaries."""

    def __init__(self) -> None:
        """Create empty task and graph collections."""
        self.tasks = InMemoryTaskCollection()
        self.graphs = InMemoryGraphCollection()

    def __repr__(self) -> str:
        """Return a concise representation with task and graph counts."""
        return f"InMemoryStore({len(self.tasks)} tasks, {len(self.graphs)} graphs)"
