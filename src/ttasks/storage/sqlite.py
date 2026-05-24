"""SQLite-backed durable task and graph ledgers."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

from ttasks.task import Task, TaskResult, TaskStatus, TaskType
from ttasks.workflow import TaskGraph

_SCHEMA_VERSION = "1"


class SQLiteTaskLedger:
    """Dictionary-like durable registry for tasks backed by SQLite.

    Assigning a task snapshot with ``ledger[task.id] = task`` saves it
    immediately. Loaded tasks are detached snapshots; mutating them later does
    not write through until they are assigned again or passed to :meth:`save`.
    """

    def __init__(self, path: str | Path) -> None:
        """Open or create a SQLite task ledger at path."""
        self.path = Path(path)
        if self.path.parent != Path(""):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def save(self, task: Task) -> None:
        """Persist task under its own ID."""
        self[task.id] = task

    def __setitem__(self, task_id: str, task: Task) -> None:
        """Store and durably save a task under its own ID."""
        if not isinstance(task, Task):
            raise TypeError(f"Expected Task, got {type(task).__name__}")
        if task_id != task.id:
            raise ValueError("task_id must match task.id")

        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO tasks (
                    id, title, description, payload, type, status,
                    error, timeout, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    payload = excluded.payload,
                    type = excluded.type,
                    status = excluded.status,
                    error = excluded.error,
                    timeout = excluded.timeout,
                    created_at = excluded.created_at
                """,
                _task_values(task),
            )
            if task.result is None:
                connection.execute(
                    "DELETE FROM task_results WHERE task_id = ?",
                    (task.id,),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO task_results (
                        task_id, status, started_at, finished_at, duration,
                        output, error, returncode
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(task_id) DO UPDATE SET
                        status = excluded.status,
                        started_at = excluded.started_at,
                        finished_at = excluded.finished_at,
                        duration = excluded.duration,
                        output = excluded.output,
                        error = excluded.error,
                        returncode = excluded.returncode
                    """,
                    _result_values(task.result),
                )

    def __getitem__(self, task_id: str) -> Task:
        """Return the task for task_id or raise KeyError if it is missing."""
        with self._connect() as connection:
            task_row = connection.execute(
                "SELECT * FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            if task_row is None:
                raise KeyError(task_id)
            result_row = connection.execute(
                "SELECT * FROM task_results WHERE task_id = ?",
                (task_id,),
            ).fetchone()

        result = _result_from_row(result_row) if result_row is not None else None
        return _task_from_row(task_row, result)

    def __iter__(self) -> Iterator[Task]:
        """Iterate over stored task snapshots by creation time, then ID."""
        for task_id in self._task_ids():
            yield self[task_id]

    def __len__(self) -> int:
        """Return the number of tasks currently stored."""
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM tasks").fetchone()
        return int(row["count"])

    def __delitem__(self, task_id: str) -> None:
        """Remove a task and its result from the ledger entirely."""
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            if cursor.rowcount == 0:
                raise KeyError(task_id)

    def cancel(self, task_id: str) -> None:
        """Cancel a task and save the updated snapshot."""
        task = self[task_id]
        task.cancel()
        self.save(task)

    def __contains__(self, task_id: str) -> bool:
        """Return whether task_id is present in the ledger."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
        return row is not None

    def __repr__(self) -> str:
        """Return a concise representation with the number of stored tasks."""
        return f"SQLiteTaskLedger({len(self)} tasks)"

    def _connect(self) -> sqlite3.Connection:
        """Return a SQLite connection configured for this ledger."""
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _init_schema(self) -> None:
        """Create ledger tables when they do not exist."""
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    timeout REAL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS task_results (
                    task_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    duration REAL NOT NULL,
                    output TEXT NOT NULL,
                    error TEXT,
                    returncode INTEGER,
                    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS graphs (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS graph_tasks (
                    graph_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    is_finally INTEGER NOT NULL DEFAULT 0,
                    is_optional INTEGER NOT NULL DEFAULT 0,
                    position INTEGER NOT NULL,
                    PRIMARY KEY(graph_id, task_id),
                    FOREIGN KEY(graph_id) REFERENCES graphs(id) ON DELETE CASCADE,
                    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS graph_dependencies (
                    graph_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    dependency_id TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    PRIMARY KEY(graph_id, task_id, dependency_id),
                    FOREIGN KEY(graph_id) REFERENCES graphs(id) ON DELETE CASCADE,
                    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
                    FOREIGN KEY(dependency_id) REFERENCES tasks(id) ON DELETE CASCADE
                )
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO metadata(key, value)
                VALUES ('schema_version', ?)
                """,
                (_SCHEMA_VERSION,),
            )

    def _task_ids(self) -> list[str]:
        """Return task IDs in stable ledger iteration order."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id FROM tasks ORDER BY created_at, id"
            ).fetchall()
        return [str(row["id"]) for row in rows]


class SQLiteGraphLedger:
    """Dictionary-like durable registry for TaskGraph objects backed by SQLite."""

    def __init__(
        self,
        path: str | Path,
        *,
        tasks: SQLiteTaskLedger | None = None,
    ) -> None:
        """Open or create a SQLite graph ledger at path."""
        self.path = Path(path)
        self.tasks = tasks if tasks is not None else SQLiteTaskLedger(self.path)
        self.tasks._init_schema()

    def save(self, graph: TaskGraph) -> None:
        """Persist graph under its own ID."""
        self[graph.id] = graph

    def __setitem__(self, graph_id: str, graph: TaskGraph) -> None:
        """Store and durably save a graph under its own ID."""
        if not isinstance(graph, TaskGraph):
            raise TypeError(f"Expected TaskGraph, got {type(graph).__name__}")
        if graph_id != graph.id:
            raise ValueError("graph_id must match graph.id")

        graph_tasks = list(graph)
        dependency_ids = {
            task.id: [dependency.id for dependency in graph[task]]
            for task in graph_tasks
        }
        finally_ids = set(graph._finally)
        optional_ids = set(graph._optional)

        for task in graph_tasks:
            self.tasks.save(task)

        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO graphs (id, title, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    title = excluded.title,
                    created_at = excluded.created_at
                """,
                (graph.id, graph.title, graph.created_at.isoformat()),
            )
            connection.execute(
                "DELETE FROM graph_dependencies WHERE graph_id = ?",
                (graph.id,),
            )
            connection.execute(
                "DELETE FROM graph_tasks WHERE graph_id = ?",
                (graph.id,),
            )
            for position, task in enumerate(graph_tasks):
                connection.execute(
                    """
                    INSERT INTO graph_tasks (
                        graph_id, task_id, is_finally, is_optional, position
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        graph.id,
                        task.id,
                        int(task.id in finally_ids),
                        int(task.id in optional_ids),
                        position,
                    ),
                )
            for task in graph_tasks:
                for position, dependency_id in enumerate(dependency_ids[task.id]):
                    connection.execute(
                        """
                        INSERT INTO graph_dependencies (
                            graph_id, task_id, dependency_id, position
                        )
                        VALUES (?, ?, ?, ?)
                        """,
                        (graph.id, task.id, dependency_id, position),
                    )

    def __getitem__(self, graph_id: str) -> TaskGraph:
        """Return the graph for graph_id or raise KeyError if it is missing."""
        with self._connect() as connection:
            graph_row = connection.execute(
                "SELECT * FROM graphs WHERE id = ?",
                (graph_id,),
            ).fetchone()
            if graph_row is None:
                raise KeyError(graph_id)
            task_rows = connection.execute(
                """
                SELECT * FROM graph_tasks
                WHERE graph_id = ?
                ORDER BY position, task_id
                """,
                (graph_id,),
            ).fetchall()
            dependency_rows = connection.execute(
                """
                SELECT * FROM graph_dependencies
                WHERE graph_id = ?
                ORDER BY task_id, position, dependency_id
                """,
                (graph_id,),
            ).fetchall()

        graph = TaskGraph(ledger=self.tasks, title=str(graph_row["title"]))
        object.__setattr__(graph, "_id", str(graph_row["id"]))
        graph.created_at = datetime.fromisoformat(str(graph_row["created_at"]))

        dependencies: dict[str, list[str]] = {
            str(row["task_id"]): [] for row in task_rows
        }
        for row in dependency_rows:
            dependencies[str(row["task_id"])].append(str(row["dependency_id"]))

        for row in task_rows:
            task_id = str(row["task_id"])
            task = self.tasks[task_id]
            graph._ledger[task.id] = task
            graph._deps[task.id] = dependencies[task.id]
            if bool(row["is_finally"]):
                graph._finally.add(task.id)
            if bool(row["is_optional"]):
                graph._optional.add(task.id)

        return graph

    def __iter__(self) -> Iterator[TaskGraph]:
        """Iterate over stored graph snapshots by creation time, then ID."""
        for graph_id in self._graph_ids():
            yield self[graph_id]

    def __len__(self) -> int:
        """Return the number of graphs currently stored."""
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM graphs").fetchone()
        return int(row["count"])

    def __delitem__(self, graph_id: str) -> None:
        """Remove a graph from the ledger without deleting its tasks."""
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM graphs WHERE id = ?", (graph_id,))
            if cursor.rowcount == 0:
                raise KeyError(graph_id)

    def __contains__(self, graph_id: str) -> bool:
        """Return whether graph_id is present in the ledger."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM graphs WHERE id = ?",
                (graph_id,),
            ).fetchone()
        return row is not None

    def __repr__(self) -> str:
        """Return a concise representation with the number of stored graphs."""
        return f"SQLiteGraphLedger({len(self)} graphs)"

    def _connect(self) -> sqlite3.Connection:
        """Return a SQLite connection configured for this ledger."""
        return self.tasks._connect()

    def _graph_ids(self) -> list[str]:
        """Return graph IDs in stable ledger iteration order."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id FROM graphs ORDER BY created_at, id"
            ).fetchall()
        return [str(row["id"]) for row in rows]


def _task_values(task: Task) -> tuple[Any, ...]:
    """Return task values in tasks-table column order."""
    return (
        task.id,
        task.title,
        task.description,
        task.payload,
        task.type.value,
        task.status.value,
        task.error,
        task.timeout,
        task.created_at.isoformat(),
    )


def _result_values(result: TaskResult) -> tuple[Any, ...]:
    """Return result values in task_results-table column order."""
    return (
        result.task_id,
        result.status.value,
        result.started_at.isoformat(),
        result.finished_at.isoformat(),
        result.duration,
        result.output,
        result.error,
        result.returncode,
    )


def _task_from_row(row: sqlite3.Row, result: TaskResult | None) -> Task:
    """Reconstruct a task snapshot from a SQLite row and optional result."""
    task = Task(
        title=str(row["title"]),
        description=str(row["description"]),
        payload=str(row["payload"]),
        type=TaskType(str(row["type"])),
        error=row["error"],
        timeout=row["timeout"],
        _id=str(row["id"]),
        created_at=datetime.fromisoformat(str(row["created_at"])),
    )
    object.__setattr__(task, "_status", TaskStatus(str(row["status"])))
    object.__setattr__(task, "result", result)
    return task


def _result_from_row(row: sqlite3.Row) -> TaskResult:
    """Reconstruct a TaskResult from a SQLite row, omitting raw data."""
    return TaskResult(
        task_id=str(row["task_id"]),
        status=TaskStatus(str(row["status"])),
        started_at=datetime.fromisoformat(str(row["started_at"])),
        finished_at=datetime.fromisoformat(str(row["finished_at"])),
        duration=float(row["duration"]),
        output=str(row["output"]),
        error=row["error"],
        returncode=row["returncode"],
        raw=None,
    )
