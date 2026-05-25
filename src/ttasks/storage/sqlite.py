"""SQLite-backed durable :class:`Store`."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, MutableMapping
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any

from ttasks.task import Task, TaskResult, TaskStatus, TaskType
from ttasks.workflow import TaskGraph

_SCHEMA_VERSION = "1"
_CONNECT_TIMEOUT_SECONDS = 30.0


class _Connection:
    """Per-store connection helper: shared schema init, per-call connections.

    Each save uses its own connection so the SQLite GIL behavior is fine for
    concurrent writes from :meth:`TaskGraph.run` thread pools. WAL mode plus
    a generous busy timeout absorbs short write contention.
    """

    def __init__(self, path: str | Path) -> None:
        """Open or create the SQLite database at ``path`` and init the schema."""
        self.path = Path(path)
        if self.path.parent != Path(""):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._schema_lock = RLock()
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        """Return a fresh SQLite connection configured for the store."""
        connection = sqlite3.connect(self.path, timeout=_CONNECT_TIMEOUT_SECONDS)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _init_schema(self) -> None:
        """Create tables and tune SQLite for concurrent writes."""
        with self._schema_lock, self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
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


class SQLiteTaskCollection(MutableMapping[str, Task]):
    """SQLite-backed task collection. Returns detached snapshots on read."""

    def __init__(self, connection: _Connection) -> None:
        """Wrap ``connection`` as a task collection."""
        self._connection = connection

    def save(self, task: Task) -> None:
        """Persist ``task`` under its own ID."""
        self[task.id] = task

    def __setitem__(self, task_id: str, task: Task) -> None:
        """Durably store ``task`` and its current result under its own ID."""
        if not isinstance(task, Task):
            raise TypeError(f"Expected Task, got {type(task).__name__}")
        if task_id != task.id:
            raise ValueError("task_id must match task.id")
        with self._connection.connect() as connection:
            _upsert_task(connection, task)

    def __getitem__(self, task_id: str) -> Task:
        """Return a detached task snapshot for ``task_id`` or raise ``KeyError``."""
        with self._connection.connect() as connection:
            task_row = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if task_row is None:
                raise KeyError(task_id)
            result_row = connection.execute(
                "SELECT * FROM task_results WHERE task_id = ?", (task_id,)
            ).fetchone()
        result = _result_from_row(result_row) if result_row is not None else None
        return _task_from_row(task_row, result)

    def __delitem__(self, task_id: str) -> None:
        """Remove the task and its result row."""
        with self._connection.connect() as connection:
            cursor = connection.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            if cursor.rowcount == 0:
                raise KeyError(task_id)

    def __iter__(self) -> Iterator[str]:
        """Iterate over task IDs in stable (created_at, id) order."""
        with self._connection.connect() as connection:
            rows = connection.execute(
                "SELECT id FROM tasks ORDER BY created_at, id"
            ).fetchall()
        return iter(str(row["id"]) for row in rows)

    def __len__(self) -> int:
        """Return the number of stored tasks."""
        with self._connection.connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM tasks").fetchone()
        return int(row["count"])

    def __contains__(self, key: object) -> bool:
        """Return whether ``key`` (task or task id) is present."""
        if isinstance(key, Task):
            key = key.id
        if not isinstance(key, str):
            return False
        with self._connection.connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM tasks WHERE id = ?", (key,)
            ).fetchone()
        return row is not None

    def cancel(self, task_id: str) -> None:
        """Cancel a task and save the updated snapshot."""
        task = self[task_id]
        task.cancel()
        self.save(task)


class SQLiteGraphCollection(MutableMapping[str, TaskGraph]):
    """SQLite-backed graph collection. Saves graph + member tasks atomically."""

    def __init__(
        self,
        connection: _Connection,
        tasks: SQLiteTaskCollection,
    ) -> None:
        """Wrap ``connection`` as a graph collection sharing ``tasks``."""
        self._connection = connection
        self._tasks = tasks

    def save(self, graph: TaskGraph) -> None:
        """Persist ``graph`` under its own ID."""
        self[graph.id] = graph

    def __setitem__(self, graph_id: str, graph: TaskGraph) -> None:
        """Atomically store ``graph`` metadata, membership, edges, and tasks."""
        if not isinstance(graph, TaskGraph):
            raise TypeError(f"Expected TaskGraph, got {type(graph).__name__}")
        if graph_id != graph.id:
            raise ValueError("graph_id must match graph.id")

        members = list(graph)
        with self._connection.connect() as connection:
            for member in members:
                _upsert_task(connection, member)
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
                "DELETE FROM graph_dependencies WHERE graph_id = ?", (graph.id,)
            )
            connection.execute(
                "DELETE FROM graph_tasks WHERE graph_id = ?", (graph.id,)
            )
            for position, task in enumerate(members):
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
                        int(graph.is_finally(task)),
                        int(graph.is_optional(task)),
                        position,
                    ),
                )
            for task in members:
                for position, dep in enumerate(graph.dependencies(task)):
                    connection.execute(
                        """
                        INSERT INTO graph_dependencies (
                            graph_id, task_id, dependency_id, position
                        )
                        VALUES (?, ?, ?, ?)
                        """,
                        (graph.id, task.id, dep.id, position),
                    )

    def __getitem__(self, graph_id: str) -> TaskGraph:
        """Return a detached graph snapshot for ``graph_id``.

        Member tasks are loaded as snapshots from the task collection; the
        returned graph is independent of any in-memory references.
        """
        with self._connection.connect() as connection:
            graph_row = connection.execute(
                "SELECT * FROM graphs WHERE id = ?", (graph_id,)
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

        graph = TaskGraph(title=str(graph_row["title"]))
        object.__setattr__(graph, "_id", str(graph_row["id"]))
        graph.created_at = datetime.fromisoformat(str(graph_row["created_at"]))

        deps_by_task: dict[str, list[str]] = {
            str(row["task_id"]): [] for row in task_rows
        }
        for row in dependency_rows:
            deps_by_task[str(row["task_id"])].append(str(row["dependency_id"]))

        for row in task_rows:
            task_id = str(row["task_id"])
            task = self._tasks[task_id]
            deps = [self._tasks[d] for d in deps_by_task[task_id]]
            if bool(row["is_finally"]):
                graph.add_finally(
                    task,
                    after=deps,
                    required=not bool(row["is_optional"]),
                )
            else:
                graph[task] = deps

        return graph

    def __delitem__(self, graph_id: str) -> None:
        """Remove the graph metadata; member tasks remain in the task collection."""
        with self._connection.connect() as connection:
            cursor = connection.execute("DELETE FROM graphs WHERE id = ?", (graph_id,))
            if cursor.rowcount == 0:
                raise KeyError(graph_id)

    def __iter__(self) -> Iterator[str]:
        """Iterate over graph IDs in stable (created_at, id) order."""
        with self._connection.connect() as connection:
            rows = connection.execute(
                "SELECT id FROM graphs ORDER BY created_at, id"
            ).fetchall()
        return iter(str(row["id"]) for row in rows)

    def __len__(self) -> int:
        """Return the number of stored graphs."""
        with self._connection.connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM graphs").fetchone()
        return int(row["count"])

    def __contains__(self, key: object) -> bool:
        """Return whether ``key`` (graph or graph id) is present."""
        if isinstance(key, TaskGraph):
            key = key.id
        if not isinstance(key, str):
            return False
        with self._connection.connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM graphs WHERE id = ?", (key,)
            ).fetchone()
        return row is not None


class SQLiteStore:
    """SQLite-backed durable :class:`Store` exposing tasks and graphs."""

    def __init__(self, path: str | Path) -> None:
        """Open or create a SQLite store at ``path``."""
        self.path = Path(path)
        self._connection = _Connection(path)
        self.tasks = SQLiteTaskCollection(self._connection)
        self.graphs = SQLiteGraphCollection(self._connection, self.tasks)

    def __repr__(self) -> str:
        """Return a concise representation including the database path."""
        return f"SQLiteStore({self.path!s})"


def _upsert_task(connection: sqlite3.Connection, task: Task) -> None:
    """Upsert ``task`` and its result row using ``connection``."""
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
            "DELETE FROM task_results WHERE task_id = ?", (task.id,)
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
