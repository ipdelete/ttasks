"""JSON shaping for the kanban server.

This module owns the wire format. The SDK (ttasks) stays free of JSON concerns;
if a different consumer wants a different shape, it writes its own adapter.
"""

from __future__ import annotations

from typing import Any

from ttasks import Task


def task_to_dict(task: Task) -> dict[str, Any]:
    """Render a Task as a JSON-friendly dictionary for the kanban UI."""
    return {
        "id": task.id,
        "title": task.title,
        "description": task.description,
        "type": task.type.value,
        "payload": task.payload,
        "status": task.status.value,
        "error": task.error,
        "timeout": task.timeout,
        "created_at": task.created_at.isoformat(),
    }
