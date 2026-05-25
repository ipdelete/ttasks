"""Shared test helpers.

Pytest inserts the directory containing this file into ``sys.path`` so test
modules can import these helpers directly with ``from conftest import ...``.
"""

from ttasks.task import Task


def _bash(title: str = "T", payload: str = "echo t") -> Task:
    """Build a bash task with sensible defaults for tests."""
    return Task.bash(payload, title=title)


def _opaque(value: object) -> object:
    """Hide ``value`` behind ``object`` so the type checker forgets its literal."""
    return value
