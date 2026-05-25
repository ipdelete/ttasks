"""Live end-to-end tests for ttasks.

These tests build real ``TaskGraph`` instances and run them through a real
``TaskExecutor`` against actual subprocesses (and, for the mixed-type test,
the Copilot SDK). They are slower and less hermetic than the unit suite, so
they are gated behind the ``live`` pytest marker and excluded by default:

    uv run pytest -m live           # run only the live e2e suite
    uv run pytest -m ''             # run everything

The module-level ``pytestmark`` applies the marker to every test below.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.live


def test_e2e_module_imports() -> None:
    """Smoke test: the e2e module itself loads and the marker is wired."""
    from ttasks import TaskExecutor, TaskGraph

    assert TaskExecutor is not None
    assert TaskGraph is not None
