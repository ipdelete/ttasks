"""Tests for Task factory constructors that hide TaskType from callers."""

import pytest

from ttasks import Task, TaskType


def test_bash_factory_sets_type_and_payload() -> None:
    """Task.bash returns a BASH Task with the supplied payload."""
    task = Task.bash("echo hi")

    assert task.type is TaskType.BASH
    assert task.payload == "echo hi"
    assert task.title == ""
    assert task.description == ""
    assert task.timeout is None


def test_powershell_factory_sets_type_and_payload() -> None:
    """Task.powershell returns a POWERSHELL Task with the supplied payload."""
    task = Task.powershell("Get-ChildItem")

    assert task.type is TaskType.POWERSHELL
    assert task.payload == "Get-ChildItem"


def test_prompt_factory_sets_type_and_payload() -> None:
    """Task.prompt returns a PROMPT Task with the supplied payload."""
    task = Task.prompt("Summarize the README")

    assert task.type is TaskType.PROMPT
    assert task.payload == "Summarize the README"


def test_agent_factory_sets_type_and_payload() -> None:
    """Task.agent returns an AGENT Task with the supplied payload."""
    task = Task.agent("Investigate the failing test")

    assert task.type is TaskType.AGENT
    assert task.payload == "Investigate the failing test"


def test_factories_accept_title_description_and_timeout() -> None:
    """Factory constructors forward optional metadata keyword arguments."""
    task = Task.bash(
        "echo hi",
        title="greeting",
        description="say hello",
        timeout=1.5,
    )

    assert task.title == "greeting"
    assert task.description == "say hello"
    assert task.timeout == 1.5


def test_factory_timeout_validation_still_applies() -> None:
    """Factories still raise ValueError for non-positive timeouts."""
    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        Task.bash("echo hi", timeout=0)


def test_factory_tasks_have_distinct_ids() -> None:
    """Each factory call produces a Task with a fresh identifier."""
    first = Task.bash("echo a")
    second = Task.bash("echo b")

    assert first.id != second.id
