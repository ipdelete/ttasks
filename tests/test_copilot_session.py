"""Tests for shared Copilot agent sessions."""

from __future__ import annotations

import asyncio
import sys
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from ttasks import (
    CopilotAgentSession,
    Task,
    TaskCancelled,
    TaskExecutor,
    TaskStatus,
    TaskType,
)


def install_fake_copilot(
    monkeypatch: pytest.MonkeyPatch,
    *,
    content: str | None = "response",
    data: object | None = None,
    delay: float = 0,
    enter_error: BaseException | None = None,
    session_enter_error: BaseException | None = None,
    session_exit_error: BaseException | None = None,
    client_exit_error: BaseException | None = None,
    no_abort: bool = False,
) -> dict[str, Any]:
    """Install fake Copilot SDK modules and return recorded calls."""
    recorded: dict[str, Any] = {
        "clients": [],
        "sessions": [],
        "prompts": [],
        "timeouts": [],
        "create_sessions": [],
        "events": [],
        "active_sends": 0,
        "max_active_sends": 0,
    }

    class AssistantMessageData:
        """Fake assistant message data."""

        def __init__(self, content: str | None) -> None:
            self.content = content

    class FakeSession:
        """Fake Copilot session."""

        def __init__(self, on_event: Any | None) -> None:
            self.on_event = on_event
            self.exited = False
            self.aborted = False
            recorded["sessions"].append(self)

        async def __aenter__(self) -> FakeSession:
            if session_enter_error is not None:
                raise session_enter_error
            return self

        async def __aexit__(self, *args: object) -> None:
            self.exited = True
            recorded["session_exited"] = True
            if session_exit_error is not None:
                raise session_exit_error

        async def send_and_wait(
            self,
            prompt: str,
            *,
            timeout: float | None,
        ) -> object:
            recorded["prompts"].append(prompt)
            recorded["timeouts"].append(timeout)
            recorded["active_sends"] += 1
            recorded["max_active_sends"] = max(
                recorded["max_active_sends"],
                recorded["active_sends"],
            )
            if self.on_event is not None:
                event = SimpleNamespace(data=AssistantMessageData(f"event:{prompt}"))
                recorded["events"].append(event)
                self.on_event(event)
            try:
                if delay:
                    await asyncio.sleep(delay)
                if data is not None:
                    return SimpleNamespace(data=data)
                return SimpleNamespace(data=AssistantMessageData(content))
            finally:
                recorded["active_sends"] -= 1

        if not no_abort:

            async def abort(self) -> None:
                self.aborted = True
                recorded["aborted"] = True

    class FakeClient:
        """Fake Copilot client."""

        def __init__(self) -> None:
            self.exited = False
            recorded["clients"].append(self)

        async def __aenter__(self) -> FakeClient:
            if enter_error is not None:
                raise enter_error
            recorded["client_entered"] = True
            return self

        async def __aexit__(self, *args: object) -> None:
            self.exited = True
            recorded["client_exited"] = True
            if client_exit_error is not None:
                raise client_exit_error

        async def create_session(self, **kwargs: object) -> FakeSession:
            recorded["create_sessions"].append(kwargs)
            return FakeSession(kwargs.get("on_event"))

    class FakePermissionHandler:
        """Fake Copilot permission handler namespace."""

        @staticmethod
        def approve_all(*args: object) -> object:
            return object()

    copilot: Any = ModuleType("copilot")
    copilot.__path__ = []
    copilot.CopilotClient = FakeClient
    generated: Any = ModuleType("copilot.generated")
    generated.__path__ = []
    session_events: Any = ModuleType("copilot.generated.session_events")
    session_events.AssistantMessageData = AssistantMessageData
    session_module: Any = ModuleType("copilot.session")
    session_module.PermissionHandler = FakePermissionHandler

    monkeypatch.setitem(sys.modules, "copilot", copilot)
    monkeypatch.setitem(sys.modules, "copilot.generated", generated)
    monkeypatch.setitem(sys.modules, "copilot.generated.session_events", session_events)
    monkeypatch.setitem(sys.modules, "copilot.session", session_module)
    return recorded


def test_shared_copilot_agent_session_reuses_one_sdk_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multiple AGENT tasks share one Copilot client/session."""
    recorded = install_fake_copilot(monkeypatch, content="done")
    with CopilotAgentSession() as session:
        executor = TaskExecutor.empty()
        executor.register(TaskType.AGENT, session.handler())

        first = executor.execute(Task.agent("first", title="First"))
        second = executor.execute(Task.agent("second", title="Second"))

    assert first.output == "done"
    assert second.output == "done"
    assert recorded["prompts"] == ["first", "second"]
    assert len(recorded["clients"]) == 1
    assert len(recorded["sessions"]) == 1
    assert recorded["client_exited"] is True
    assert recorded["session_exited"] is True


def test_shared_copilot_agent_session_rejects_invalid_configuration() -> None:
    """Session construction rejects invalid model and timeout values."""
    with pytest.raises(ValueError, match="model must not be empty"):
        CopilotAgentSession(model="")
    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        CopilotAgentSession(timeout=0)


def test_shared_copilot_agent_session_passes_session_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Session configuration is forwarded to CopilotClient.create_session."""
    recorded = install_fake_copilot(monkeypatch)

    with CopilotAgentSession(
        model="agent-custom",
        reasoning_effort="medium",
        working_directory="/tmp/repo",
        streaming=True,
        excluded_tools=["bash"],
    ):
        pass

    create_session = recorded["create_sessions"][0]
    assert create_session["model"] == "agent-custom"
    assert create_session["reasoning_effort"] == "medium"
    assert create_session["working_directory"] == "/tmp/repo"
    assert create_session["streaming"] is True
    assert create_session["excluded_tools"] == ["bash"]
    assert callable(create_session["on_permission_request"])
    assert callable(create_session["on_event"])


def test_shared_copilot_agent_session_handler_uses_task_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task timeout overrides the session default timeout for handler calls."""
    recorded = install_fake_copilot(monkeypatch)
    with CopilotAgentSession(timeout=30) as session:
        executor = TaskExecutor.empty()
        executor.register(TaskType.AGENT, session.handler())

        executor.execute(Task.agent("default timeout", title="Default"))
        executor.execute(Task.agent("task timeout", title="Task", timeout=2.5))

    assert recorded["timeouts"] == [30, 2.5]


def test_shared_copilot_agent_session_handler_uses_no_default_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When neither session nor task timeout is set, timeout is forwarded as None."""
    recorded = install_fake_copilot(monkeypatch)
    with CopilotAgentSession() as session:
        executor = TaskExecutor.empty()
        executor.register(TaskType.AGENT, session.handler())

        executor.execute(Task.agent("no timeout", title="No timeout"))

    assert recorded["timeouts"] == [None]


def test_shared_copilot_agent_session_on_subscribes_to_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Constructor and explicit event subscribers receive session events."""
    install_fake_copilot(monkeypatch)
    seen: list[str] = []
    explicit: list[str] = []

    def on_event(event: Any) -> None:
        seen.append(event.data.content)

    with CopilotAgentSession(on_event=on_event) as session:
        unsubscribe = session.on(lambda event: explicit.append(event.data.content))
        executor = TaskExecutor.empty()
        executor.register(TaskType.AGENT, session.handler())
        executor.execute(Task.agent("hello", title="Hello"))
        unsubscribe()
        executor.execute(Task.agent("again", title="Again"))

    assert seen == ["event:hello", "event:again"]
    assert explicit == ["event:hello"]


def test_shared_copilot_agent_session_on_rejects_non_callable() -> None:
    """Event subscription requires a callable handler."""
    with pytest.raises(TypeError, match="handler must be callable"):
        CopilotAgentSession().on("not callable")  # type: ignore[arg-type]


def test_shared_copilot_agent_session_event_errors_are_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One broken event subscriber does not prevent later subscribers."""
    install_fake_copilot(monkeypatch)
    seen: list[str] = []

    def broken(_event: Any) -> None:
        raise RuntimeError("observer failed")

    with CopilotAgentSession() as session:
        session.on(broken)
        session.on(lambda event: seen.append(event.data.content))
        executor = TaskExecutor.empty()
        executor.register(TaskType.AGENT, session.handler())
        executor.execute(Task.agent("hello", title="Hello"))

    assert seen == ["event:hello"]
    assert len(session.event_errors) == 1
    assert str(session.event_errors[0]) == "observer failed"


def test_shared_copilot_agent_session_rejects_handler_outside_sync_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The synchronous handler fails clearly outside a sync session lifecycle."""
    install_fake_copilot(monkeypatch)
    session = CopilotAgentSession()
    executor = TaskExecutor.empty()
    executor.register(TaskType.AGENT, session.handler())

    with pytest.raises(RuntimeError, match="requires an active sync context"):
        executor.execute(Task.agent("hello", title="Hello"))


def test_shared_copilot_agent_session_rejects_double_enter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An active session cannot be entered again."""
    install_fake_copilot(monkeypatch)
    session = CopilotAgentSession()
    session.__enter__()
    try:
        with pytest.raises(RuntimeError, match="already active"):
            session.__enter__()
    finally:
        session.__exit__(None, None, None)


def test_shared_copilot_agent_session_rejects_double_async_enter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An active async session cannot be entered again."""
    install_fake_copilot(monkeypatch)

    async def run() -> None:
        session = CopilotAgentSession()
        await session.__aenter__()
        try:
            with pytest.raises(RuntimeError, match="already active"):
                await session.__aenter__()
        finally:
            await session.__aexit__(None, None, None)

    asyncio.run(run())


def test_shared_copilot_agent_session_serializes_concurrent_handler_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One shared session processes concurrent handler calls one at a time."""
    recorded = install_fake_copilot(monkeypatch, delay=0.05)
    with CopilotAgentSession() as session:
        executor = TaskExecutor.empty()
        executor.register(TaskType.AGENT, session.handler())
        tasks = [
            Task.agent("one", title="One"),
            Task.agent("two", title="Two"),
        ]

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(executor.execute, tasks))

    assert [result.output for result in results] == ["response", "response"]
    assert recorded["max_active_sends"] == 1
    assert sorted(recorded["prompts"]) == ["one", "two"]


def test_shared_copilot_agent_session_cancels_in_flight_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelling a running task aborts the active Copilot session turn."""
    recorded = install_fake_copilot(monkeypatch, delay=1)
    with CopilotAgentSession() as session:
        executor = TaskExecutor.empty()
        executor.register(TaskType.AGENT, session.handler())
        task = Task.agent("slow", title="Slow")
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(executor.execute, task)
            while recorded["active_sends"] == 0:
                time.sleep(0.01)
            executor.cancel(task)

            with pytest.raises(TaskCancelled):
                future.result(timeout=2)

    assert recorded["aborted"] is True
    assert task.status is TaskStatus.CANCELLED


def test_shared_copilot_agent_session_cancels_without_abort_method(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation still succeeds if the installed SDK lacks session.abort."""
    recorded = install_fake_copilot(monkeypatch, delay=1, no_abort=True)
    with CopilotAgentSession() as session:
        executor = TaskExecutor.empty()
        executor.register(TaskType.AGENT, session.handler())
        task = Task.agent("slow", title="Slow")
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(executor.execute, task)
            while recorded["active_sends"] == 0:
                time.sleep(0.01)
            executor.cancel(task)

            with pytest.raises(TaskCancelled):
                future.result(timeout=2)

    assert "aborted" not in recorded
    assert task.status is TaskStatus.CANCELLED


def test_shared_copilot_agent_session_wraps_cancelled_future(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancelled sync bridge future is surfaced as TaskCancelled."""
    install_fake_copilot(monkeypatch)

    class FakeFuture:
        def result(self, *, timeout: float) -> str:
            raise CancelledError

    def fake_run_coroutine_threadsafe(
        coroutine: Any,
        _loop: asyncio.AbstractEventLoop,
    ) -> FakeFuture:
        coroutine.close()
        return FakeFuture()

    original_run_coroutine_threadsafe = asyncio.run_coroutine_threadsafe
    with CopilotAgentSession() as session:
        monkeypatch.setattr(
            asyncio,
            "run_coroutine_threadsafe",
            fake_run_coroutine_threadsafe,
        )
        executor = TaskExecutor.empty()
        executor.register(TaskType.AGENT, session.handler())
        with pytest.raises(TaskCancelled, match="was cancelled"):
            executor.execute(Task.agent("cancelled", title="Cancelled"))
        monkeypatch.setattr(
            asyncio,
            "run_coroutine_threadsafe",
            original_run_coroutine_threadsafe,
        )


def test_shared_copilot_agent_session_abort_without_loop_returns() -> None:
    """Best-effort abort is a no-op before the sync loop is started."""
    CopilotAgentSession()._abort_active()


def test_shared_copilot_agent_session_enter_failure_cleans_up_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sync enter failures do not leave the background loop thread running."""
    install_fake_copilot(monkeypatch, enter_error=RuntimeError("connect failed"))
    session = CopilotAgentSession()

    with pytest.raises(RuntimeError, match="connect failed"), session:
        pass

    thread = session._thread
    assert thread is None or not thread.is_alive()


def test_shared_copilot_agent_session_session_enter_failure_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Session enter failures close the already-entered client."""
    recorded = install_fake_copilot(
        monkeypatch,
        session_enter_error=RuntimeError("session failed"),
    )

    with pytest.raises(RuntimeError, match="session failed"), CopilotAgentSession():
        pass

    assert recorded["client_exited"] is True


def test_shared_copilot_agent_session_close_raises_session_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Session close errors are propagated after close is attempted."""
    install_fake_copilot(monkeypatch, session_exit_error=RuntimeError("session close"))

    with pytest.raises(RuntimeError, match="session close"), CopilotAgentSession():
        pass


def test_shared_copilot_agent_session_close_raises_client_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Client close errors are propagated when session close succeeds."""
    install_fake_copilot(monkeypatch, client_exit_error=RuntimeError("client close"))

    with pytest.raises(RuntimeError, match="client close"), CopilotAgentSession():
        pass


def test_shared_copilot_agent_session_async_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The async context manager can send directly through the shared session."""
    recorded = install_fake_copilot(monkeypatch, content="async done")

    async def run() -> str:
        async with CopilotAgentSession(timeout=7) as session:
            return await session.send_and_wait("hello")

    output = asyncio.run(run())

    assert output == "async done"
    assert recorded["prompts"] == ["hello"]
    assert recorded["timeouts"] == [7]


def test_shared_copilot_agent_session_async_validation_and_empty_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The async API validates input and normalizes non-assistant responses."""
    install_fake_copilot(monkeypatch, content=None)

    async def run() -> None:
        session = CopilotAgentSession()
        with pytest.raises(RuntimeError, match="not active"):
            await session.send_and_wait("inactive")
        async with session:
            with pytest.raises(TypeError, match="prompt must be a str"):
                await session.send_and_wait(123)  # type: ignore[arg-type]
            with pytest.raises(ValueError, match="timeout must be greater than 0"):
                await session.send_and_wait("bad timeout", timeout=0)
            session._send_lock = None
            assert await session.send_and_wait("empty") == ""

    asyncio.run(run())


def test_shared_copilot_agent_session_unknown_response_data_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unexpected Copilot response data normalizes to an empty string."""
    install_fake_copilot(monkeypatch, data=object())

    async def run() -> str:
        async with CopilotAgentSession() as session:
            return await session.send_and_wait("unknown")

    assert asyncio.run(run()) == ""


def test_shared_copilot_agent_session_run_on_loop_requires_loop() -> None:
    """The sync loop bridge fails clearly if no background loop exists."""
    session = CopilotAgentSession()

    async def noop() -> None:
        return None

    coroutine = noop()
    try:
        with pytest.raises(RuntimeError, match="event loop is not running"):
            session._run_on_loop(coroutine)
    finally:
        coroutine.close()
