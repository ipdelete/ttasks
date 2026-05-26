"""Copilot-backed task handlers and session helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import CancelledError as FutureCancelledError
from concurrent.futures import TimeoutError as FutureTimeoutError
from threading import Event, Lock, Thread
from typing import Any, Self

from ._exceptions import TaskCancelled
from ._executor import DEFAULT_COPILOT_AGENT_MODEL, TaskContext, TaskHandler


class CopilotAgentSession:
    """Long-lived Copilot agent session with a ``TaskExecutor`` handler.

    A shared session preserves Copilot conversation state across multiple
    ``Task.agent(...)`` executions. Use a fresh ``CopilotAgentSession`` for an
    independent conversational lane.
    """

    def __init__(
        self,
        *,
        model: str = DEFAULT_COPILOT_AGENT_MODEL,
        reasoning_effort: str | None = None,
        working_directory: str | None = None,
        timeout: float | None = None,
        on_event: Callable[[Any], None] | None = None,
        **session_options: Any,
    ) -> None:
        """Configure a reusable Copilot agent session.

        ``session_options`` are passed through to
        ``CopilotClient.create_session(...)``. Permission requests default to
        ``PermissionHandler.approve_all`` to match the existing one-shot AGENT
        handler unless ``on_permission_request`` is supplied.
        """
        if not model:
            raise ValueError("model must not be empty")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than 0")

        self.model = model
        self.reasoning_effort = reasoning_effort
        self.working_directory = working_directory
        self.timeout = timeout
        self._session_options = dict(session_options)

        self._event_handlers: list[Callable[[Any], None]] = []
        self.event_errors: list[BaseException] = []
        self._event_lock = Lock()
        if on_event is not None:
            self._event_handlers.append(on_event)

        self._client: Any | None = None
        self._session: Any | None = None
        self._send_lock: asyncio.Lock | None = None
        self._active = False
        self._sync_active = False

        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Thread | None = None
        self._thread_ready = Event()
        self._handler_lock = Lock()

    async def __aenter__(self) -> Self:
        """Open the Copilot client and session in the current event loop."""
        if self._active:
            raise RuntimeError("CopilotAgentSession is already active")
        await self._open_async()
        self._sync_active = False
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        """Close the Copilot session and client."""
        await self._close_async(exc_type, exc, traceback)

    def __enter__(self) -> Self:
        """Open the Copilot client and session on a background event loop."""
        if self._active:
            raise RuntimeError("CopilotAgentSession is already active")
        self._start_loop_thread()
        try:
            self._run_on_loop(self._open_async())
        except BaseException:
            self._stop_loop_thread()
            raise
        self._sync_active = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        """Close the Copilot session/client and stop the background loop."""
        try:
            if self._loop is not None:
                self._run_on_loop(self._close_async(exc_type, exc, traceback))
        finally:
            self._sync_active = False
            self._stop_loop_thread()

    async def send_and_wait(
        self,
        prompt: str,
        *,
        timeout: float | None = None,
    ) -> str:
        """Send ``prompt`` through the shared session and return assistant text."""
        if not self._active or self._session is None:
            raise RuntimeError("CopilotAgentSession is not active")
        if not isinstance(prompt, str):
            raise TypeError("prompt must be a str")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than 0")

        effective_timeout = self.timeout if timeout is None else timeout
        if self._send_lock is None:
            self._send_lock = asyncio.Lock()
        async with self._send_lock:
            response = await self._session.send_and_wait(
                prompt,
                timeout=effective_timeout,
            )
        return self._response_text(response)

    def on(self, handler: Callable[[Any], None]) -> Callable[[], None]:
        """Subscribe to Copilot session events.

        The returned callable unsubscribes ``handler``. Handlers may be called
        from the session's event-loop thread when using the synchronous context
        manager.
        """
        if not callable(handler):
            raise TypeError("handler must be callable")
        with self._event_lock:
            self._event_handlers.append(handler)

        def unsubscribe() -> None:
            with self._event_lock:
                if handler in self._event_handlers:
                    self._event_handlers.remove(handler)

        return unsubscribe

    def handler(self) -> TaskHandler:
        """Return a synchronous AGENT task handler backed by this session."""

        def run(context: TaskContext) -> str:
            context.raise_if_cancelled()
            if not self._sync_active or self._loop is None:
                raise RuntimeError(
                    "CopilotAgentSession.handler() requires an active sync context",
                )
            with self._handler_lock:
                context.raise_if_cancelled()
                future = asyncio.run_coroutine_threadsafe(
                    self.send_and_wait(context.payload, timeout=context.timeout),
                    self._loop,
                )
                while True:
                    try:
                        result = future.result(timeout=0.05)
                    except FutureTimeoutError:
                        if context.cancelled:
                            future.cancel()
                            self._abort_active()
                            raise TaskCancelled(
                                f"Task {context.id!r} was cancelled",
                            ) from None
                        continue
                    except FutureCancelledError as error:
                        raise TaskCancelled(
                            f"Task {context.id!r} was cancelled",
                        ) from error
                    context.raise_if_cancelled()
                    return result

        return run

    async def _open_async(self) -> None:
        """Create the SDK client/session pair."""
        from copilot import CopilotClient
        from copilot.session import PermissionHandler

        session_options = dict(self._session_options)
        session_options.setdefault(
            "on_permission_request",
            PermissionHandler.approve_all,
        )
        session_options["model"] = self.model
        if self.reasoning_effort is not None:
            session_options["reasoning_effort"] = self.reasoning_effort
        if self.working_directory is not None:
            session_options["working_directory"] = self.working_directory
        session_options["on_event"] = self._dispatch_event

        client = CopilotClient()
        entered_client = await client.__aenter__()
        try:
            session = await entered_client.create_session(**session_options)
            entered_session = await session.__aenter__()
        except BaseException:
            await entered_client.__aexit__(None, None, None)
            raise

        self._client = entered_client
        self._session = entered_session
        self._send_lock = asyncio.Lock()
        self._active = True

    async def _close_async(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        traceback: object | None = None,
    ) -> None:
        """Close active SDK resources in reverse creation order."""
        session = self._session
        client = self._client
        self._session = None
        self._client = None
        self._send_lock = None
        self._active = False

        close_error: BaseException | None = None
        if session is not None:
            try:
                await session.__aexit__(exc_type, exc, traceback)
            except BaseException as error:
                close_error = error
        if client is not None:
            try:
                await client.__aexit__(exc_type, exc, traceback)
            except BaseException as error:
                if close_error is None:
                    close_error = error
        if close_error is not None:
            raise close_error

    async def _abort_active_async(self) -> None:
        """Abort the active Copilot turn if the SDK session supports it."""
        session = self._session
        abort = getattr(session, "abort", None)
        if callable(abort):
            await abort()

    def _abort_active(self) -> None:
        """Best-effort abort for a cancelled synchronous handler call."""
        if self._loop is None:
            return
        future = asyncio.run_coroutine_threadsafe(
            self._abort_active_async(),
            self._loop,
        )
        future.result(timeout=5)

    @staticmethod
    def _response_text(response: Any) -> str:
        """Normalize a Copilot SDK response event to assistant text."""
        from copilot.generated.session_events import AssistantMessageData

        if response is None or not isinstance(response.data, AssistantMessageData):
            return ""
        return response.data.content or ""

    def _dispatch_event(self, event: Any) -> None:
        """Fan out one SDK event to registered session subscribers."""
        with self._event_lock:
            handlers = list(self._event_handlers)
        for handler in handlers:
            try:
                handler(event)
            except BaseException as error:
                self.event_errors.append(error)

    def _start_loop_thread(self) -> None:
        """Start the background event loop used by the sync API."""
        self._thread_ready.clear()

        def run_loop() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            self._thread_ready.set()
            try:
                loop.run_forever()
            finally:
                loop.close()
                self._loop = None

        self._thread = Thread(
            target=run_loop,
            name="ttasks-copilot-agent-session",
            daemon=True,
        )
        self._thread.start()
        self._thread_ready.wait()

    def _stop_loop_thread(self) -> None:
        """Stop and join the background event-loop thread."""
        loop = self._loop
        thread = self._thread
        if loop is not None:
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join(timeout=5)
        self._thread = None
        self._thread_ready.clear()

    def _run_on_loop(self, coroutine: Any) -> Any:
        """Run ``coroutine`` on the sync background loop and return its result."""
        if self._loop is None:
            raise RuntimeError("CopilotAgentSession event loop is not running")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result()


__all__ = ["CopilotAgentSession"]
