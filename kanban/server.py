"""Tiny HTTP server that exposes a TaskRunner through a kanban UI.

The server is one consumer of the ttasks SDK. It speaks HTTP and JSON; the SDK
does not. All JSON shaping lives in kanban.adapter; all task logic lives in
ttasks.
"""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from kanban.adapter import task_to_dict
from ttasks import Task, TaskRunner, TaskType

INDEX_PATH = Path(__file__).with_name("index.html")


def _read_json(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    """Read and decode a JSON request body."""
    length = int(handler.headers.get("Content-Length") or 0)
    if length == 0:
        return {}
    raw = handler.rfile.read(length)
    return json.loads(raw.decode("utf-8"))


def make_handler(runner: TaskRunner) -> type[BaseHTTPRequestHandler]:
    """Build a request handler class bound to the given TaskRunner."""

    class KanbanHandler(BaseHTTPRequestHandler):
        # -- helpers ------------------------------------------------------

        def _send_json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_text(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_error_json(self, status: int, message: str) -> None:
            self._send_json(status, {"error": message})

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            # Keep the console quiet; uncomment for debugging.
            return

        # -- routing ------------------------------------------------------

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/" or self.path == "/index.html":
                self._send_text(
                    HTTPStatus.OK,
                    INDEX_PATH.read_bytes(),
                    "text/html; charset=utf-8",
                )
                return

            if self.path == "/api/tasks":
                tasks = [task_to_dict(t) for t in runner.ledger]
                self._send_json(HTTPStatus.OK, {"tasks": tasks})
                return

            self._send_error_json(HTTPStatus.NOT_FOUND, "not found")

        def do_POST(self) -> None:  # noqa: N802
            if self.path == "/api/tasks":
                self._create_task()
                return

            if self.path.startswith("/api/tasks/") and self.path.endswith("/run"):
                task_id = self.path[len("/api/tasks/") : -len("/run")]
                self._run_task(task_id)
                return

            if self.path.startswith("/api/tasks/") and self.path.endswith("/cancel"):
                task_id = self.path[len("/api/tasks/") : -len("/cancel")]
                self._cancel_task(task_id)
                return

            self._send_error_json(HTTPStatus.NOT_FOUND, "not found")

        def do_DELETE(self) -> None:  # noqa: N802
            if self.path.startswith("/api/tasks/"):
                task_id = self.path[len("/api/tasks/") :]
                self._delete_task(task_id)
                return

            self._send_error_json(HTTPStatus.NOT_FOUND, "not found")

        # -- actions ------------------------------------------------------

        def _create_task(self) -> None:
            try:
                data = _read_json(self)
                title = (data.get("title") or "").strip()
                payload = data.get("payload") or ""
                type_str = data.get("type") or "bash"
                timeout = data.get("timeout")
                if not title:
                    self._send_error_json(HTTPStatus.BAD_REQUEST, "title is required")
                    return
                try:
                    task_type = TaskType(type_str)
                except ValueError:
                    self._send_error_json(
                        HTTPStatus.BAD_REQUEST, f"unknown task type {type_str!r}"
                    )
                    return
                task = Task(
                    title=title,
                    payload=payload,
                    type=task_type,
                    description=data.get("description", ""),
                    timeout=float(timeout) if timeout else None,
                )
                runner.add(task)
                self._send_json(HTTPStatus.CREATED, task_to_dict(task))
            except (ValueError, json.JSONDecodeError) as e:
                self._send_error_json(HTTPStatus.BAD_REQUEST, str(e))

        def _run_task(self, task_id: str) -> None:
            try:
                runner.submit(task_id)
                self._send_json(
                    HTTPStatus.ACCEPTED, task_to_dict(runner.ledger[task_id])
                )
            except KeyError:
                self._send_error_json(HTTPStatus.NOT_FOUND, "task not found")
            except ValueError as e:
                self._send_error_json(HTTPStatus.CONFLICT, str(e))

        def _cancel_task(self, task_id: str) -> None:
            try:
                runner.cancel(task_id)
                self._send_json(HTTPStatus.OK, task_to_dict(runner.ledger[task_id]))
            except KeyError:
                self._send_error_json(HTTPStatus.NOT_FOUND, "task not found")
            except ValueError as e:
                self._send_error_json(HTTPStatus.CONFLICT, str(e))

        def _delete_task(self, task_id: str) -> None:
            try:
                runner.remove(task_id)
                self._send_json(HTTPStatus.OK, {"ok": True})
            except KeyError:
                self._send_error_json(HTTPStatus.NOT_FOUND, "task not found")
            except ValueError as e:
                self._send_error_json(HTTPStatus.CONFLICT, str(e))

    return KanbanHandler


def serve(
    runner: TaskRunner | None = None,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> None:
    """Run the kanban server until interrupted."""
    owned_runner = runner is None
    runner = runner or TaskRunner()
    handler = make_handler(runner)
    httpd = ThreadingHTTPServer((host, port), handler)
    print(f"ttasks kanban serving on http://{host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down...")
    finally:
        httpd.server_close()
        if owned_runner:
            runner.shutdown(wait=False)
