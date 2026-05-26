"""Demonstrate TaskGraph finally-task patterns.

Run from the repository root with:

    uv run python examples/finally_tasks.py
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from ttasks import Task, TaskContext, TaskExecutor, TaskGraph, TaskType


def main() -> None:
    """Run a small graph with cleanup, reporting, and optional recommendation."""
    with TemporaryDirectory() as tmp:
        workspace = Path(tmp)
        artifact = workspace / "artifact.txt"
        report = workspace / "report.txt"

        build = Task.bash("build", title="Build")
        cleanup = Task.bash("cleanup", title="Cleanup")
        summarize = Task.bash("report", title="Write report")
        recommend = Task.bash("recommend", title="Optional recommendation")

        def handler(context: TaskContext) -> str:
            if context.payload == "build":
                artifact.write_text("partial artifact\n")
                raise RuntimeError("compiler failed")
            if context.payload == "cleanup":
                artifact.unlink(missing_ok=True)
                return "cleanup complete"
            if context.payload == "report":
                lines = [
                    f"{task.title}: {task.status.value}"
                    for task in context.upstream.values()
                ]
                report.write_text("\n".join(lines) + "\n")
                return report.read_text()
            if context.payload == "recommend":
                raise RuntimeError("recommendation service unavailable")
            raise ValueError(f"unknown payload: {context.payload}")

        executor = TaskExecutor.empty()
        executor.register(TaskType.BASH, handler)

        graph = TaskGraph(title="finally task demo")
        graph.add(build)
        graph.add(cleanup, after=[build], finally_=True)
        graph.add(summarize, after=[build, cleanup], finally_=True)
        graph.add(
            recommend,
            after=[build, cleanup, summarize],
            finally_=True,
            required=False,
        )

        graph.run(executor)

        print(f"graph ok: {graph.ok}")
        print("required failed:", [task.title for task in graph.required_failed])
        print("optional failed:", [task.title for task in graph.optional_failed])
        print("cleanup output:", cleanup.result.output if cleanup.result else "")
        print("report:")
        print(report.read_text(), end="")


if __name__ == "__main__":
    main()
