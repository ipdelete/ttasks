"""Local CI preflight runner powered by ttasks.

Run with:

    uv run python scripts/preflight.py
"""

from __future__ import annotations

import argparse

from ttasks import (
    Task,
    TaskContext,
    TaskEvent,
    TaskExecutor,
    TaskGraph,
    TaskStatus,
    TaskType,
    make_copilot_prompt_handler,
)


def bash(title: str, payload: str, *, timeout: float | None = None) -> Task:
    """Create one trusted bash task for the preflight graph."""
    return Task(title=title, payload=payload, type=TaskType.BASH, timeout=timeout)


def print_event(event: TaskEvent) -> None:
    """Print one lifecycle event as it happens."""
    previous = event.previous_status.value if event.previous_status else "none"
    error = f" error={event.error!r}" if event.error else ""
    print(
        f"event: {event.type.value:<9} "
        f"task={event.task.title!r} "
        f"{previous}->{event.status.value}{error}"
    )


def print_summary(graph: TaskGraph) -> None:
    """Print final preflight status and details for failed/blocked tasks."""
    print("\npreflight summary")
    print(f"ok: {graph.ok}")

    for task in graph:
        result = task.result
        duration = f"{result.duration:.2f}s" if result else "-"
        print(f"{task.status.value:<9} {duration:>8}  {task.title}")

        if task.status == TaskStatus.FAILED and result is not None:
            if result.output:
                print("  stdout:")
                print(_indent(result.output.rstrip()))
            if result.error:
                print("  stderr/error:")
                print(_indent(result.error.rstrip()))
        elif task.type == TaskType.PROMPT and result is not None and result.output:
            print("  recommendation:")
            print(_indent(result.output.rstrip()))

    if graph.blocked:
        blocked = ", ".join(task.title for task in graph.blocked)
        print(f"blocked: {blocked}")


def _indent(text: str) -> str:
    """Indent multiline output for readable summaries."""
    return "\n".join(f"    {line}" for line in text.splitlines())


def _clip(text: str | None, *, limit: int = 4_000) -> str:
    """Limit command output embedded in the Copilot recommendation prompt."""
    if not text:
        return ""
    if len(text) <= limit:
        return text
    omitted = len(text) - limit
    return f"{text[:limit]}\n... <truncated {omitted} characters>"


def make_recommendation_handler():
    """Build a PROMPT handler that summarizes upstream preflight results."""
    prompt_handler = make_copilot_prompt_handler(timeout=120)

    def handler(context: TaskContext) -> str:
        sections = []
        for task in context.upstream.values():
            result = task.result
            if result is None:
                sections.append(f"## {task.title}\nstatus: {task.status.value}\n")
                continue

            sections.append(
                "\n".join(
                    [
                        f"## {task.title}",
                        f"status: {task.status.value}",
                        f"duration: {result.duration:.2f}s",
                        f"returncode: {result.returncode}",
                        "stdout:",
                        "```",
                        _clip(result.output),
                        "```",
                        "stderr/error:",
                        "```",
                        _clip(result.error),
                        "```",
                    ]
                )
            )

        prompt = "\n\n".join(
            [
                "You are reviewing local CI preflight results for the ttasks ",
                "Python project. Give a concise recommendation for the next ",
                "developer action. Mention any suspicious warnings, failures, ",
                "coverage gaps, documentation issues, or say that the branch ",
                "looks ready if everything is clean.",
                "",
                *sections,
            ]
        )
        prompt_task = Task(
            title=context.title,
            payload=prompt,
            type=TaskType.PROMPT,
            timeout=context.timeout,
        )
        return prompt_handler(TaskContext(prompt_task))

    return handler


def build_graph(*, recommend: bool = False) -> TaskGraph:
    """Build the repository's local CI preflight DAG."""
    graph = TaskGraph(title="ttasks preflight")

    lock_check = bash("Check lockfile", "uv lock --check", timeout=30)
    lint = bash("Lint", "uv run ruff check .", timeout=60)
    types = bash("Type check", "uv run ty check", timeout=60)
    tests = bash("Tests", "uv run pytest", timeout=180)
    docs = bash(
        "Build docs",
        "uv run pdoc ttasks --output-directory site",
        timeout=60,
    )

    graph[lock_check] = []
    graph[lint] = [lock_check]
    graph[types] = [lock_check]
    graph[tests] = [lock_check]
    graph[docs] = [lock_check]

    if recommend:
        recommendation = Task(
            title="Copilot recommendation",
            payload="Built dynamically from upstream preflight results.",
            type=TaskType.PROMPT,
            timeout=120,
        )
        graph.add_finally(
            recommendation,
            after=[lock_check, lint, types, tests, docs],
            required=False,
        )

    return graph


def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--recommend",
        action="store_true",
        help="ask Copilot to recommend a next action from successful check output",
    )
    return parser.parse_args()


def main() -> int:
    """Run the preflight graph and return a shell-friendly exit code."""
    args = parse_args()
    executor = TaskExecutor()
    if args.recommend:
        executor.register(TaskType.PROMPT, make_recommendation_handler())
    unsubscribe = executor.events.subscribe(print_event)
    graph = build_graph(recommend=args.recommend)

    try:
        graph.run(executor)
    finally:
        unsubscribe()

    print_summary(graph)
    return 0 if graph.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
