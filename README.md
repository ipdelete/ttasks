# ttasks

A small Python task ledger, executor, and DAG workflow library.

`ttasks` models work as `Task` objects, executes them through a configurable
`TaskExecutor`, persists them through an optional `Store`, and runs dependency
graphs with `TaskGraph`.

## Requirements

- Python 3.12+
- `uv` for the development workflow used by this repository
- GitHub Copilot authentication for `TaskType.PROMPT` and `TaskType.AGENT` tasks

## Quick start

```python
from ttasks import Task, TaskExecutor

executor = TaskExecutor()
task = Task.bash("echo hello", title="Say hello")

result = executor.execute(task)

assert task.is_done
assert result.output == "hello\n"
assert task.result is result
```

## What it provides

- `Task` and `TaskResult` domain objects for tracking work and outcomes.
- `TaskExecutor` for running Bash, PowerShell, Copilot prompt, Copilot agent, or
  custom handler tasks.
- Event streams for lifecycle, progress, and subprocess output updates.
- `TaskGraph` for dependency-ordered DAG workflows with finally tasks.
- In-memory and SQLite stores for task and graph persistence.
- Async single-task submission, graceful shutdown, cancellation, timeouts, and
  opt-in single-task retries.

## Documentation

User docs are published with the generated API reference on GitHub Pages:

- [Quickstart](https://ipdelete.github.io/ttasks/quickstart/)
- [Tutorials](https://ipdelete.github.io/ttasks/tutorials/task-execution/)
- [Patterns](https://ipdelete.github.io/ttasks/patterns/finally-tasks/)
- [API reference](https://ipdelete.github.io/ttasks/api/)

Build the docs locally:

```bash
uv run mkdocs build --strict --site-dir site
uv run pdoc ttasks --output-directory site/api
```

## Development

Run the project checks:

```bash
uv run ruff check .
uv run ty check
uv run pytest
```

Run the full suite including live tests:

```bash
uv run pytest -o addopts='' --cov=ttasks --cov-report=term-missing --cov-fail-under=100
```
