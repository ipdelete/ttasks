# ttasks

A small Python task ledger, executor, and DAG workflow library.

`ttasks` models work as `Task` objects, stores them in an in-memory
`TaskLedger`, executes them through a configurable `TaskExecutor`, and can run
simple dependency graphs with `TaskGraph`.

## Requirements

- Python 3.12+
- `uv` for the development workflow used by this repository

## Quick start

```python
from ttasks import Task, TaskType, make_default_executor

executor = make_default_executor()
task = Task(title="Say hello", payload="echo hello", type=TaskType.BASH)

result = executor.execute(task)

assert task.status.value == "done"
assert result.output == "hello\n"
assert task.result is result
```

## Core concepts

### Task

A `Task` is the unit of work tracked by the system.

```python
from ttasks import Task, TaskType

task = Task(
    title="List files",
    description="Show files in the current directory",
    payload="ls -la",
    type=TaskType.BASH,
)
```

Task status is read-only from the outside. Use `transition_to()` for explicit
state-machine transitions or `cancel()` for cancellation. Once a task reaches
`DONE`, normal public field assignment is rejected so completed tasks can be
safely shared by reference with downstream handlers.

Valid lifecycle states are:

- `PENDING`
- `RUNNING`
- `DONE`
- `FAILED`
- `CANCELLED`

### TaskResult

Every terminal execution path attaches a `TaskResult` to `task.result`:

```python
result = executor.execute(task)

print(result.status)
print(result.output)
print(result.error)
print(result.returncode)
print(result.started_at)
print(result.finished_at)
print(result.duration)
```

`started_at` and `finished_at` are wall-clock `datetime` values. `duration` is
measured with a monotonic clock and reported in seconds.

For subprocess tasks, `TaskResult.raw` is the underlying
`subprocess.CompletedProcess`.

### TaskLedger

`TaskLedger` is a small dictionary-like in-memory registry keyed by task ID.

```python
from ttasks import TaskLedger

ledger = TaskLedger()
ledger[task.id] = task

assert ledger[task.id] is task
```

The ledger enforces that tasks are stored under their own immutable IDs.

### TaskExecutor

`TaskExecutor` dispatches tasks to handlers registered by `TaskType`.

```python
from ttasks import TaskExecutor, TaskType

executor = TaskExecutor()
executor.register(TaskType.BASH, lambda context: "handled")
```

Handler contract:

- returning a value means success
- raising `TaskCancelled` means cancellation
- raising any other exception means failure
- handlers should not mutate task lifecycle state directly
- `context.upstream` exposes direct upstream task refs keyed by task ID

For single-task execution, upstream refs can be passed manually:

```python
executor.execute(child_task, upstream={parent_task.id: parent_task})
```

The default executor registers built-in handlers for:

- `TaskType.BASH`
- `TaskType.POWERSHELL`
- `TaskType.PROMPT` placeholder
- `TaskType.AGENT` placeholder

`PROMPT` and `AGENT` currently raise `NotImplementedError` until real backends
are configured.

## Event stream

Every executor has an `EventBus` for task lifecycle events:

```python
from ttasks import TaskEvent, TaskEventType

seen: list[TaskEvent] = []
unsubscribe = executor.events.subscribe(seen.append)

executor.execute(task)
unsubscribe()

assert [event.type for event in seen] == [
    TaskEventType.STARTED,
    TaskEventType.SUCCEEDED,
]
```

Events include:

- `type`: `STARTED`, `SUCCEEDED`, `FAILED`, or `CANCELLED`
- `task_id`
- `task`: the live task object
- `previous_status`
- `status`: the task status at event time
- `timestamp`
- `error`, when relevant

Subscriber exceptions do not fail task execution. They are recorded on
`executor.events.errors` so observers cannot break the work they observe.

## Timeout policy

`Task.timeout` defaults to `None` intentionally.

```python
Task(title="Long task", payload="sleep 30", type=TaskType.BASH)
```

`None` means no automatic timeout is applied. The subprocess is allowed to run
until it exits unless another caller cancels it with:

```python
executor.cancel(task)
```

Use a positive timeout for bounded subprocess execution:

```python
Task(
    title="Bounded task",
    payload="sleep 30",
    type=TaskType.BASH,
    timeout=5,
)
```

If the timeout is exceeded, the executor terminates the subprocess, marks the
task `FAILED`, stores the timeout message in `task.error`, attaches a failed
`TaskResult`, and raises `TaskTimeoutError`.

`TaskTimeoutError` subclasses `TimeoutError`, so callers can catch either the
specific ttasks exception or the standard timeout base class.

## Error handling

### Subprocess failures

A non-zero subprocess exit raises `TaskExecutionError`, marks the task `FAILED`,
and preserves structured process details:

```python
from ttasks import Task, TaskExecutionError, TaskType

task = Task(title="Fail", payload="echo boom >&2; exit 7", type=TaskType.BASH)

try:
    executor.execute(task)
except TaskExecutionError:
    assert task.status.value == "failed"
    assert task.result is not None
    print(task.result.error)
    print(task.result.returncode)
```

For failed subprocesses, `task.result` includes:

- captured stdout
- captured stderr or fallback error text
- return code
- raw `CompletedProcess`

### Cancellation

Cancel a task through the executor to also terminate any active subprocess:

```python
executor.cancel(task)
```

Handlers may cooperatively abort by raising `TaskCancelled`. The executor owns
the transition to `CANCELLED` and records the terminal `TaskResult`.

## DAG workflows

`TaskGraph` runs tasks as a directed acyclic graph. Dependencies must be
registered in the graph before `run()`.

```python
from ttasks import TaskGraph, Task, TaskType, make_default_executor

build = Task(title="Build", payload="echo build", type=TaskType.BASH)
test = Task(title="Test", payload="echo test", type=TaskType.BASH)
package = Task(title="Package", payload="echo package", type=TaskType.BASH)

graph = TaskGraph()
graph[build] = []
graph[test] = [build]
graph[package] = [test]

graph.run(make_default_executor())

assert graph.ok
assert graph.succeeded == [build, test, package]
```

Useful graph views:

- `graph.succeeded`
- `graph.failed`
- `graph.cancelled`
- `graph.blocked`
- `graph.errors`
- `graph.roots()`
- `graph.leaves()`

If a task fails or is cancelled, downstream tasks are blocked and not submitted.
Executor/setup errors raised by submitted futures are available in
`graph.errors`, keyed by task ID.

Already-`DONE` tasks count as satisfied dependencies, so a graph can be rerun or
extended after partial completion.

When a graph submits a task, its handler receives direct dependency task refs in
`context.upstream`. The refs come from the graph ledger and are keyed by task ID:

```python
def handler(context):
    parent = context.upstream[build.id]
    assert parent.result is not None
    return parent.result.output.upper()
```

Only direct dependencies are included. If a task needs an earlier ancestor, add
that ancestor as an explicit graph dependency.

## Development

Run the full test suite:

```bash
uv run pytest
```

Run linting and type checks:

```bash
uv run ruff check .
uv run ty check
```
