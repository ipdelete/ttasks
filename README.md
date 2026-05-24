# ttasks

A small Python task ledger, executor, and DAG workflow library.

`ttasks` models work as `Task` objects, stores them in an in-memory
`TaskLedger`, executes them through a configurable `TaskExecutor`, and can run
simple dependency graphs with `TaskGraph`.

## Requirements

- Python 3.12+
- `uv` for the development workflow used by this repository
- GitHub Copilot authentication for `TaskType.PROMPT` tasks

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

### GraphLedger

`GraphLedger` is the graph-level companion to `TaskLedger`. It stores
`TaskGraph` objects under their own immutable graph IDs.

```python
from ttasks import GraphLedger, TaskGraph

graphs = GraphLedger()
graph = TaskGraph(title="build")
graphs[graph.id] = graph

assert graphs[graph.id] is graph
```

`TaskGraph` also records display metadata:

- `graph.id`
- `graph.title`
- `graph.created_at`

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
- `TaskType.PROMPT`
- `TaskType.AGENT`

`PROMPT` uses the GitHub Copilot SDK for a no-tools, single-turn text prompt.
`AGENT` uses the SDK for a tool-capable, single-turn instruction with permission
requests approved automatically. Treat `AGENT` payloads as trusted executable
instructions, similar to `BASH` payloads.

### Prompt tasks

Prompt tasks send `Task.payload` to Copilot and store the assistant message text
in `TaskResult.output`:

```python
from ttasks import Task, TaskType, make_default_executor

executor = make_default_executor()
task = Task(
    title="Explain DAGs",
    payload="Explain a DAG in one concise sentence.",
    type=TaskType.PROMPT,
)

result = executor.execute(task)
print(result.output)
```

Prompt task behavior:

- default model: `gpt-5.4-mini`
- no tools are exposed to the Copilot session
- `Task.timeout` overrides the prompt handler's default wait timeout
- users must already be authenticated with GitHub Copilot

Register a custom Copilot prompt handler to choose a different model or default
timeout:

```python
from ttasks import TaskType, make_copilot_prompt_handler

executor.register(
    TaskType.PROMPT,
    make_copilot_prompt_handler(model="gpt-5", timeout=120),
)
```

### Agent tasks

Agent tasks send `Task.payload` to Copilot with the SDK's default tools enabled
and permission requests approved automatically:

```python
from ttasks import Task, TaskType, make_default_executor

executor = make_default_executor()
task = Task(
    title="Inspect README",
    payload="Read README.md and summarize this project in one paragraph.",
    type=TaskType.AGENT,
)

result = executor.execute(task)
print(result.output)
```

Agent task behavior:

- default model: `gpt-5.5`
- Copilot SDK default tools are available
- permission requests are approved automatically
- no handler-level timeout is applied unless `Task.timeout` is set
- users must already be authenticated with GitHub Copilot

Register a custom Copilot agent handler to choose a different model:

```python
from ttasks import TaskType, make_copilot_agent_handler

executor.register(
    TaskType.AGENT,
    make_copilot_agent_handler(model="gpt-5"),
)
```

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
from ttasks import GraphLedger, TaskGraph, Task, TaskType, make_default_executor

build = Task(title="Build", payload="echo build", type=TaskType.BASH)
test = Task(title="Test", payload="echo test", type=TaskType.BASH)
package = Task(title="Package", payload="echo package", type=TaskType.BASH)

graphs = GraphLedger()
graph = TaskGraph(title="build pipeline")
graph[build] = []
graph[test] = [build]
graph[package] = [test]
graphs[graph.id] = graph

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

### Finally tasks

Use `add_finally()` for reporting or cleanup tasks that should run after other
tasks are no longer active, even when those tasks failed or were blocked:

```python
recommend = Task(
    title="Recommend next action",
    payload="Summarize preflight output",
    type=TaskType.PROMPT,
)

graph.add_finally(
    recommend,
    after=[lint, test, docs],
    required=False,
)
```

A finally task receives the listed `after` tasks through `context.upstream`, just
like normal dependencies. `required=False` makes failures visible in graph views
without making `graph.ok` false, which is useful for optional reporting tasks
such as AI recommendations or artifact collection.

## Documentation

API documentation is generated from docstrings with `pdoc` and published to
GitHub Pages by `.github/workflows/docs.yml`.

Build the docs locally:

```bash
uv run pdoc ttasks --output-directory site
```

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
