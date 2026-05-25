# ttasks

A small Python task ledger, executor, and DAG workflow library.

`ttasks` models work as `Task` objects, executes them through a configurable
`TaskExecutor`, persists them through an optional `Store`, and runs simple
dependency graphs with `TaskGraph`.

## Requirements

- Python 3.12+
- `uv` for the development workflow used by this repository
- GitHub Copilot authentication for `TaskType.PROMPT` tasks

## Quick start

```python
from ttasks import Task, make_default_executor

executor = make_default_executor()
task = Task.bash("echo hello", title="Say hello")

result = executor.execute(task)

assert task.is_done
assert result.output == "hello\n"
assert task.result is result
```

## Core concepts

### Task

A `Task` is the unit of work tracked by the system.

```python
from ttasks import Task

task = Task.bash(
    "ls -la",
    title="List files",
    description="Show files in the current directory",
)
```

Convenience factories `Task.bash()`, `Task.powershell()`, `Task.prompt()`, and
`Task.agent()` create tasks of the corresponding `TaskType` without making
callers import `TaskType`.

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

### Store

A `Store` is the single seam between live runtime objects (`Task`,
`TaskGraph`) and any durable backend. It exposes two `MutableMapping`-style
collections keyed by the object's own immutable ID:

- `store.tasks` — task storage
- `store.graphs` — graph storage

`InMemoryStore` keeps live references; reads return the same objects you
saved:

```python
from ttasks import InMemoryStore, Task, TaskGraph

store = InMemoryStore()
task = Task.bash("echo hi", title="hello")
store.tasks.save(task)            # alias for store.tasks[task.id] = task
assert store.tasks[task.id] is task

graph = TaskGraph(title="build")
graph[task] = []
store.graphs[graph.id] = graph
assert graph in store.graphs
```

The collections enforce that objects are stored under their own immutable
IDs. `task in store.tasks` and `graph in store.graphs` are id-membership
shortcuts.

### SQLiteStore

`SQLiteStore` is the durable backend with the same surface as
`InMemoryStore`. A single SQLite file holds both tasks and graphs.

```python
from ttasks import Task, TaskGraph, make_default_executor
from ttasks.storage.sqlite import SQLiteStore

store = SQLiteStore("ttasks.db")

build = Task.bash("echo build", title="build")
test  = Task.bash("echo test",  title="test")

graph = TaskGraph(title="build pipeline")
graph[build] = []
graph[test] = [build]

# Atomic: graph metadata + edges + member task snapshots in one transaction.
store.graphs[graph.id] = graph

executor = make_default_executor(store=store)
graph.run(executor)               # tasks auto-persist on each transition

restored = SQLiteStore("ttasks.db").tasks[build.id]
assert restored.status.value == "done"
```

Reads return detached snapshots: mutating a loaded object does not write
through, so call `save()` again after later changes. `TaskResult.raw` is
intentionally not persisted because raw handler objects are not generally
serializable.

Deleting a graph removes graph metadata and edges but leaves member tasks in
the task collection so other graphs can still reach them. Graphs are stored
topology-only; run-scoped views such as `graph.blocked` and `graph.errors`
are not persisted, but each task's terminal status and `TaskResult` are
restored through `store.tasks`.

### Auto-persistence

When `TaskExecutor` is constructed with a `store`, it auto-saves the task to
`store.tasks` on every lifecycle transition (`RUNNING`, `DONE`, `FAILED`,
`CANCELLED`). Saves run *before* the corresponding lifecycle event is emitted,
so subscribers reading the store see a consistent view.

Persistence failures are isolated from execution: they are recorded on
`executor.persistence_errors` and emitted as
`TaskEventType.PERSISTENCE_FAILED` events. A failing save does not propagate
out of `execute()` and does not transition the task to `FAILED`.

```python
from ttasks import make_default_executor
from ttasks.storage.sqlite import SQLiteStore

store = SQLiteStore("ttasks.db")
executor = make_default_executor(store=store)
# Every task executed through this executor is durably persisted automatically.
```

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
from ttasks import Task, make_default_executor

executor = make_default_executor()
task = Task.prompt(
    "Explain a DAG in one concise sentence.",
    title="Explain DAGs",
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
from ttasks import Task, make_default_executor

executor = make_default_executor()
task = Task.agent(
    "Read README.md and summarize this project in one paragraph.",
    title="Inspect README",
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

with executor.events.subscribed(seen.append):
    executor.execute(task)

assert [event.type for event in seen] == [
    TaskEventType.STARTED,
    TaskEventType.SUCCEEDED,
]
```

For long-lived subscribers, use `executor.events.subscribe(callback)`, which
returns an idempotent unsubscribe callable.

Events include:

- `type`: `STARTED`, `SUCCEEDED`, `FAILED`, `CANCELLED`, or `PERSISTENCE_FAILED`
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
Task.bash("sleep 30", title="Long task")
```

`None` means no automatic timeout is applied. The subprocess is allowed to run
until it exits unless another caller cancels it with:

```python
executor.cancel(task)
```

Use a positive timeout for bounded subprocess execution:

```python
Task.bash("sleep 30", title="Bounded task", timeout=5)
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
from ttasks import Task, TaskExecutionError

task = Task.bash("echo boom >&2; exit 7", title="Fail")

try:
    executor.execute(task)
except TaskExecutionError:
    assert task.is_failed
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
from ttasks import TaskGraph, Task, make_default_executor

build = Task.bash("echo build", title="Build")
test = Task.bash("echo test", title="Test")
package = Task.bash("echo package", title="Package")

graph = TaskGraph(title="build pipeline")
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
`context.upstream`. The refs come from the graph itself and are keyed by task ID:

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
recommend = Task.prompt(
    "Summarize preflight output",
    title="Recommend next action",
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
