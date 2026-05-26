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
from ttasks import Task, TaskExecutor

executor = TaskExecutor()
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
store.tasks.save(task)
assert store.tasks[task.id] is task

graph = TaskGraph(title="build")
graph.add(task)
store.graphs.save(graph)
assert graph in store.graphs
```

`save()` is the canonical write idiom — it stores the object under its own
immutable ID. The collections also implement the full `MutableMapping`
protocol (`__iter__`, `__getitem__`, `__contains__`, `__setitem__`,
`.values()`, `.items()`, etc.) so comprehensions, `dict(store.tasks)`, and
membership tests work naturally:

```python
done = [t for t in store.tasks.values() if t.is_done]
assert task in store.tasks   # id-membership shortcut
```

### SQLiteStore

`SQLiteStore` is the durable backend with the same surface as
`InMemoryStore`. A single SQLite file holds both tasks and graphs.

```python
from ttasks import Task, TaskGraph, TaskExecutor, SQLiteStore

store = SQLiteStore("ttasks.db")

build = Task.bash("echo build", title="build")
test  = Task.bash("echo test",  title="test")

graph = TaskGraph(title="build pipeline")
graph.add(build)
graph.add(test, after=[build])

# Atomic: graph metadata + edges + member task snapshots in one transaction.
store.graphs.save(graph)

executor = TaskExecutor(store=store)
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
from ttasks import TaskExecutor, SQLiteStore

store = SQLiteStore("ttasks.db")
executor = TaskExecutor(store=store)
# Every task executed through this executor is durably persisted automatically.
```

### TaskExecutor

`TaskExecutor` dispatches tasks to handlers registered by `TaskType`. The
default constructor pre-registers built-in handlers for every `TaskType`:

- `TaskType.BASH`
- `TaskType.POWERSHELL`
- `TaskType.PROMPT`
- `TaskType.AGENT`

```python
from ttasks import TaskExecutor, TaskType

executor = TaskExecutor()
executor.register(TaskType.BASH, lambda context: "handled")   # override
```

`PROMPT` uses the GitHub Copilot SDK for a no-tools, single-turn text prompt.
`AGENT` uses the SDK for a tool-capable, single-turn instruction with permission
requests approved automatically. Treat `AGENT` payloads as trusted executable
instructions, similar to `BASH` payloads.

Use `TaskExecutor.empty()` to construct an executor with no built-in handlers
when you want a clean slate:

```python
executor = TaskExecutor.empty()
executor.register(TaskType.BASH, my_handler)
```

Handler contract:

- returning a value means success
- raising `TaskCancelled` means cancellation
- raising any other exception means failure
- handlers should not mutate task lifecycle state directly
- `context.upstream` exposes direct upstream task refs keyed by task ID
- `context.emit_progress(percent, message)` emits progress events for observers

For single-task execution, upstream refs can be passed manually:

```python
executor.execute(child_task, upstream={parent_task.id: parent_task})
```

For asynchronous single-task execution, use `submit()` and shut the executor
down when submitted work is done:

```python
from ttasks import Task, TaskExecutor

with TaskExecutor() as executor:
    task = Task.bash("echo async", title="Async example")
    future = executor.submit(task)
    result = future.result()

assert result.output == "async\n"
```

`submit()` runs the same `execute()` path, so lifecycle events,
auto-persistence, progress events, output events, results, and handler errors
behave the same way as synchronous execution. The worker pool is created lazily
on the first `submit()` call. `shutdown()` is idempotent, rejects later
`submit()` calls, and waits for already-submitted tasks to finish, including
queued tasks that have not started yet. It does not cancel running or queued
tasks. `close()` is an alias for `shutdown()`, and `executor.is_shutdown`
reports whether async submission has been shut down. Use `executor.cancel(task)`
for cooperative cancellation because `future.cancel()` can only cancel work that
has not started yet.

### Prompt tasks

Prompt tasks send `Task.payload` to Copilot and store the assistant message text
in `TaskResult.output`:

```python
from ttasks import Task, TaskExecutor

executor = TaskExecutor()
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
from ttasks import Task, TaskExecutor

executor = TaskExecutor()
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

- `type`: `STARTED`, `PROGRESS`, `OUTPUT`, `SUCCEEDED`, `FAILED`,
  `CANCELLED`, `BLOCKED`, or `PERSISTENCE_FAILED`
- `task_id`
- `task`: the live task object
- `previous_status`
- `status`: the task status at event time
- `timestamp`
- `error`, when relevant
- `progress_percent` and `progress_message`, when `type` is `PROGRESS`
- `output_stream` (`"stdout"` or `"stderr"`) and `output_chunk`, when `type`
  is `OUTPUT`

Subscriber exceptions do not fail task execution. They are recorded on
`executor.events.errors` so observers cannot break the work they observe.

Handlers can report progress without changing task lifecycle state:

```python
def handler(context):
    context.emit_progress(25, "warming up")
    context.emit_progress(message="still working")
    return "done"
```

Progress percentages are optional finite values from 0 through 100. They are
not required to be monotonic.

Built-in subprocess handlers emit `OUTPUT` events as stdout and stderr lines are
read. Complete stdout and stderr are still retained on the terminal
`TaskResult`. Output subscribers run synchronously on reader threads, so keep
callbacks fast.

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
from ttasks import TaskGraph, Task, TaskExecutor

build = Task.bash("echo build", title="Build")
test = Task.bash("echo test", title="Test")
package = Task.bash("echo package", title="Package")

graph = TaskGraph(title="build pipeline")
graph.add(build)
graph.add(test, after=[build])
graph.add(package, after=[test])

graph.run(TaskExecutor())

assert graph.ok
assert graph.succeeded == [build, test, package]
```

Useful graph views:

- `graph.succeeded`
- `graph.failed`
- `graph.cancelled`
- `graph.blocked`
- `graph.finally_tasks`
- `graph.optional_tasks`
- `graph.required_tasks`
- `graph.optional_failed`
- `graph.required_failed`
- `graph.required_blocked`
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

Use `graph.add(..., finally_=True)` for reporting or cleanup tasks that should
run after other tasks are no longer active, even when those tasks failed or
were blocked:

```python
recommend = Task.prompt(
    "Summarize preflight output",
    title="Recommend next action",
)

graph.add(
    recommend,
    after=[lint, test, docs],
    finally_=True,
    required=False,
)
```

A finally task receives the listed `after` tasks through `context.upstream`, just
like normal dependencies. `required=False` makes failures visible in graph views
without making `graph.ok` false, which is useful for optional reporting tasks
such as AI recommendations or artifact collection.

Use `graph.finally_tasks`, `graph.optional_tasks`, and `graph.required_tasks` to
display task metadata without relying on private internals. After a run,
`graph.optional_failed`, `graph.required_failed`, and `graph.required_blocked`
split status-specific outcome views for reporting; `graph.ok` remains the
authoritative success predicate.

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
