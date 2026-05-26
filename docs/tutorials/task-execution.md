# Task execution

## Tasks and results

A `Task` is the unit of work tracked by ttasks.

```python
from ttasks import Task

task = Task.bash(
    "ls -la",
    title="List files",
    description="Show files in the current directory",
)
```

Convenience factories create the corresponding task type without making callers
import `TaskType`:

- `Task.bash()`
- `Task.powershell()`
- `Task.prompt()`
- `Task.agent()`

Every terminal execution path attaches a `TaskResult` to `task.result`.

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

For subprocess tasks, `TaskResult.raw` is the underlying
`subprocess.CompletedProcess`.

## Executor handlers

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
- handlers should not mutate lifecycle state directly
- `context.upstream` exposes direct upstream task refs keyed by task ID
- `context.emit_progress(percent, message)` emits progress events for observers

For single-task execution, upstream refs can be passed manually:

```python
executor.execute(child_task, upstream={parent_task.id: parent_task})
```

## Prompt and agent tasks

Prompt tasks send `Task.payload` to Copilot and store the assistant message text
in `TaskResult.output`.

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

Agent tasks send `Task.payload` to Copilot with the SDK's default tools enabled
and permission requests approved automatically. Treat agent task payloads as
trusted executable instructions, similar to Bash payloads.

## Persistence

A `Store` is the seam between live runtime objects and durable backends. It
exposes two `MutableMapping`-style collections keyed by object ID:

- `store.tasks`
- `store.graphs`

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

`SQLiteStore` provides the same surface using a SQLite file. Reads return
detached snapshots, so call `save()` again after later changes.

When `TaskExecutor` is constructed with a store, it auto-saves the task on every
lifecycle transition before emitting the corresponding event.
