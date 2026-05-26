# Quickstart

## Run one task

```python
from ttasks import Task, TaskExecutor

executor = TaskExecutor()
task = Task.bash("echo hello", title="Say hello")

result = executor.execute(task)

assert task.is_done
assert result.output == "hello\n"
assert task.result is result
```

`TaskExecutor` registers built-in handlers for Bash, PowerShell, Copilot prompt,
and Copilot agent tasks. You can override any handler or create an executor with
no defaults by using `TaskExecutor.empty()`.

## Subscribe to events

Every executor has an event bus for lifecycle, progress, and output events. Use
`subscribe(...)` for long-lived observers and call the returned function when you
are done:

```python
from ttasks import Task, TaskEvent, TaskExecutor

def print_event(event: TaskEvent) -> None:
    print(f"{event.type.value}: {event.task.title} -> {event.status.value}")

executor = TaskExecutor()
unsubscribe = executor.events.subscribe(print_event)
try:
    executor.execute(Task.bash("echo hello", title="Say hello"))
finally:
    unsubscribe()
```

For scoped subscriptions in tests or short blocks, use
`with executor.events.subscribed(callback): ...`. See
[Progress and output](patterns/progress-and-output.md) for progress events,
streamed subprocess output, and event payload details.

## Share a Copilot agent session

The default Copilot agent handler creates a fresh Copilot session for each task.
Use `CopilotAgentSession` when multiple `Task.agent(...)` tasks should share one
conversation:

```python
from ttasks import CopilotAgentSession, Task, TaskExecutor, TaskType

with CopilotAgentSession(working_directory="/path/to/repo") as agent:
    executor = TaskExecutor()
    executor.register(TaskType.AGENT, agent.handler())

    executor.execute(Task.agent("Create a first change."))
    executor.execute(Task.agent("Continue from the previous change."))
```

Shared sessions preserve conversation state across agent tasks. The handler
serializes turns through the session, including when used by `TaskGraph`.

## Run a graph

```python
from ttasks import Task, TaskExecutor, TaskGraph

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

If a task fails or is cancelled, downstream tasks are marked blocked and are not
submitted. Use [finally tasks](patterns/finally-tasks.md) for cleanup and
reporting work that should still run after failure.

## Persist tasks and graphs

Use `InMemoryStore` when you want an explicit store for tests or short-lived
programs. Use `SQLiteStore` when you want task and graph state to survive process
restarts.

```python
from pathlib import Path

from ttasks import InMemoryStore, SQLiteStore, Task, TaskExecutor, TaskGraph

# Swap this for InMemoryStore() when durability is not needed.
store = SQLiteStore(Path("ttasks.db"))
executor = TaskExecutor(store=store)

build = Task.bash("echo build", title="Build")
test = Task.bash("echo test", title="Test")

graph = TaskGraph(title="stored pipeline")
graph.add(build)
graph.add(test, after=[build])

graph.run(executor)

assert store.tasks[build.id].status == build.status
assert store.graphs[graph.id].ok is True

memory_store = InMemoryStore()
memory_store.tasks.save(Task.bash("echo scratch", title="Scratch"))
```

When an executor has a store, it saves tasks on lifecycle transitions and saves
graphs when `TaskGraph.run(...)` completes. `SQLiteStore` reads return detached
snapshots, so save changed objects again if you mutate them after loading.

## Run the full demo

The repository includes `main.py`, a runnable example that combines a
`SQLiteStore`, task events, three graphs, graph outcome views, and stored graph
inspection:

```bash
uv run python main.py
```

The demo includes Copilot prompt/agent tasks, so it needs Copilot authentication
for those steps.

## Build docs locally

```bash
uv run mkdocs build --strict --site-dir site
uv run pdoc ttasks --output-directory site/api
```
