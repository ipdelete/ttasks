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

## Build docs locally

```bash
uv run mkdocs build --strict --site-dir site
uv run pdoc ttasks --output-directory site/api
```
