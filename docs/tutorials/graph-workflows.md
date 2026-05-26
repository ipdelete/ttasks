# Graph workflows

`TaskGraph` runs tasks as a directed acyclic graph. Dependencies must be
registered in the graph before `run()`.

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
```

## Reading outcomes

Useful graph views include:

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

`graph.ok` is the authoritative success predicate. Optional finally-task
failures are visible through outcome views without making `graph.ok` false.

## Failure behavior

If a task fails or is cancelled, downstream tasks are blocked and not submitted.
Executor or setup errors raised by submitted futures are available in
`graph.errors`, keyed by task ID.

Already-succeeded tasks count as satisfied dependencies, so a graph can be rerun
or extended after partial completion.

## Upstream context

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

## Finally tasks

Use [finally tasks](../patterns/finally-tasks.md) for cleanup, reporting, and
artifact collection work that should run after dependencies are inactive even if
they failed or were blocked.
