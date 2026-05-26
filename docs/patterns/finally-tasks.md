# Finally tasks

Finally tasks are graph-scheduling semantics for work that should run after one
or more upstream tasks become inactive. They are useful for cleanup, reporting,
artifact collection, and recommendations.

They are not Python `finally` blocks, and they are not graph-level retry. They
are regular tasks with special readiness rules inside `TaskGraph`.

## Basic form

```python
from ttasks import Task, TaskGraph

lint = Task.bash("ruff check .", title="Lint")
test = Task.bash("pytest", title="Tests")
report = Task.bash("python scripts/report.py", title="Write report")

graph = TaskGraph(title="preflight")
graph.add(lint)
graph.add(test)
graph.add(report, after=[lint, test], finally_=True)
```

The finally task becomes ready once every listed `after` task is no longer
active, even if one failed, was cancelled, or became blocked.

## Cleanup after failed work

Use a required finally task for cleanup that must succeed for the graph to be
healthy.

```python
build = Task.bash("make build", title="Build")
cleanup = Task.bash("rm -rf .tmp-build", title="Clean temporary files")

graph.add(build)
graph.add(cleanup, after=[build], finally_=True)
```

If `build` fails, `cleanup` still runs. If `cleanup` fails, it is a required
task by default, so `graph.ok` is false.

## Reports and artifacts

A finally task receives the listed `after` tasks through `context.upstream`, just
like a normal dependency. Use that to summarize results or collect artifact
paths.

```python
def write_report(context):
    lines = []
    for upstream in context.upstream.values():
        result = upstream.result
        lines.append(f"{upstream.title}: {upstream.status.value}")
        if result and result.error:
            lines.append(result.error)
    return "\n".join(lines)
```

Only direct dependencies are included. If a report needs an earlier ancestor,
add that ancestor as an explicit dependency.

## Optional recommendations

Use `required=False` for best-effort reporting, artifact collection, or Copilot
recommendation tasks. Their failures are visible, but they do not make
`graph.ok` false by themselves.

```python
recommend = Task.prompt(
    "Summarize preflight output and recommend the next action.",
    title="Copilot recommendation",
)

graph.add(
    recommend,
    after=[lint, test, report],
    finally_=True,
    required=False,
)
```

This is useful when the primary work should remain authoritative, but optional
diagnostics should still appear in the run summary.

## Required vs optional

| Setting | Failure effect | Good use |
| --- | --- | --- |
| `required=True` | Failure makes `graph.ok` false | Cleanup that must complete |
| `required=False` | Failure is reported but does not make `graph.ok` false | Reports, artifact collection, AI recommendations |

Required and optional finally tasks are available through introspection views:

- `graph.finally_tasks`
- `graph.optional_tasks`
- `graph.required_tasks`
- `graph.optional_failed`
- `graph.required_failed`
- `graph.required_blocked`

`graph.ok` remains the authoritative success predicate.

## Runnable example

The repository includes a local deterministic example:

```bash
uv run python examples/finally_tasks.py
```

It demonstrates failed primary work, cleanup that still runs, report generation,
and an optional recommendation task whose failure is visible without changing
the required outcome.
