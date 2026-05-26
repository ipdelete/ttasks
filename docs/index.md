# ttasks

`ttasks` is a small Python task ledger, executor, and DAG workflow library.

Use it when you want to model units of work as durable Python objects, execute
them through explicit handlers, observe progress and output, and compose them
into dependency graphs.

## Start here

- [Quickstart](quickstart.md): run one task and one graph.
- [Task execution tutorial](tutorials/task-execution.md): learn tasks, handlers,
  results, events, and persistence.
- [Graph workflows tutorial](tutorials/graph-workflows.md): build DAGs with
  dependencies and outcome views.
- [Finally tasks pattern](patterns/finally-tasks.md): run cleanup, reporting, and
  artifact tasks after success or failure.

## API reference

The API reference is generated from docstrings with `pdoc` and published
alongside these guides.

<a href="api/">Open the API reference</a>
