# Async execution

Use `submit()` for asynchronous single-task execution.

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
behave the same way as synchronous execution.

## Shutdown

The worker pool is created lazily on the first `submit()` call.

```python
executor.shutdown()
assert executor.is_shutdown
```

`shutdown()` is idempotent, rejects later `submit()` calls, and waits for
already-submitted tasks to finish, including queued tasks that have not started
yet. It does not cancel running or queued tasks.

`close()` is an alias for `shutdown()`, and the context-manager protocol closes
the executor on exit.

## Cancellation

Use `executor.cancel(task)` for cooperative cancellation because
`future.cancel()` can only cancel work that has not started yet.

```python
future = executor.submit(task)
executor.cancel(task)
```

Graph execution owns graph-level scheduling. The async submit API is a
single-task primitive; graph dependencies and finally-task semantics remain on
`TaskGraph`.
