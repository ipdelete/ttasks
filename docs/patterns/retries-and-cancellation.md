# Retries and cancellation

## Single-task retries

Single-task retries are opt-in with `RetryPolicy`.

```python
from ttasks import RetryPolicy, Task, TaskExecutor

executor = TaskExecutor()
task = Task.bash("./flaky-command", title="Flaky command")

result = executor.execute(
    task,
    retry_policy=RetryPolicy(max_attempts=3, backoff=0.5),
)
```

`max_attempts` is the total attempt count, including the first run. `backoff` is
the number of seconds to sleep between failed attempts.

Retries apply to single-task `execute()` and `submit()` calls. Graph-level retry
is not provided by this policy.

Each attempt emits its normal lifecycle events, and `task.result` reflects only
the final attempt.

## Cancellation is not retried

Cancellation is terminal for retry policy purposes. Handlers may cooperatively
abort by raising `TaskCancelled`; the executor owns the transition to
`CANCELLED` and records the terminal `TaskResult`.

Cancel a task through the executor to also terminate any active subprocess:

```python
executor.cancel(task)
```

For asynchronous work, prefer `executor.cancel(task)` over `future.cancel()`.
`future.cancel()` can only cancel work that has not started yet.

## Timeouts

`Task.timeout` defaults to `None`, which means no automatic timeout is applied.

```python
Task.bash("sleep 30", title="Long task")
```

Use a positive timeout for bounded subprocess execution:

```python
Task.bash("sleep 30", title="Bounded task", timeout=5)
```

If the timeout is exceeded, the executor terminates the subprocess, marks the
task failed, stores the timeout message in `task.error`, attaches a failed
`TaskResult`, and raises `TaskTimeoutError`.
