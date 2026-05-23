# ttasks

A small Python task ledger and executor experiment.

## Timeout policy

`Task.timeout` defaults to `None` intentionally.

```python
Task(title="Long task", payload="sleep 30", type=TaskType.BASH)
```

`None` means no automatic timeout is applied. The subprocess is allowed to run
until it exits unless another caller cancels it with:

```python
executor.cancel(task)
```

Use a positive timeout for bounded subprocess execution:

```python
Task(
    title="Bounded task",
    payload="sleep 30",
    type=TaskType.BASH,
    timeout=5,
)
```

If the timeout is exceeded, the executor terminates the subprocess, marks the
task `FAILED`, stores the timeout message in `task.error`, and raises
`TimeoutError`.
