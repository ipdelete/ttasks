# Progress and output

Every executor has an `EventBus` for task lifecycle events.

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

## Event payloads

Events include:

- `type`: `STARTED`, `PROGRESS`, `OUTPUT`, `SUCCEEDED`, `FAILED`, `CANCELLED`,
  `BLOCKED`, or `PERSISTENCE_FAILED`
- `task_id`
- `task`
- `previous_status`
- `status`
- `timestamp`
- `error`, when relevant
- `progress_percent` and `progress_message`, when `type` is `PROGRESS`
- `output_stream` and `output_chunk`, when `type` is `OUTPUT`

Subscriber exceptions do not fail task execution. They are recorded on
`executor.events.errors` so observers cannot break the work they observe.

## Progress events

Handlers can report progress without changing task lifecycle state:

```python
def handler(context):
    context.emit_progress(25, "warming up")
    context.emit_progress(message="still working")
    return "done"
```

Progress percentages are optional finite values from 0 through 100. They are not
required to be monotonic.

## Streaming subprocess output

Built-in subprocess handlers emit `OUTPUT` events as stdout and stderr lines are
read. Complete stdout and stderr are still retained on the terminal
`TaskResult`.

Output subscribers run synchronously on reader threads, so keep callbacks fast.
