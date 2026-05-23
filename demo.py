"""Executable demo for the task ledger and executor."""

import threading
import time

from ttasks.executor import TaskCancelled, default_executor
from ttasks.ledger import TaskLedger
from ttasks.task import Task, TaskStatus, TaskType


def main():
    """Run a small end-to-end demo of create, execute, cancel, and delete."""
    ledger = TaskLedger()
    executor = default_executor()

    # Create/read/list a task.
    task = Task(
        title="List files",
        description="Demo successful bash execution",
        type=TaskType.BASH,
        payload="ls -all",
    )
    ledger[task.id] = task

    print(ledger[task.id])
    print(list(ledger))

    # Execute moves PENDING -> RUNNING -> DONE.
    result = executor.execute(task)
    print(result.output)
    print(task)

    # Cancel a task before it starts: PENDING -> CANCELLED.
    skipped = Task(title="Skip task", type=TaskType.BASH, payload="sleep 10")
    ledger[skipped.id] = skipped
    ledger.cancel(skipped.id)
    print(skipped)

    # Cancel a task while it is running: RUNNING -> CANCELLED.
    long_running = Task(title="Long running", type=TaskType.BASH, payload="sleep 30")
    ledger[long_running.id] = long_running

    def run_long_task() -> None:
        """Execute the long task and report expected cancellation."""
        try:
            executor.execute(long_running)
        except TaskCancelled:
            print(f"Cancelled in-flight task: {long_running}")

    thread = threading.Thread(target=run_long_task)
    thread.start()

    # Wait until the subprocess is actually tracked before cancelling it.
    while not (
        long_running.status == TaskStatus.RUNNING
        and executor.is_running(long_running.id)
    ):
        time.sleep(0.01)

    executor.cancel(long_running)
    thread.join()

    # Delete removes the task from the ledger entirely.
    del ledger[task.id]


if __name__ == "__main__":
    main()
