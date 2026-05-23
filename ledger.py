from task import Task


class TaskLedger:
    def __init__(self):
        self._tasks: dict[str, Task] = {}

    # create: ledger[task.id] = task
    def __setitem__(self, task_id: str, task: Task) -> None:
        if not isinstance(task, Task):
            raise TypeError(f"Expected Task, got {type(task).__name__}")
        if task_id != task.id:
            raise ValueError("task_id must match task.id")
        self._tasks[task_id] = task

    # read: task = ledger[task_id]
    def __getitem__(self, task_id: str) -> Task:
        return self._tasks[task_id]

    # list: for task in ledger
    def __iter__(self):
        return iter(self._tasks.values())

    def __len__(self) -> int:
        return len(self._tasks)

    # delete: del ledger[task_id]  — removes the task
    def __delitem__(self, task_id: str) -> None:
        del self._tasks[task_id]

    # cancel: ledger.cancel(task_id)  — marks as cancelled, keeps it
    def cancel(self, task_id: str) -> None:
        self._tasks[task_id].cancel()

    def __contains__(self, task_id: str) -> bool:
        return task_id in self._tasks

    def __repr__(self) -> str:
        return f"TaskLedger({len(self._tasks)} tasks)"