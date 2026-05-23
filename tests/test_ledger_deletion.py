from ledger import TaskLedger
from task import Task, TaskStatus, TaskType


def test_del_removes_task_from_ledger() -> None:
    ledger = TaskLedger()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    ledger[task.id] = task

    del ledger[task.id]

    assert task.id not in ledger
    assert len(ledger) == 0


def test_cancel_marks_task_cancelled_and_keeps_it_in_ledger() -> None:
    ledger = TaskLedger()
    task = Task(title="Example", payload="echo hi", type=TaskType.BASH)
    ledger[task.id] = task

    ledger.cancel(task.id)

    assert task.id in ledger
    assert len(ledger) == 1
    assert ledger[task.id] is task
    assert ledger[task.id].status == TaskStatus.CANCELLED
