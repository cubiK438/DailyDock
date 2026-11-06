"""Unit tests for background TaskManager execution, queue limits, cancellation, and shutdown."""

import asyncio
import pytest
from app.core.errors import TaskExecutionError, TaskQueueFullError
from app.core.tasks import TaskManager, TaskStatus


@pytest.mark.anyio
async def test_task_submission_and_successful_completion():
    """Tasks should progress from PENDING to RUNNING to COMPLETED with stored result."""
    manager = TaskManager(max_concurrency=2, max_pending=5)

    async def sample_work(x: int, y: int):
        await asyncio.sleep(0.01)
        return x + y

    task_id = manager.submit("add_numbers", sample_work, 3, 4, module="math")
    record = manager.get_task(task_id)
    assert record is not None
    assert record.name == "add_numbers"
    assert record.module == "math"

    # Wait for completion
    while record.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
        await asyncio.sleep(0.01)

    assert record.status == TaskStatus.COMPLETED
    assert record.result == 7
    assert record.started_at is not None
    assert record.completed_at is not None
    assert record.error is None


@pytest.mark.anyio
async def test_task_failure_does_not_crash_application():
    """Failing tasks must transition to FAILED with captured error message."""
    manager = TaskManager(max_concurrency=2, max_pending=5)

    async def broken_work():
        await asyncio.sleep(0.01)
        raise ValueError("Simulated computation error")

    task_id = manager.submit("broken_task", broken_work, module="test")
    record = manager.get_task(task_id)

    while record.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
        await asyncio.sleep(0.01)

    assert record.status == TaskStatus.FAILED
    assert "Simulated computation error" in record.error
    assert record.completed_at is not None


@pytest.mark.anyio
async def test_task_cancellation():
    """Cancellable active tasks should transition to CANCELLED."""
    manager = TaskManager(max_concurrency=2, max_pending=5)

    async def long_running_task():
        await asyncio.sleep(1.0)

    task_id = manager.submit("long_task", long_running_task)
    await asyncio.sleep(0.02)  # Give time to start

    cancelled = manager.cancel(task_id)
    assert cancelled is True

    record = manager.get_task(task_id)
    await asyncio.sleep(0.02)
    assert record.status == TaskStatus.CANCELLED


@pytest.mark.anyio
async def test_concurrency_limit_enforced():
    """Concurrent executions should not exceed max_concurrency."""
    manager = TaskManager(max_concurrency=2, max_pending=10)
    current_concurrent = 0
    max_observed = 0

    async def track_concurrency():
        nonlocal current_concurrent, max_observed
        current_concurrent += 1
        max_observed = max(max_observed, current_concurrent)
        await asyncio.sleep(0.03)
        current_concurrent -= 1

    # Submit 4 tasks
    task_ids = [manager.submit(f"task_{i}", track_concurrency) for i in range(4)]

    # Await all tasks
    for tid in task_ids:
        rec = manager.get_task(tid)
        while rec.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
            await asyncio.sleep(0.01)

    assert max_observed <= 2


@pytest.mark.anyio
async def test_pending_queue_limit_and_rejection():
    """Submissions exceeding max_pending capacity must be rejected with TaskQueueFullError."""
    # max_concurrency=1, max_pending=2
    manager = TaskManager(max_concurrency=1, max_pending=2)

    release_event = asyncio.Event()

    async def blocking_task():
        await release_event.wait()

    # Task 1 starts running (acquires concurrency slot)
    manager.submit("running_task", blocking_task)
    await asyncio.sleep(0.01)

    # Tasks 2 & 3 fill the pending capacity (pending=2)
    manager.submit("pending_1", blocking_task)
    manager.submit("pending_2", blocking_task)
    assert manager.pending_count == 2

    # Task 4 should exceed max_pending=2 and raise TaskQueueFullError
    with pytest.raises(TaskQueueFullError) as exc_info:
        manager.submit("pending_3", blocking_task)

    assert "capacity exceeded" in str(exc_info.value)

    # Release running tasks
    release_event.set()
    await asyncio.sleep(0.05)


@pytest.mark.anyio
async def test_task_manager_shutdown_cancels_active_and_rejects_new():
    """Shutdown should reject new submissions and cancel unfinished tasks after grace period."""
    manager = TaskManager(max_concurrency=2, max_pending=5)

    async def perpetual_task():
        await asyncio.sleep(10.0)

    task_id = manager.submit("perpetual", perpetual_task)
    await asyncio.sleep(0.02)

    # Shut down with small grace period
    await manager.shutdown(grace_period=0.05)

    record = manager.get_task(task_id)
    assert record.status == TaskStatus.CANCELLED

    # Subsequent submissions must be rejected
    with pytest.raises(TaskExecutionError):
        manager.submit("new_task", perpetual_task)

