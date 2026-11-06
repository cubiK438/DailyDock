"""Background task manager with bounded concurrency, queue limits, and graceful shutdown."""

import asyncio
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional
from uuid import uuid4

from app.core.errors import TaskExecutionError, TaskQueueFullError
from app.core.logging import get_logger

logger = get_logger("tasks")


class TaskStatus(str, Enum):
    """Execution states for background tasks."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskRecord:
    """In-memory tracking record for a background task."""

    def __init__(self, task_id: str, name: str, module: str) -> None:
        self.task_id = task_id
        self.name = name
        self.module = module
        self.status = TaskStatus.PENDING
        self.created_at = datetime.now(timezone.utc)
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.error: Optional[str] = None
        self.result: Any = None

    def to_dict(self) -> Dict[str, Any]:
        """Return a dictionary view of the task record."""
        return {
            "task_id": self.task_id,
            "name": self.name,
            "module": self.module,
            "status": self.status.value,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
        }

    def __repr__(self) -> str:
        return f"<TaskRecord {self.task_id} name={self.name} status={self.status.value}>"


class TaskManager:
    """Coordinates execution, concurrency bounds, pending capacity, and lifecycle of background tasks."""

    def __init__(self, max_concurrency: int = 5, max_pending: int = 20) -> None:
        self.max_concurrency = max_concurrency
        self.max_pending = max_pending
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._accepting = True
        self._pending_count = 0
        self._tasks: Dict[str, TaskRecord] = {}
        self._asyncio_tasks: Dict[str, asyncio.Task] = {}

    @property
    def pending_count(self) -> int:
        """Return the current number of tasks waiting in queue for execution."""
        return self._pending_count

    @property
    def running_count(self) -> int:
        """Return the current number of actively executing tasks."""
        return sum(1 for t in self._tasks.values() if t.status == TaskStatus.RUNNING)

    def submit(
        self,
        name: str,
        coroutine_fn: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        module: str = "app",
        **kwargs: Any,
    ) -> str:
        """Submit a coroutine for background execution.

        Args:
            name: Human-readable task name.
            coroutine_fn: An async callable returning a Coroutine.
            *args: Positional arguments for the coroutine function.
            module: Originating module name.
            **kwargs: Keyword arguments for the coroutine function.

        Returns:
            The unique task_id string.

        Raises:
            TaskExecutionError: If the TaskManager is shutting down.
            TaskQueueFullError: If the pending queue capacity has been reached.
        """
        if not self._accepting:
            raise TaskExecutionError(
                "TaskManager is shutting down; cannot accept new tasks.",
                module="tasks",
                context={"task_name": name, "module": module},
            )

        if self._pending_count >= self.max_pending:
            raise TaskQueueFullError(
                f"Task queue capacity exceeded ({self.max_pending}); cannot accept task '{name}'.",
                module="tasks",
                context={
                    "task_name": name,
                    "pending_count": self._pending_count,
                    "max_pending": self.max_pending,
                },
            )

        task_id = uuid4().hex
        record = TaskRecord(task_id=task_id, name=name, module=module)
        self._tasks[task_id] = record
        self._pending_count += 1

        async_task = asyncio.create_task(
            self._execute_task(task_id, coroutine_fn, *args, **kwargs),
            name=f"dailydock-task-{name}-{task_id[:8]}",
        )
        self._asyncio_tasks[task_id] = async_task

        logger.debug(
            "Submitted task [%s:%s] (id=%s, pending=%d)",
            module,
            name,
            task_id,
            self._pending_count,
        )
        return task_id

    async def _execute_task(
        self,
        task_id: str,
        coroutine_fn: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Execute task coroutine gated by the concurrency semaphore."""
        record = self._tasks[task_id]
        try:
            async with self._semaphore:
                self._pending_count = max(0, self._pending_count - 1)
                record.status = TaskStatus.RUNNING
                record.started_at = datetime.now(timezone.utc)
                logger.debug("Task [%s:%s] started execution.", record.module, record.name)

                # Execute coroutine
                result = await coroutine_fn(*args, **kwargs)
                record.result = result
                record.status = TaskStatus.COMPLETED
                logger.debug("Task [%s:%s] completed successfully.", record.module, record.name)

        except asyncio.CancelledError:
            record.status = TaskStatus.CANCELLED
            record.error = "Task was cancelled."
            logger.info("Task [%s:%s] was cancelled.", record.module, record.name)
            raise

        except Exception as exc:
            record.status = TaskStatus.FAILED
            record.error = str(exc)
            logger.error(
                "Task [%s:%s] failed: %s",
                record.module,
                record.name,
                exc,
                exc_info=True,
            )

        finally:
            record.completed_at = datetime.now(timezone.utc)
            self._asyncio_tasks.pop(task_id, None)

    def cancel(self, task_id: str) -> bool:
        """Request cancellation of an active or pending task.

        Returns:
            True if cancellation was requested; False if task is not cancellable.
        """
        record = self._tasks.get(task_id)
        if not record:
            return False

        if record.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
            return False

        async_task = self._asyncio_tasks.get(task_id)
        if async_task and not async_task.done():
            async_task.cancel()
            return True

        # If not yet running in asyncio task
        record.status = TaskStatus.CANCELLED
        record.error = "Cancelled before execution."
        record.completed_at = datetime.now(timezone.utc)
        self._pending_count = max(0, self._pending_count - 1)
        return True

    def get_task(self, task_id: str) -> Optional[TaskRecord]:
        """Retrieve task record by identifier."""
        return self._tasks.get(task_id)

    def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
        module: Optional[str] = None,
    ) -> List[TaskRecord]:
        """List tasks with optional status and module filtering."""
        tasks = list(self._tasks.values())
        if status:
            tasks = [t for t in tasks if t.status == status]
        if module:
            tasks = [t for t in tasks if t.module == module]
        return tasks

    async def shutdown(self, grace_period: float = 5.0) -> None:
        """Gracefully shut down task execution.

        1. Rejects new task submissions.
        2. Allows currently active tasks a grace period to complete.
        3. Cancels any tasks still executing after timeout.
        """
        self._accepting = False
        active_async_tasks = [t for t in self._asyncio_tasks.values() if not t.done()]

        if not active_async_tasks:
            logger.info("TaskManager shutdown: no active tasks to drain.")
            return

        logger.info(
            "TaskManager shutdown: waiting up to %.1fs for %d active tasks to complete...",
            grace_period,
            len(active_async_tasks),
        )

        done, pending = await asyncio.wait(active_async_tasks, timeout=grace_period)

        if pending:
            logger.warning(
                "TaskManager shutdown: %d tasks did not finish within grace period; cancelling...",
                len(pending),
            )
            for t in pending:
                t.cancel()

            # Await cancelled tasks to allow clean unwinding
            await asyncio.gather(*pending, return_exceptions=True)

        # Ensure all unfinished tasks are marked CANCELLED
        for task_id, record in self._tasks.items():
            if record.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                record.status = TaskStatus.CANCELLED
                record.error = "Cancelled during application shutdown."
                record.completed_at = datetime.now(timezone.utc)

        self._asyncio_tasks.clear()
        self._pending_count = 0
        logger.info("TaskManager shutdown complete.")

