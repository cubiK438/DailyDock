"""Repository handling persistent storage for background tasks."""

from datetime import datetime, timezone
from typing import Any, List, Optional
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models.task import BackgroundTaskModel


class TaskRepository:
    """Encapsulates database operations for background tasks."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_task(
        self,
        task_id: str,
        name: str,
        module: str,
        status: str = "PENDING",
        created_at: Optional[datetime] = None,
    ) -> BackgroundTaskModel:
        """Create and persist a new background task record."""
        model = BackgroundTaskModel(
            task_id=task_id,
            name=name,
            module=module,
            status=status,
            created_at=created_at or datetime.now(timezone.utc),
        )
        self.session.add(model)
        await self.session.flush()
        return model

    async def get_by_id(self, task_id: str) -> Optional[BackgroundTaskModel]:
        """Retrieve a task record by its task_id."""
        stmt = select(BackgroundTaskModel).where(BackgroundTaskModel.task_id == task_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def update_status(
        self,
        task_id: str,
        status: str,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        error: Optional[str] = None,
        result: Optional[Any] = None,
    ) -> Optional[BackgroundTaskModel]:
        """Update the execution status and timestamps of an existing task."""
        task = await self.get_by_id(task_id)
        if not task:
            return None

        task.status = status
        if started_at is not None:
            task.started_at = started_at
        if completed_at is not None:
            task.completed_at = completed_at
        if error is not None:
            task.error = error
        if result is not None:
            task.result = result

        await self.session.flush()
        return task

    async def list_tasks(
        self,
        status: Optional[str] = None,
        module: Optional[str] = None,
        limit: int = 100,
    ) -> List[BackgroundTaskModel]:
        """List tasks with optional status and module filtering."""
        stmt = select(BackgroundTaskModel).order_by(BackgroundTaskModel.created_at.desc())
        if status:
            stmt = stmt.where(BackgroundTaskModel.status == status)
        if module:
            stmt = stmt.where(BackgroundTaskModel.module == module)
        stmt = stmt.limit(limit)

        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def mark_interrupted(self, recovery_reason: str) -> int:
        """Mark unfinished tasks (PENDING or RUNNING) as INTERRUPTED upon application recovery."""
        now = datetime.now(timezone.utc)
        stmt = (
            update(BackgroundTaskModel)
            .where(BackgroundTaskModel.status.in_(["PENDING", "RUNNING"]))
            .values(
                status="INTERRUPTED",
                completed_at=now,
                error=recovery_reason,
            )
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount

