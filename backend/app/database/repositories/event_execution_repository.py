"""Repository handling persistent storage for event handler executions."""

from datetime import datetime, timezone
from typing import List, Optional
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models.event import EventModel
from app.database.models.event_execution import EventHandlerExecutionModel


class EventExecutionRepository:
    """Encapsulates database operations for event handler executions."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_execution(
        self,
        execution_id: str,
        event_id: str,
        handler_id: str,
        module: str,
        status: str = "PENDING",
        retry_condition: Optional[str] = None,
        created_at: Optional[datetime] = None,
    ) -> EventHandlerExecutionModel:
        """Create and persist an individual event handler execution record."""
        now = created_at or datetime.now(timezone.utc)
        model = EventHandlerExecutionModel(
            execution_id=execution_id,
            event_id=event_id,
            handler_id=handler_id,
            module=module,
            status=status,
            attempt_count=0,
            retry_condition=retry_condition,
            created_at=now,
            updated_at=now,
        )
        self.session.add(model)
        await self.session.flush()
        return model

    async def create_event_with_executions(
        self,
        event_model: EventModel,
        execution_models: List[EventHandlerExecutionModel],
    ) -> None:
        """Atomically persist an event and its subscribed handler executions in one transaction."""
        self.session.add(event_model)
        for exec_model in execution_models:
            self.session.add(exec_model)
        await self.session.flush()

    async def get_by_id(
        self, execution_id: str
    ) -> Optional[EventHandlerExecutionModel]:
        """Retrieve a handler execution record by execution_id."""
        stmt = select(EventHandlerExecutionModel).where(
            EventHandlerExecutionModel.execution_id == execution_id
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def update_status(
        self,
        execution_id: str,
        status: str,
        error: Optional[str] = None,
        increment_attempt: bool = False,
        retry_condition: Optional[str] = None,
        clear_error: bool = False,
    ) -> Optional[EventHandlerExecutionModel]:
        """Update status, error, and attempt count of an execution record."""
        record = await self.get_by_id(execution_id)
        if not record:
            return None

        record.status = status
        record.updated_at = datetime.now(timezone.utc)
        if error is not None:
            record.error = error
        elif clear_error:
            record.error = None
        if retry_condition is not None:
            record.retry_condition = retry_condition
        if increment_attempt:
            record.attempt_count += 1

        await self.session.flush()
        return record

    async def list_executions(
        self,
        event_id: Optional[str] = None,
        status: Optional[str] = None,
        retry_condition: Optional[str] = None,
    ) -> List[EventHandlerExecutionModel]:
        """List execution records with optional filtering."""
        stmt = select(EventHandlerExecutionModel).order_by(
            EventHandlerExecutionModel.created_at.desc()
        )
        if event_id:
            stmt = stmt.where(EventHandlerExecutionModel.event_id == event_id)
        if status:
            stmt = stmt.where(EventHandlerExecutionModel.status == status)
        if retry_condition:
            stmt = stmt.where(EventHandlerExecutionModel.retry_condition == retry_condition)

        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def mark_interrupted(self, recovery_reason: str) -> int:
        """Mark unfinished handler executions (PENDING) as INTERRUPTED upon application recovery."""
        now = datetime.now(timezone.utc)
        stmt = (
            update(EventHandlerExecutionModel)
            .where(EventHandlerExecutionModel.status == "PENDING")
            .values(
                status="INTERRUPTED",
                updated_at=now,
                error=recovery_reason,
            )
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount
