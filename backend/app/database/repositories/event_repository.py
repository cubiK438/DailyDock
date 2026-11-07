"""Repository handling persistent storage for events."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models.event import EventModel


class EventRepository:
    """Encapsulates database operations for published events."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_event(
        self,
        event_id: str,
        event_type: str,
        payload: Dict[str, Any],
        source_module: str,
        timestamp: Optional[datetime] = None,
    ) -> EventModel:
        """Create and persist an event record."""
        model = EventModel(
            event_id=event_id,
            event_type=event_type,
            payload=payload,
            source_module=source_module,
            timestamp=timestamp or datetime.now(timezone.utc),
        )
        self.session.add(model)
        await self.session.flush()
        return model

    async def get_by_id(self, event_id: str) -> Optional[EventModel]:
        """Retrieve an event record by its event_id."""
        stmt = select(EventModel).where(EventModel.event_id == event_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_by_type(
        self,
        event_type: str,
        limit: int = 100,
    ) -> List[EventModel]:
        """Retrieve events matching a specific event type."""
        stmt = (
            select(EventModel)
            .where(EventModel.event_type == event_type)
            .order_by(EventModel.timestamp.desc())
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

