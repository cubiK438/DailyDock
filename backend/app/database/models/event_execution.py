"""SQLAlchemy model for event handler execution records."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database.base import Base

if TYPE_CHECKING:
    from app.database.models.event import EventModel


class EventHandlerExecutionModel(Base):
    """Persistent storage model for individual event handler executions."""

    __tablename__ = "event_handler_executions"

    execution_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    event_id: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("events.event_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    handler_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    module: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    retry_condition: Mapped[Optional[str]] = mapped_column(
        String(128), nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    event: Mapped["EventModel"] = relationship("EventModel", back_populates="executions")

    def __repr__(self) -> str:
        return (
            f"<EventHandlerExecutionModel {self.execution_id} "
            f"handler={self.handler_id} status={self.status}>"
        )

