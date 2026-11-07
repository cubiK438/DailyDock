"""SQLAlchemy models for infrastructure monitoring history."""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database.base import Base


class SystemMetricSample(Base):
    """Historical system CPU, memory, and uptime sample."""

    __tablename__ = "monitor_system_samples"
    __table_args__ = (Index("ix_monitor_system_observed_id", "observed_at", "id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    cpu_percent: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    memory_total_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    memory_used_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    memory_available_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    memory_percent: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    uptime_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    check_result: Mapped[str] = mapped_column(String(16), nullable=False)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    consecutive_successes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_transition_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class _TargetSample:
    """Common target identity and durable status fields for monitoring samples."""

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    target_id: Mapped[str] = mapped_column(String(128), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    check_result: Mapped[str] = mapped_column(String(16), nullable=False)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    consecutive_successes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_transition_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class StorageSample(_TargetSample, Base):
    """Historical filesystem observation for one configured storage target."""

    __tablename__ = "monitor_storage_samples"
    __table_args__ = (
        Index("ix_monitor_storage_target_observed_id", "target_id", "observed_at", "id"),
    )

    mount_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    mount_source: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    total_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    used_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    available_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    used_percent: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class ConnectivitySample(_TargetSample, Base):
    """Historical connectivity probe result."""

    __tablename__ = "monitor_connectivity_samples"
    __table_args__ = (
        Index("ix_monitor_connectivity_target_observed_id", "target_id", "observed_at", "id"),
    )

    status_code: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class HttpCheckSample(_TargetSample, Base):
    """Historical configured HTTP endpoint result."""

    __tablename__ = "monitor_http_samples"
    __table_args__ = (
        Index("ix_monitor_http_target_observed_id", "target_id", "observed_at", "id"),
    )

    status_code: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class ServiceCheckSample(_TargetSample, Base):
    """Historical registered DailyDock service health result."""

    __tablename__ = "monitor_service_samples"
    __table_args__ = (
        Index("ix_monitor_service_target_observed_id", "target_id", "observed_at", "id"),
    )

    latency_ms: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    detail: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
