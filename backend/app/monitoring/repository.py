"""Persistence and bounded history queries for monitoring samples."""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Type

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database.models.monitoring import (
    ConnectivitySample,
    HttpCheckSample,
    ServiceCheckSample,
    StorageSample,
    SystemMetricSample,
)
from app.database.session import get_session


MODEL_BY_DOMAIN: Dict[str, Type[Any]] = {
    "system": SystemMetricSample,
    "storage": StorageSample,
    "connectivity": ConnectivitySample,
    "http": HttpCheckSample,
    "services": ServiceCheckSample,
}


class MonitoringRepository:
    """Writes monitoring observations and exposes bounded latest/history queries."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory
        self._write_lock = asyncio.Lock()

    async def add_sample(self, domain: str, values: Dict[str, Any]) -> Any:
        model_type = MODEL_BY_DOMAIN[domain]
        async with self._write_lock:
            async with get_session(self.session_factory) as session:
                sample = model_type(**values)
                session.add(sample)
                await session.flush()
                return sample

    async def latest(
        self, domain: str, target_id: Optional[str] = None
    ) -> Optional[Any]:
        model_type = MODEL_BY_DOMAIN[domain]
        async with self.session_factory() as session:
            statement = select(model_type)
            if domain != "system":
                statement = statement.where(model_type.target_id == target_id)
            statement = statement.order_by(
                model_type.observed_at.desc(), model_type.id.desc()
            ).limit(1)
            result = await session.execute(statement)
            return result.scalar_one_or_none()

    async def latest_many(self, domain: str, target_ids: List[str]) -> Dict[str, Any]:
        if domain == "system":
            latest = await self.latest(domain)
            return {"system": latest} if latest is not None else {}
        if not target_ids:
            return {}

        model_type = MODEL_BY_DOMAIN[domain]
        ranked = (
            select(
                model_type.id.label("sample_id"),
                model_type.target_id.label("target_id"),
                func.row_number()
                .over(
                    partition_by=model_type.target_id,
                    order_by=(model_type.observed_at.desc(), model_type.id.desc()),
                )
                .label("row_number"),
            )
            .where(model_type.target_id.in_(target_ids))
            .subquery()
        )
        async with self.session_factory() as session:
            statement = (
                select(model_type)
                .join(ranked, ranked.c.sample_id == model_type.id)
                .where(ranked.c.row_number == 1)
            )
            result = await session.execute(statement)
            return {sample.target_id: sample for sample in result.scalars().all()}

    async def history(
        self,
        domain: str,
        target_id: Optional[str],
        since: Optional[datetime],
        until: Optional[datetime],
        limit: int,
    ) -> Tuple[List[Any], bool]:
        model_type = MODEL_BY_DOMAIN[domain]
        statement = select(model_type)
        if domain != "system":
            statement = statement.where(model_type.target_id == target_id)
        if since is not None:
            statement = statement.where(model_type.observed_at >= since)
        if until is not None:
            statement = statement.where(model_type.observed_at < until)
        statement = statement.order_by(
            model_type.observed_at.desc(), model_type.id.desc()
        ).limit(limit + 1)
        async with self.session_factory() as session:
            result = await session.execute(statement)
            rows = list(result.scalars().all())
        return rows[:limit], len(rows) > limit

    async def cleanup_batch(self, cutoff: datetime, batch_size: int) -> int:
        """Delete at most one bounded batch from each sample table, preserving latest rows."""
        async with self._write_lock:
            deleted = 0
            for domain, model_type in MODEL_BY_DOMAIN.items():
                async with get_session(self.session_factory) as session:
                    latest_ids = await self._latest_ids(session, domain, model_type)
                    statement = (
                        select(model_type.id)
                        .where(model_type.observed_at < cutoff)
                        .order_by(model_type.observed_at.asc(), model_type.id.asc())
                        .limit(batch_size)
                    )
                    if latest_ids:
                        statement = statement.where(model_type.id.not_in(latest_ids))
                    ids_result = await session.execute(statement)
                    ids = list(ids_result.scalars().all())
                    if ids:
                        result = await session.execute(
                            delete(model_type).where(model_type.id.in_(ids))
                        )
                        deleted += result.rowcount or 0
            return deleted

    @staticmethod
    async def _latest_ids(
        session: AsyncSession, domain: str, model_type: Type[Any]
    ) -> List[int]:
        order = (model_type.observed_at.desc(), model_type.id.desc())
        if domain == "system":
            result = await session.execute(
                select(model_type.id).order_by(*order).limit(1)
            )
            return list(result.scalars().all())

        ranked = (
            select(
                model_type.id.label("sample_id"),
                func.row_number()
                .over(
                    partition_by=model_type.target_id,
                    order_by=order,
                )
                .label("row_number"),
            )
            .subquery()
        )
        result = await session.execute(
            select(ranked.c.sample_id).where(ranked.c.row_number == 1)
        )
        return list(result.scalars().all())
