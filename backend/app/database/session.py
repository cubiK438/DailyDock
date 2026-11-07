"""Database session management utilities and context managers."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logging import get_logger

logger = get_logger("database.session")


@asynccontextmanager
async def get_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """Provide a scoped AsyncSession that commits on success and rolls back on exception.

    Ensures the session is cleanly closed upon exit.

    Args:
        session_factory: The configured async_sessionmaker instance.

    Yields:
        An active AsyncSession instance.
    """
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

