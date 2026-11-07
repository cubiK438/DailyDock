"""Startup recovery procedure for interrupted persistent tasks and event executions."""

from typing import Dict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logging import get_logger
from app.database.repositories.event_execution_repository import EventExecutionRepository
from app.database.repositories.task_repository import TaskRepository
from app.database.session import get_session

logger = get_logger("database.recovery")


async def run_startup_recovery(
    session_factory: async_sessionmaker[AsyncSession],
) -> Dict[str, int]:
    """Inspect the database for orphaned or incomplete operations and safely mark them INTERRUPTED.

    Avoids blind automatic replay to prevent unintended duplicate side effects.

    Args:
        session_factory: The configured async_sessionmaker instance.

    Returns:
        Dictionary reporting counts of interrupted tasks and event executions marked.
    """
    logger.info("Running startup recovery check for interrupted operations...")
    async with get_session(session_factory) as session:
        task_repo = TaskRepository(session)
        exec_repo = EventExecutionRepository(session)

        interrupted_tasks = await task_repo.mark_interrupted(
            recovery_reason="Application restarted before task completed."
        )
        interrupted_executions = await exec_repo.mark_interrupted(
            recovery_reason="Application restarted before handler completed."
        )

    if interrupted_tasks > 0 or interrupted_executions > 0:
        logger.warning(
            "Startup recovery: marked %d tasks and %d event executions as INTERRUPTED.",
            interrupted_tasks,
            interrupted_executions,
        )
    else:
        logger.info("Startup recovery complete: no interrupted operations found.")

    return {
        "interrupted_tasks": interrupted_tasks,
        "interrupted_executions": interrupted_executions,
    }

