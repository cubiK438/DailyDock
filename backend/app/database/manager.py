"""Asynchronous database manager for DailyDock using SQLAlchemy and SQLite."""

from pathlib import Path
from typing import Optional
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings
from app.core.errors import DailyDockError
from app.core.logging import get_logger

logger = get_logger("database.manager")


class DatabaseManager:
    """Manages the asynchronous SQLAlchemy engine, connection pragmas, and session factory."""

    def __init__(
        self,
        database_url: Optional[str] = None,
        data_dir: Optional[Path] = None,
        echo: Optional[bool] = None,
        busy_timeout: Optional[int] = None,
        wal_enabled: Optional[bool] = None,
    ) -> None:
        self.data_dir = data_dir or settings.data_dir
        self.database_url = database_url or settings.get_resolved_database_url()
        self.echo = echo if echo is not None else settings.database_echo
        self.busy_timeout = busy_timeout if busy_timeout is not None else settings.database_busy_timeout
        self.wal_enabled = wal_enabled if wal_enabled is not None else settings.database_wal_enabled

        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[async_sessionmaker[AsyncSession]] = None

    @property
    def engine(self) -> AsyncEngine:
        """Return the active AsyncEngine instance."""
        if self._engine is None:
            raise DailyDockError(
                "Database engine has not been initialized. Call initialize() first.",
                module="database",
            )
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """Return the active async session factory."""
        if self._session_factory is None:
            raise DailyDockError(
                "Database session factory has not been initialized. Call initialize() first.",
                module="database",
            )
        return self._session_factory

    async def initialize(self) -> None:
        """Initialize the database engine, configure SQLite PRAGMAs, and verify connectivity."""
        # Ensure parent data directory exists when using SQLite file
        if self.database_url.startswith("sqlite+aiosqlite:///"):
            path_part = self.database_url[len("sqlite+aiosqlite:///") :]
            if path_part != ":memory:":
                db_path = Path(path_part)
                db_path.parent.mkdir(parents=True, exist_ok=True)
                logger.debug("Ensured database directory exists: %s", db_path.parent)

        logger.info("Initializing database engine with URL: %s", self._mask_url(self.database_url))

        # Create async engine
        self._engine = create_async_engine(
            self.database_url,
            echo=self.echo,
            future=True,
        )

        # Configure SQLite connection PRAGMAs on the underlying sync engine
        if "sqlite" in self.database_url:
            busy_timeout = self.busy_timeout
            wal_enabled = self.wal_enabled

            @event.listens_for(self._engine.sync_engine, "connect")
            def _configure_sqlite(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys = ON;")
                cursor.execute(f"PRAGMA busy_timeout = {busy_timeout};")
                if wal_enabled:
                    cursor.execute("PRAGMA journal_mode = WAL;")
                cursor.close()

        # Create asynchronous session factory
        self._session_factory = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )

        # Verify connectivity
        await self.verify_connectivity()
        logger.info("Database initialized successfully.")

    async def verify_connectivity(self) -> None:
        """Verify database connectivity by executing a lightweight SELECT 1 check."""
        try:
            async with self.session_factory() as session:
                result = await session.execute(text("SELECT 1"))
                assert result.scalar() == 1
        except Exception as exc:
            logger.error("Failed to connect to database: %s", exc, exc_info=True)
            raise DailyDockError(
                f"Database connectivity verification failed: {exc}",
                module="database",
            ) from exc

    async def close(self) -> None:
        """Cleanly dispose of the database engine and release open connections."""
        if self._engine is not None:
            logger.info("Disposing database engine...")
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
            logger.info("Database engine disposed.")

    @staticmethod
    def _mask_url(url: str) -> str:
        """Mask credentials if present in a database URL."""
        if "@" in url and "://" in url:
            scheme, rest = url.split("://", 1)
            credentials, location = rest.split("@", 1)
            return f"{scheme}://***:***@{location}"
        return url

