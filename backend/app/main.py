"""Main application entry point for DailyDock backend."""

from contextlib import AsyncExitStack, asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.api.health import router as health_router
from app.api.monitoring import router as monitoring_router
from app.config import settings
from app.core.errors import (
    DailyDockError,
    daily_dock_exception_handler,
    unhandled_exception_handler,
)
from app.core.events import EventBus
from app.core.logging import get_logger, setup_logging
from app.core.registry import ModuleRegistry
from app.core.tasks import TaskManager
from app.database.manager import DatabaseManager
from app.database.recovery import run_startup_recovery
from app.monitoring.module import MonitoringModule

logger = get_logger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup, core component initialization, persistence, and graceful shutdown."""
    # 1. Validate configuration
    settings.validate()

    # 2. Configure logging
    setup_logging(settings.log_level)
    logger.info("Starting DailyDock backend in %s mode...", settings.env)

    async with AsyncExitStack() as stack:
        # Register cleanup immediately after constructing each resource so partial
        # startup failures unwind everything that was successfully initialized.
        db_manager = DatabaseManager()
        stack.push_async_callback(db_manager.close)
        await db_manager.initialize()

        # Execute startup recovery check for interrupted operations.
        await run_startup_recovery(db_manager.session_factory)

        task_manager = TaskManager(
            max_concurrency=settings.task_max_concurrency,
            max_pending=settings.task_max_pending,
            session_factory=db_manager.session_factory,
        )
        stack.push_async_callback(
            task_manager.shutdown,
            grace_period=settings.shutdown_grace_period_seconds,
        )
        event_bus = EventBus(
            max_concurrency=settings.event_max_concurrency,
            session_factory=db_manager.session_factory,
        )
        stack.push_async_callback(
            event_bus.shutdown,
            grace_period=settings.shutdown_grace_period_seconds,
        )
        module_registry = ModuleRegistry()
        monitoring_module = MonitoringModule(settings, db_manager.session_factory)

        async def check_database_health():
            async with db_manager.session_factory() as session:
                await session.execute(text("SELECT 1"))
            return {"healthy": True, "detail": "Database query succeeded"}

        monitoring_module.register_service_check(
            "database", check_database_health, name="Database"
        )
        module_registry.register(monitoring_module)
        stack.push_async_callback(module_registry.shutdown)

        # Attach to app state for access across endpoints and handlers.
        app.state.settings = settings
        app.state.db_manager = db_manager
        app.state.session_factory = db_manager.session_factory
        app.state.task_manager = task_manager
        app.state.event_bus = event_bus
        app.state.module_registry = module_registry
        app.state.monitoring_module = monitoring_module

        # Initialize application modules in dependency order.
        await module_registry.startup()

        logger.info("DailyDock core initialization complete.")
        try:
            yield
        finally:
            logger.info("DailyDock backend shutting down...")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application instance."""
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan,
    )

    # Register standardized exception handlers
    app.add_exception_handler(DailyDockError, daily_dock_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)

    # Configure CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register API routes under /api
    app.include_router(health_router, prefix="/api")
    app.include_router(monitoring_router, prefix="/api")

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.env == "development",
    )
