"""Main application entry point for DailyDock backend."""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.health import router as health_router
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

logger = get_logger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup, core component initialization, and graceful shutdown."""
    # 1. Validate configuration
    settings.validate()

    # 2. Configure logging
    setup_logging(settings.log_level)
    logger.info("Starting DailyDock backend in %s mode...", settings.env)

    # 3. Instantiate core infrastructure
    task_manager = TaskManager(
        max_concurrency=settings.task_max_concurrency,
        max_pending=settings.task_max_pending,
    )
    event_bus = EventBus(max_concurrency=settings.event_max_concurrency)
    module_registry = ModuleRegistry()

    # Attach to app state for access across endpoints and handlers
    app.state.settings = settings
    app.state.task_manager = task_manager
    app.state.event_bus = event_bus
    app.state.module_registry = module_registry

    # 4. Initialize application modules in dependency order
    await module_registry.startup()

    logger.info("DailyDock core initialization complete.")

    try:
        yield
    finally:
        logger.info("DailyDock backend shutting down...")

        # Application shutdown sequence:
        # 1 & 2. Stop accepting work and drain active background tasks and events
        grace_period = settings.shutdown_grace_period_seconds
        logger.info("Draining background tasks and events (grace period: %.1fs)...", grace_period)

        await task_manager.shutdown(grace_period=grace_period)
        await event_bus.shutdown(grace_period=grace_period)

        # 3. Shut down modules in reverse dependency order
        await module_registry.shutdown()

        logger.info("DailyDock shutdown complete.")


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
