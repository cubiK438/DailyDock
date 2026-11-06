"""Centralized error handling and standardized exception types for DailyDock."""

import logging
from typing import Any, Dict, Optional
from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger("dailydock.errors")


class DailyDockError(Exception):
    """Base exception for all DailyDock application and core errors.

    Attributes:
        message: Human-readable error description.
        module: Originating module or subsystem name.
        context: Optional dictionary containing debugging context.
        status_code: HTTP status code to return when surfaced via the API.
    """

    def __init__(
        self,
        message: str,
        module: str = "core",
        context: Optional[Dict[str, Any]] = None,
        status_code: int = 500,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.module = module
        self.context = context or {}
        self.status_code = status_code

    def to_dict(self) -> Dict[str, Any]:
        """Return a safe dictionary representation for responses (without leaking internal context)."""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "module": self.module,
        }


class ConfigurationError(DailyDockError):
    """Raised when application or module configuration is invalid or missing."""

    def __init__(
        self,
        message: str,
        module: str = "config",
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, module=module, context=context, status_code=500)


class ModuleLifecycleError(DailyDockError):
    """Raised when a module fails during registration, startup, or shutdown."""

    def __init__(
        self,
        message: str,
        module: str = "lifecycle",
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, module=module, context=context, status_code=500)


class DependencyResolutionError(ModuleLifecycleError):
    """Raised when module dependency resolution fails."""

    pass


class CircularDependencyError(DependencyResolutionError):
    """Raised when a circular dependency is detected among modules."""

    pass


class MissingDependencyError(DependencyResolutionError):
    """Raised when a required module dependency is not registered."""

    pass


class TaskExecutionError(DailyDockError):
    """Raised when background task coordination or execution fails."""

    def __init__(
        self,
        message: str,
        module: str = "tasks",
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, module=module, context=context, status_code=500)


class TaskQueueFullError(TaskExecutionError):
    """Raised when the task manager pending-task queue exceeds its configured capacity."""

    def __init__(
        self,
        message: str = "Task queue capacity reached; cannot accept new tasks.",
        module: str = "tasks",
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, module=module, context=context)


class EventHandlingError(DailyDockError):
    """Raised when an internal event handler encounters an error."""

    def __init__(
        self,
        message: str,
        module: str = "events",
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, module=module, context=context, status_code=500)


async def daily_dock_exception_handler(
    request: Request, exc: DailyDockError
) -> JSONResponse:
    """FastAPI exception handler for DailyDockError instances.

    Logs error details with context internally while returning a sanitized,
    structured JSON error response without exposing internal traces or secrets.
    """
    logger.warning(
        "Application error in module [%s]: %s (context=%s, path=%s)",
        exc.module,
        exc.message,
        exc.context,
        request.url.path,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.to_dict(),
    )


async def unhandled_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    """FastAPI fallback handler for unhandled internal exceptions.

    Logs the full traceback securely on the server and returns a generic 500
    response to avoid leaking internal implementation details or stack traces.
    """
    logger.critical(
        "Unhandled exception processing request to %s: %s",
        request.url.path,
        exc,
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": "InternalServerError",
            "message": "An unexpected internal error occurred.",
        },
    )
