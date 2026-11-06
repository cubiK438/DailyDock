"""Unit tests for standardized error handling and FastAPI exception translation."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.errors import (
    DailyDockError,
    TaskExecutionError,
    daily_dock_exception_handler,
    unhandled_exception_handler,
)


@pytest.fixture
def error_test_app():
    """Create a minimal FastAPI test application registering DailyDock error handlers."""
    app = FastAPI()
    app.add_exception_handler(DailyDockError, daily_dock_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)

    @app.get("/trigger-dailydock-error")
    def trigger_dailydock_error():
        raise TaskExecutionError(
            "Background task failed to execute.",
            module="backup",
            context={"internal_path": "/var/secret/path"},
        )

    @app.get("/trigger-unhandled-error")
    def trigger_unhandled_error():
        raise ZeroDivisionError("division by zero in internal algorithm")

    return app


def test_exception_properties_and_safe_dict():
    """DailyDockError should retain module and context while to_dict() returns safe fields."""
    err = DailyDockError(
        message="Test failure",
        module="storage",
        context={"drive_id": "sda1", "internal_secret": "hidden"},
        status_code=503,
    )
    assert err.message == "Test failure"
    assert err.module == "storage"
    assert err.context == {"drive_id": "sda1", "internal_secret": "hidden"}
    assert err.status_code == 503

    safe_dict = err.to_dict()
    assert safe_dict == {
        "error": "DailyDockError",
        "message": "Test failure",
        "module": "storage",
    }
    assert "internal_secret" not in safe_dict


def test_fastapi_handles_dailydock_error(error_test_app):
    """DailyDockError should be formatted into structured JSON without leaking context."""
    client = TestClient(error_test_app)
    response = client.get("/trigger-dailydock-error")
    assert response.status_code == 500
    data = response.json()
    assert data["error"] == "TaskExecutionError"
    assert data["message"] == "Background task failed to execute."
    assert data["module"] == "backup"
    assert "internal_path" not in data


def test_fastapi_handles_unhandled_exception_safely(error_test_app):
    """Unhandled exceptions must return generic 500 without leaking stack traces or internal errors."""
    client = TestClient(error_test_app, raise_server_exceptions=False)
    response = client.get("/trigger-unhandled-error")
    assert response.status_code == 500
    data = response.json()
    assert data["error"] == "InternalServerError"
    assert data["message"] == "An unexpected internal error occurred."
    assert "division by zero" not in data["message"]

