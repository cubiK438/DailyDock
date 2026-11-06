"""Tests for the /api/health endpoint and basic app behavior."""

import pytest
from fastapi.testclient import TestClient
from app.main import app


@pytest.fixture
def client():
    """Provide a TestClient instance running within the app lifespan."""
    with TestClient(app) as test_client:
        yield test_client


def test_health_endpoint_returns_200(client):
    """GET /api/health should return an HTTP 200 OK status."""
    response = client.get("/api/health")
    assert response.status_code == 200


def test_health_endpoint_structure_and_values(client):
    """GET /api/health should return valid JSON with expected keys and values."""
    response = client.get("/api/health")
    data = response.json()

    assert isinstance(data, dict)
    assert "status" in data
    assert "app" in data
    assert "version" in data
    assert "environment" in data

    assert data["status"] == "healthy"
    assert data["app"] == "DailyDock"


def test_health_endpoint_disallows_post(client):
    """POST /api/health should return HTTP 405 Method Not Allowed."""
    response = client.post("/api/health", json={})
    assert response.status_code == 405


def test_nonexistent_route_returns_404(client):
    """GET to an unregistered route should return HTTP 404 Not Found."""
    response = client.get("/api/nonexistent")
    assert response.status_code == 404

