"""Focused tests for Task 003 persistence and lifecycle integration."""

import asyncio
import os
from pathlib import Path
import subprocess
import sys

import pytest
from fastapi import FastAPI
from sqlalchemy import select

from app import main
from app.config import PROJECT_ROOT, Settings
from app.core.errors import EventHandlingError
from app.core.events import Event, EventBus, ExecutionStatus
from app.database.base import Base
from app.database.manager import DatabaseManager
from app.database.models.event_execution import EventHandlerExecutionModel


async def _create_test_database(path: Path) -> DatabaseManager:
    manager = DatabaseManager(
        database_url=f"sqlite+aiosqlite:///{path}",
        wal_enabled=False,
    )
    await manager.initialize()
    async with manager.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return manager


async def _get_execution(manager: DatabaseManager, execution_id: str):
    async with manager.session_factory() as session:
        result = await session.execute(
            select(EventHandlerExecutionModel).where(
                EventHandlerExecutionModel.execution_id == execution_id
            )
        )
        return result.scalar_one()


@pytest.mark.anyio
async def test_automatic_retry_waits_for_persisted_eligibility(tmp_path: Path):
    manager = await _create_test_database(tmp_path / "retry-order.db")
    bus = EventBus(session_factory=manager.session_factory)
    attempts = 0
    persisted_statuses = []

    async def handler(event: Event):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("initial failure")
        persisted = await _get_execution(manager, records[0].execution_id)
        persisted_statuses.append(persisted.status)

    bus.subscribe(
        "retry.order",
        handler,
        handler_id="retry-handler",
        module="test",
        retry_condition="test.ready",
    )

    try:
        records = await bus.publish_and_wait(Event("retry.order"))
        triggered = bus.notify_condition("test.ready")
        assert records[0].execution_id in triggered

        await bus.shutdown(grace_period=1)

        assert attempts == 2
        assert persisted_statuses == [ExecutionStatus.RETRY_ELIGIBLE.value]
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_successful_retry_clears_persisted_error(tmp_path: Path):
    manager = await _create_test_database(tmp_path / "retry-error.db")
    bus = EventBus(session_factory=manager.session_factory)
    attempts = 0

    async def handler(event: Event):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient failure")

    bus.subscribe(
        "retry.error",
        handler,
        handler_id="retry-handler",
        module="test",
        retry_condition="test.ready",
    )

    try:
        records = await bus.publish_and_wait(Event("retry.error"))
        failed = await _get_execution(manager, records[0].execution_id)
        assert failed.status == ExecutionStatus.FAILED.value
        assert failed.error == "transient failure"

        await bus.notify_condition_and_wait("test.ready")

        succeeded = await _get_execution(manager, records[0].execution_id)
        assert succeeded.status == ExecutionStatus.SUCCESS.value
        assert succeeded.error is None
        assert succeeded.attempt_count == 2
    finally:
        await bus.shutdown(grace_period=1)
        await manager.close()


@pytest.mark.anyio
async def test_shutdown_persists_interrupted_handler(tmp_path: Path):
    manager = await _create_test_database(tmp_path / "interrupted.db")
    bus = EventBus(max_concurrency=1, session_factory=manager.session_factory)
    started = asyncio.Event()
    hold_handler = asyncio.Event()

    async def handler(event: Event):
        started.set()
        await hold_handler.wait()

    async def queued_handler(event: Event):
        started.set()
        await hold_handler.wait()

    bus.subscribe(
        "shutdown.interrupt",
        handler,
        handler_id="slow-handler",
        module="test",
    )
    bus.subscribe(
        "shutdown.interrupt",
        queued_handler,
        handler_id="queued-handler",
        module="test",
    )

    try:
        event_id = await bus.publish(Event("shutdown.interrupt"))
        await asyncio.wait_for(started.wait(), timeout=1)
        await bus.shutdown(grace_period=0)

        records = bus.list_execution_records(event_id=event_id)
        assert {record.status for record in records} == {
            ExecutionStatus.INTERRUPTED
        }
        for record in records:
            persisted = await _get_execution(manager, record.execution_id)
            assert persisted.status == ExecutionStatus.INTERRUPTED.value
            assert persisted.error == "Handler execution was interrupted during shutdown."
        attempts_by_handler = {
            record.handler_id: (await _get_execution(manager, record.execution_id)).attempt_count
            for record in records
        }
        assert attempts_by_handler == {"slow-handler": 1, "queued-handler": 0}
    finally:
        await manager.close()


def test_relative_data_directory_is_resolved_from_project_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DAILYDOCK_DATA_DIR", "relative-storage")
    monkeypatch.delenv("DAILYDOCK_DATABASE_URL", raising=False)

    settings = Settings()

    expected_data_dir = (PROJECT_ROOT / "relative-storage").resolve()
    assert settings.data_dir == expected_data_dir
    assert settings.get_resolved_database_url() == (
        f"sqlite+aiosqlite:///{expected_data_dir / 'dailydock.db'}"
    )


def test_explicit_relative_data_directory_and_database_url_are_resolved():
    settings = Settings(
        data_dir=Path("custom-storage"),
        database_url="sqlite+aiosqlite:///nested/custom.db",
    )

    expected_data_dir = (PROJECT_ROOT / "custom-storage").resolve()
    assert settings.data_dir == expected_data_dir
    assert settings.get_resolved_database_url() == (
        f"sqlite+aiosqlite:///{expected_data_dir / 'custom.db'}"
    )


@pytest.mark.anyio
async def test_database_manager_creates_configured_path_parent(tmp_path: Path):
    database_path = tmp_path / "nested" / "data" / "dailydock.db"
    manager = DatabaseManager(
        database_url=f"sqlite+aiosqlite:///{database_path}",
        wal_enabled=False,
    )
    try:
        await manager.initialize()
        assert database_path.parent.is_dir()
        assert database_path.is_file()
    finally:
        await manager.close()


@pytest.mark.parametrize("working_directory", ["repository", "outside"])
def test_alembic_migrations_work_from_different_working_directories(
    tmp_path: Path, working_directory: str
):
    backend_dir = PROJECT_ROOT / "backend"
    config_path = backend_dir / "alembic.ini"
    db_path = tmp_path / f"{working_directory}.db"
    cwd = PROJECT_ROOT if working_directory == "repository" else tmp_path
    env = os.environ.copy()
    env["DAILYDOCK_DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path}"
    env["DAILYDOCK_DATA_DIR"] = str(tmp_path / "data")
    env["PYTHONDONTWRITEBYTECODE"] = "1"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(config_path),
            "upgrade",
            "head",
        ],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert db_path.is_file()


@pytest.mark.anyio
async def test_lifespan_cleans_up_resources_after_startup_failure(
    monkeypatch: pytest.MonkeyPatch,
):
    calls = []

    class FakeDatabaseManager:
        session_factory = object()

        async def initialize(self):
            calls.append("database.initialize")

        async def close(self):
            calls.append("database.close")

    class FakeTaskManager:
        def __init__(self, **kwargs):
            calls.append("tasks.initialize")

        async def shutdown(self, grace_period):
            calls.append("tasks.shutdown")

    class FakeEventBus:
        def __init__(self, **kwargs):
            calls.append("events.initialize")

        async def shutdown(self, grace_period):
            calls.append("events.shutdown")

    class FakeModuleRegistry:
        def __init__(self):
            calls.append("modules.initialize")

        def register(self, module):
            calls.append(f"modules.register:{module.name}")

        async def startup(self):
            calls.append("modules.startup")
            raise RuntimeError("module startup failed")

        async def shutdown(self):
            calls.append("modules.shutdown")

    async def fake_recovery(session_factory):
        calls.append("recovery")

    monkeypatch.setattr(main.settings, "validate", lambda: None)
    monkeypatch.setattr(main, "setup_logging", lambda log_level: None)
    monkeypatch.setattr(main, "DatabaseManager", FakeDatabaseManager)
    monkeypatch.setattr(main, "TaskManager", FakeTaskManager)
    monkeypatch.setattr(main, "EventBus", FakeEventBus)
    monkeypatch.setattr(main, "ModuleRegistry", FakeModuleRegistry)
    monkeypatch.setattr(main, "run_startup_recovery", fake_recovery)

    with pytest.raises(RuntimeError, match="module startup failed"):
        async with main.lifespan(FastAPI()):
            pytest.fail("Lifespan yielded despite startup failure")

    assert calls == [
        "database.initialize",
        "recovery",
        "tasks.initialize",
        "events.initialize",
        "modules.initialize",
        "modules.register:monitoring",
        "modules.startup",
        "modules.shutdown",
        "events.shutdown",
        "tasks.shutdown",
        "database.close",
    ]


@pytest.mark.anyio
async def test_retry_eligibility_persistence_failure_prevents_retry(
    monkeypatch: pytest.MonkeyPatch,
):
    bus = EventBus()
    attempts = 0

    async def handler(event: Event):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("handler failure")

    bus.subscribe(
        "retry.persistence",
        handler,
        handler_id="retry-handler",
        module="test",
        retry_condition="test.ready",
    )
    records = await bus.publish_and_wait(Event("retry.persistence"))

    async def fail_persist(*args, **kwargs):
        raise EventHandlingError("database unavailable", module="events")

    monkeypatch.setattr(bus, "_update_db_execution", fail_persist)

    try:
        with pytest.raises(EventHandlingError, match="database unavailable"):
            await bus.notify_condition_and_wait("test.ready")
        assert records[0].status == ExecutionStatus.FAILED
        assert attempts == 1
    finally:
        await bus.shutdown(grace_period=0)
