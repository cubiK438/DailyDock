"""Tests for infrastructure monitoring collection, state, persistence, and API."""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import func, select

from app.api.monitoring import router
from app.config import Settings
from app.core.errors import ConfigurationError
from app.database.base import Base
from app.database.manager import DatabaseManager
from app.database.models.monitoring import ConnectivitySample, SystemMetricSample
from app.monitoring.collectors import LinuxMountProvider, SystemCollector
from app.monitoring.module import MonitoringModule
from app.monitoring.repository import MonitoringRepository
from app.monitoring.state import aggregate_status, metric_status, transition_state


async def _database(path: Path) -> DatabaseManager:
    manager = DatabaseManager(
        database_url=f"sqlite+aiosqlite:///{path}",
        wal_enabled=False,
    )
    await manager.initialize()
    async with manager.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return manager


@pytest.mark.anyio
async def test_sqlite_pragmas_are_configured_at_initialization_and_per_connection(
    tmp_path: Path,
):
    busy_timeout = 4321
    manager = DatabaseManager(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'pragmas.db'}",
        busy_timeout=busy_timeout,
        wal_enabled=True,
    )
    try:
        await manager.initialize()

        async with manager.engine.connect() as connection:
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode")
            assert journal_mode.scalar_one().lower() == "wal"

        connections = await asyncio.gather(
            *(manager.engine.connect() for _ in range(3))
        )
        try:
            async def read_pragmas(connection):
                foreign_keys = await connection.exec_driver_sql("PRAGMA foreign_keys")
                timeout = await connection.exec_driver_sql("PRAGMA busy_timeout")
                return foreign_keys.scalar_one(), timeout.scalar_one()

            pragmas = await asyncio.gather(
                *(read_pragmas(connection) for connection in connections)
            )
            assert pragmas == [(1, busy_timeout)] * len(connections)
        finally:
            await asyncio.gather(*(connection.close() for connection in connections))
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_concurrent_monitoring_writes_and_retention_do_not_lock_database(
    tmp_path: Path,
):
    manager = await _database(tmp_path / "concurrent-writes.db")
    repository = MonitoringRepository(manager.session_factory)
    sample_count = 40
    observed_at = datetime.now(timezone.utc)

    def system_sample(index: int):
        return {
            "observed_at": observed_at,
            "cpu_percent": float(index),
            "memory_total_bytes": 1000,
            "memory_used_bytes": 500,
            "memory_available_bytes": 500,
            "memory_percent": 50.0,
            "uptime_seconds": float(index),
            "status": "OK",
            "check_result": "SUCCESS",
            "error": None,
            "consecutive_failures": 0,
            "consecutive_successes": index + 1,
            "last_transition_at": observed_at,
        }

    try:
        writes = [
            repository.add_sample("system", system_sample(index))
            for index in range(sample_count)
        ]
        cleanups = [
            repository.cleanup_batch(
                observed_at + timedelta(seconds=1), batch_size=2
            )
            for _ in range(5)
        ]
        await asyncio.gather(*writes, *cleanups)

        async with manager.session_factory() as session:
            count = await session.scalar(
                select(func.count(SystemMetricSample.id))
            )
        assert 1 <= count <= sample_count
    finally:
        await manager.close()


class FakeMountProvider:
    def __init__(self, records, uuid_path=None):
        self.records = records
        self.uuid_path = uuid_path

    def mounts(self):
        return self.records

    def source_for_uuid(self, uuid):
        return self.uuid_path if uuid == "expected-drive" else None


def test_linux_mountinfo_decodes_escaped_mount_points(tmp_path: Path):
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        "36 25 8:1 / /mnt/external\\040drive rw,relatime - ext4 /dev/sda1 rw\n",
        encoding="utf-8",
    )
    provider = LinuxMountProvider(mountinfo_path=mountinfo)

    assert list(provider.mounts()) == [("/mnt/external drive", "/dev/sda1")]


def test_mount_verification_requires_exact_mount_and_uuid_identity(tmp_path: Path):
    existing_directory = tmp_path / "external"
    existing_directory.mkdir()
    provider = FakeMountProvider([("/", "/dev/root")])
    collector = SystemCollector(mount_provider=provider, platform_name="Linux")

    assert collector._find_mount_source(str(existing_directory)) is None

    expected_device = tmp_path / "device"
    mounted_device = tmp_path / "mounted-device"
    expected_device.touch()
    mounted_device.touch()
    provider.records.append((str(existing_directory), str(mounted_device)))
    provider.uuid_path = expected_device
    assert collector._find_mount_source(str(existing_directory)) == str(mounted_device)


@pytest.mark.anyio
async def test_storage_check_rejects_mismatched_expected_uuid(tmp_path: Path):
    mount_path = tmp_path / "usb"
    mount_path.mkdir()
    expected_device = tmp_path / "expected"
    mounted_device = tmp_path / "mounted"
    expected_device.touch()
    mounted_device.touch()
    collector = SystemCollector(
        mount_provider=FakeMountProvider(
            [(str(mount_path), str(mounted_device))], uuid_path=expected_device
        ),
        platform_name="Linux",
    )

    result = await collector.collect_storage(
        {
            "id": "usb",
            "mount_path": str(mount_path),
            "expected_uuid": "expected-drive",
        }
    )

    assert result["check_result"] == "FAILURE"
    assert "expected UUID" in result["error"]


@pytest.mark.anyio
async def test_storage_check_reports_unmounted_directory_as_failure(tmp_path: Path):
    existing_directory = tmp_path / "not-mounted"
    existing_directory.mkdir()
    collector = SystemCollector(
        mount_provider=FakeMountProvider([("/", "/dev/root")]),
        platform_name="Linux",
    )

    result = await collector.collect_storage(
        {"id": "usb", "mount_path": str(existing_directory)}
    )

    assert result["check_result"] == "FAILURE"
    assert result["mount_source"] is None
    assert result["total_bytes"] is None
    assert "not mounted" in result["error"]


@pytest.mark.anyio
async def test_system_collector_marks_first_cpu_sample_uninitialized():
    class FakePsutil:
        def __init__(self):
            self.cpu_values = iter([0.0, 27.5])

        def cpu_percent(self, interval):
            return next(self.cpu_values)

        def virtual_memory(self):
            return SimpleNamespace(total=1000, available=400, percent=60.0)

        def boot_time(self):
            return datetime.now().timestamp() - 42

    collector = SystemCollector(psutil_module=FakePsutil())
    first = await collector.collect_system()
    second = await collector.collect_system()

    assert first["cpu_percent"] is None
    assert second["cpu_percent"] == 27.5
    assert second["memory_total_bytes"] == 1000
    assert second["uptime_seconds"] >= 40
    assert second["observed_at"].tzinfo == timezone.utc


@pytest.mark.anyio
async def test_http_collector_reports_status_and_latency_without_url_secrets():
    async def responder(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    transport = httpx.MockTransport(responder)
    async with httpx.AsyncClient(transport=transport) as client:
        collector = SystemCollector(http_client=client)
        result = await collector.check_http(
            {
                "id": "endpoint",
                "url": "https://example.test/health?api_key=private",
                "accepted_status_min": 200,
                "accepted_status_max": 299,
            }
        )

    assert result["check_result"] == "FAILURE"
    assert result["status_code"] == 503
    assert result["latency_ms"] >= 0
    assert "private" not in str(result)


def test_system_metric_thresholds_and_aggregation_precedence():
    assert metric_status(95, 10, 80, 95, 80, 90) == "CRITICAL"
    assert metric_status(81, 10, 80, 95, 80, 90) == "WARNING"
    assert metric_status(None, None, 80, 95, 80, 90) == "UNKNOWN"
    assert aggregate_status(["OK", "STALE", "WARNING"]) == "STALE"
    assert aggregate_status(["DOWN", "CRITICAL", "UNKNOWN"]) == "CRITICAL"
    assert aggregate_status([]) == "UNKNOWN"


@pytest.mark.parametrize(
    "values",
    [
        {"monitor_cpu_warning_percent": 96, "monitor_cpu_critical_percent": 95},
        {"monitor_ram_warning_percent": -1},
        {"monitor_max_concurrency": 0},
        {"monitor_max_concurrency": 11},
    ],
)
def test_monitoring_configuration_rejects_invalid_thresholds(values):
    with pytest.raises(ConfigurationError):
        Settings(**values).validate()


@pytest.mark.anyio
async def test_target_status_and_counters_restore_from_latest_sample(tmp_path: Path):
    manager = await _database(tmp_path / "restore.db")
    repository = MonitoringRepository(manager.session_factory)
    now = datetime.now(timezone.utc)
    try:
        await repository.add_sample(
            "connectivity",
            {
                "target_id": "router",
                "observed_at": now,
                "status": "DOWN",
                "check_result": "FAILURE",
                "error": "probe failed",
                "consecutive_failures": 3,
                "consecutive_successes": 0,
                "last_transition_at": now - timedelta(minutes=2),
                "latency_ms": None,
            },
        )
        module = MonitoringModule(
            Settings(monitor_connectivity_probes=[{"id": "router", "url": "https://example.test"}]),
            manager.session_factory,
        )
        await module._restore_state()
        restored = module._states[("connectivity", "router")]

        assert restored.status == "DOWN"
        assert restored.consecutive_failures == 3
        assert restored.consecutive_successes == 0
        assert restored.last_transition_at == (
            now - timedelta(minutes=2)
        ).replace(tzinfo=None)
    finally:
        await manager.close()


def test_transition_hysteresis_preserves_transition_timestamp_until_change():
    now = datetime.now(timezone.utc)
    previous = SimpleNamespace(
        status="DOWN",
        consecutive_failures=2,
        consecutive_successes=0,
        last_transition_at=now - timedelta(minutes=1),
    )
    not_recovered = transition_state(previous, "SUCCESS", "OK", 2, 2, now)
    assert not_recovered["status"] == "DOWN"
    assert not_recovered["consecutive_successes"] == 1
    assert not_recovered["last_transition_at"] == previous.last_transition_at

    recovered = transition_state(previous, "SUCCESS", "OK", 2, 1, now)
    assert recovered["status"] == "OK"
    assert recovered["last_transition_at"] == now


@pytest.mark.anyio
async def test_monitoring_history_api_is_bounded_and_time_filtered(tmp_path: Path):
    manager = await _database(tmp_path / "history.db")
    settings = Settings(
        monitor_connectivity_probes=[
            {"id": "router", "url": "https://example.test"}
        ]
    )
    module = MonitoringModule(settings, manager.session_factory)
    now = datetime.now(timezone.utc)
    for offset in range(5):
        await module.repository.add_sample(
            "connectivity",
            {
                "target_id": "router",
                "observed_at": now - timedelta(minutes=offset),
                "status": "OK",
                "check_result": "SUCCESS",
                "error": None,
                "consecutive_failures": 0,
                "consecutive_successes": offset + 1,
                "last_transition_at": now - timedelta(minutes=10),
                "latency_ms": 3.5,
            },
        )

    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.state.monitoring_module = module
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/api/monitoring/connectivity/router/history",
                params={
                    "limit": 2,
                        "since": (now - timedelta(minutes=4))
                        .astimezone(timezone(timedelta(hours=5, minutes=30)))
                        .isoformat(),
                        "until": (now - timedelta(minutes=1))
                        .astimezone(timezone(timedelta(hours=5, minutes=30)))
                        .isoformat(),
                },
            )
            assert response.status_code == 200
            result = response.json()
            assert len(result["items"]) == 2
            assert result["has_more"] is True
            assert result["limit"] == 2

            oversized = await client.get(
                "/api/monitoring/connectivity/router/history",
                params={"limit": 501},
            )
            assert oversized.status_code == 422
            invalid_range = await client.get(
                "/api/monitoring/connectivity/router/history",
                params={
                    "since": now.isoformat(),
                    "until": (now - timedelta(minutes=1)).isoformat(),
                },
            )
            assert invalid_range.status_code == 422
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_latest_api_reports_unknown_and_stale_states(tmp_path: Path):
    manager = await _database(tmp_path / "latest.db")
    settings = Settings(
        monitor_connectivity_probes=[
            {"id": "internet", "url": "https://example.test"}
        ],
        monitor_system_interval=30,
        monitor_stale_multiplier=2,
    )
    module = MonitoringModule(settings, manager.session_factory)
    old = datetime.now(timezone.utc) - timedelta(minutes=10)
    await module.repository.add_sample(
        "connectivity",
        {
            "target_id": "internet",
            "observed_at": old,
            "status": "DOWN",
            "check_result": "FAILURE",
            "error": "probe failed",
            "consecutive_failures": 2,
            "consecutive_successes": 0,
            "last_transition_at": old,
            "status_code": None,
            "latency_ms": None,
        },
    )
    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.state.monitoring_module = module
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/api/monitoring/summary")
            summary = response.json()
            assert response.status_code == 200
            assert summary["status"] == "STALE"
            assert summary["domains"]["system"][0]["status"] == "UNKNOWN"
            internet = summary["domains"]["connectivity"][0]
            assert internet["status"] == "STALE"
            assert internet["last_status"] == "DOWN"
            assert internet["is_stale"] is True
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_retention_uses_bounded_batches_and_preserves_latest_sample(tmp_path: Path):
    manager = await _database(tmp_path / "retention.db")
    settings = Settings(monitor_retention_days=1, monitor_retention_batch_size=1)
    module = MonitoringModule(settings, manager.session_factory)
    old = datetime.now(timezone.utc) - timedelta(days=3)
    try:
        for offset in range(3):
            await module.repository.add_sample(
                "connectivity",
                {
                    "target_id": "internet",
                    "observed_at": old + timedelta(minutes=offset),
                    "status": "OK",
                    "check_result": "SUCCESS",
                    "error": None,
                    "consecutive_failures": 0,
                    "consecutive_successes": offset + 1,
                    "last_transition_at": old,
                    "latency_ms": 10,
                },
            )
        deleted = await module.cleanup_retention()
        assert deleted == 2
        async with manager.session_factory() as session:
            rows = (
                await session.execute(
                    select(ConnectivitySample).where(
                        ConnectivitySample.target_id == "internet"
                    )
                )
            ).scalars().all()
        assert len(rows) == 1
        assert rows[0].consecutive_successes == 3
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_service_checks_are_scheduled_with_bounded_concurrency(tmp_path: Path):
    manager = await _database(tmp_path / "scheduler.db")
    settings = Settings(
        monitor_max_concurrency=1,
        monitor_system_interval=100,
        monitor_service_interval=0.05,
        monitor_retention_days=1,
    )
    module = MonitoringModule(settings, manager.session_factory)
    current = 0
    maximum = 0

    async def slow_service_check():
        nonlocal current, maximum
        current += 1
        maximum = max(maximum, current)
        await asyncio.sleep(0.01)
        current -= 1
        return True

    module.register_service_check("service-a", slow_service_check)
    module.register_service_check("service-b", slow_service_check)
    try:
        await module.startup()
        await asyncio.sleep(0.15)
        await module.shutdown()
        assert maximum == 1
        assert await module.repository.latest("services", "service-a") is not None
        assert await module.repository.latest("services", "service-b") is not None
    finally:
        await module.shutdown()
        await manager.close()


@pytest.mark.anyio
async def test_http_target_timeout_overrides_global_check_timeout(tmp_path: Path):
    manager = await _database(tmp_path / "timeout.db")

    class SlowCollector:
        async def check_http(self, target):
            await asyncio.sleep(0.03)
            return {
                "observed_at": datetime.now(timezone.utc),
                "check_result": "SUCCESS",
                "error": None,
                "status_code": 200,
                "latency_ms": 30,
            }

    target = {
        "id": "slow-endpoint",
        "url": "https://example.test/health",
        "timeout": 0.1,
    }
    module = MonitoringModule(
        Settings(
            monitor_check_timeout=0.01,
            monitor_http_endpoints=[target],
        ),
        manager.session_factory,
        collector=SlowCollector(),
    )
    try:
        await module._run_job("http", target["id"], target)
        sample = await module.repository.latest("http", target["id"])
        assert sample is not None
        assert sample.check_result == "SUCCESS"
    finally:
        await manager.close()
