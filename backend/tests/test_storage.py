"""Tests for Phase 1 external storage safety and status reporting."""

import asyncio
import os
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import main
from app.api.storage import router
from app.config import Settings
from app.core.errors import ConfigurationError
from app.storage.module import StorageModule
from app.storage.volume import (
    LinuxMountInfoProvider,
    MountRecord,
    StorageUnavailableError,
    StorageVolumeManager,
)


def _device_number(path: Path) -> tuple[int, int]:
    device = path.stat().st_dev
    return os.major(device), os.minor(device)


class FakeMountProvider:
    def __init__(self, records=(), uuid_device=None):
        self.records = list(records)
        self.uuid_device = uuid_device

    def mount_records(self):
        return list(self.records)

    def uuid_device_number(self, volume_uuid):
        return self.uuid_device if volume_uuid == "test-volume" else None


def _settings(mountpoint: Path, **kwargs) -> Settings:
    return Settings(
        storage_mountpoint=mountpoint,
        storage_volume_uuid="test-volume",
        storage_check_interval=0.05,
        **kwargs,
    )


@pytest.mark.anyio
async def test_verified_volume_initializes_expected_directory_layout(tmp_path: Path):
    mountpoint = tmp_path / "external"
    mountpoint.mkdir()
    device = _device_number(mountpoint)
    provider = FakeMountProvider(
        [MountRecord(str(mountpoint), device)],
        uuid_device=device,
    )
    manager = StorageVolumeManager(_settings(mountpoint), mount_provider=provider)

    status = await manager.initialize_layout()

    assert status.availability == "available"
    assert status.is_mounted is True
    assert status.uuid_verified is True
    assert status.storage_root_exists is True
    assert status.writable is True
    for relative in (
        "dailydock/vault",
        "dailydock/vault/images",
        "dailydock/vault/documents",
        "dailydock/vault/other",
        "dailydock/sync",
        "dailydock/staging",
        "dailydock/staging/uploads",
    ):
        assert (mountpoint / relative).is_dir()


@pytest.mark.anyio
async def test_missing_volume_and_unmounted_directory_are_unavailable(tmp_path: Path):
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    provider = FakeMountProvider()
    manager = StorageVolumeManager(_settings(mountpoint), mount_provider=provider)

    status = await manager.initialize_layout()

    assert status.availability == "unavailable"
    assert status.is_mounted is False
    assert status.reason == "volume_not_mounted"
    assert not (mountpoint / "dailydock").exists()


@pytest.mark.anyio
async def test_missing_mountpoint_is_unavailable(tmp_path: Path):
    mountpoint = tmp_path / "missing"
    manager = StorageVolumeManager(
        _settings(mountpoint), mount_provider=FakeMountProvider()
    )

    status = await manager.check()

    assert status.availability == "unavailable"
    assert status.mountpoint_exists is False
    assert status.reason == "mountpoint_missing"


@pytest.mark.anyio
async def test_wrong_volume_at_mountpoint_is_rejected(tmp_path: Path):
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    provider = FakeMountProvider(
        [MountRecord(str(mountpoint), _device_number(mountpoint))],
        uuid_device=(8, 2),
    )
    manager = StorageVolumeManager(_settings(mountpoint), mount_provider=provider)

    status = await manager.initialize_layout()

    assert status.availability == "unavailable"
    assert status.is_mounted is True
    assert status.uuid_verified is False
    assert status.reason == "uuid_verification_failed"
    assert not (mountpoint / "dailydock").exists()


@pytest.mark.anyio
async def test_missing_storage_root_is_reported_without_access(tmp_path: Path):
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    device = _device_number(mountpoint)
    provider = FakeMountProvider(
        [MountRecord(str(mountpoint), device)],
        uuid_device=device,
    )
    manager = StorageVolumeManager(_settings(mountpoint), mount_provider=provider)

    status = await manager.check()

    assert status.availability == "unavailable"
    assert status.uuid_verified is True
    assert status.storage_root_exists is False
    assert status.reason == "storage_root_missing"
    assert not (mountpoint / "dailydock").exists()


@pytest.mark.anyio
async def test_permission_denied_volume_is_unavailable(tmp_path: Path):
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    device = _device_number(mountpoint)
    provider = FakeMountProvider(
        [MountRecord(str(mountpoint), device)],
        uuid_device=device,
    )
    manager = StorageVolumeManager(
        _settings(mountpoint),
        mount_provider=provider,
        access_checker=lambda *_args, **_kwargs: False,
    )

    status = await manager.initialize_layout()

    assert status.availability == "unavailable"
    assert status.reason == "storage_root_parent_not_writable"
    assert not (mountpoint / "dailydock").exists()


@pytest.mark.anyio
async def test_existing_writable_storage_root_does_not_require_writable_mount_root(
    tmp_path: Path,
):
    mountpoint = tmp_path / "mount"
    storage_root = mountpoint / "dailydock"
    storage_root.mkdir(parents=True)
    device = _device_number(mountpoint)
    provider = FakeMountProvider(
        [MountRecord(str(mountpoint), device)],
        uuid_device=device,
    )

    def storage_root_only_is_writable(path, mode, *, dir_fd=None):
        return (
            path == "."
            and dir_fd is not None
            and os.fstat(dir_fd).st_ino == storage_root.stat().st_ino
        )

    manager = StorageVolumeManager(
        _settings(mountpoint),
        mount_provider=provider,
        access_checker=storage_root_only_is_writable,
    )

    status = await manager.initialize_layout()

    assert status.availability == "available"
    assert status.storage_root_exists is True
    assert status.writable is True
    assert (storage_root / "vault" / "images").is_dir()


@pytest.mark.anyio
async def test_missing_storage_root_is_not_created_when_mount_root_is_not_writable(
    tmp_path: Path,
):
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    device = _device_number(mountpoint)
    provider = FakeMountProvider(
        [MountRecord(str(mountpoint), device)],
        uuid_device=device,
    )
    manager = StorageVolumeManager(
        _settings(mountpoint),
        mount_provider=provider,
        access_checker=lambda *_args, **_kwargs: False,
    )

    status = await manager.initialize_layout()

    assert status.availability == "unavailable"
    assert status.storage_root_exists is False
    assert status.reason == "storage_root_parent_not_writable"
    assert not (mountpoint / "dailydock").exists()


@pytest.mark.anyio
async def test_inaccessible_existing_storage_root_is_unavailable(tmp_path: Path):
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    device = _device_number(mountpoint)
    provider = FakeMountProvider(
        [MountRecord(str(mountpoint), device)],
        uuid_device=device,
    )
    initial_manager = StorageVolumeManager(
        _settings(mountpoint), mount_provider=provider
    )
    assert (await initial_manager.initialize_layout()).availability == "available"

    restricted_manager = StorageVolumeManager(
        _settings(mountpoint),
        mount_provider=provider,
        access_checker=lambda *_args, **_kwargs: False,
    )
    status = await restricted_manager.check()

    assert status.availability == "unavailable"
    assert status.storage_root_exists is True
    assert status.reason == "volume_not_writable"


@pytest.mark.anyio
async def test_storage_access_gate_rejects_unavailable_or_disconnected_volume(
    tmp_path: Path,
):
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    device = _device_number(mountpoint)
    provider = FakeMountProvider(
        [MountRecord(str(mountpoint), device)],
        uuid_device=device,
    )
    manager = StorageVolumeManager(_settings(mountpoint), mount_provider=provider)
    await manager.initialize_layout()

    async with manager.access() as access:
        await access.revalidate()
        provider.records.clear()
        with pytest.raises(StorageUnavailableError):
            await access.revalidate()

    with pytest.raises(StorageUnavailableError):
        async with manager.access():
            pytest.fail("Unavailable volume must not grant access")


@pytest.mark.anyio
async def test_storage_module_detects_disconnect_and_recovers_reconnection(
    tmp_path: Path,
):
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    device = _device_number(mountpoint)
    provider = FakeMountProvider(
        [MountRecord(str(mountpoint), device)],
        uuid_device=device,
    )
    volume_manager = StorageVolumeManager(
        _settings(mountpoint), mount_provider=provider
    )
    module = StorageModule(
        _settings(mountpoint), volume_manager=volume_manager
    )
    try:
        await module.startup()
        assert (await module.get_status()).availability == "available"

        provider.records.clear()
        assert (await module.refresh()).availability == "unavailable"

        provider.records.append(MountRecord(str(mountpoint), device))
        recovered = await module.refresh()
        assert recovered.availability == "available"
        assert recovered.uuid_verified is True
    finally:
        await module.shutdown()


@pytest.mark.anyio
async def test_directory_initialization_never_touches_unmounted_mountpoint(
    tmp_path: Path,
):
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    manager = StorageVolumeManager(
        _settings(mountpoint), mount_provider=FakeMountProvider()
    )

    status = await manager.initialize_layout()

    assert status.availability == "unavailable"
    assert list(mountpoint.iterdir()) == []


def test_optional_volume_marker_must_exist_on_verified_volume(tmp_path: Path):
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    device = _device_number(mountpoint)
    provider = FakeMountProvider(
        [MountRecord(str(mountpoint), device)],
        uuid_device=device,
    )
    manager = StorageVolumeManager(
        _settings(mountpoint, storage_marker_path=".dailydock-volume"),
        mount_provider=provider,
    )

    status = asyncio.run(manager.initialize_layout())
    assert status.reason == "volume_marker_missing"
    assert not (mountpoint / "dailydock").exists()

    (mountpoint / ".dailydock-volume").write_text("expected volume", encoding="utf-8")
    status = asyncio.run(manager.initialize_layout())
    assert status.availability == "available"


def test_linux_mountinfo_provider_decodes_mountpoint(tmp_path: Path):
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        "36 25 8:1 / /mnt/external\\040drive rw - ext4 /dev/sda1 rw\n",
        encoding="utf-8",
    )
    provider = LinuxMountInfoProvider(mountinfo_path=mountinfo)

    assert provider.mount_records() == [
        MountRecord("/mnt/external drive", (8, 1))
    ]


def test_linux_mountinfo_provider_rejects_missing_uuid_link(tmp_path: Path):
    provider = LinuxMountInfoProvider(uuid_root=tmp_path / "by-uuid")

    assert provider.uuid_device_number("missing-volume") is None


@pytest.mark.parametrize(
    "values",
    [
        {"storage_mountpoint": Path("relative")},
        {"storage_mountpoint": Path("/")},
        {"storage_directory_name": "../unsafe"},
        {"storage_volume_uuid": "../uuid"},
        {"storage_check_interval": 0},
        {"storage_marker_path": "../marker"},
        {"storage_marker_path": "/absolute/marker"},
    ],
)
def test_invalid_storage_configuration_is_rejected(values):
    defaults = {
        "storage_mountpoint": Path("/mnt/dailydock"),
        "storage_volume_uuid": "test-volume",
    }
    defaults.update(values)
    with pytest.raises(ConfigurationError):
        Settings(**defaults).validate()


@pytest.mark.anyio
async def test_storage_status_api_returns_unavailable_state(tmp_path: Path):
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    module = StorageModule(
        _settings(mountpoint),
        volume_manager=StorageVolumeManager(
            _settings(mountpoint), mount_provider=FakeMountProvider()
        ),
    )
    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.state.storage_module = module
    try:
        await module.startup()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/api/storage/status")

        assert response.status_code == 200
        result = response.json()
        assert result["availability"] == "unavailable"
        assert result["is_mounted"] is False
        assert result["uuid_verified"] is False
        assert result["reason"] == "volume_not_mounted"
    finally:
        await module.shutdown()


def test_backend_starts_and_health_remains_available_without_hdd(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.setattr(main.settings, "storage_mountpoint", tmp_path / "empty-mount")
    monkeypatch.setattr(main.settings, "storage_volume_uuid", "")

    with TestClient(main.app) as client:
        health = client.get("/api/health")
        status = client.get("/api/storage/status")

    assert health.status_code == 200
    assert status.status_code == 200
    assert status.json()["availability"] == "unavailable"
    assert status.json()["reason"] == "storage_not_configured"
