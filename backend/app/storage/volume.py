"""Verified access to the configured external storage volume."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import stat
from typing import AsyncIterator, List, Optional, Tuple

from app.config import Settings
from app.core.errors import DailyDockError


_MOUNT_ESCAPE = re.compile(r"\\([0-7]{3})")
_DIRECTORIES = (
    ("vault",),
    ("vault", "images"),
    ("vault", "documents"),
    ("vault", "other"),
    ("sync",),
    ("staging",),
    ("staging", "uploads"),
)


@dataclass(frozen=True)
class MountRecord:
    mountpoint: str
    device_number: Tuple[int, int]


@dataclass
class StorageStatus:
    availability: str
    mountpoint: str
    mountpoint_exists: bool
    is_mounted: bool
    uuid_verified: bool
    storage_root_exists: bool
    writable: bool
    total_capacity_bytes: Optional[int]
    available_capacity_bytes: Optional[int]
    last_successful_verification_at: Optional[datetime]
    reason: Optional[str]

    def to_dict(self) -> dict:
        return {
            "availability": self.availability,
            "mountpoint": self.mountpoint,
            "mountpoint_exists": self.mountpoint_exists,
            "is_mounted": self.is_mounted,
            "uuid_verified": self.uuid_verified,
            "storage_root_exists": self.storage_root_exists,
            "writable": self.writable,
            "total_capacity_bytes": self.total_capacity_bytes,
            "available_capacity_bytes": self.available_capacity_bytes,
            "last_successful_verification_at": (
                self.last_successful_verification_at.isoformat().replace("+00:00", "Z")
                if self.last_successful_verification_at
                else None
            ),
            "reason": self.reason,
        }


class LinuxMountInfoProvider:
    """Reads Linux mountinfo and resolves UUID symlinks to block-device identity."""

    def __init__(
        self,
        mountinfo_path: Path = Path("/proc/self/mountinfo"),
        uuid_root: Path = Path("/dev/disk/by-uuid"),
    ) -> None:
        self.mountinfo_path = mountinfo_path
        self.uuid_root = uuid_root

    def mount_records(self) -> List[MountRecord]:
        records = []
        with self.mountinfo_path.open("r", encoding="utf-8") as mountinfo:
            for line in mountinfo:
                before, separator, _ = line.partition(" - ")
                if not separator:
                    continue
                fields = before.split()
                if len(fields) < 5:
                    continue
                try:
                    major, minor = (int(value) for value in fields[2].split(":", 1))
                except ValueError:
                    continue
                mountpoint = _MOUNT_ESCAPE.sub(
                    lambda match: chr(int(match.group(1), 8)), fields[4]
                )
                records.append(MountRecord(mountpoint, (major, minor)))
        return records

    def uuid_device_number(self, volume_uuid: str) -> Optional[Tuple[int, int]]:
        try:
            device = (self.uuid_root / volume_uuid).resolve(strict=True)
            device_stat = device.stat()
        except (OSError, RuntimeError):
            return None
        if not stat.S_ISBLK(device_stat.st_mode):
            return None
        return os.major(device_stat.st_rdev), os.minor(device_stat.st_rdev)


class StorageUnavailableError(DailyDockError):
    """Raised when an operation cannot safely access the configured HDD."""

    def __init__(self) -> None:
        super().__init__(
            "The configured external storage volume is unavailable.",
            module="storage",
            status_code=503,
        )


class StorageAccess:
    """Verified directory handle for future storage operations.

    Consumers should use root_fd for directory-relative I/O and call revalidate()
    before committing writes or promoting staged files.
    """

    def __init__(
        self,
        manager: "StorageVolumeManager",
        root: Path,
        root_fd: int,
    ) -> None:
        self._manager = manager
        self.root = root
        self.root_fd = root_fd

    async def revalidate(self) -> None:
        await self._manager._revalidate_access(self)


class StorageVolumeManager:
    """Verifies the HDD and provides a fail-closed storage access gate."""

    def __init__(
        self,
        settings: Settings,
        mount_provider: Optional[LinuxMountInfoProvider] = None,
        access_checker=os.access,
    ) -> None:
        self.settings = settings
        self.mount_provider = mount_provider or LinuxMountInfoProvider()
        self.access_checker = access_checker
        self._last_successful_verification_at: Optional[datetime] = None

    @property
    def storage_root(self) -> Path:
        return self.settings.storage_mountpoint / self.settings.storage_directory_name

    async def check(self) -> StorageStatus:
        status, _ = await asyncio.to_thread(self._inspect, False, False)
        if status.availability == "available":
            self._last_successful_verification_at = datetime.now(timezone.utc)
        status.last_successful_verification_at = (
            self._last_successful_verification_at
        )
        return status

    async def initialize_layout(self) -> StorageStatus:
        """Create the required layout only through a verified mounted-volume FD."""
        status, _ = await asyncio.to_thread(self._inspect, True, False)
        if status.availability == "available":
            self._last_successful_verification_at = datetime.now(timezone.utc)
        status.last_successful_verification_at = (
            self._last_successful_verification_at
        )
        return status

    @asynccontextmanager
    async def access(self) -> AsyncIterator[StorageAccess]:
        status, root_fd = await asyncio.to_thread(self._inspect, False, True)
        if status.availability != "available" or root_fd is None:
            raise StorageUnavailableError()
        self._last_successful_verification_at = datetime.now(timezone.utc)
        status.last_successful_verification_at = (
            self._last_successful_verification_at
        )
        access = StorageAccess(self, self.storage_root, root_fd)
        try:
            yield access
        finally:
            os.close(root_fd)

    async def _revalidate_access(self, access: StorageAccess) -> None:
        status = await self.check()
        if status.availability != "available":
            raise StorageUnavailableError()
        try:
            current_stat = await asyncio.to_thread(
                os.stat, access.root, follow_symlinks=False
            )
            opened_stat = await asyncio.to_thread(os.fstat, access.root_fd)
        except OSError as exc:
            raise StorageUnavailableError() from exc
        if (
            stat.S_ISLNK(current_stat.st_mode)
            or (current_stat.st_dev, current_stat.st_ino)
            != (opened_stat.st_dev, opened_stat.st_ino)
        ):
            raise StorageUnavailableError()

    def _inspect(
        self, create_layout: bool, keep_root_fd: bool
    ) -> Tuple[StorageStatus, Optional[int]]:
        mountpoint = self.settings.storage_mountpoint
        base = self._unavailable("storage_not_configured")
        if not self.settings.storage_volume_uuid:
            return base, None

        try:
            mount_stat = mountpoint.lstat()
        except FileNotFoundError:
            return self._unavailable("mountpoint_missing"), None
        except OSError:
            return self._unavailable("mountpoint_inaccessible"), None
        if stat.S_ISLNK(mount_stat.st_mode) or not stat.S_ISDIR(mount_stat.st_mode):
            return self._unavailable("mountpoint_invalid"), None

        try:
            normalized_mountpoint = os.path.normpath(str(mountpoint))
            matching_mounts = [
                record
                for record in self.mount_provider.mount_records()
                if os.path.normpath(record.mountpoint) == normalized_mountpoint
            ]
        except OSError:
            return self._unavailable("mount_information_unavailable"), None
        if not matching_mounts:
            return self._unavailable("volume_not_mounted"), None

        mount = matching_mounts[-1]
        try:
            expected_device = self.mount_provider.uuid_device_number(
                self.settings.storage_volume_uuid
            )
            mount_fd = _open_directory(mountpoint)
        except OSError:
            return self._unavailable(
                "uuid_verification_failed", True, mountpoint_exists=True
            ), None

        root_fd: Optional[int] = None
        try:
            actual_stat = os.fstat(mount_fd)
            actual_device = (os.major(actual_stat.st_dev), os.minor(actual_stat.st_dev))
            if (
                expected_device is None
                or mount.device_number != expected_device
                or actual_device != expected_device
            ):
                return self._unavailable(
                    "uuid_verification_failed", True, mountpoint_exists=True
                ), None

            if self.settings.storage_marker_path and not _marker_exists(
                mount_fd, self.settings.storage_marker_path
            ):
                return self._unavailable(
                    "volume_marker_missing",
                    True,
                    uuid_verified=True,
                    mountpoint_exists=True,
                ), None

            try:
                root_fd = _open_child_directory(
                    mount_fd, self.settings.storage_directory_name, create=False
                )
            except FileNotFoundError:
                if not create_layout:
                    return self._unavailable(
                        "storage_root_missing",
                        True,
                        uuid_verified=True,
                        mountpoint_exists=True,
                    ), None
                if not self._writable(mount_fd):
                    return self._unavailable(
                        "storage_root_parent_not_writable",
                        True,
                        uuid_verified=True,
                        mountpoint_exists=True,
                    ), None
                try:
                    root_fd = _open_child_directory(
                        mount_fd,
                        self.settings.storage_directory_name,
                        create=True,
                    )
                except PermissionError:
                    return self._unavailable(
                        "storage_root_creation_denied",
                        True,
                        uuid_verified=True,
                        mountpoint_exists=True,
                    ), None

            try:
                if create_layout:
                    for parts in _DIRECTORIES:
                        _open_directory_chain(root_fd, parts, create=True)
            except OSError:
                return self._unavailable(
                    "storage_root_inaccessible",
                    True,
                    uuid_verified=True,
                    mountpoint_exists=True,
                    storage_root_exists=self.storage_root.exists(),
                ), None

            if not self._writable(root_fd):
                return self._unavailable(
                    "volume_not_writable",
                    True,
                    uuid_verified=True,
                    mountpoint_exists=True,
                    storage_root_exists=True,
                ), None

            try:
                usage = os.fstatvfs(root_fd)
                total = usage.f_blocks * usage.f_frsize
                available = usage.f_bavail * usage.f_frsize
            except OSError:
                total = available = None
            status = StorageStatus(
                availability="available",
                mountpoint=str(mountpoint),
                mountpoint_exists=True,
                is_mounted=True,
                uuid_verified=True,
                storage_root_exists=True,
                writable=True,
                total_capacity_bytes=total,
                available_capacity_bytes=available,
                last_successful_verification_at=None,
                reason=None,
            )
            if keep_root_fd:
                result_fd = root_fd
                root_fd = None
                return status, result_fd
            return status, None
        finally:
            if root_fd is not None:
                os.close(root_fd)
            os.close(mount_fd)

    def _writable(self, directory_fd: int) -> bool:
        try:
            return self.access_checker(
                ".", os.W_OK | os.X_OK, dir_fd=directory_fd
            )
        except (OSError, TypeError):
            return False

    def _unavailable(
        self,
        reason: str,
        is_mounted: bool = False,
        uuid_verified: bool = False,
        mountpoint_exists: Optional[bool] = None,
        storage_root_exists: bool = False,
    ) -> StorageStatus:
        if mountpoint_exists is None:
            mountpoint_exists = self.settings.storage_mountpoint.is_dir()
        return StorageStatus(
            availability="unavailable",
            mountpoint=str(self.settings.storage_mountpoint),
            mountpoint_exists=mountpoint_exists,
            is_mounted=is_mounted,
            uuid_verified=uuid_verified,
            storage_root_exists=storage_root_exists,
            writable=False,
            total_capacity_bytes=None,
            available_capacity_bytes=None,
            last_successful_verification_at=self._last_successful_verification_at,
            reason=reason,
        )


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _open_directory(path: Path) -> int:
    return os.open(path, _directory_flags())


def _open_child_directory(parent_fd: int, name: str, create: bool) -> int:
    try:
        return os.open(name, _directory_flags(), dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            raise
        try:
            os.mkdir(name, mode=0o750, dir_fd=parent_fd)
        except FileExistsError:
            pass
        return os.open(name, _directory_flags(), dir_fd=parent_fd)


def _open_directory_chain(
    root_fd: int, parts: Tuple[str, ...], create: bool
) -> None:
    current_fd = os.dup(root_fd)
    try:
        for part in parts:
            next_fd = _open_child_directory(current_fd, part, create)
            os.close(current_fd)
            current_fd = next_fd
    finally:
        os.close(current_fd)


def _marker_exists(mount_fd: int, marker_path: str) -> bool:
    parts = Path(marker_path).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        return False
    parent_fd = os.dup(mount_fd)
    try:
        for part in parts[:-1]:
            next_fd = _open_child_directory(parent_fd, part, create=False)
            os.close(parent_fd)
            parent_fd = next_fd
        marker_stat = os.stat(
            parts[-1], dir_fd=parent_fd, follow_symlinks=False
        )
        return not stat.S_ISLNK(marker_stat.st_mode)
    except OSError:
        return False
    finally:
        os.close(parent_fd)
