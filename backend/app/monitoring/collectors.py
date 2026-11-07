"""Domain-specific infrastructure monitoring collectors."""

import asyncio
import os
from pathlib import Path
import platform
import re
import time
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional, Tuple

import httpx
import psutil


_MOUNT_ESCAPE = re.compile(r"\\([0-7]{3})")


def decode_mount_field(value: str) -> str:
    """Decode octal escapes used for mount paths in Linux mountinfo."""
    return _MOUNT_ESCAPE.sub(lambda match: chr(int(match.group(1), 8)), value)


class LinuxMountProvider:
    """Reads Linux mount records and resolves optional filesystem UUID identities."""

    def __init__(
        self,
        mountinfo_path: Path = Path("/proc/self/mountinfo"),
        uuid_root: Path = Path("/dev/disk/by-uuid"),
    ) -> None:
        self.mountinfo_path = mountinfo_path
        self.uuid_root = uuid_root

    def mounts(self) -> Iterable[Tuple[str, str]]:
        records = []
        with self.mountinfo_path.open("r", encoding="utf-8") as mountinfo:
            for line in mountinfo:
                before, separator, after = line.partition(" - ")
                if not separator:
                    continue
                mount_fields = before.split()
                source_fields = after.split()
                if len(mount_fields) < 5 or len(source_fields) < 2:
                    continue
                records.append(
                    (
                        decode_mount_field(mount_fields[4]),
                        decode_mount_field(source_fields[1]),
                    )
                )
        return records

    def source_for_uuid(self, uuid: str) -> Optional[Path]:
        try:
            candidate = self.uuid_root / uuid
            return candidate.resolve(strict=True)
        except (OSError, RuntimeError):
            return None


class SystemCollector:
    """Collects host CPU, memory, uptime, filesystem, network, and HTTP measurements."""

    def __init__(
        self,
        mount_provider: Optional[Any] = None,
        platform_name: Optional[str] = None,
        psutil_module: Any = psutil,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.mount_provider = mount_provider or LinuxMountProvider()
        self.platform_name = platform_name or platform.system()
        self.psutil = psutil_module
        self.http_client = http_client
        self._cpu_primed = False

    async def collect_system(self) -> Dict[str, Any]:
        observed_at = _utc_now()
        try:
            cpu = await asyncio.to_thread(self.psutil.cpu_percent, None)
            cpu_percent: Optional[float] = float(cpu) if self._cpu_primed else None
            self._cpu_primed = True
            memory = await asyncio.to_thread(self.psutil.virtual_memory)
            boot_time = await asyncio.to_thread(self.psutil.boot_time)
            uptime = max(0.0, time.time() - boot_time)
            return {
                "observed_at": observed_at,
                "check_result": "SUCCESS",
                "cpu_percent": cpu_percent,
                "memory_total_bytes": int(memory.total),
                "memory_used_bytes": int(memory.total - memory.available),
                "memory_available_bytes": int(memory.available),
                "memory_percent": float(memory.percent),
                "uptime_seconds": uptime,
                "error": None,
            }
        except Exception as exc:
            return _failed_observation(observed_at, exc)

    async def collect_storage(self, target: Dict[str, Any]) -> Dict[str, Any]:
        observed_at = _utc_now()
        mount_path = str(target["mount_path"])
        try:
            source = await asyncio.to_thread(self._find_mount_source, mount_path)
            if source is None:
                raise OSError("Expected mount point is not mounted")
            expected_uuid = target.get("expected_uuid")
            if expected_uuid:
                expected_source = await asyncio.to_thread(
                    self.mount_provider.source_for_uuid, str(expected_uuid)
                )
                if expected_source is None or _real_path(source) != _real_path(expected_source):
                    raise OSError("Mounted filesystem does not match expected UUID")
            usage = await asyncio.to_thread(self.psutil.disk_usage, mount_path)
            return {
                "observed_at": observed_at,
                "check_result": "SUCCESS",
                "mount_path": mount_path,
                "mount_source": source,
                "total_bytes": int(usage.total),
                "used_bytes": int(usage.used),
                "available_bytes": int(usage.free),
                "used_percent": float(usage.percent),
                "error": None,
            }
        except Exception as exc:
            return {
                "observed_at": observed_at,
                "check_result": "FAILURE",
                "error": f"{exc.__class__.__name__}: {exc}"[:500],
                "mount_path": mount_path,
                "mount_source": None,
                "total_bytes": None,
                "used_bytes": None,
                "available_bytes": None,
                "used_percent": None,
            }

    def _find_mount_source(self, mount_path: str) -> Optional[str]:
        expected = os.path.normpath(mount_path)
        if self.platform_name == "Linux":
            for observed_mount, source in self.mount_provider.mounts():
                if os.path.normpath(observed_mount) == expected:
                    return source
            return None

        # Non-Linux fallback keeps development usable; Linux mount semantics are
        # tested through the injected mount provider.
        for partition in self.psutil.disk_partitions(all=True):
            if os.path.normpath(partition.mountpoint) == expected:
                return partition.device
        return None

    async def check_http(
        self, target: Dict[str, Any], client: Optional[httpx.AsyncClient] = None
    ) -> Dict[str, Any]:
        return await self._request(target, client=client)

    async def check_connectivity(
        self, target: Dict[str, Any], client: Optional[httpx.AsyncClient] = None
    ) -> Dict[str, Any]:
        return await self._request(target, client=client)

    async def _request(
        self, target: Dict[str, Any], client: Optional[httpx.AsyncClient]
    ) -> Dict[str, Any]:
        observed_at = _utc_now()
        request_client = client or self.http_client
        owns_client = request_client is None
        started = time.perf_counter()
        try:
            if owns_client:
                request_client = httpx.AsyncClient(
                    timeout=float(target.get("timeout", 5.0)),
                    follow_redirects=True,
                )
            response = await request_client.get(
                target["url"],
                timeout=float(target.get("timeout", 5.0)),
            )
            latency_ms = (time.perf_counter() - started) * 1000
            minimum = int(target.get("accepted_status_min", 200))
            maximum = int(target.get("accepted_status_max", 399))
            success = minimum <= response.status_code <= maximum
            result = {
                "observed_at": observed_at,
                "check_result": "SUCCESS" if success else "FAILURE",
                "latency_ms": latency_ms,
                "status_code": response.status_code,
                "error": None if success else f"Unexpected HTTP status {response.status_code}",
            }
            if not success:
                result["error"] = _sanitize_error(result["error"], target["url"])
            return result
        except Exception as exc:
            return {
                "observed_at": observed_at,
                "check_result": "FAILURE",
                "error": f"{exc.__class__.__name__}: HTTP request failed",
                "latency_ms": (time.perf_counter() - started) * 1000,
                "status_code": None,
            }
        finally:
            if owns_client and request_client is not None:
                await request_client.aclose()

    async def check_service(
        self, callback: Callable[[], Awaitable[Any]]
    ) -> Dict[str, Any]:
        observed_at = _utc_now()
        started = time.perf_counter()
        try:
            result = await callback()
            if isinstance(result, dict):
                healthy = bool(result.get("healthy", result.get("status") in {"OK", "healthy", True}))
                detail = result.get("detail")
            else:
                healthy = bool(result)
                detail = None
            return {
                "observed_at": observed_at,
                "check_result": "SUCCESS" if healthy else "FAILURE",
                "latency_ms": (time.perf_counter() - started) * 1000,
                "detail": str(detail)[:500] if detail is not None else None,
                "error": None if healthy else "Service health check reported unhealthy",
            }
        except Exception as exc:
            return {
                **_failed_observation(observed_at, exc),
                "latency_ms": (time.perf_counter() - started) * 1000,
                "detail": None,
            }


def _utc_now():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)


def _failed_observation(
    observed_at: Any, exc: Exception, url: Optional[str] = None
) -> Dict[str, Any]:
    message = f"{exc.__class__.__name__}: check failed"
    if url:
        message = _sanitize_error(message, url)
    return {
        "observed_at": observed_at,
        "check_result": "FAILURE",
        "error": message[:500],
    }


def _sanitize_error(message: str, url: str) -> str:
    parsed = httpx.URL(url)
    safe_url = str(parsed.copy_with(username=None, password=None, query=None, fragment=None))
    return message.replace(url, safe_url)


def _real_path(path: Any) -> Path:
    return Path(path).resolve(strict=False)
