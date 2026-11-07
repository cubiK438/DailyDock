"""Monitoring module lifecycle, periodic collection, and retention maintenance."""

import asyncio
from datetime import datetime, timedelta, timezone
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import httpx

from app.config import Settings
from app.core.module import BaseModule
from app.core.logging import get_logger
from app.monitoring.collectors import SystemCollector
from app.monitoring.repository import MonitoringRepository
from app.monitoring.state import metric_status, transition_state

logger = get_logger("monitoring")
ServiceCheck = Callable[[], Awaitable[Any]]


class MonitoringModule(BaseModule):
    """Collects and persists configured infrastructure checks."""

    def __init__(
        self,
        settings: Settings,
        session_factory: Any,
        collector: Optional[SystemCollector] = None,
    ) -> None:
        super().__init__("monitoring")
        self.settings = settings
        self.repository = MonitoringRepository(session_factory)
        self.collector = collector
        self._service_checks: Dict[str, Tuple[str, ServiceCheck]] = {}
        self._states: Dict[Tuple[str, str], Any] = {}
        self._semaphore = asyncio.Semaphore(settings.monitor_max_concurrency)
        self._stop_event = asyncio.Event()
        self._scheduler_task: Optional[asyncio.Task] = None
        self._retention_task: Optional[asyncio.Task] = None
        self._http_client: Optional[httpx.AsyncClient] = None

    def register_service_check(
        self, service_id: str, callback: ServiceCheck, name: Optional[str] = None
    ) -> None:
        if service_id in self._service_checks:
            raise ValueError(f"Duplicate service health check '{service_id}'.")
        self._service_checks[service_id] = (name or service_id, callback)

    async def startup(self) -> None:
        await self._restore_state()
        if not self.settings.monitor_enabled:
            logger.info("Infrastructure monitoring is disabled.")
            return

        self._http_client = httpx.AsyncClient(follow_redirects=True)
        if self.collector is None:
            self.collector = SystemCollector(http_client=self._http_client)
        else:
            self.collector.http_client = self._http_client
        self._stop_event.clear()
        self._scheduler_task = asyncio.create_task(
            self._schedule_checks(), name="dailydock-monitor-scheduler"
        )
        self._retention_task = asyncio.create_task(
            self._run_retention(), name="dailydock-monitor-retention"
        )
        logger.info("Infrastructure monitoring started.")

    async def shutdown(self) -> None:
        self._stop_event.set()
        tasks = [
            task
            for task in (self._scheduler_task, self._retention_task)
            if task is not None and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None
        self._scheduler_task = None
        self._retention_task = None

    async def _restore_state(self) -> None:
        for domain, target_id in self._configured_targets():
            sample = await self.repository.latest(domain, target_id)
            if sample is not None:
                self._states[(domain, target_id)] = sample

    def _configured_targets(self) -> List[Tuple[str, str]]:
        targets = [("system", "system")]
        targets.extend(
            ("storage", item["id"])
            for item in self.settings.monitor_storage_targets
            if item.get("enabled", True)
        )
        targets.extend(
            ("connectivity", item["id"])
            for item in self.settings.monitor_connectivity_probes
            if item.get("enabled", True)
        )
        targets.extend(
            ("http", item["id"])
            for item in self.settings.monitor_http_endpoints
            if item.get("enabled", True)
        )
        targets.extend(("services", service_id) for service_id in self._service_checks)
        return targets

    def _jobs(self) -> List[Tuple[str, str, float, Any]]:
        jobs: List[Tuple[str, str, float, Any]] = [
            ("system", "system", self.settings.monitor_system_interval, None)
        ]
        jobs.extend(
            ("storage", item["id"], self.settings.monitor_storage_interval, item)
            for item in self.settings.monitor_storage_targets
            if item.get("enabled", True)
        )
        jobs.extend(
            (
                "connectivity",
                item["id"],
                float(item.get("interval", self.settings.monitor_connectivity_interval)),
                item,
            )
            for item in self.settings.monitor_connectivity_probes
            if item.get("enabled", True)
        )
        jobs.extend(
            (
                "http",
                item["id"],
                float(item.get("interval", self.settings.monitor_http_interval)),
                item,
            )
            for item in self.settings.monitor_http_endpoints
            if item.get("enabled", True)
        )
        jobs.extend(
            ("services", service_id, self.settings.monitor_service_interval, callback)
            for service_id, (_, callback) in self._service_checks.items()
        )
        return jobs

    async def _schedule_checks(self) -> None:
        jobs = self._jobs()
        next_due = {(domain, target_id): 0.0 for domain, target_id, _, _ in jobs}
        poll_interval = min((interval for _, _, interval, _ in jobs), default=1.0)
        while not self._stop_event.is_set():
            now = time.monotonic()
            due_jobs = []
            for domain, target_id, interval, target in jobs:
                key = (domain, target_id)
                if now >= next_due[key]:
                    due_jobs.append((domain, target_id, target))
                    next_due[key] = now + interval
            if due_jobs:
                results = await asyncio.gather(
                    *(self._run_job(domain, target_id, target) for domain, target_id, target in due_jobs),
                    return_exceptions=True,
                )
                for (domain, target_id, _), result in zip(due_jobs, results):
                    if isinstance(result, Exception):
                        logger.error(
                            "Monitoring check failed unexpectedly for %s target %s: %s",
                            domain,
                            target_id,
                            result,
                            exc_info=(type(result), result, result.__traceback__),
                        )
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=poll_interval)
            except asyncio.TimeoutError:
                continue

    async def _run_job(self, domain: str, target_id: str, target: Any) -> None:
        async with self._semaphore:
            check_timeout = self.settings.monitor_check_timeout
            if domain in {"connectivity", "http"} and isinstance(target, dict):
                check_timeout = float(target.get("timeout", check_timeout))
            try:
                observation = await asyncio.wait_for(
                    self._collect(domain, target),
                    timeout=check_timeout,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                observation = {
                    "observed_at": datetime.now(timezone.utc),
                    "check_result": "FAILURE",
                    "error": f"{exc.__class__.__name__}: {exc}"[:500],
                }

            observed_at = observation["observed_at"]
            previous = self._states.get((domain, target_id))
            if domain == "system":
                measured_status = metric_status(
                    observation.get("cpu_percent"),
                    observation.get("memory_percent"),
                    self.settings.monitor_cpu_warning_percent,
                    self.settings.monitor_cpu_critical_percent,
                    self.settings.monitor_ram_warning_percent,
                    self.settings.monitor_ram_critical_percent,
                )
            elif domain == "storage" and observation["check_result"] == "SUCCESS":
                percent = observation["used_percent"]
                if percent >= self.settings.monitor_storage_critical_percent:
                    measured_status = "CRITICAL"
                elif percent >= self.settings.monitor_storage_warning_percent:
                    measured_status = "WARNING"
                else:
                    measured_status = "OK"
            else:
                measured_status = "OK"

            state = transition_state(
                previous,
                observation["check_result"],
                measured_status
                if observation["check_result"] == "SUCCESS"
                else "UNKNOWN",
                self.settings.monitor_failure_threshold,
                self.settings.monitor_recovery_threshold,
                observed_at,
            )
            values = {
                **observation,
                **state,
            }
            if domain != "system":
                values["target_id"] = target_id
            if domain == "services":
                values["detail"] = observation.get("detail")
            try:
                sample = await self.repository.add_sample(domain, values)
            except Exception:
                logger.exception(
                    "Failed to persist %s monitoring sample for target %s.",
                    domain,
                    target_id,
                )
                return
            self._states[(domain, target_id)] = sample

    async def _collect(self, domain: str, target: Any) -> Dict[str, Any]:
        if self.collector is None:
            raise RuntimeError("Monitoring collector is not initialized.")
        if domain == "system":
            observation = await self.collector.collect_system()
            return observation
        if domain == "storage":
            observation = await self.collector.collect_storage(target)
            return observation
        if domain == "connectivity":
            observation = await self.collector.check_connectivity(target)
            return observation
        if domain == "http":
            observation = await self.collector.check_http(target)
            return observation
        if domain == "services":
            return await self.collector.check_service(target)
        raise ValueError(f"Unsupported monitoring domain '{domain}'.")

    async def _run_retention(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=86400)
            except asyncio.TimeoutError:
                cutoff = datetime.now(timezone.utc) - timedelta(
                    days=self.settings.monitor_retention_days
                )
                while True:
                    deleted = await self.repository.cleanup_batch(
                        cutoff, self.settings.monitor_retention_batch_size
                    )
                    logger.info("Monitoring retention removed %d old samples.", deleted)
                    if deleted == 0:
                        break
                    await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Monitoring retention cleanup failed.")

    def interval_for(self, domain: str, target_id: Optional[str] = None) -> float:
        if domain == "system":
            return self.settings.monitor_system_interval
        if domain == "storage":
            return self.settings.monitor_storage_interval
        if domain == "connectivity":
            target = next(
                (item for item in self.settings.monitor_connectivity_probes if item["id"] == target_id),
                {},
            )
            return float(target.get("interval", self.settings.monitor_connectivity_interval))
        if domain == "http":
            target = next(
                (item for item in self.settings.monitor_http_endpoints if item["id"] == target_id),
                {},
            )
            return float(target.get("interval", self.settings.monitor_http_interval))
        return self.settings.monitor_service_interval

    def all_target_ids(self, domain: str) -> List[str]:
        if domain == "system":
            return ["system"]
        if domain == "storage":
            return [
                item["id"]
                for item in self.settings.monitor_storage_targets
                if item.get("enabled", True)
            ]
        if domain == "connectivity":
            return [
                item["id"]
                for item in self.settings.monitor_connectivity_probes
                if item.get("enabled", True)
            ]
        if domain == "http":
            return [
                item["id"]
                for item in self.settings.monitor_http_endpoints
                if item.get("enabled", True)
            ]
        if domain == "services":
            return list(self._service_checks)
        return []

    def display_name(self, domain: str, target_id: str) -> str:
        if domain == "services":
            return self._service_checks[target_id][0]
        targets = {
            "storage": self.settings.monitor_storage_targets,
            "connectivity": self.settings.monitor_connectivity_probes,
            "http": self.settings.monitor_http_endpoints,
        }.get(domain, [])
        target = next((item for item in targets if item["id"] == target_id), {})
        return str(target.get("name", target_id))

    async def cleanup_retention(self) -> int:
        """Run retention batches until no more rows qualify; exposed for tests."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.settings.monitor_retention_days)
        total = 0
        while True:
            deleted = await self.repository.cleanup_batch(
                cutoff, self.settings.monitor_retention_batch_size
            )
            total += deleted
            if deleted == 0:
                return total
            await asyncio.sleep(0)
