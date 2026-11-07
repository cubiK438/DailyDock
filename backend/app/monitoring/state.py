"""Monitoring status evaluation and API freshness helpers."""

from datetime import datetime, timezone
from typing import Any, Dict, Optional


STATUS_PRECEDENCE = {
    "OK": 0,
    "UNKNOWN": 1,
    "WARNING": 2,
    "STALE": 3,
    "DOWN": 4,
    "CRITICAL": 5,
}


def transition_state(
    previous: Optional[Any],
    check_result: str,
    measured_status: str,
    failure_threshold: int,
    recovery_threshold: int,
    observed_at: datetime,
) -> Dict[str, Any]:
    previous_status = previous.status if previous is not None else "UNKNOWN"
    failures = previous.consecutive_failures if previous is not None else 0
    successes = previous.consecutive_successes if previous is not None else 0

    if check_result != "SUCCESS":
        failures += 1
        successes = 0
        status = "DOWN" if failures >= failure_threshold else previous_status
    else:
        failures = 0
        successes += 1
        if previous_status == "DOWN" and successes < recovery_threshold:
            status = "DOWN"
        else:
            status = measured_status

    transition_at = (
        observed_at
        if status != previous_status
        else previous.last_transition_at if previous is not None else None
    )
    return {
        "status": status,
        "consecutive_failures": failures,
        "consecutive_successes": successes,
        "last_transition_at": transition_at,
    }


def metric_status(
    cpu_percent: Optional[float],
    memory_percent: Optional[float],
    cpu_warning: float,
    cpu_critical: float,
    ram_warning: float,
    ram_critical: float,
) -> str:
    available = []
    if cpu_percent is not None:
        available.append((cpu_percent, cpu_warning, cpu_critical))
    if memory_percent is not None:
        available.append((memory_percent, ram_warning, ram_critical))
    if not available:
        return "UNKNOWN"
    if any(value >= critical for value, _, critical in available):
        return "CRITICAL"
    if any(value >= warning for value, warning, _ in available):
        return "WARNING"
    return "OK"


def with_freshness(
    sample: Optional[Any], interval: float, stale_multiplier: float
) -> Dict[str, Any]:
    if sample is None:
        return {
            "status": "UNKNOWN",
            "last_status": None,
            "is_stale": False,
            "last_check_at": None,
            "age_seconds": None,
        }
    observed_at = _as_utc(sample.observed_at)
    age_seconds = max(0.0, (datetime.now(timezone.utc) - observed_at).total_seconds())
    is_stale = age_seconds > interval * stale_multiplier
    return {
        "status": "STALE" if is_stale else sample.status,
        "last_status": sample.status,
        "is_stale": is_stale,
        "last_check_at": observed_at.isoformat().replace("+00:00", "Z"),
        "age_seconds": int(age_seconds),
    }


def serialize_sample(sample: Any) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for column in sample.__table__.columns:
        value = getattr(sample, column.name)
        if isinstance(value, datetime):
            value = _as_utc(value).isoformat().replace("+00:00", "Z")
        result[column.name] = value
    return result


def aggregate_status(statuses: list[str]) -> str:
    if not statuses:
        return "UNKNOWN"
    return max(statuses, key=lambda status: STATUS_PRECEDENCE.get(status, 1))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
