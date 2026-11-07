"""Read-only infrastructure monitoring API."""

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, Request

from app.monitoring.module import MonitoringModule
from app.monitoring.state import aggregate_status, serialize_sample, with_freshness

router = APIRouter(prefix="/monitoring", tags=["monitoring"])
MAX_HISTORY_LIMIT = 500
DEFAULT_HISTORY_LIMIT = 100


def _module(request: Request) -> MonitoringModule:
    module = getattr(request.app.state, "monitoring_module", None)
    if module is None:
        raise HTTPException(status_code=503, detail="Monitoring is unavailable.")
    return module


def _normalize_range(
    since: Optional[datetime], until: Optional[datetime]
) -> Tuple[Optional[datetime], Optional[datetime]]:
    for label, value in (("since", since), ("until", until)):
        if value is not None and value.tzinfo is None:
            raise HTTPException(
                status_code=422,
                detail=f"{label} must include a UTC offset.",
            )
    since = since.astimezone(timezone.utc) if since is not None else None
    until = until.astimezone(timezone.utc) if until is not None else None
    if since is not None and until is not None and since >= until:
        raise HTTPException(
            status_code=422, detail="'since' must be earlier than 'until'."
        )
    return since, until


def _fresh_sample(module: MonitoringModule, domain: str, target_id: str, sample: Any):
    freshness = with_freshness(
        sample,
        module.interval_for(domain, target_id),
        module.settings.monitor_stale_multiplier,
    )
    return {**serialize_sample(sample), **freshness} if sample is not None else {
        "target_id": target_id,
        **freshness,
        "consecutive_failures": 0,
        "consecutive_successes": 0,
        "last_transition_at": None,
    }


async def _latest_domain(request: Request, domain: str) -> Dict[str, Any]:
    module = _module(request)
    target_ids = module.all_target_ids(domain)
    latest = await module.repository.latest_many(domain, target_ids)
    items = []
    for target_id in target_ids:
        sample = latest.get(target_id)
        item = _fresh_sample(module, domain, target_id, sample)
        if domain != "system":
            item["name"] = module.display_name(domain, target_id)
        items.append(item)
    return {"items": items}


async def _history(
    request: Request,
    domain: str,
    target_id: Optional[str],
    limit: int,
    since: Optional[datetime],
    until: Optional[datetime],
) -> Dict[str, Any]:
    since, until = _normalize_range(since, until)
    module = _module(request)
    if domain != "system" and target_id not in module.all_target_ids(domain):
        raise HTTPException(status_code=404, detail="Monitoring target was not found.")
    rows, has_more = await module.repository.history(
        domain, target_id, since, until, limit
    )
    return {
        "target_id": target_id or "system",
        "items": [serialize_sample(row) for row in rows],
        "limit": limit,
        "has_more": has_more,
    }


@router.get("/summary")
async def get_summary(request: Request) -> Dict[str, Any]:
    module = _module(request)
    domains = ("system", "storage", "connectivity", "http", "services")
    summary: Dict[str, List[Dict[str, Any]]] = {}
    statuses = []
    for domain in domains:
        result = await _latest_domain(request, domain)
        summary[domain] = result["items"]
        statuses.extend(item["status"] for item in result["items"])
    return {"status": aggregate_status(statuses), "domains": summary}


@router.get("/system")
async def get_system(request: Request) -> Dict[str, Any]:
    module = _module(request)
    result = await _latest_domain(request, "system")
    return result["items"][0] if result["items"] else {
        "target_id": "system",
        **with_freshness(None, module.interval_for("system"), module.settings.monitor_stale_multiplier),
    }


@router.get("/storage")
async def get_storage(request: Request) -> Dict[str, Any]:
    return await _latest_domain(request, "storage")


@router.get("/connectivity")
async def get_connectivity(request: Request) -> Dict[str, Any]:
    return await _latest_domain(request, "connectivity")


@router.get("/http")
async def get_http(request: Request) -> Dict[str, Any]:
    return await _latest_domain(request, "http")


@router.get("/services")
async def get_services(request: Request) -> Dict[str, Any]:
    return await _latest_domain(request, "services")


@router.get("/system/history")
async def get_system_history(
    request: Request,
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_HISTORY_LIMIT),
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> Dict[str, Any]:
    return await _history(request, "system", None, limit, since, until)


@router.get("/storage/{target_id}/history")
async def get_storage_history(
    target_id: str,
    request: Request,
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_HISTORY_LIMIT),
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> Dict[str, Any]:
    return await _history(request, "storage", target_id, limit, since, until)


@router.get("/connectivity/{target_id}/history")
async def get_connectivity_history(
    target_id: str,
    request: Request,
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_HISTORY_LIMIT),
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> Dict[str, Any]:
    return await _history(request, "connectivity", target_id, limit, since, until)


@router.get("/http/{target_id}/history")
async def get_http_history(
    target_id: str,
    request: Request,
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_HISTORY_LIMIT),
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> Dict[str, Any]:
    return await _history(request, "http", target_id, limit, since, until)


@router.get("/services/{target_id}/history")
async def get_service_history(
    target_id: str,
    request: Request,
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_HISTORY_LIMIT),
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> Dict[str, Any]:
    return await _history(request, "services", target_id, limit, since, until)
