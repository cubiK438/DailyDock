"""External storage status API."""

from fastapi import APIRouter, HTTPException, Request

from app.storage.module import StorageModule

router = APIRouter(prefix="/storage", tags=["storage"])


@router.get("/status")
async def get_storage_status(request: Request) -> dict:
    module = getattr(request.app.state, "storage_module", None)
    if not isinstance(module, StorageModule):
        raise HTTPException(
            status_code=503,
            detail="Storage status is unavailable.",
        )
    status = await module.get_status()
    return status.to_dict()
