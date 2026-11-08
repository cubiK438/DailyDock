"""Storage availability module integrated with DailyDock lifecycle."""

import asyncio
from typing import Optional

from app.config import Settings
from app.core.logging import get_logger
from app.core.module import BaseModule
from app.storage.volume import StorageStatus, StorageVolumeManager

logger = get_logger("storage")


class StorageModule(BaseModule):
    """Monitors external storage availability without blocking application startup."""

    def __init__(
        self,
        settings: Settings,
        volume_manager: Optional[StorageVolumeManager] = None,
    ) -> None:
        super().__init__("storage")
        self.settings = settings
        self.volume_manager = volume_manager or StorageVolumeManager(settings)
        self._status: Optional[StorageStatus] = None
        self._refresh_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._monitor_task: Optional[asyncio.Task] = None

    async def startup(self) -> None:
        await self.refresh(initialize_layout=True)
        self._stop_event.clear()
        self._monitor_task = asyncio.create_task(
            self._monitor(), name="dailydock-storage-monitor"
        )

    async def shutdown(self) -> None:
        self._stop_event.set()
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            await asyncio.gather(self._monitor_task, return_exceptions=True)
            self._monitor_task = None

    async def refresh(self, initialize_layout: bool = False) -> StorageStatus:
        async with self._refresh_lock:
            previous = self._status.availability if self._status else None
            if initialize_layout:
                status = await self.volume_manager.initialize_layout()
            else:
                status = await self.volume_manager.check()
                if (
                    status.is_mounted
                    and status.uuid_verified
                    and not status.storage_root_exists
                ):
                    status = await self.volume_manager.initialize_layout()
            self._status = status
            if status.availability != previous:
                if status.availability == "available":
                    logger.info("External storage volume is available.")
                elif previous is not None:
                    logger.warning(
                        "External storage volume is unavailable (%s).", status.reason
                    )
                else:
                    logger.info(
                        "External storage volume is unavailable (%s).", status.reason
                    )
            return status

    async def get_status(self) -> StorageStatus:
        return await self.refresh()

    async def _monitor(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.settings.storage_check_interval,
                )
            except asyncio.TimeoutError:
                await self.refresh()
