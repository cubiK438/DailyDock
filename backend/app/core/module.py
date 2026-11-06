"""Module interface and lifecycle status definitions for DailyDock."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional


class ModuleStatus(str, Enum):
    """Lifecycle states of an application module."""

    UNINITIALIZED = "UNINITIALIZED"
    STARTING = "STARTING"
    STARTED = "STARTED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class BaseModule(ABC):
    """Abstract base class for all DailyDock application modules.

    Attributes:
        name: Unique identifier for the module.
        dependencies: List of module names that must be initialized before this module.
        status: Current lifecycle state of the module.
        error: Error message if the module encountered a failure.
    """

    def __init__(self, name: str, dependencies: Optional[List[str]] = None) -> None:
        self.name = name
        self.dependencies = dependencies or []
        self.status = ModuleStatus.UNINITIALIZED
        self.error: Optional[str] = None

    @abstractmethod
    async def startup(self) -> None:
        """Initialize module resources and prepare for operation."""
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """Cleanly release module resources during application shutdown."""
        pass

    def __repr__(self) -> str:
        return f"<Module {self.name} status={self.status.value}>"

