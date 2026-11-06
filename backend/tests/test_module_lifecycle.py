"""Unit tests for module lifecycle management and startup rollback."""

import pytest
from typing import List
from app.core.errors import ModuleLifecycleError
from app.core.module import BaseModule, ModuleStatus
from app.core.registry import ModuleRegistry


class MockModule(BaseModule):
    """Test module recording startup and shutdown lifecycle events."""

    def __init__(
        self,
        name: str,
        dependencies: List[str] = None,
        should_fail_startup: bool = False,
        event_log: List[str] = None,
    ):
        super().__init__(name, dependencies)
        self.should_fail_startup = should_fail_startup
        self.event_log = event_log if event_log is not None else []

    async def startup(self) -> None:
        if self.should_fail_startup:
            raise RuntimeError(f"Deliberate failure in module {self.name}")
        self.event_log.append(f"start:{self.name}")

    async def shutdown(self) -> None:
        self.event_log.append(f"stop:{self.name}")


@pytest.mark.anyio
async def test_modules_initialize_and_shutdown_in_reverse_order():
    """Modules should start in dependency order and shut down in reverse dependency order."""
    registry = ModuleRegistry()
    event_log = []

    # Module C depends on B; B depends on A
    mod_a = MockModule("mod_a", dependencies=[], event_log=event_log)
    mod_b = MockModule("mod_b", dependencies=["mod_a"], event_log=event_log)
    mod_c = MockModule("mod_c", dependencies=["mod_b"], event_log=event_log)

    registry.register(mod_c)
    registry.register(mod_a)
    registry.register(mod_b)

    await registry.startup()

    assert event_log == ["start:mod_a", "start:mod_b", "start:mod_c"]
    assert mod_a.status == ModuleStatus.STARTED
    assert mod_b.status == ModuleStatus.STARTED
    assert mod_c.status == ModuleStatus.STARTED

    await registry.shutdown()

    assert event_log == [
        "start:mod_a",
        "start:mod_b",
        "start:mod_c",
        "stop:mod_c",
        "stop:mod_b",
        "stop:mod_a",
    ]
    assert mod_a.status == ModuleStatus.STOPPED
    assert mod_b.status == ModuleStatus.STOPPED
    assert mod_c.status == ModuleStatus.STOPPED


@pytest.mark.anyio
async def test_startup_failure_triggers_rollback():
    """If a module fails during startup, previously started modules must roll back in reverse order."""
    registry = ModuleRegistry()
    event_log = []

    mod_a = MockModule("mod_a", dependencies=[], event_log=event_log)
    mod_b = MockModule("mod_b", dependencies=["mod_a"], event_log=event_log)
    # mod_c fails during startup
    mod_c = MockModule(
        "mod_c",
        dependencies=["mod_b"],
        should_fail_startup=True,
        event_log=event_log,
    )

    registry.register(mod_a)
    registry.register(mod_b)
    registry.register(mod_c)

    with pytest.raises(ModuleLifecycleError) as exc_info:
        await registry.startup()

    assert "mod_c" in str(exc_info.value)
    assert mod_c.status == ModuleStatus.FAILED

    # mod_b and mod_a should have been stopped in reverse order
    assert event_log == ["start:mod_a", "start:mod_b", "stop:mod_b", "stop:mod_a"]
    assert mod_b.status == ModuleStatus.STOPPED
    assert mod_a.status == ModuleStatus.STOPPED


@pytest.mark.anyio
async def test_duplicate_module_registration_rejected():
    """Attempting to register two modules with the same name must raise ModuleLifecycleError."""
    registry = ModuleRegistry()
    mod1 = MockModule("database")
    mod2 = MockModule("database")

    registry.register(mod1)
    with pytest.raises(ModuleLifecycleError):
        registry.register(mod2)

