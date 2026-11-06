"""Module registry and lifecycle manager for DailyDock."""

from typing import Dict, List, Optional
from app.core.errors import (
    CircularDependencyError,
    MissingDependencyError,
    ModuleLifecycleError,
)
from app.core.logging import get_logger
from app.core.module import BaseModule, ModuleStatus

logger = get_logger("registry")


class ModuleRegistry:
    """Coordinates module registration, dependency resolution, startup rollback, and shutdown."""

    def __init__(self) -> None:
        self._modules: Dict[str, BaseModule] = {}
        self._started_modules: List[BaseModule] = []

    def register(self, module: BaseModule) -> None:
        """Explicitly register an application module.

        Args:
            module: An instance of BaseModule.

        Raises:
            ModuleLifecycleError: If a module with the same name is already registered.
        """
        if module.name in self._modules:
            raise ModuleLifecycleError(
                f"Module '{module.name}' is already registered.",
                module="registry",
                context={"module": module.name},
            )
        self._modules[module.name] = module
        logger.debug("Registered module [%s]", module.name)

    def get(self, name: str) -> Optional[BaseModule]:
        """Retrieve a registered module by name."""
        return self._modules.get(name)

    def list_modules(self) -> List[BaseModule]:
        """Return a list of all registered modules."""
        return list(self._modules.values())

    def resolve_dependencies(self) -> List[BaseModule]:
        """Resolve module startup order using topological sorting (Kahn's algorithm).

        Returns:
            A list of BaseModule instances ordered such that dependencies appear
            before the modules that require them.

        Raises:
            MissingDependencyError: If a module declares a dependency that is not registered.
            CircularDependencyError: If a circular dependency cycle is detected.
        """
        # 1. Validate that all declared dependencies are registered
        for name, module in self._modules.items():
            for dep in module.dependencies:
                if dep not in self._modules:
                    raise MissingDependencyError(
                        f"Module '{name}' requires unregistered dependency '{dep}'.",
                        module="registry",
                        context={"module": name, "missing_dependency": dep},
                    )

        # 2. Build graph and compute in-degrees
        # Edge: dep -> dependent (dep must run before dependent)
        in_degree: Dict[str, int] = {name: 0 for name in self._modules}
        adjacency: Dict[str, List[str]] = {name: [] for name in self._modules}

        for name, module in self._modules.items():
            in_degree[name] = len(module.dependencies)
            for dep in module.dependencies:
                adjacency[dep].append(name)

        # 3. Process nodes with 0 in-degree (sorted deterministically)
        queue = [name for name, deg in in_degree.items() if deg == 0]
        queue.sort()

        ordered_names: List[str] = []
        while queue:
            current = queue.pop(0)
            ordered_names.append(current)
            for dependent in sorted(adjacency[current]):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # 4. Check for cycles
        if len(ordered_names) != len(self._modules):
            cyclic = [name for name, deg in in_degree.items() if deg > 0]
            raise CircularDependencyError(
                f"Circular dependency detected among modules: {sorted(cyclic)}",
                module="registry",
                context={"cyclic_modules": sorted(cyclic)},
            )

        return [self._modules[name] for name in ordered_names]

    async def _rollback_startup(self, failed_module_name: str, reason: str) -> None:
        """Roll back startup by stopping previously started modules in reverse order."""
        rollback_order = list(reversed(self._started_modules))
        rolled_back_names = [m.name for m in rollback_order]
        logger.warning(
            "Startup failure in module [%s]: %s. Initiating rollback of %d started modules: %s",
            failed_module_name,
            reason,
            len(rollback_order),
            rolled_back_names,
        )

        for module in rollback_order:
            module.status = ModuleStatus.STOPPING
            try:
                await module.shutdown()
                module.status = ModuleStatus.STOPPED
                logger.info("Rollback: module [%s] stopped.", module.name)
            except Exception as exc:
                module.status = ModuleStatus.FAILED
                module.error = f"Rollback shutdown error: {exc}"
                logger.error(
                    "Rollback: module [%s] failed during shutdown: %s",
                    module.name,
                    exc,
                    exc_info=True,
                )

        self._started_modules.clear()

    async def startup(self) -> None:
        """Initialize all registered modules in resolved dependency order.

        If any module fails during startup, all previously started modules are
        rolled back (shut down in reverse dependency order) and ModuleLifecycleError is raised.

        Raises:
            ModuleLifecycleError: If any module encounters a failure during startup.
        """
        startup_order = self.resolve_dependencies()
        self._started_modules.clear()

        logger.info(
            "Starting up %d modules in dependency order: %s",
            len(startup_order),
            [m.name for m in startup_order],
        )

        for module in startup_order:
            # Check if any required dependency failed or was not started
            failed_deps = [
                dep
                for dep in module.dependencies
                if self._modules[dep].status != ModuleStatus.STARTED
            ]

            if failed_deps:
                module.status = ModuleStatus.FAILED
                module.error = f"Dependencies failed to start: {failed_deps}"
                await self._rollback_startup(
                    module.name, f"Dependencies failed to start: {failed_deps}"
                )
                raise ModuleLifecycleError(
                    f"Module '{module.name}' startup failed because dependencies failed: {failed_deps}",
                    module="registry",
                    context={"module": module.name, "failed_dependencies": failed_deps},
                )

            module.status = ModuleStatus.STARTING
            logger.info("Initializing module [%s]...", module.name)
            try:
                await module.startup()
                module.status = ModuleStatus.STARTED
                self._started_modules.append(module)
                logger.info("Module [%s] started successfully.", module.name)
            except Exception as exc:
                module.status = ModuleStatus.FAILED
                module.error = str(exc)
                logger.error(
                    "Module [%s] failed during startup: %s",
                    module.name,
                    exc,
                    exc_info=True,
                )
                await self._rollback_startup(module.name, str(exc))
                raise ModuleLifecycleError(
                    f"Module '{module.name}' failed during startup: {exc}",
                    module="registry",
                    context={"module": module.name, "error": str(exc)},
                ) from exc

    async def shutdown(self) -> None:
        """Shut down initialized modules in reverse dependency order.

        Errors encountered by a single module are logged and do not prevent
        remaining modules from shutting down.
        """
        shutdown_order = list(reversed(self._started_modules))
        logger.info(
            "Shutting down %d started modules in reverse order: %s",
            len(shutdown_order),
            [m.name for m in shutdown_order],
        )

        for module in shutdown_order:
            module.status = ModuleStatus.STOPPING
            logger.info("Stopping module [%s]...", module.name)
            try:
                await module.shutdown()
                module.status = ModuleStatus.STOPPED
                logger.info("Module [%s] stopped successfully.", module.name)
            except Exception as exc:
                module.status = ModuleStatus.FAILED
                module.error = str(exc)
                logger.error(
                    "Module [%s] encountered an error during shutdown: %s",
                    module.name,
                    exc,
                    exc_info=True,
                )

        self._started_modules.clear()

