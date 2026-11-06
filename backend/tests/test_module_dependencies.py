"""Unit tests for module dependency resolution, cycle detection, and missing dependencies."""

import pytest
from app.core.errors import CircularDependencyError, MissingDependencyError
from app.core.module import BaseModule
from app.core.registry import ModuleRegistry


class SimpleModule(BaseModule):
    """Stub module for dependency testing."""

    async def startup(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass


def test_missing_dependency_raises_error():
    """Declaring a dependency that is not registered must raise MissingDependencyError."""
    registry = ModuleRegistry()
    registry.register(SimpleModule("mod_b", dependencies=["nonexistent_mod"]))

    with pytest.raises(MissingDependencyError) as exc_info:
        registry.resolve_dependencies()

    assert "nonexistent_mod" in str(exc_info.value)
    assert exc_info.value.module == "registry"


def test_direct_circular_dependency_raises_error():
    """Direct A -> B -> A cycle must raise CircularDependencyError."""
    registry = ModuleRegistry()
    registry.register(SimpleModule("mod_a", dependencies=["mod_b"]))
    registry.register(SimpleModule("mod_b", dependencies=["mod_a"]))

    with pytest.raises(CircularDependencyError) as exc_info:
        registry.resolve_dependencies()

    assert "Circular dependency" in str(exc_info.value)
    assert "mod_a" in str(exc_info.value)
    assert "mod_b" in str(exc_info.value)


def test_indirect_circular_dependency_raises_error():
    """Indirect cycle A -> B -> C -> A must raise CircularDependencyError."""
    registry = ModuleRegistry()
    registry.register(SimpleModule("mod_a", dependencies=["mod_c"]))
    registry.register(SimpleModule("mod_b", dependencies=["mod_a"]))
    registry.register(SimpleModule("mod_c", dependencies=["mod_b"]))

    with pytest.raises(CircularDependencyError):
        registry.resolve_dependencies()


def test_diamond_dependency_resolves_correctly():
    """Diamond dependencies (D depends on B & C, which depend on A) resolve with A first and D last."""
    registry = ModuleRegistry()
    registry.register(SimpleModule("D", dependencies=["B", "C"]))
    registry.register(SimpleModule("B", dependencies=["A"]))
    registry.register(SimpleModule("C", dependencies=["A"]))
    registry.register(SimpleModule("A", dependencies=[]))

    resolved = registry.resolve_dependencies()
    names = [m.name for m in resolved]

    # A must precede B and C; B and C must precede D
    assert names.index("A") < names.index("B")
    assert names.index("A") < names.index("C")
    assert names.index("B") < names.index("D")
    assert names.index("C") < names.index("D")

