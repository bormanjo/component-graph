from typing import Any

import pytest

from modulon.core.dependency import (
    AbstractDependencyMixin,
    requires,
    get_unresolved_dependencies,
)
from modulon.core.error import DependencyRegistrationError
from modulon.core.factory import AbstractFactory
from modulon.event.archiver import AbstractEventArchiver, JsonlFileEventArchiver


class ResolvableXYZDependency:
    def __call__(self, **kwargs: Any) -> set[str]:
        return {"xyz"}


def test_get_unresolved_dependencies() -> None:
    class MyClass:
        pass

    assert get_unresolved_dependencies(MyClass) == set()
    assert get_unresolved_dependencies(MyClass()) == set()
    assert get_unresolved_dependencies(1) == set()
    assert get_unresolved_dependencies("abc") == set()


def test_resolve_dependencies():
    obj = AbstractDependencyMixin()
    assert obj._resolve_dependencies() == set()


def test_requires_on_mixin_subclass() -> None:
    @requires("abc")
    class MyClass(AbstractDependencyMixin):
        pass

    assert MyClass.__class_dependencies__ == {"abc"}
    assert not hasattr(MyClass, "__instance_dependencies__")
    assert get_unresolved_dependencies(MyClass) == {"abc"}

    obj = MyClass()
    assert obj.__class_dependencies__ == {"abc"}
    assert obj.__instance_dependencies__ == set()
    assert get_unresolved_dependencies(obj) == {"abc"}
    assert obj._resolve_dependencies() == {"abc"}


def test_requires_resolvable_dependency_on_mixin_subclass() -> None:
    resolves_to_xyz = ResolvableXYZDependency()

    @requires(resolves_to_xyz)
    class MyClass(AbstractDependencyMixin):
        pass

    assert MyClass.__class_dependencies__ == {resolves_to_xyz}
    assert not hasattr(MyClass, "__instance_dependencies__")
    assert get_unresolved_dependencies(MyClass) == {resolves_to_xyz}

    obj = MyClass()
    assert obj.__class_dependencies__ == {resolves_to_xyz}
    assert obj.__instance_dependencies__ == set()
    assert get_unresolved_dependencies(obj) == {resolves_to_xyz}
    assert obj._resolve_dependencies() == {"xyz"}


def test_requires_illegal_dependency() -> None:
    class NotAResolvableDependency:
        pass

    @requires(NotAResolvableDependency())  # type: ignore
    class MyClass(AbstractDependencyMixin):
        pass

    obj = MyClass()
    with pytest.raises(TypeError, match="Expected ResolvableDependency or str"):
        obj._resolve_dependencies()


def test_mixin_child_inherits_class_deps_from_parent() -> None:
    @requires("abc")
    class Parent(AbstractDependencyMixin):
        pass

    @requires("xyz")
    class Child(Parent):
        pass

    assert get_unresolved_dependencies(Parent) == {"abc"}
    assert get_unresolved_dependencies(Child) == {"abc", "xyz"}


def test_requires_on_arbitrary_object() -> None:
    with pytest.raises(DependencyRegistrationError):

        @requires("abc")
        class MyClass1:
            pass

    class MyClass2:
        pass

    with pytest.raises(DependencyRegistrationError):
        requires("abc")(MyClass2)

    obj = requires("abc")(MyClass2())

    assert not hasattr(obj, "__class_dependencies__")
    assert getattr(obj, "__instance_dependencies__") == {"abc"}
    assert get_unresolved_dependencies(obj) == {"abc"}


def test_different_instance_dependencies() -> None:
    class MyClass(AbstractDependencyMixin):
        pass

    obj1 = requires("abc")(MyClass())
    obj2 = requires("xyz")(MyClass())

    assert get_unresolved_dependencies(obj1) == {"abc"}
    assert get_unresolved_dependencies(obj2) == {"xyz"}


@pytest.mark.parametrize(
    argnames=["obj", "expected"],
    argvalues=[
        (AbstractFactory, {"log"}),
        (AbstractEventArchiver, {"event.sender", "log"}),
        (JsonlFileEventArchiver, {"event.sender", "log"}),
    ],
)
def test_factory_get_unresolved_dependencies(obj: Any, expected: set[Any]) -> None:
    unresolved_deps = get_unresolved_dependencies(obj)
    assert unresolved_deps == expected
