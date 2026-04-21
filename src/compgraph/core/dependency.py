import inspect
from abc import ABC
from textwrap import dedent
from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable


@runtime_checkable
class ResolvableDependency(Protocol):
    def __call__(self, **kwargs: Any) -> set[str]: ...


UnresolvedDependency = ResolvableDependency | str
ResolvedDependency = str


def get_unresolved_dependencies(obj: Any) -> set[UnresolvedDependency]:
    bases = obj.__bases__ if inspect.isclass(obj) else type(obj).__bases__
    inherited_deps = {
        dep for base in bases for dep in getattr(base, "__class_dependencies__", set())
    }
    class_deps = getattr(obj, "__class_dependencies__", set())
    inst_deps = getattr(obj, "__instance_dependencies__", set())

    return inherited_deps | class_deps | inst_deps


class AbstractDependencyMixin(ABC):
    __class_dependencies__: ClassVar[set[UnresolvedDependency]]
    __instance_dependencies__: set[UnresolvedDependency]

    @classmethod
    def __init_subclass__(cls):
        cls.__class_dependencies__ = set()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.__instance_dependencies__ = set()
        super().__init__(*args, **kwargs)

    def _resolve_dependencies(self, **kwargs: Any) -> set[ResolvedDependency]:
        resolved_deps: set[ResolvedDependency] = set()
        unresolved_deps = get_unresolved_dependencies(self)
        for obj in unresolved_deps:
            match obj:
                case str(dep):
                    resolved_deps.add(dep)
                case ResolvableDependency():
                    resolved_deps |= obj(**kwargs)
                case _:
                    raise TypeError(
                        f"Expected ResolvableDependency or str, got {type(obj)}"
                    )

        return resolved_deps


class Requires:
    """
    A decorator used to register unresolved dependencies on any object
    """

    T = TypeVar("T")

    def __init__(self, *dependencies: UnresolvedDependency) -> None:
        """
        Args:
            dependencies (set[UnresolvedDependency]): The set of unresolved dependencies
        """
        self.dependencies = set(dependencies)

    def __call__(self, obj: T) -> T:
        """
        Args:
            obj (Any): The object on which the initialzed dependencies are registered.
                If `obj` is a class then `.__class_dependencies__` attribute is set,
                otherwise uses `.__instance_dependencies__`
        """
        if inspect.isclass(obj):
            if not issubclass(obj, AbstractDependencyMixin):
                # TODO: add error type
                msg = f"""
                Dependencies cannot be registered on {obj} because it does not
                subclass `DependencyMixin`. Either a). call `requires()` on an instance
                of {obj} or make {obj} a subclass of `DependencyMixin`.
                """
                raise ValueError(dedent(msg))

            obj.__class_dependencies__ |= self.dependencies
        else:
            if not isinstance(obj, AbstractDependencyMixin) and not hasattr(
                obj, "__instance_dependencies__"
            ):
                setattr(obj, "__instance_dependencies__", set())

            obj.__instance_dependencies__ |= self.dependencies

        return obj


requires = Requires
