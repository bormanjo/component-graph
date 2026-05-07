from typing import Any, ClassVar, TypeVar

from modulon.core.component import AbstractComponent
from modulon.core.dependency import requires
from modulon.core.node import (
    AbstractNode,
    LogMixin,
    create_node_from,
    inject_node_with,
)

ComponentT = TypeVar("ComponentT", bound=AbstractComponent)


class NamespaceMixin:
    __node_namespace__: ClassVar[str]

    def __init_subclass__(
        cls,
        *args: Any,
        node_namespace: str | None = None,
        **kwargs: Any,
    ):
        super().__init_subclass__(*args, **kwargs)
        if node_namespace:
            cls.__node_namespace__ = node_namespace

    @property
    def namespace(self) -> str:
        return self.__node_namespace__


class AbstractNoLogFactory(LogMixin, NamespaceMixin, AbstractNode):
    """
    A factory is a top-level node in a graph and is uniquely identified by its namespace.

    All factory interfaces should inherit from this class.
    """

    async def _create_component(
        self,
        klass: type[ComponentT],
        **config: Any,
    ) -> ComponentT:
        component = create_node_from(klass, config)
        inject_node_with(component, self.dep)
        await component.__node_setup__()
        return component


@requires("log")
class AbstractFactory(AbstractNoLogFactory):
    pass
