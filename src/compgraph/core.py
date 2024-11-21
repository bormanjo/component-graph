import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Generator
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, TypeVar, runtime_checkable

import networkx as nx
from pydantic import BaseModel

from compgraph.utils import import_object


class GraphException(Exception):
    pass


class LookupError(GraphException):
    """The namespace does not exist in the Lookup instance"""

    def __init__(self, namespace: str) -> None:
        super().__init__(namespace)


class LookupExistsError(GraphException):
    """Attempting to overwrite a value that has already been set"""

    def __init__(self, namespace: str) -> None:
        super().__init__(namespace)


class MissingDependenciesError(GraphException):
    """The expected dependencies were not configured"""

    def __init__(self, *dependencies: str) -> None:
        super().__init__(*dependencies)


class WrongNamespaceError(GraphException):
    """A node was configured under the wrong namespace"""

    def __init__(
        self, node_type: type["AbstractFactory"], invalid_namespace: str
    ) -> None:
        node_identifier = f"{node_type.__module__}.{node_type.__qualname__}"
        node_namespace = node_type.__node_namespace__
        msg = (
            f"Expected `{node_identifier}` to be registered under "
            f"`{node_namespace}`, got `{invalid_namespace}`"
        )
        super().__init__(msg)


class DependenciesNotReadyError(GraphException):
    def __init__(self, node: "AbstractNode") -> None:
        super().__init__(f"Dependencies for `{type(node)}` node are not ready")


class Lookup(dict):
    def __getattr__(self, name: str, set_default=False) -> Any:
        if (result := self.get(name)) is not None:
            return result

        if set_default:
            self[name] = Lookup()
            return self[name]

        raise AttributeError(name)

    def iter_namespaces(self) -> Generator[tuple[str, Any], None, None]:
        def iter_paths(
            lookup: Lookup,
            context: list[str] | None = None,
        ) -> list[tuple[str, Any]]:
            context = context or []
            items = []
            for key, value in lookup.items():
                path = context + [key]
                if isinstance(value, Lookup):
                    for item in iter_paths(value, path):
                        yield item
                else:
                    yield (".".join(path), value)

            return items

        return iter(iter_paths(self))

    def get_namespace(self, namespace: str) -> Any | None:
        """Lookup a namespace and return its value (if any)"""
        path_items = namespace.split(".")

        try:
            current = self
            for item in path_items:
                current = current.__getattr__(item, set_default=False)
        except AttributeError:
            current = None

        return current

    def set_namespace(self, namespace: str, value: Any) -> None:
        """Set a value for a namespace"""
        path_items = namespace.split(".")
        last_item = path_items[-1]
        path_items = path_items[:-1]

        current = self
        for item in path_items:
            current = current.__getattr__(item, set_default=True)

            if not isinstance(current, Lookup):
                raise LookupError(namespace=namespace)

        if last_item in current:
            raise LookupExistsError(namespace=namespace)

        current[last_item] = value


class NodeSetupState(Enum):
    """
    Enumerates the set of possible states a node transitions through during Graph
    setup.
    """
    INITIALIZED = auto()  # Node is initialized from config
    DEPS_RESOLVED = auto()  # Node's dependencies have been inferred
    DEPS_AVAILABLE = auto()  # Node's dependencies are available for use
    STARTING = auto()  # Node's internal setup is running
    READY = auto()  # Node's dependencies are accessible

    def is_ready(self) -> bool:
        return self == self.READY


@runtime_checkable
class DependencyResolver(Protocol):
    """A protocol for resolving a set of required namespace dependencies"""
    def __call__(self, **kwargs: Any) -> set[str]: ...  # pragma: no cover


class AbstractNode(BaseModel, ABC, arbitrary_types_allowed=True):
    __node_dep__: "Graph"
    __node_setup_state__: NodeSetupState = NodeSetupState.INITIALIZED
    __resolved_dependencies__: set[str] = set()
    __dependencies__: set[str | DependencyResolver] = set()

    async def __asetup__(self) -> None:
        self.__node_setup_state__ = NodeSetupState.STARTING
        await self._setup()
        self.__node_setup_state__ = NodeSetupState.READY

    async def __arun__(self) -> None:
        await self._run()

    async def _setup(self) -> None:
        """
        Can optionally be implemented by subclasses to perform setup tasks.

        Dependencies are guaranteed to be available at this point.
        """
        pass

    async def _run(self) -> None:
        """
        Can optionally be implemented by subclasses to perform lengthy runtime tasks.
        """
        pass

    @property
    def dep(self) -> "Graph":
        """
        An accessor to reach other nodes in the graph.

        If the subgraph of dependencies has not been injected, raises a
        `DepednenciesNotReadyError`
        """

        try:
            return self.__node_dep__
        except AttributeError as err:
            raise DependenciesNotReadyError(self) from err

    @property
    def state(self) -> NodeSetupState:
        """The setup state of the node"""
        return self.__node_setup_state__

    @property
    def resolved_dependencies(self) -> set[str]:
        """A set of namespace strings resolved from requirements"""
        return self.__resolved_dependencies__

    @abstractmethod
    def _resolve_dependencies(self, **kwargs) -> set[str]: ...

    def _resolve_and_set_dependencies(self, **kwargs) -> None:
        self.__resolved_dependencies__ = self._resolve_dependencies(**kwargs)
        self.__node_setup_state__ = NodeSetupState.DEPS_RESOLVED

    def _inject_dependencies(self, graph: "Graph") -> None:
        self.__node_dep__ = graph
        self.__node_setup_state__ = NodeSetupState.DEPS_AVAILABLE


class Requires:
    NodeT = TypeVar("NodeT", bound=AbstractNode)

    def __init__(self, *dependencies: str | DependencyResolver) -> None:
        """
        Args:
            dependencies (set[str]): The set of graph namespaces that this object
                depends on.
        """
        self.dependencies = set(dependencies)

    def __call__(self, obj: NodeT) -> NodeT:
        obj.__dependencies__ = obj.__dependencies__ | self.dependencies
        return obj


requires = Requires


@requires("log")
class BaseComponent(AbstractNode):
    """
    A component is a node within a `Graph` that is produced by a factory of the same
    namespace. Each component within that namespace is unique by name.
    """

    @property
    def log(self) -> logging.Logger:
        return self.__node_dep__.log(self.__repr_name__())

    def _resolve_dependencies(self, **kwargs: Any) -> None:
        """
        Determine the set of namespaces depended on by this node and assign to the
        private attribute
        """
        dependencies: set[str] = set()
        for obj in self.__dependencies__:
            # TODO: Can add additional means to resolve dependencies
            if isinstance(obj, str):
                dependencies.add(obj)
            else:  # pragma: no cover
                raise TypeError(f"Expected str, got {type(obj)}")
        return dependencies


ComponentT = TypeVar("ComponentT", bound=BaseComponent)


class AbstractFactory(AbstractNode):
    """
    A factory produces components or python objects.

    While multiple factories can be declared, only one can exist at runtime under the
    given namespace.
    """

    __node_namespace__: ClassVar[str]

    @property
    def namespace(self) -> str:
        return self.__node_namespace__

    def __hash__(self) -> int:
        return hash(self.__node_namespace__)

    def _resolve_dependencies(self, config: dict[str, Any], **kwargs) -> set[str]:
        """
        Determine the set of namespaces depended on by this node and assign to the
        private attribute
        """
        dependencies: set[str] = set()
        for obj in self.__dependencies__:
            if isinstance(obj, str):
                dependencies.add(obj)
            elif isinstance(obj, DependencyResolver):  # pragma: no cover
                dependencies |= obj(config=config)
            else:  # pragma: no cover
                raise TypeError(f"Expected str or DependencyResolver, got {type(obj)}")
        return dependencies

    def _create_component(self, klass: type[ComponentT], **params: Any) -> ComponentT:
        # Create the component instance and determine dependencies
        component = klass.model_validate(params)
        component._resolve_and_set_dependencies()

        # Inject subgraph with dependencies
        subgraph = self.dep.get_subgraph(component.resolved_dependencies)
        component._inject_dependencies(subgraph)

        return component


class NamespaceMixin:
    def __init_subclass__(cls, node_namespace: str | None = None, **kwargs: Any):
        super().__init_subclass__(**kwargs)
        if node_namespace:
            cls.__node_namespace__ = node_namespace


@requires("log")
class BaseFactory(AbstractFactory, NamespaceMixin):
    @property
    def log(self) -> logging.Logger:
        return self.dep.log(self.namespace)


class BaseNoLogFactory(AbstractFactory, NamespaceMixin):
    pass


class Graph(Lookup):
    if TYPE_CHECKING:

        def iter_namespaces(
            self,
        ) -> Generator[tuple[str, AbstractNode], None, None]: ...

    @staticmethod
    def create(config: dict[str, Any]) -> AbstractNode:
        cls_location = config.pop("class")
        cls: type[AbstractNode] = import_object(cls_location)

        obj = cls.model_validate(config)
        obj._resolve_and_set_dependencies(config=config)

        return obj

    @classmethod
    async def from_config(cls, graph_config: dict[str, dict[str, Any]]) -> "Graph":
        graph = cls()

        # Create factory nodes and assign to corresponding namespaces
        for namespace, factory_config in graph_config.items():
            factory_node: AbstractFactory = graph.create(factory_config)
            if namespace != factory_node.namespace:
                raise WrongNamespaceError(
                    node_type=type(factory_node), invalid_namespace=namespace
                )
            graph.set_namespace(namespace, factory_node)

        # For each factory node, inject the dependencies
        for _, node in graph.iter_namespaces():
            subgraph = graph.get_subgraph(node.resolved_dependencies)
            node._inject_dependencies(subgraph)

        # Schedule startup tasks
        async with asyncio.TaskGroup() as tg:
            _ = [
                tg.create_task(node.__asetup__(), name=namespace)
                for namespace, node in graph.iter_namespaces()
            ]

        return graph

    async def run(self) -> None:
        """Run all nodes inside a task group and wait until completion"""
        async with asyncio.TaskGroup() as tg:
            _ = [
                tg.create_task(node.__arun__(), name=namespace)
                for namespace, node in self.iter_namespaces()
            ]

    def get_subgraph(self, nodes: set[str]) -> "Graph":
        subgraph = Graph()
        for namespace in nodes:
            subgraph.set_namespace(namespace, self.get_namespace(namespace))

        missing_nodes = [ns for ns, factory in subgraph.items() if factory is None]
        if any(missing_nodes):
            raise MissingDependenciesError(*missing_nodes)

        return subgraph

    def to_networkx(self) -> nx.DiGraph:
        graph = nx.DiGraph()

        node_map: dict[str, AbstractFactory] = dict(self.iter_namespaces())
        for factory_node in node_map.values():
            graph.add_node(factory_node)
            for dep_namespace in factory_node.resolved_dependencies:
                graph.add_edge(node_map[dep_namespace], factory_node)

        return graph
