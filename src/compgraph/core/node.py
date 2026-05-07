import logging
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, TypeVar

from pydantic import BaseModel

from compgraph.core.dependency import (
    AbstractDependencyMixin,
    ResolvedDependency,
)
from compgraph.core.log import dependency_logger as dep_logger
from compgraph.core.lookup import Lookup


class NodeState(Enum):
    INITIALIZED = auto()  # object initialized, but dependencies not yet known
    DEPENDENCIES_RESOLVED = auto()  # dependencies resolved, but not yet available
    DEPENDENCIES_INJECTED = auto()  # dependencies available for use
    SETUP = auto()  # node setup in progress
    READY = auto()  # node ready for use


NodeT = TypeVar("NodeT", bound="AbstractNode")


class AbstractNode(
    AbstractDependencyMixin,
    ABC,
    BaseModel,
):
    # Private Interface ----------------------------------------------------------------
    __node_dep__: Lookup
    __node_state__: NodeState = NodeState.INITIALIZED
    __node_resolved_dependencies__: set[ResolvedDependency] = set()

    def __init_subclass__(
        cls,
        *args: Any,
        skip_setup: bool = False,
        skip_run: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(*args, **kwargs)

        async def no_op(self) -> None:  # pragma: no cover
            pass

        if skip_setup:
            cls._setup = no_op  # type: ignore

        if skip_run:
            cls._run = no_op  # type: ignore

    def __node_resolve_and_set_dependencies__(self, **kwargs: Any) -> None:
        self.__node_resolved_dependencies__ = self._resolve_dependencies(**kwargs)
        self.__node_state__ = NodeState.DEPENDENCIES_RESOLVED

        msg = f"Resolved dependencies set on node [id: {id(self)}] {self.__repr_name__()}: {self.__node_resolved_dependencies__}"
        dep_logger.info(msg)

    def __node_inject_dependencies__(self, dep: Lookup) -> None:
        self.__node_dep__ = dep
        self.__node_state__ = NodeState.DEPENDENCIES_INJECTED

    async def __node_setup__(self) -> None:
        self.__node_state__ = NodeState.SETUP
        await self._setup()
        self.__node_state__ = NodeState.READY

    async def __node_run__(self) -> None:
        await self._run()

    # Public Interface -----------------------------------------------------------------

    @abstractmethod
    async def _setup(self) -> None: ...

    @abstractmethod
    async def _run(self) -> None: ...

    @property
    def dep(self) -> Lookup:
        return self.__node_dep__

    @property
    def resolved_dependencies(self) -> set[ResolvedDependency]:
        return self.__node_resolved_dependencies__


class LogMixin(AbstractNode):
    @property
    def log(self) -> logging.Logger:
        return self.dep.log(self.__repr_name__())


def create_node_from(cls: type[NodeT], config: Any) -> NodeT:
    """Create a new node instance from the given `cls` and `config`"""
    node = cls.model_validate(config)
    node.__node_resolve_and_set_dependencies__()
    return node


def inject_node_with(node: NodeT, dep: Lookup) -> NodeT:
    """Inject the `node` with its subset of dependencies from `dep`"""
    subset = dep.get_subset(node.resolved_dependencies)
    node.__node_inject_dependencies__(subset)
    return node
