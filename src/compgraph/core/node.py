import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Generator
from enum import Enum, auto
from typing import Any, ClassVar, Protocol, TypeVar

from pydantic import BaseModel

from compgraph.core.dependency import AbstractDependencyMixin, ResolvedDependency
from compgraph.core.log import dependency_logger as dep_logger
from compgraph.core.lookup import Lookup


class NodeState(Enum):
    INITIALIZED = auto()
    DEPENDENCIES_RESOLVED = auto()
    DEPENDENCIES_INJECTED = auto()
    SETUP = auto()
    READY = auto()

    def is_ready(self) -> bool:
        return self == NodeState.READY


class AbstractNode(
    BaseModel,
    ABC,
    AbstractDependencyMixin,
    arbitrary_types_allowed=True,
):
    __node_dep__: Lookup
    __node_state__: NodeState = NodeState.INITIALIZED
    __node_resolved_dependencies__: set[ResolvedDependency] = set()

    async def __node_resolve_and_set_dependencies(self, **kwargs) -> None:
        self.__node_resolved_dependencies__ = self._resolve_dependencies(**kwargs)
        self.__node_state__ = NodeState.DEPENDENCIES_RESOLVED

        msg = f"Resolved dependencies set on node [id: {id(self)}] {id.__name__}: {self.__node_resolved_dependencies__}"
        dep_logger.info(msg)

    async def __node_inject_dependencies(self, dep: Lookup) -> None:
        self.__node_dep__ = dep
        self.__node_state__ = NodeState.DEPENDENCIES_INJECTED

    async def __node_setup(self) -> None:
        self.__node_state__ = NodeState.SETUP
        await self._setup()
        self.__node_state__ = NodeState.READY

    async def __node_run(self) -> None:
        await self._run()

    @abstractmethod
    async def _setup(self) -> None: ...

    @abstractmethod
    async def _run(self) -> None: ...

    @property
    def dep(self) -> Lookup:
        try:
            return self.__node_dep__
        except AttributeError as err:
            # TODO: replace with DependenciesNotReadyError
            raise ValueError("Dependencies not yet ready") from err

    @property
    def resolved_dependencies(self) -> set[ResolvedDependency]:
        return self.__node_resolved_dependencies__
