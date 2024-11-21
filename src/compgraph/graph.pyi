import logging
from collections.abc import Generator
from typing import Any

import networkx as nx

from compgraph import core
from compgraph.calendar import AbstractCalendarFactory
from compgraph.core import AbstractNode, DependencyResolver, NodeSetupState, requires
from compgraph.date import AbstractDateFactory
from compgraph.event import EventSenderFactory
from compgraph.log import AbstractLogFactory

class Graph:
    calendar: AbstractCalendarFactory
    date: AbstractDateFactory
    event_sender: EventSenderFactory
    log: AbstractLogFactory

    @classmethod
    async def from_config(cls, config: dict[str, dict[str, Any]]) -> "Graph": ...
    async def run(self) -> None: ...
    def get_subgraph(self, nodes: set[str]) -> "Graph": ...
    def to_networkx(self) -> nx.DiGraph: ...
    def iter_namespaces(self) -> Generator[tuple[str, AbstractNode], None, None]: ...
    def get_namespace(self, namespace: str) -> Any | None: ...
    def set_namespace(self, namespace: str, value: Any) -> None: ...

class BaseNoLogFactory(core.BaseNoLogFactory):
    @property
    def dep(self) -> Graph: ...

class BaseFactory(core.BaseFactory):
    @property
    def dep(self) -> Graph: ...
    @property
    def log(self) -> logging.Logger: ...

class BaseComponent(core.BaseComponent):
    @property
    def dep(self) -> Graph: ...
    @property
    def log(self) -> logging.Logger: ...

__all__ = [
    "BaseComponent",
    "BaseFactory",
    "BaseNoLogFactory",
    "Graph",
    "NodeSetupState",
    "requires",
    "DependencyResolver",
]
