from typing import TYPE_CHECKING

from modulon.core.dependency import requires

if TYPE_CHECKING:
    from modulon.core import component, factory, graph
    from modulon.date import AbstractDateFactory
    from modulon.event import EventSubGraph
    from modulon.log import AbstractLogFactory
    from modulon.system_check import SystemCheckFactory

    class Graph(graph.Graph):
        # Factories
        date: AbstractDateFactory
        log: AbstractLogFactory
        system_check: SystemCheckFactory

        # SubGraphs
        event: EventSubGraph

    class AbstractComponent(component.AbstractComponent):
        @property
        def dep(self) -> Graph: ...

    class AbstractFactory(factory.AbstractFactory):
        @property
        def dep(self) -> Graph: ...
else:
    from modulon.core.component import AbstractComponent
    from modulon.core.factory import AbstractFactory
    from modulon.core.graph import Graph


__all__ = [
    "AbstractComponent",
    "AbstractFactory",
    "Graph",
    "requires",
]
