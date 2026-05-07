from typing import TYPE_CHECKING

from compgraph.core.dependency import requires

if TYPE_CHECKING:
    from compgraph.core import component, factory, graph
    from compgraph.date import AbstractDateFactory
    from compgraph.event import EventSubGraph
    from compgraph.log import AbstractLogFactory

    class Graph(graph.Graph):
        # Factories
        date: AbstractDateFactory
        log: AbstractLogFactory

        # SubGraphs
        event: EventSubGraph

    class AbstractComponent(component.AbstractComponent):
        @property
        def dep(self) -> Graph: ...

    class AbstractFactory(factory.AbstractFactory):
        @property
        def dep(self) -> Graph: ...
else:
    from compgraph.core.component import AbstractComponent
    from compgraph.core.factory import AbstractFactory
    from compgraph.core.graph import Graph


__all__ = [
    "AbstractComponent",
    "AbstractFactory",
    "Graph",
    "requires",
]
