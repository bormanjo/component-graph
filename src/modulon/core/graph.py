import asyncio
from collections.abc import Callable
from graphlib import TopologicalSorter
from typing import Any, Self

from modulon.core.error import FactoryInvalidNamespaceError
from modulon.core.factory import AbstractFactory, AbstractNoLogFactory
from modulon.core.log import core_logger as logger
from modulon.core.lookup import Lookup
from modulon.core.node import create_node_from, inject_node_with
from modulon.utils import import_object


FactoryConfig = dict[str, Any]
GraphConfig = dict[str, FactoryConfig]


class Graph(Lookup[AbstractFactory]):
    def _initialize_and_set_factory(self, namespace: str, config: FactoryConfig):
        cls_location = config.pop("class")
        cls = import_object(cls_location)

        if namespace != "log" and not issubclass(cls, AbstractFactory):
            raise TypeError(f"{cls_location} does not implement AbstractFactory")

        factory = create_node_from(cls, config)
        if factory.namespace != namespace:
            raise FactoryInvalidNamespaceError(cls, namespace)

        self.set_namespace(namespace, factory)

    async def _topological_setup(self) -> None:
        node_mapping = {
            ns: factory.resolved_dependencies for ns, factory in self.iter_namespaces()
        }
        topological_sorter = TopologicalSorter(node_mapping)
        topological_sorter.prepare()

        def get_done_callback(namespace: str) -> Callable[[asyncio.Task], None]:
            def done_callback(task: asyncio.Task) -> None:
                task.result()  # if exception was triggered, raise it
                topological_sorter.done(namespace)

            return done_callback

        while topological_sorter.is_active():
            namespaces = topological_sorter.get_ready()
            logger.info("Namespaces in setup: %s", namespaces)

            async with asyncio.TaskGroup() as tg:
                for namespace in namespaces:
                    factory = self.get_namespace(namespace)
                    assert isinstance(factory, (AbstractFactory, AbstractNoLogFactory))
                    task = tg.create_task(
                        coro=factory.__node_setup__(),
                        name=f"factory-setup({namespace})",
                    )
                    task.add_done_callback(get_done_callback(namespace))

    @classmethod
    async def from_config(cls, config: dict[str, dict[str, Any]]) -> Self:
        graph = cls()

        # Create factory nodes and assign to corresponding namespaces

        for namespace, factory_cfg in config.items():
            try:
                graph._initialize_and_set_factory(namespace, factory_cfg)
            except Exception as err:
                err.add_note(
                    f"Occured while initializing factory @ namespace: {namespace}"
                )
                raise

        # Inject factory with dependencies
        for namespace, factory in graph.iter_namespaces():
            try:
                inject_node_with(factory, graph)
            except Exception as err:  # pragma: no cover
                err.add_note(
                    f"Occured while injecting factory @ namespace: {namespace}"
                )
                raise

        # Setup factory in topological order
        await graph._topological_setup()

        return graph

    async def run(self) -> None:
        """Run all factoryies inside a task group and wait until completion"""
        logger.info("Graph runtime starting")
        async with asyncio.TaskGroup() as tg:
            _ = [
                tg.create_task(
                    factory.__node_run__(), name=f"factory-runtime({namespace})"
                )
                for namespace, factory in self.iter_namespaces()
            ]
        logger.info("Graph runtime complete")
