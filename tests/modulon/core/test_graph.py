import logging

import pytest

from modulon.core.error import FactoryInvalidNamespaceError
from modulon.graph import Graph


@pytest.mark.asyncio
async def test_log_graph() -> None:
    config = {"log": {"class": "modulon.log.LogFactory"}}

    graph = await Graph.from_config(config)

    logger = graph.log("my-logger")
    assert isinstance(logger, logging.Logger)

    await graph.run()


@pytest.mark.asyncio
async def test_graph_invalid_factory_namespace() -> None:
    config = {"event": {"class": "modulon.event.sender.EventSenderFactory"}}

    with pytest.raises(FactoryInvalidNamespaceError):
        await Graph.from_config(config)


@pytest.mark.asyncio
async def test_graph_invalid_factory_class() -> None:
    config = {"abc": {"class": "asyncio.TaskGroup"}}

    with pytest.raises(TypeError, match="asyncio.TaskGroup does not implement*"):
        await Graph.from_config(config)
