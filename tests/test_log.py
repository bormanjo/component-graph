import logging

import pytest

from cg.graph import Graph


def assert_sends_logs(caplog: pytest.LogCaptureFixture, graph: Graph) -> None:
    caplog.clear()
    caplog.set_level(logging.DEBUG)

    test_logger = graph.log("test")
    test_logger.debug("debug")
    test_logger.info("info")
    test_logger.warning("warning")
    test_logger.error("error")
    test_logger.critical("critical")

    expected = [
        (logging.DEBUG, "debug"),
        (logging.INFO, "info"),
        (logging.WARNING, "warning"),
        (logging.ERROR, "error"),
        (logging.CRITICAL, "critical"),
    ]
    records = caplog.get_records("call")

    assert len(records) == len(expected)
    for record, (level, message) in zip(records, expected):
        assert record.levelno == level
        assert record.message == message


@pytest.mark.asyncio
async def test_basic_config_log_factory(caplog: pytest.LogCaptureFixture) -> None:
    config = {
        "log": {
            "class": "cg.log.BasicConfigLogFactory",
            "level": "DEBUG",
        },
    }

    graph = await Graph.from_config(config)
    logger = graph.log("test")
    assert logger.level == logging.DEBUG

    assert_sends_logs(caplog, graph)
