import logging
import logging.config
from abc import abstractmethod
from typing import Any

from pydantic import BaseModel

from compgraph.core.factory import AbstractFactory
from compgraph.core.log import graph_logger


class AbstractLogFactory(AbstractFactory, node_namespace="log"):
    @abstractmethod
    def __call__(self, name: str) -> logging.Logger: ...

    async def _setup(self) -> None:
        pass

    async def _run(self) -> None:
        pass


class LogFactory(AbstractLogFactory):
    """
    A log factory that uses the logging modules `dictConfig`.

    See: https://docs.python.org/3/library/logging.config.html#dictionary-schema-details
    """

    class LoggingConfig(BaseModel):
        version: int = 1
        formatters: dict[str, Any] = {
            "default": {"format": "%(levelname)-8s - %(message)s"}
        }
        filters: dict[str, Any] = {}
        handlers: dict[str, Any] = {
            "stdout": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "stream": "ext://sys.stdout",
            },
        }
        loggers: dict[str, Any] = {
            "compgraph": {"handlers": ["stdout"], "level": "DEBUG"}
        }
        root: dict[str, Any] = {}
        incremental: bool = False
        disable_existing_loggers: bool = True

    config: LoggingConfig = LoggingConfig()

    def model_post_init(self, __context: Any) -> None:
        logging.config.dictConfig(self.config.model_dump())

    def __call__(self, name: str) -> logging.Logger:
        return graph_logger.getChild(name)
