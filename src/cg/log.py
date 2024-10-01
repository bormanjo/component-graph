import logging
from abc import abstractmethod
from typing import Any

from pydantic import PrivateAttr

from cg.graph import BaseNoLogFactory


class AbstractLogFactory(BaseNoLogFactory, node_namespace="log"):
    @abstractmethod
    def __call__(self, name: str) -> logging.Logger: ...


class BasicConfigLogFactory(AbstractLogFactory):
    level: int | str = logging.INFO
    name: str = "cg"
    _base_logger: logging.Logger = PrivateAttr()

    def model_post_init(self, __context: Any) -> None:
        logging.basicConfig()
        self._base_logger = logging.Logger(self.name)
        self._base_logger.setLevel(self.level)

    def __call__(self, name: str) -> logging.Logger:
        child_logger = self._base_logger.getChild(name)
        child_logger.setLevel(self.level)
        return child_logger
