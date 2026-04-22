from textwrap import dedent
from typing import Any


class GraphException(Exception):
    """The base exception type for errors raised within the graph"""


class FactoryInvalidNamespaceError(GraphException):
    """The factory class was configured under the wrong namespace"""

    def __init__(
        self,
        factory_cls: Any,
        config_namespace: str,
    ) -> None:
        msg = f"""
        The factory ({factory_cls}) expects to be configured @ namespace '{factory_cls.namespace}'
        but got '{config_namespace}' instead
        """
        super().__init__(dedent(msg))


class DependencyRegistrationError(GraphException):
    """Dependencies failed to be assigned to a given object"""


class MissingDependenciesError(GraphException):
    """The expected dependencies do not exist in the graph"""
