from modulon.core.dependency import requires
from modulon.core.node import AbstractNode, LogMixin


@requires("log")
class AbstractComponent(LogMixin, AbstractNode):
    """
    Components are nodes in the graph and are created by subclasses of `AbstractFactory`.

    Dependencies may be accessed through `.dep`. All components by default have a `.log`
    attribute making available a pre-configured logger for use.
    """

    pass
