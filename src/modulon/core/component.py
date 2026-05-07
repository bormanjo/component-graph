from modulon.core.dependency import requires
from modulon.core.node import AbstractNode, LogMixin


@requires("log")
class AbstractComponent(LogMixin, AbstractNode):
    pass
