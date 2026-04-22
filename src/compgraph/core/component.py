from compgraph.core.dependency import requires
from compgraph.core.node import AbstractNode, LogMixin


@requires("log")
class AbstractComponent(LogMixin, AbstractNode):
    pass
