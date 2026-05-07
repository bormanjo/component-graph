import pytest

from modulon.core.lookup import Lookup
from modulon.core.node import AbstractNode, create_node_from, inject_node_with


@pytest.mark.asyncio
async def test_node() -> None:
    class MyNode(AbstractNode, skip_setup=True, skip_run=True):
        pass

    node = create_node_from(MyNode, config={})  # type: ignore
    assert not hasattr(node, "__node_dep__")
    assert node.resolved_dependencies == set()

    node = inject_node_with(node, Lookup())
    assert hasattr(node, "__node_dep__")
    assert isinstance(node.dep, Lookup)
