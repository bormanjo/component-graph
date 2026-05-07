import pytest

from modulon.core.component import AbstractComponent
from modulon.core.factory import AbstractFactory
from modulon.core.lookup import Lookup
from modulon.core.node import create_node_from, inject_node_with


@pytest.mark.asyncio
async def test_factory() -> None:
    class MyFactory(
        AbstractFactory,
        node_namespace="abc",
        skip_setup=True,
        skip_run=True,
    ):
        pass

    class MyComponent(AbstractComponent, skip_setup=True, skip_run=True):
        pass

    factory = create_node_from(MyFactory, config={})  # type: ignore
    factory = inject_node_with(factory, Lookup({"log": 1}))
    assert factory.namespace == "abc"

    component = await factory._create_component(MyComponent)  # type: ignore
    assert isinstance(component, MyComponent)
