from typing import Any

import pytest

from compgraph.core import (
    DependenciesNotReadyError,
    Lookup,
    LookupError,
    LookupExistsError,
    MissingDependenciesError,
    WrongNamespaceError,
)
from compgraph.graph import (
    BaseFactory,
    BaseNoLogFactory,
    Graph,
    requires,
)


def test_lookup() -> None:
    lookup = Lookup()
    lookup.set_namespace("a.nested", "value")
    lookup.set_namespace("some.other.nested", [1, 2, 3])

    assert lookup.a.nested == "value"
    assert lookup.some.other.nested == [1, 2, 3]

    with pytest.raises(LookupError):
        lookup.set_namespace("a.nested.value", "cant be set here")

    with pytest.raises(LookupExistsError):
        lookup.set_namespace("a.nested", "value was already set")

    with pytest.raises(AttributeError):
        lookup.b

    assert lookup.get_namespace("b") is None

    expected = [("a.nested", "value"), ("some.other.nested", [1, 2, 3])]
    assert list(lookup.iter_namespaces()) == expected


@pytest.mark.asyncio
async def test_graph_missing_dependencies(log_config: dict[str, Any]) -> None:
    graph = await Graph.from_config(log_config)

    with pytest.raises(MissingDependenciesError):
        graph.get_subgraph({"date"})


@pytest.mark.asyncio
async def test_factory_under_wrong_namespace() -> None:
    config = {
        "time": {"class": "compgraph.date.DateFactory", "date": "2023-01-01"},
    }
    with pytest.raises(WrongNamespaceError):
        await Graph.from_config(config)


@pytest.mark.parametrize(
    argnames=["base_cls", "expected_deps"],
    argvalues=[(BaseFactory, {"abc", "log"}), (BaseNoLogFactory, {"abc"})],
)
def test_requires(base_cls: type[BaseFactory], expected_deps: set[str]) -> None:
    @requires("abc")
    class DummyFactory(base_cls, node_namespace="test.dummy"):
        pass

    assert DummyFactory.__dependencies__ == expected_deps


def test_hashable_factory() -> None:
    class DummyFactory(BaseFactory, node_namespace="test.dummy"):
        pass

    assert isinstance(hash(DummyFactory()), int)


@pytest.mark.asyncio
async def test_run_graph(log_config: dict[str, Any]) -> None:
    graph = await Graph.from_config(log_config)
    await graph.run()


@pytest.mark.asyncio
async def test_graph_to_networkx(log_config: dict[str, Any]) -> None:
    config = log_config | {
        "date": {
            "class": "compgraph.date.DateFactory",
            "date": "2023-01-01",
        },
    }

    graph = await Graph.from_config(config)
    nx_graph = graph.to_networkx()
    assert len(nx_graph.nodes) == 2


class DummyFactoryA(BaseFactory, node_namespace="dummy.a"):
    pass


class DummyFactoryB(BaseFactory, node_namespace="dummy.b"):
    pass


@pytest.mark.asyncio
async def test_nested_subgraph(log_config: dict[str, Any]) -> None:
    config = log_config | {
        "dummy.a": {
            "class": "tests.test_core.DummyFactoryA",
        },
        "dummy.b": {
            "class": "tests.test_core.DummyFactoryB",
        },
    }

    graph = await Graph.from_config(config)
    subgraph = graph.get_subgraph(["dummy.a", "dummy.b"])

    assert isinstance(subgraph.dummy.a, DummyFactoryA)
    assert isinstance(subgraph.dummy.b, DummyFactoryB)


def test_dependencies_not_ready_error() -> None:
    class DummyFactory(BaseNoLogFactory, node_namespace="dummy"):
        def model_post_init(self, __context: Any) -> None:
            self.dep

    with pytest.raises(DependenciesNotReadyError):
        DummyFactory()
