from typing import Any

import pytest

from compgraph.core.lookup import SetNamespaceError, Lookup, is_valid_namespace


@pytest.mark.parametrize(
    argnames="namespace",
    argvalues=[
        "abc",
        "abc.def",
        "abc.def.ghi",
        "a_b_c",
        "a_b_c.def",
        "abc.d_e_f",
    ],
)
def test_valid_namespace_strings(namespace: str) -> None:
    assert is_valid_namespace(namespace)


@pytest.mark.parametrize(
    argnames="namespace",
    argvalues=[
        "Abc",
        "abc.Def",
        "123",
        "abc123",
        "abc.123",
        "abc.def123",
    ]
)
def test_invalid_namespace_strings(namespace: str) -> None:
    assert not is_valid_namespace(namespace)


def test_dict_operations() -> None:
    lookup = Lookup({"abc": 1, "xyz": 2})
    assert lookup.abc == 1 == lookup["abc"]
    assert lookup.xyz == 2 == lookup["xyz"]


def test_namespace_operations() -> None:
    lookup = Lookup()
    lookup.set_namespace("abc", 1)

    assert lookup.get_namespace("abc") == 1
    assert lookup.get_namespace("xyz") is None

    with pytest.raises(AttributeError):
        lookup.xyz

    assert list(lookup.iter_namespaces()) == [("abc", 1)]


def test_set_namespace_cannot_overwrite_existing_node() -> None:
    lookup = Lookup()
    lookup.set_namespace("abc", 1)

    msg = "Cannot overwrite existing namespace: `abc`"
    with pytest.raises(SetNamespaceError, match=msg):
        lookup.set_namespace("abc.def", 2)


def test_subgraph_operations() -> None:
    lookup = Lookup()
    lookup.set_namespace("abc.ijk", 1)
    lookup.set_namespace("abc.xyz", 2)

    assert lookup.abc.ijk == lookup["abc"]["ijk"] == lookup.get_namespace("abc.ijk")
    assert lookup.abc.xyz == lookup["abc"]["xyz"] == lookup.get_namespace("abc.xyz")

    with pytest.raises(SetNamespaceError):
        lookup.set_namespace("abc", 3)

    assert list(lookup.iter_namespaces()) == [("abc.ijk", 1), ("abc.xyz", 2)]
