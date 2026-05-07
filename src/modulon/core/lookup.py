import re
from collections.abc import Generator
from typing import Any, TypeVar

from modulon.core.error import MissingDependenciesError

K = TypeVar("K")
V = TypeVar("V")

VALID_NAMESPACE_PATTERN = re.compile(r"^([a-z_]\.?)+$")


def is_valid_namespace(s: str) -> bool:
    return VALID_NAMESPACE_PATTERN.match(s) is not None


class BaseLookupException(Exception):
    pass


class SetNamespaceError(BaseLookupException):
    """An error occured while trying to set a namespace"""


class Lookup(dict[str, V]):
    """
    An extension of the builtin `dict` with a few extra properties:

    - Lookup keys must be valid namespaces containing only lowercase alphabetic
      characters, `_` and `.`
    """

    def __init__(self, dct: dict[str, V] | None = None):
        super().__init__()

        if dct is not None:
            for ns, v in dct.items():
                self.set_namespace(ns, v)

    def __getattr__(self, name: str, set_nested: bool = False) -> V:
        if (result := self.get(name)) is not None:
            return result

        if set_nested:
            self[name] = Lookup()  # type: ignore
            return self[name]

        raise AttributeError(name)

    def iter_namespaces(self) -> Generator[tuple[str, V], None, None]:
        """Iterate over all registered namespaces"""

        def iter_paths(
            lookup: Lookup,
            *,
            context: list[str] | None = None,
        ) -> Generator[tuple[str, Any]]:
            context = context or []
            for key, value in lookup.items():
                path = context + [key]
                if isinstance(value, Lookup):
                    for item in iter_paths(value, context=path):
                        yield item
                else:
                    yield (".".join(path), value)

        return iter(iter_paths(self))

    def get_namespace(self, namespace: str) -> "Lookup[V]" | V | None:
        """Lookup a namespace and return its value (if any)"""
        path_items = namespace.split(".")

        current: Lookup[V] | V | None = self
        try:
            for item in path_items:
                current = current.__getattr__(item, set_nested=False)  # type: ignore
        except AttributeError:
            current = None

        return current

    def set_namespace(self, namespace: str, value: V) -> None:
        """Set a value for a namespace"""
        *path_items, name = namespace.split(".")

        current = self
        for i, item in enumerate(path_items, start=1):
            current = current.__getattr__(item, set_nested=True)  # type: ignore

            if not isinstance(current, Lookup):
                current_ns = ".".join(path_items[:i])
                msg = f"Cannot overwrite existing namespace: `{current_ns}`"
                raise SetNamespaceError(msg)

        if name in current:
            msg = f"Cannot overwrite existing namespace: `{namespace}`"
            raise SetNamespaceError(msg)

        current[name] = value

    def get_subset(self, nodes: set[str]) -> "Lookup[V]":
        subset = Lookup[V]()
        missing: set[str] = set()
        for namespace in nodes:
            node = self.get_namespace(namespace)
            if node is None:
                missing.add(namespace)
                continue

            subset.set_namespace(namespace, node)  # type: ignore

        if any(missing):
            raise MissingDependenciesError(*missing)

        return subset
