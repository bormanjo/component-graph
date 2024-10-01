import importlib
from typing import Any


def import_object(location: str) -> Any:
    module_path, obj_name = location.rsplit(".", maxsplit=1)

    try:
        module = importlib.import_module(module_path)
        return getattr(module, obj_name)
    except Exception as err:
        raise ImportError(f"Could not locate: `{location}`") from err
