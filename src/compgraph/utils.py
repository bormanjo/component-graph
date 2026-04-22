import datetime
import importlib
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime

TZ_EST = ZoneInfo("America/New_York")


def est_now() -> AwareDatetime:
    return datetime.datetime.now(tz=TZ_EST)


def import_object(location: str) -> Any:
    module_path, obj_name = location.rsplit(".", maxsplit=1)

    try:
        module = importlib.import_module(module_path)
        return getattr(module, obj_name)
    except Exception as err:
        raise ImportError(f"Could not locate: `{location}`") from err
