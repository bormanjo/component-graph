import datetime
import importlib
from pathlib import Path
from typing import Annotated, Any
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, AfterValidator

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


def get_type_location(obj: Any) -> str:
    cls = type(obj)
    return f"{cls.__module__}.{cls.__qualname__}"


def get_fpath_suffix_validator(suffix: str):
    def validate_fpath_suffix(fpath: Path) -> Path:
        if fpath.suffix != suffix:
            raise ValueError(f"Expected {suffix} in {fpath}")
        return fpath

    return validate_fpath_suffix


JsonlFilePath = Annotated[Path, AfterValidator(get_fpath_suffix_validator(".jsonl"))]
