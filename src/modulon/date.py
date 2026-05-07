import datetime
from abc import abstractmethod
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import field_validator

from modulon.graph import AbstractFactory


class AbstractDateFactory(
    AbstractFactory,
    node_namespace="date",
    skip_setup=True,
    skip_run=True,
):
    @abstractmethod
    def __call__(self) -> datetime.date: ...


class DateFactory(AbstractDateFactory):
    date: datetime.date

    def __call__(self) -> datetime.date:
        return self.date


def zoneinfo_from(v: Any) -> ZoneInfo:
    if isinstance(v, str):
        return ZoneInfo(v)
    if isinstance(v, ZoneInfo):
        return v
    raise TypeError(f"Expected `str` or `ZoneInfo`, got: {type(v)}")


class SystemDateFactory(AbstractDateFactory):
    timezone: ZoneInfo = "America/New_York"  # type: ignore

    _tz_validator = field_validator("timezone", mode="before")(zoneinfo_from)

    def __call__(self) -> datetime.date:
        return datetime.datetime.now(tz=self.timezone).date()
