import datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pytest

from cg.date import zoneinfo_from
from cg.graph import Graph


@pytest.mark.asyncio
@pytest.mark.parametrize(
    argnames=["factory", "params", "expected"],
    argvalues=[
        ("cg.date.DateFactory", {"date": "2023-01-01"}, datetime.date(2023, 1, 1)),
        (
            "cg.date.SystemDateFactory",
            {"timezone": "UTC"},
            datetime.datetime.utcnow().date(),
        ),
    ],
)
async def test_date_factory(
    log_config: dict[str, Any],
    factory: str,
    params: dict[str, Any],
    expected: datetime.date,
) -> None:
    config = {
        "date": {
            "class": factory,
            **params,
        },
    }

    graph = await Graph.from_config(config | log_config)

    assert graph.date.state.is_ready()
    assert graph.date() == expected


@pytest.mark.parametrize(
    argnames=["value", "expected"],
    argvalues=[
        ("UTC", ZoneInfo("UTC")),
        (ZoneInfo("UTC"), ZoneInfo("UTC")),
        ("America/New_York", ZoneInfo("America/New_York")),
        ("NYC", ZoneInfoNotFoundError()),
        (None, TypeError()),
    ],
)
def test_zoneinfo_from(value: Any, expected: Any) -> None:
    if isinstance(expected, Exception):
        with pytest.raises(type(expected)):
            zoneinfo_from(value)
    else:
        assert zoneinfo_from(value) == expected
