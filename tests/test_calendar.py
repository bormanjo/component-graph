import datetime
from typing import Any

import pytest
from pydantic import BaseModel

import compgraph as cg


class IsBusinessDayScenario(BaseModel):
    factory: str
    params: dict[str, Any] = {}
    date: datetime.date
    expected: bool

    @property
    def test_id(self) -> str:
        return f"{self.factory}_{self.date.isoformat()}"


IS_BUSINESS_DAY_SCENARIOS = [
    IsBusinessDayScenario(
        factory="AllDaysCalendarFactory", date="2023-01-01", expected=True
    ),
    IsBusinessDayScenario(
        factory="AllDaysCalendarFactory", date="2023-01-02", expected=True
    ),
    IsBusinessDayScenario(
        factory="AllDaysCalendarFactory", date="2023-01-03", expected=True
    ),
    IsBusinessDayScenario(
        factory="AllWeekDaysCalendarFactory", date="2023-01-01", expected=False
    ),
    IsBusinessDayScenario(
        factory="AllWeekDaysCalendarFactory", date="2023-01-02", expected=True
    ),
    IsBusinessDayScenario(
        factory="AllWeekDaysCalendarFactory", date="2023-01-03", expected=True
    ),
    IsBusinessDayScenario(
        factory="PandasMarketCalendarFactory",
        params={"name": "NYSE"},
        date="2023-01-01",
        expected=False,
    ),
    IsBusinessDayScenario(
        factory="PandasMarketCalendarFactory",
        params={"name": "NYSE"},
        date="2023-01-02",
        expected=False,
    ),
    IsBusinessDayScenario(
        factory="PandasMarketCalendarFactory",
        params={"name": "NYSE"},
        date="2023-01-03",
        expected=True,
    ),
    IsBusinessDayScenario(
        factory="PandasMarketCalendarFactory",
        params={"name": "NYSE"},
        date="2023-01-06",
        expected=True,
    ),
    IsBusinessDayScenario(
        factory="PandasMarketCalendarFactory",
        params={"name": "NYSE"},
        date="2023-01-07",
        expected=False,
    ),
    IsBusinessDayScenario(
        factory="PandasMarketCalendarFactory",
        params={"name": "NYSE"},
        date="2023-01-08",
        expected=False,
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    argnames="scenario",
    argvalues=IS_BUSINESS_DAY_SCENARIOS,
    ids=[scenario.test_id for scenario in IS_BUSINESS_DAY_SCENARIOS],
)
async def test_calendar_factory_is_business_day(
    log_config: dict[str, Any],
    scenario: IsBusinessDayScenario,
) -> None:
    config = log_config | {
        "calendar": {
            "class": f"compgraph.calendar.{scenario.factory}",
            **scenario.params,
        },
    }
    graph = await cg.Graph.from_config(config)
    assert graph.calendar.is_business_day(scenario.date) is scenario.expected


class AddBusinessDayScenario(BaseModel):
    factory: str
    params: dict[str, Any] = {}
    date: datetime.date
    num_days: int
    expected: datetime.date

    @property
    def test_id(self) -> str:
        return f"{self.factory}_{self.date.isoformat()}_+{self.num_days}"


ADD_BUSINESS_DAY_SCENARIOS = [
    AddBusinessDayScenario(
        factory="AllDaysCalendarFactory",
        date="2023-01-01",
        num_days=0,
        expected="2023-01-01",
    ),
    AddBusinessDayScenario(
        factory="AllDaysCalendarFactory",
        date="2023-01-01",
        num_days=1,
        expected="2023-01-02",
    ),
    AddBusinessDayScenario(
        factory="AllDaysCalendarFactory",
        date="2023-01-01",
        num_days=2,
        expected="2023-01-03",
    ),
    AddBusinessDayScenario(
        factory="AllDaysCalendarFactory",
        date="2023-01-01",
        num_days=3,
        expected="2023-01-04",
    ),
    AddBusinessDayScenario(
        factory="AllWeekDaysCalendarFactory",
        date="2023-01-01",
        num_days=0,
        expected="2023-01-02",
    ),
    AddBusinessDayScenario(
        factory="AllWeekDaysCalendarFactory",
        date="2023-01-01",
        num_days=1,
        expected="2023-01-02",
    ),
    AddBusinessDayScenario(
        factory="AllWeekDaysCalendarFactory",
        date="2023-01-01",
        num_days=2,
        expected="2023-01-03",
    ),
    AddBusinessDayScenario(
        factory="AllWeekDaysCalendarFactory",
        date="2023-01-01",
        num_days=3,
        expected="2023-01-04",
    ),
    AddBusinessDayScenario(
        factory="PandasMarketCalendarFactory",
        params={"name": "NYSE"},
        date="2023-01-01",
        num_days=0,
        expected="2023-01-03",
    ),
    AddBusinessDayScenario(
        factory="PandasMarketCalendarFactory",
        params={"name": "NYSE"},
        date="2023-01-01",
        num_days=1,
        expected="2023-01-03",
    ),
    AddBusinessDayScenario(
        factory="PandasMarketCalendarFactory",
        params={"name": "NYSE"},
        date="2023-01-01",
        num_days=2,
        expected="2023-01-04",
    ),
    AddBusinessDayScenario(
        factory="PandasMarketCalendarFactory",
        params={"name": "NYSE"},
        date="2023-01-01",
        num_days=3,
        expected="2023-01-05",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    argnames="scenario",
    argvalues=ADD_BUSINESS_DAY_SCENARIOS,
    ids=[scenario.test_id for scenario in ADD_BUSINESS_DAY_SCENARIOS],
)
async def test_calendar_factory_add_business_day(
    log_config: dict[str, Any],
    scenario: AddBusinessDayScenario,
) -> None:
    config = log_config | {
        "calendar": {
            "class": f"compgraph.calendar.{scenario.factory}",
            **scenario.params,
        },
    }
    graph = await cg.Graph.from_config(config)
    actual = graph.calendar.add_business_days(scenario.date, scenario.num_days)
    assert actual == scenario.expected


class GetBusinessDaysScenario(BaseModel):
    factory: str
    start: datetime.date = datetime.date(2023, 1, 1)
    end: datetime.date = datetime.date(2023, 1, 8)
    expected: list[datetime.date]
    params: dict[str, Any] = {}

    @property
    def test_id(self) -> str:
        return f"{self.factory}_{self.start.isoformat()}_{self.end.isoformat()}"


GET_BUSINESS_DAYS_SCENARIOS = [
    GetBusinessDaysScenario(
        factory="AllDaysCalendarFactory",
        expected=[datetime.date(2023, 1, i) for i in range(1, 9)],
    ),
    GetBusinessDaysScenario(
        factory="AllWeekDaysCalendarFactory",
        expected=[datetime.date(2023, 1, i) for i in range(2, 7)],
    ),
    GetBusinessDaysScenario(
        factory="PandasMarketCalendarFactory",
        params={"name": "NYSE"},
        expected=[datetime.date(2023, 1, i) for i in range(3, 7)],
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    argnames="scenario",
    argvalues=GET_BUSINESS_DAYS_SCENARIOS,
    ids=[scenario.test_id for scenario in GET_BUSINESS_DAYS_SCENARIOS],
)
async def test_calendar_factory_get_business_days(
    log_config: dict[str, Any],
    scenario: GetBusinessDaysScenario,
) -> None:
    config = log_config | {
        "calendar": {
            "class": f"compgraph.calendar.{scenario.factory}",
            **scenario.params,
        },
    }
    graph = await cg.Graph.from_config(config)
    calendar = graph.calendar
    business_days = calendar.get_business_days(scenario.start, scenario.end)
    for biz_day in business_days:
        assert calendar.is_business_day(biz_day), biz_day.isoformat()
    assert business_days == scenario.expected
