import datetime
from abc import abstractmethod
from typing import Any

import pandas as pd
import pandas_market_calendars as mcal

from cg.graph import BaseFactory


class AbstractCalendarFactory(BaseFactory, node_namespace="calendar"):
    @abstractmethod
    def is_business_day(self, date: datetime.date) -> bool: ...

    @abstractmethod
    def add_business_days(self, date: datetime.date, days: int) -> datetime.date: ...

    @abstractmethod
    def get_business_days(
        self, start: datetime.date, end: datetime.date
    ) -> list[datetime.date]: ...


class _BaseCalendarFactory(AbstractCalendarFactory):
    _offset: pd.offsets.DateOffset

    def is_business_day(self, date: datetime.date) -> bool:
        return self.add_business_days(date, 0) == date

    def add_business_days(self, date: datetime.date, days: int) -> datetime.date:
        return (date + (days * self._offset)).date()

    def get_business_days(
        self, start: datetime.date, end: datetime.date
    ) -> list[datetime.date]:
        return (
            pd.bdate_range(
                start=start,
                end=end,
                freq="C",
                holidays=self._offset.kwds.get("holidays"),
                weekmask=self._offset.kwds.get("weekmask"),
            )
            .map(lambda d: d.date())
            .to_list()
        )


class AllDaysCalendarFactory(_BaseCalendarFactory):
    _offset = pd.offsets.CustomBusinessDay(weekmask="Mon Tue Wed Thu Fri Sat Sun")


class AllWeekDaysCalendarFactory(_BaseCalendarFactory):
    _offset = pd.offsets.CustomBusinessDay(weekmask="Mon Tue Wed Thu Fri")


class PandasMarketCalendarFactory(_BaseCalendarFactory):
    name: str
    _market_calendar: mcal.MarketCalendar

    def model_post_init(self, __context: Any) -> None:
        self._market_calendar = mcal.get_calendar(self.name)
        self._offset = self._market_calendar.holidays()

    def is_business_day(self, date: datetime.date) -> bool:
        return date in self.get_business_days(date, date)

    def get_business_days(
        self, start: datetime.date, end: datetime.date
    ) -> list[datetime.date]:
        return (
            self._market_calendar.schedule(start, end)
            .index.map(lambda ts: ts.date())
            .tolist()
        )
