import datetime

import pytest
from zoneinfo import ZoneInfo

from forex.critical_timezone import critical_timezone, is_market_open_at_time

TZ = ZoneInfo('America/Toronto')
# 2024-01-01 was a Monday — use it as the anchor for weekday arithmetic
_MONDAY = datetime.datetime(2024, 1, 1, tzinfo=TZ)


def _at(weekday_offset: int, hour: int) -> datetime.datetime:
    return _MONDAY + datetime.timedelta(days=weekday_offset, hours=hour)


class TestMarketHours:
    def test_monday_noon_is_open(self):
        assert is_market_open_at_time(_at(0, 12))

    def test_thursday_afternoon_is_open(self):
        assert is_market_open_at_time(_at(3, 16))

    def test_friday_morning_is_open(self):
        assert is_market_open_at_time(_at(4, 9))

    def test_friday_16_is_open(self):
        assert is_market_open_at_time(_at(4, 16))

    def test_friday_17_is_closed(self):
        assert not is_market_open_at_time(_at(4, 17))

    def test_friday_18_is_closed(self):
        assert not is_market_open_at_time(_at(4, 18))

    def test_saturday_is_closed(self):
        assert not is_market_open_at_time(_at(5, 12))

    def test_sunday_08_is_closed(self):
        assert not is_market_open_at_time(_at(6, 8))

    def test_sunday_16_is_closed(self):
        assert not is_market_open_at_time(_at(6, 16))

    def test_sunday_17_is_open(self):
        assert is_market_open_at_time(_at(6, 17))

    def test_sunday_20_is_open(self):
        assert is_market_open_at_time(_at(6, 20))


class TestTimezoneObject:
    def test_critical_timezone_is_zoneinfo(self):
        assert isinstance(critical_timezone, ZoneInfo)

    def test_critical_timezone_is_toronto(self):
        assert str(critical_timezone) == 'America/Toronto'

    def test_can_use_as_tzinfo(self):
        dt = datetime.datetime(2024, 6, 15, 12, 0, tzinfo=critical_timezone)
        assert dt.tzname() in ('EDT', 'EST')
