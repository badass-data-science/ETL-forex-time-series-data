import datetime
from zoneinfo import ZoneInfo

critical_timezone_str = 'America/Toronto'
critical_timezone = ZoneInfo(critical_timezone_str)  # canonical timezone for this pipeline


def is_market_open() -> bool:
    return is_market_open_at_time(datetime.datetime.now(critical_timezone))


def is_market_open_at_time(given_time: datetime.datetime) -> bool:
    weekday = given_time.weekday()
    hour = given_time.hour

    if weekday == 4 and hour >= 17:   # Friday close
        return False
    if weekday == 5:                   # Saturday
        return False
    if weekday == 6 and hour < 17:    # Sunday pre-open
        return False
    return True
