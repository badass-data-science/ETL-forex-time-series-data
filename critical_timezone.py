import pytz
import datetime

critical_timezone_str = 'America/New_York'
critical_timezone = pytz.timezone(critical_timezone_str)

def is_market_open() -> bool:
    now = datetime.datetime.now(critical_timezone)
    return is_market_open_at_time(now)

def is_market_open_at_time(given_time : datetime.datetime) -> bool:
    weekday = given_time.weekday()
    hour = given_time.hour
    is_open = True

    if (weekday == 4) & (hour >= 17):  # Friday
        is_open = False
    if weekday == 5:  # Saturday
        is_open = False
    if (weekday == 6) & (hour < 17):  # Sunday
        is_open = False

    return is_open

