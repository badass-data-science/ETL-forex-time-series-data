import datetime
from forex.critical_timezone import critical_timezone

from python_tools_and_shortcuts.time_series_essentials.time_conversions import seconds_in_one_hour
from python_tools_and_shortcuts.time_series_essentials.time_conversions import seconds_in_one_day
from python_tools_and_shortcuts.time_series_essentials.time_conversions import seconds_in_one_week

cutoff_timestamp = datetime.datetime(2015, 1, 1, 0, 0, 0, tzinfo = critical_timezone).timestamp()

instrument_list = [
    'AUD/USD',
    'EUR/USD',
    'GBP/USD',
    'NZD/USD',
    'USD/CAD',
    'USD/CHF',
    'USD/JPY',
]

granularity_list = ['H1', 'M15']

granularity_to_seconds_map = {
    'M15' : 60 * 15,
    'H1' : seconds_in_one_hour,
    'D' : seconds_in_one_day,
    'W' : seconds_in_one_week,
}
