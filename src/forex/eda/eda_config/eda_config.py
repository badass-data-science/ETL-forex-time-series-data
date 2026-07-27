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
    # XAU/USD (gold, a commodity CFD, not a currency pair) and the crosses below
    # added 2026-07-14 to explore whether less USD-major-crowded markets carry
    # more signal -- see forex-ML's README. Mirrors
    # forex.flows.candlestick_flow.TRACKED_INSTRUMENTS.
    'XAU/USD',
    'GBP/JPY',
    'EUR/JPY',
    'AUD/JPY',
    'EUR/GBP',
    'AUD/NZD',
    'EUR/CHF',  # added 2026-07-14, same reason as above
]

granularity_list = ['H1', 'H4', 'M15']

granularity_to_seconds_map = {
    'M15' : 60 * 15,
    'H1' : seconds_in_one_hour,
    'H4' : 4 * seconds_in_one_hour,
    'D' : seconds_in_one_day,
    'W' : seconds_in_one_week,
}
