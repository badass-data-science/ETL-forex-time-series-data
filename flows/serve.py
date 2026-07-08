"""
Start all scheduled forex deployments.

Usage:
    OANDA_CONFIG_FILE=/path/to/oanda_config.json python -m forex.flows.serve

Deployments
-----------
candlestick-D      daily at 00:05 UTC        (D candles, all major pairs)
candlestick-H1     5 min past each hour UTC  (H1 candles, all major pairs)
candlestick-M15    every 15 min UTC          (M15 candles, all major pairs)
forward-fill-D     daily at 00:15 UTC        (10 min after candlestick-D)
forward-fill-H1    15 min past each hour UTC (10 min after candlestick-H1)
forward-fill-M15   every 15 min UTC          (10 min after candlestick-M15)
swap-rate-D        daily at 20:45 UTC        (all major pairs)
economic-calendar-D daily at 00:30 UTC       (rolling 14-day-ahead window, Finnhub)
positioning        every 20 min UTC          (order-book + position-book, all major pairs)

Each forward-fill deployment is offset 10 minutes after its candlestick
counterpart so it always runs against freshly-landed candles rather than
racing the fetch that feeds it. Previously forward-fill only ran when
triggered manually (a direct function call, or the one-off
scripts/recompute_forward_fill_history.py) -- nothing scheduled it, so
forward-filled data never got produced on an ongoing basis.

swap-rate-D runs ~15 minutes before the 5pm New York rollover cutoff (a
fixed UTC time, not DST-aware -- the same simplification the downstream
forex-ML session-window features already make) so a fresh rate is on hand
right as any position held past the cutoff would actually be charged one.

economic-calendar-D pulls a rolling 14-day-ahead window from Finnhub (not
OANDA -- a separate provider/credential, see etl/config/finnhub_config.py)
each run, rather than a single fixed date: the whole point is having known-
in-advance event times on hand before they happen, and re-pulling the same
window daily is cheap and naturally idempotent (it also picks up newly-
published `actual` values for events that already occurred).

positioning pulls OANDA's order-book/position-book snapshots (back to
OANDA's own API/token, same as candlesticks) every 20 minutes -- matching
how often OANDA itself has historically refreshed these snapshots, so
polling faster wouldn't surface anything new.

The market-hours gate inside candlestick_flow/forward_fill_flow skips runs
that land outside trading hours (Fri 17:00 ET → Sun 17:00 ET); swap_rate_flow,
economic_calendar_flow, and positioning_flow have no such gate -- none of them
are tied to candle formation, and all three data sources remain available
outside forex trading hours (OANDA just won't have refreshed recently).
"""

import os
import sys

from prefect import serve

from forex.flows.candlestick_flow import candlestick_batch_flow
from forex.flows.economic_calendar_flow import economic_calendar_flow
from forex.flows.forward_fill_flow import forward_fill_batch_flow
from forex.flows.positioning_flow import positioning_flow
from forex.flows.swap_rate_flow import swap_rate_flow

_config_file = os.environ.get('OANDA_CONFIG_FILE', '')
if not _config_file:
    sys.exit('OANDA_CONFIG_FILE environment variable must be set to the path of your Oanda config JSON.')

daily = candlestick_batch_flow.to_deployment(
    name='candlestick-D',
    cron='5 0 * * *',       # 00:05 UTC — well after the 17:00 ET daily candle close
    parameters={'config_file': _config_file, 'granularity': 'D'},
)

hourly = candlestick_batch_flow.to_deployment(
    name='candlestick-H1',
    cron='5 * * * *',       # 5 min past each hour — gives H1 candle time to close
    parameters={'config_file': _config_file, 'granularity': 'H1'},
)

quarter_hourly = candlestick_batch_flow.to_deployment(
    name='candlestick-M15',
    cron='2,17,32,47 * * * *',  # 2 min into each 15-min window
    parameters={'config_file': _config_file, 'granularity': 'M15'},
)

forward_fill_daily = forward_fill_batch_flow.to_deployment(
    name='forward-fill-D',
    cron='15 0 * * *',      # 10 min after candlestick-D
    parameters={'granularity': 'D'},
)

forward_fill_hourly = forward_fill_batch_flow.to_deployment(
    name='forward-fill-H1',
    cron='15 * * * *',      # 10 min after candlestick-H1
    parameters={'granularity': 'H1'},
)

forward_fill_quarter_hourly = forward_fill_batch_flow.to_deployment(
    name='forward-fill-M15',
    cron='12,27,42,57 * * * *',  # 10 min after candlestick-M15
    parameters={'granularity': 'M15'},
)

swap_rates_daily = swap_rate_flow.to_deployment(
    name='swap-rate-D',
    cron='45 20 * * *',    # ~15 min before the 5pm NY rollover cutoff (fixed UTC, not DST-aware)
    parameters={'config_file': _config_file},
)

economic_calendar_daily = economic_calendar_flow.to_deployment(
    name='economic-calendar-D',
    cron='30 0 * * *',     # after candlestick-D/forward-fill-D's 00:05/00:15 slots
    parameters={'days_ahead': 14},
)

positioning = positioning_flow.to_deployment(
    name='positioning',
    cron='*/20 * * * *',   # matches OANDA's own historical order/position-book refresh cadence
    parameters={'config_file': _config_file},
)

if __name__ == '__main__':
    serve(
        daily, hourly, quarter_hourly,
        forward_fill_daily, forward_fill_hourly, forward_fill_quarter_hourly,
        swap_rates_daily, economic_calendar_daily, positioning,
    )
