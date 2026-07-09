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

economic_calendar_flow and positioning_flow are NOT scheduled here.
Economic calendar data is blocked on Finnhub's free tier not including the
`/calendar/economic` endpoint (confirmed with a valid key -- a paid-tier
problem, not a bug); positioning data is blocked because OANDA discontinued
the orderBook/positionBook endpoints entirely, for every account type, now
offered only through a separate enterprise product. Both are otherwise
complete and unit-tested -- see forex/README.md's Architecture section for
the current status of each -- but scheduling a flow that can only ever fail
serves no purpose, so both are left out of `serve()` below until (if ever)
either becomes viable again.

The market-hours gate inside candlestick_flow/forward_fill_flow skips runs
that land outside trading hours (Fri 17:00 ET → Sun 17:00 ET); swap_rate_flow
has no such gate -- it isn't tied to candle formation, and the data source
remains available outside forex trading hours (OANDA just won't have
refreshed recently).
"""

import os
import sys

from prefect import serve

from forex.flows.candlestick_flow import candlestick_batch_flow
from forex.flows.forward_fill_flow import forward_fill_batch_flow
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

if __name__ == '__main__':
    serve(
        daily, hourly, quarter_hourly,
        forward_fill_daily, forward_fill_hourly, forward_fill_quarter_hourly,
        swap_rates_daily,
    )
