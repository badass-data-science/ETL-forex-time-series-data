"""
Start all scheduled forex deployments.

Usage:
    OANDA_CONFIG_FILE=/path/to/oanda_config.json python -m forex.flows.serve

Deployments
-----------
candlestick-D     daily at 00:05 UTC        (D candles, all major pairs)
candlestick-H1    5 min past each hour UTC  (H1 candles, all major pairs)
candlestick-M15   every 15 min UTC          (M15 candles, all major pairs)

The market-hours gate inside each flow skips runs that land outside
trading hours (Fri 17:00 ET → Sun 17:00 ET).
"""

import os
import sys

from prefect import serve

from forex.flows.candlestick_flow import candlestick_batch_flow

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

if __name__ == '__main__':
    serve(daily, hourly, quarter_hourly)
