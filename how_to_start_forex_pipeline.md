There are two entry points depending on whether you want a one-off run or a scheduled deployment.

# Prerequisites

cd Data-Science/Data-Engineering/ETL
pip install -e ".[dev]"          # installs prefect, pydantic, tenacity, etc.

You also need:
- An Oanda config JSON file with server, account_id, and access_token keys (same format the pipeline always used)
- AWS credentials in your environment (AWS_PROFILE or AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY) so boto3 can reach Secrets Manager to fetch the InfluxDB credentials

# Option 1 — One-off run (no Prefect server needed)

```
python -c "
from forex.flows.candlestick_flow import candlestick_flow
candlestick_flow(
    config_file='/path/to/oanda_config.json',
    instrument='EUR_USD',
    granularity='H1',
)
"
```

For the forward-fill:

```
python -c "
from forex.flows.forward_fill_flow import forward_fill_flow
forward_fill_flow(instrument='EUR/USD', granularity='H1')
"

# Option 2 — Persistent deployment (replaces the cron job)

Start a local Prefect server once:

```
prefect server start
```

Then deploy and serve each flow in its own terminal (or as a systemd service):

```
# Terminal 1 — candlestick fetch
python -m forex.flows.candlestick_flow
```

```
# Terminal 2 — forward fill
python -m forex.flows.forward_fill_flow
```

Then trigger runs from the Prefect UI (http://localhost:4200) or CLI:

```
prefect deployment run 'forex-candlestick-etl/forex-candlestick-etl' \
  --param config_file=/path/to/oanda_config.json \
  --param instrument=EUR/USD \
  --param granularity=H1
```
# Scheduling (equivalent to the old cron job)

Add a schedule to the .serve() call in candlestick_flow.py:

```
# forex/flows/candlestick_flow.py
if __name__ == '__main__':
    from prefect.schedules import CronSchedule
    candlestick_flow.serve(
        name='forex-candlestick-etl',
        schedules=[CronSchedule(cron='*/15 * * * 1-5')]  # every 15min Mon-Fri
    )
```

The market-hours gate (check_market_open_task) already handles runs that land outside trading hours — it just returns early without fetching, so the schedule can be aggressive without wasted API calls.

# Running the tests

```
cd Data-Science/Data-Engineering/ETL
pytest                    # runs tests/test_critical_timezone.py + 
tests/test_models.py
pytest -v                 # verbose output (configured in pyproject.toml)
```

The tests have no external dependencies — no Oanda, no InfluxDB, no AWS.
