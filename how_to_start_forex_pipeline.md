There are two entry points: a one-off run for a single pair, and a scheduled deployment that covers all seven major pairs across three granularities.

# Prerequisites

```
cd Data-Science/Data-Engineering/ETL
pip install -e ".[dev]"          # installs prefect, pydantic, tenacity, etc.
```

You also need:
- An Oanda config JSON file with `server`, `account_id`, and `access_token` keys
- AWS credentials in your environment (`AWS_PROFILE` or `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`) so boto3 can reach Secrets Manager to fetch the InfluxDB credentials

# Option 1 — One-off run (no Prefect server needed)

Single candlestick pair:

```python
from forex.flows.candlestick_flow import candlestick_flow
candlestick_flow(
    config_file='/path/to/oanda_config.json',
    instrument='EUR_USD',
    granularity='H1',
)
```

All major pairs for one granularity:

```python
from forex.flows.candlestick_flow import candlestick_batch_flow
candlestick_batch_flow(config_file='/path/to/oanda_config.json', granularity='H1')
```

Forward-fill gaps for one pair:

```python
from forex.flows.forward_fill_flow import forward_fill_flow
forward_fill_flow(instrument='EUR/USD', granularity='H1')
```

# Option 2 — Scheduled deployment (all major pairs, three granularities)

Start a local Prefect server once (in its own terminal or as a service):

```
prefect server start
```

Then start the serve process, which registers and runs all three deployments:

```
OANDA_CONFIG_FILE=/path/to/oanda_config.json python -m forex.flows.serve
```

This registers three deployments visible at http://localhost:4200:

| Deployment | Cron | Granularity | Pairs |
|---|---|---|---|
| `candlestick-D` | `5 0 * * *` | D | all 7 majors |
| `candlestick-H1` | `5 * * * *` | H1 | all 7 majors |
| `candlestick-M15` | `2,17,32,47 * * * *` | M15 | all 7 majors |

The seven major pairs are: EUR/USD, USD/JPY, GBP/USD, USD/CHF, USD/CAD, AUD/USD, NZD/USD.

The market-hours gate (`check_market_open_task`) no-ops any run outside forex trading hours (Fri 17:00 ET → Sun 17:00 ET), so no extra cron filtering is needed.

To trigger a deployment run manually from the CLI:

```
prefect deployment run 'forex-candlestick-batch/candlestick-H1' \
  --param config_file=/path/to/oanda_config.json \
  --param granularity=H1
```

To run a single pair ad-hoc against a live Prefect server:

```
prefect deployment run 'forex-candlestick-etl/forex-candlestick-etl' \
  --param config_file=/path/to/oanda_config.json \
  --param instrument=EUR_USD \
  --param granularity=H1
```

# Scheduling (custom pairs or granularities)

To customise which instruments or add a new granularity, pass `instruments` when triggering a batch run:

```python
from forex.flows.candlestick_flow import candlestick_batch_flow
candlestick_batch_flow(
    config_file='/path/to/oanda_config.json',
    granularity='M5',
    instruments=['EUR_USD', 'GBP_USD'],
)
```

Or modify `MAJOR_PAIRS` in `flows/candlestick_flow.py` and restart `serve.py`.

# Data model

`CandlestickRecord` in `etl/models.py` is the single source of truth for the candlestick schema:

- **Validation** — Pydantic enforces types on ingestion
- **Serialisation** — `.to_influx_dict()` produces the InfluxDB write payload
- **Schema constants** — `CandlestickRecord.TAGS`, `CandlestickRecord.FIELDS`, and `CandlestickRecord.MEASUREMENT` are used by the pipeline and Prefect flow for the InfluxDB write call

# Running the tests

```
cd Data-Science/Data-Engineering/ETL
pytest        # runs tests/test_critical_timezone.py + tests/test_models.py
pytest -v     # verbose output (configured in pyproject.toml)
```

The tests have no external dependencies — no Oanda, no InfluxDB, no AWS.
