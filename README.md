# Forex ETL Pipeline

Fetches OHLCV candlestick data from the [Oanda REST API](https://developer.oanda.com/rest-live-v20/introduction/) and writes it to InfluxDB. A second pipeline forward-fills gaps left by weekends, holidays, and market-closed periods.

## Architecture

```
Oanda REST API
      │
      ▼
CandlestickETL          ← fetches, retries, validates
      │
      ▼
CandlestickRecord       ← Pydantic model; single source of truth for schema
      │
      ▼
CandlestickPipeline     ← orchestrates ETL, QA, InfluxDB write
      │
      ▼
InfluxDB (forex bucket)
      │
      ▼
ForwardFillInator       ← fills market-closed gaps with last known price
```

Both pipelines are wrapped as **Prefect flows** (`flows/`) for scheduling and observability.

## Project layout

```
forex/
├── critical_timezone.py          # market-hours gate (Toronto tz)
├── etl/
│   ├── CandlestickETL.py         # API fetch + transform
│   ├── models.py                 # CandlestickRecord (Pydantic)
│   ├── config/
│   │   └── database_config.py    # InfluxDB credentials (via AWS Secrets Manager)
│   └── pipelines/
│       ├── CandlestickPipeline.py
│       └── ForwardFillInator.py
├── flows/
│   ├── candlestick_flow.py       # Prefect: fetch → InfluxDB (single pair + batch)
│   ├── forward_fill_flow.py      # Prefect: forward-fill gaps
│   └── serve.py                  # scheduled deployments for all major pairs
├── oanda/
│   ├── headers.py                # builds Oanda auth headers
│   └── config/price_type_map.py  # bid/ask/mid label mapping
└── tests/
    ├── test_critical_timezone.py
    └── test_models.py
```

## Prerequisites

- Python ≥ 3.11
- An **Oanda config JSON** file with `server`, `account_id`, and `access_token` keys
- **AWS credentials** in the environment (`AWS_PROFILE` or `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`) — InfluxDB credentials are fetched at runtime from AWS Secrets Manager under the key `Forex/InfluxDbPassword`
- A running **InfluxDB** instance

## Installation

```
cd Data-Science/Data-Engineering/ETL
pip install -e ".[dev]"
```

## Running

See [`how_to_start_forex_pipeline.md`](how_to_start_forex_pipeline.md) for full instructions.

**Scheduled production run — all major pairs:**

```
OANDA_CONFIG_FILE=/path/to/oanda_config.json python -m forex.flows.serve
```

This starts three Prefect deployments against a locally running `prefect server`:

| Deployment | Schedule | Granularity |
|---|---|---|
| `candlestick-D` | daily at 00:05 UTC | D |
| `candlestick-H1` | 5 min past each hour | H1 |
| `candlestick-M15` | 2,17,32,47 min past each hour | M15 |

Each deployment runs all seven major pairs (EUR/USD, USD/JPY, GBP/USD, USD/CHF, USD/CAD, AUD/USD, NZD/USD) sequentially. The market-hours gate (`critical_timezone.py`) no-ops any run that falls outside forex trading hours (Fri 17:00 ET → Sun 17:00 ET), so no extra cron filtering is needed.

**Ad-hoc single pair:**

```python
from forex.flows.candlestick_flow import candlestick_flow
candlestick_flow(config_file='/path/to/oanda_config.json', instrument='EUR_USD', granularity='H1')
```

## Data model

`CandlestickRecord` (`etl/models.py`) is the single source of truth for the candlestick schema:

| Attribute | Purpose |
|---|---|
| `MEASUREMENT` | InfluxDB measurement name (`'candlestick'`) |
| `TAGS` | InfluxDB tag set (`instrument`, `granularity`) |
| `FIELDS` | InfluxDB field set with types (bid/ask OHLCV) |
| `.to_influx_dict()` | Serialises a record to the InfluxDB write payload |

## Tests

```
cd Data-Science/Data-Engineering/ETL
pytest        # test_critical_timezone.py + test_models.py
```

No external dependencies — no Oanda, no InfluxDB, no AWS required to run the test suite.
