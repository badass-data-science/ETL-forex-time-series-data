# Forex ETL Pipeline

Fetches OHLCV candlestick data from the [Oanda REST API](https://developer.oanda.com/rest-live-v20/introduction/) and writes it to InfluxDB. A second pipeline forward-fills gaps left by weekends, holidays, and market-closed periods, tags every bar with whether it was forward-filled, and writes the result back to InfluxDB as its own measurement.

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
InfluxDB ('candlestick' measurement)
      │
      ▼
ForwardFillInator            ← fills market-closed gaps with last known price;
                                tags each bar is_forward_filled: True/False
      │
      ▼
ForwardFilledCandlestickRecord    ← Pydantic model for the forward-filled schema
      │
      ▼
InfluxDB ('forward-filled candlestick' measurement)
```

Downstream consumers (e.g. `forex-ML`) can use `is_forward_filled` to distinguish
real market data from imputed placeholder bars — a forward-filled bar has zero
return and zero volatility by construction, which is otherwise indistinguishable
from a genuinely quiet real market.

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
    ├── test_models.py
    └── test_forward_fill_inator.py
```

## Prerequisites

```
cd Data-Science/Data-Engineering/ETL
pip install -e ".[dev]"          # installs prefect, pydantic, tenacity, etc.
```

You also need:
- An **Oanda config JSON** file with `server`, `account_id`, and `access_token` keys
- **AWS credentials** in the environment (`AWS_PROFILE` or `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`) — InfluxDB credentials are fetched at runtime from AWS Secrets Manager under the key `Forex/InfluxDbPassword`
- A running **InfluxDB** instance

## Running

There are two entry points: a one-off run for a single pair, and a scheduled deployment that covers all seven major pairs across three granularities.

### Option 1 — One-off run (no Prefect server needed)

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

Forward-fill gaps for one pair (fills gaps, tags each bar `is_forward_filled`, and
writes the result to InfluxDB's `forward-filled candlestick` measurement):

```python
from forex.flows.forward_fill_flow import forward_fill_flow
forward_fill_flow(instrument='EUR/USD', granularity='H1')
```

### Option 2 — Scheduled deployment (all major pairs, three granularities)

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

### Scheduling (custom pairs or granularities)

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

## Data model

`CandlestickRecord` (`etl/models.py`) is the single source of truth for the candlestick schema:

| Attribute | Purpose |
|---|---|
| `MEASUREMENT` | InfluxDB measurement name (`'candlestick'`) |
| `TAGS` | InfluxDB tag set (`instrument`, `granularity`) |
| `FIELDS` | InfluxDB field set with types (bid/ask OHLCV) |
| `.to_influx_dict()` | Serialises a record to the InfluxDB write payload |

Pydantic enforces types on ingestion; `to_influx_dict()` produces the InfluxDB write payload.

`ForwardFilledCandlestickRecord` (`etl/models.py`) is the schema for the forward-filled output:

| Attribute | Purpose |
|---|---|
| `MEASUREMENT` | InfluxDB measurement name (`'forward-filled candlestick'`) |
| `TAGS` | InfluxDB tag set (`instrument`, `granularity`) |
| `FIELDS` | `mid_open/high/low/close`, `spread_close`, `volume`, `is_forward_filled` |
| `.to_influx_dict()` | Serialises a record to the InfluxDB write payload |

`is_forward_filled` is set once, right after `ForwardFillInator` merges the pulled
candles onto the full market-open time grid — `True` for a timestamp with no real
candle at that point, `False` otherwise. It survives the subsequent forward-fill
step untouched (`ffill()` only fills genuine `NaN`s in the OHLCV columns; this field
is never null to begin with).

## Tests

```
cd Data-Science/Data-Engineering/ETL
pytest        # test_critical_timezone.py + test_models.py + test_forward_fill_inator.py
pytest -v     # verbose output (configured in pyproject.toml)
```

No external dependencies — no Oanda, no InfluxDB, no AWS required to run the test suite.

`test_forward_fill_inator.py` covers the `is_forward_filled` flag, the actual
forward-fill propagation, and the InfluxDB record schema. It's also the regression
test for a real bug: `account_for_holiday_market_closure()` used to run *before*
`forward_fill_it()` and call a bare `dropna()` (no `subset=`) on the pre-ffill frame,
which drops every row with *any* missing OHLCV value — i.e. every gap, not just
holiday closures. That made `forward_fill_it()`'s `ffill()` a no-op: nothing was
left with a `NaN` by the time it ran. Fixed by reordering so `forward_fill_it()`
runs first, and narrowing `account_for_holiday_market_closure()` to drop only rows
still `NaN` after forward-filling (leading rows before any real candle exists to
fill from). A `TODO` remains in that method to replace it with an explicit holiday
calendar, so extended multi-day closures (e.g. Christmas week) get dropped/flagged
instead of bridged over with a stale price.
