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

Oanda REST API
      │
      ▼
SwapRateETL             ← fetches per-instrument long/short financing rates
      │
      ▼
SwapRateRecord          ← Pydantic model; single source of truth for schema
      │
      ▼
InfluxDB ('swap-rate' measurement)

Finnhub API (NOT Oanda -- separate provider/credential)
      │
      ▼
EconomicCalendarETL     ← fetches upcoming scheduled economic release events
      │
      ▼
EconomicCalendarEventRecord   ← Pydantic model; single source of truth for schema
      │
      ▼
InfluxDB ('economic-calendar-event' measurement)
```

Downstream consumers (e.g. `forex-ML`) can use `is_forward_filled` to distinguish
real market data from imputed placeholder bars — a forward-filled bar has zero
return and zero volatility by construction, which is otherwise indistinguishable
from a genuinely quiet real market.

`swap-rate` is a separate, much simpler pipeline: a single current snapshot of
long/short financing (rollover) rates per instrument, not a historical time series
like candlesticks, so there's no ETL/pipeline/QA class hierarchy for it — just
`SwapRateETL` directly. Downstream consumers (e.g. `forex-strategy`) use this to
account for the cost of holding a position past the 5pm New York rollover cutoff.

`economic-calendar-event` is the one pipeline here NOT sourced from Oanda — economic
calendar data (scheduled release times, country, impact level, actual/estimate/
previous values) isn't part of Oanda's API at all, so it comes from
[Finnhub](https://finnhub.io/), with its own separate API-key credential (see
"Prerequisites" below). Like swap rates, it's a forward-looking pull over a date
range rather than an incremental backfill — re-pulling the same rolling window daily
is cheap and naturally idempotent, and picks up newly-published `actual` values for
events that already occurred.

Every pipeline is wrapped as a **Prefect flow** (`flows/`) for scheduling and observability.

## Project layout

```
forex/
├── critical_timezone.py          # market-hours gate (Toronto tz)
├── etl/
│   ├── CandlestickETL.py         # API fetch + transform
│   ├── SwapRateETL.py            # per-instrument financing (swap/rollover) rate fetch
│   ├── EconomicCalendarETL.py    # scheduled economic release event fetch (Finnhub)
│   ├── models.py                 # CandlestickRecord/SwapRateRecord/
│   │                              # EconomicCalendarEventRecord (Pydantic)
│   ├── config/
│   │   ├── database_config.py    # InfluxDB credentials (via AWS Secrets Manager)
│   │   └── finnhub_config.py     # Finnhub API key (via AWS Secrets Manager)
│   └── pipelines/
│       ├── CandlestickPipeline.py
│       └── ForwardFillInator.py
├── flows/
│   ├── candlestick_flow.py       # Prefect: fetch → InfluxDB (single pair + batch)
│   ├── forward_fill_flow.py      # Prefect: forward-fill gaps
│   ├── swap_rate_flow.py         # Prefect: fetch swap rates → InfluxDB
│   ├── economic_calendar_flow.py # Prefect: fetch calendar events → InfluxDB
│   └── serve.py                  # scheduled deployments for all major pairs
├── oanda/
│   ├── headers.py                # builds Oanda auth headers
│   └── config/price_type_map.py  # bid/ask/mid label mapping
└── tests/
    ├── test_critical_timezone.py
    ├── test_models.py
    ├── test_forward_fill_inator.py
    ├── test_swap_rate_etl.py
    ├── test_economic_calendar_etl.py
    └── test_secrets_isolation.py
```

## Prerequisites

```
cd Data-Science/Data-Engineering/ETL
pip install -e ".[dev]"          # installs prefect, pydantic, tenacity, etc.
```

You also need:
- An **Oanda config JSON** file with `server`, `token`, and `oanda_date_time_format`
  keys (`CandlestickETL`/`SwapRateETL` read these). `SwapRateETL` also uses an
  `account_id` key if present, resolving it via `/v3/accounts` otherwise (see
  "Swap/rollover rates" below).
- **AWS credentials** in the environment (`AWS_PROFILE` or `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`) — InfluxDB credentials AND the Finnhub API key are both fetched at runtime from AWS Secrets Manager, under `Forex/InfluxDbPassword` and `Forex/FinnhubApiKey` respectively (two separate secrets — Finnhub isn't part of Oanda's credential set at all)
- A **Finnhub API key** (a free tier is enough for the economic calendar endpoint), stored in the `Forex/FinnhubApiKey` secret as `{"FINNHUB_API_KEY": "..."}`
- A running **InfluxDB** instance

`database_config`/`finnhub_config` both lazy-load their credentials via a
module-level `__getattr__` triggered on attribute access. Every module that needs
them accesses it as `database_config.INFLUXDB_URL` (resolved fresh each call) rather
than `from database_config import INFLUXDB_URL` — the latter freezes the resolved
secret into the importing module's own namespace the moment it's imported (including
just pytest collecting a test file), permanently, for the life of the process, with
no way to substitute different credentials afterward. See
`forex/tests/test_secrets_isolation.py` for the regression test and the real bug
this guards against — a downstream consumer's "flaky" integration test turned out to
be silently querying this real InfluxDB instead of its intended local Docker
container, because of exactly this.

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

Swap/rollover rates for all major pairs (a single current snapshot, not a
historical backfill):

```python
from forex.flows.swap_rate_flow import swap_rate_flow
swap_rate_flow(config_file='/path/to/oanda_config.json')
```

Economic calendar events for a rolling 14-day-ahead window (no `config_file` needed
— this pulls from Finnhub, not Oanda):

```python
from forex.flows.economic_calendar_flow import economic_calendar_flow
economic_calendar_flow(days_ahead=14)
```

### Option 2 — Scheduled deployment (all major pairs, three granularities)

Start a local Prefect server once (in its own terminal or as a service):

```
prefect server start
```

Then start the serve process, which registers and runs all seven deployments:

```
OANDA_CONFIG_FILE=/path/to/oanda_config.json python -m forex.flows.serve
```

This registers eight deployments visible at http://localhost:4200 — one candlestick-fetch
deployment per granularity, each paired with a forward-fill deployment offset 10 minutes
later so it always runs against freshly-landed candles rather than racing the fetch that
feeds it:

| Deployment | Cron | Granularity | Pairs |
|---|---|---|---|
| `candlestick-D` | `5 0 * * *` | D | all 7 majors |
| `candlestick-H1` | `5 * * * *` | H1 | all 7 majors |
| `candlestick-M15` | `2,17,32,47 * * * *` | M15 | all 7 majors |
| `forward-fill-D` | `15 0 * * *` | D | all 7 majors |
| `forward-fill-H1` | `15 * * * *` | H1 | all 7 majors |
| `forward-fill-M15` | `12,27,42,57 * * * *` | M15 | all 7 majors |
| `swap-rate-D` | `45 20 * * *` | n/a | all 7 majors |
| `economic-calendar-D` | `30 0 * * *` | n/a | n/a (global calendar) |

The seven major pairs are: EUR/USD, USD/JPY, GBP/USD, USD/CHF, USD/CAD, AUD/USD, NZD/USD.

`swap-rate-D` runs at 20:45 UTC — about 15 minutes before the 5pm New York rollover
cutoff (a fixed UTC time, not DST-aware, the same simplification forex-ML's own
trading-session features already make) — so a fresh rate is on hand right as any
position held past the cutoff would actually be charged one.

`economic-calendar-D` runs at 00:30 UTC, after the daily candlestick/forward-fill
slots, pulling a rolling 14-day-ahead window from Finnhub each time (not a single
fixed date — see "Architecture" above for why that's cheap and idempotent).

The forward-fill deployments were missing entirely until 2026-07-06 — `serve.py` only
ever registered the three candlestick deployments, so forward-filled data never got
produced on an ongoing basis; it only ran when triggered manually (a direct function
call, or the one-off `scripts/recompute_forward_fill_history.py`). If you deployed this
service before that date, restart `python -m forex.flows.serve` to pick up the three new
deployments.

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

`SwapRateRecord` (`etl/models.py`) is the schema for per-instrument financing rates:

| Attribute | Purpose |
|---|---|
| `MEASUREMENT` | InfluxDB measurement name (`'swap-rate'`) |
| `TAGS` | InfluxDB tag set (`instrument` only — no `granularity`, see below) |
| `FIELDS` | `long_rate`, `short_rate` |
| `.to_influx_dict()` | Serialises a record to the InfluxDB write payload |

Unlike the candlestick records, there's no `granularity` tag — a financing rate is
an account-level daily snapshot per instrument, not tied to any candle timeframe.
`long_rate`/`short_rate` are OANDA's daily financing rates (as a fraction, e.g.
`-0.0067` for the long side), charged (or credited, if positive) once per day a
position is held past the 5pm New York rollover cutoff.

`EconomicCalendarEventRecord` (`etl/models.py`) is the schema for scheduled
economic release events (Finnhub, not Oanda):

| Attribute | Purpose |
|---|---|
| `MEASUREMENT` | InfluxDB measurement name (`'economic-calendar-event'`) |
| `TAGS` | `country`, `impact`, `event` |
| `FIELDS` | `actual`, `estimate`, `prev` (all optional), `unit` |
| `.to_influx_dict()` | Serialises a record to the InfluxDB write payload |

`event` (e.g. "Non-Farm Payrolls") is a tag despite having more distinct values than
any other tag in this pipeline — it's a bounded, recurring set of named releases
(not free text), and being able to filter/group by event name is the whole point of
ingesting this data. `actual`/`estimate`/`prev` are all optional: a future-scheduled
event has no `actual` yet (and possibly no `estimate` either), so `to_influx_dict()`
omits any `None` field entirely rather than writing it as null — the one place this
schema's serialization differs from the other three records above.

## Tests

```
cd Data-Science/Data-Engineering/ETL
pytest        # test_critical_timezone.py + test_models.py + test_forward_fill_inator.py
              # + test_swap_rate_etl.py + test_economic_calendar_etl.py
              # + test_secrets_isolation.py
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
