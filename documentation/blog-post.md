# A Time-Series Database for Forex Data Engineering

***SUBTITLE:***  Our heroine builds a production-grade OHLCV pipeline using InfluxDB and Prefect

Our heroine—a mild-mannered data scientist by day—ruthlessly shorts flailing assets by night. She wants to build AI/ML-based systematic trading strategies, but such trading strategies require data. Reliable, clean, continuously updated data. And so, before she can do anything interesting with Forex prices using ML, she has to solve a more fundamental problem: how does one store and maintain over a decade's worth of candlestick data in a way that makes it fast to query, easy to update, and is honest about what that data storage contains?

So our intrepid heroine built a robust ETL pipeline to perform that exact task. In the future she will use the database this pipeline maintains to generate AI/ML-based forecasting strategies. But this current post is about the pipeline and database themselves:

## Why Forex, and Why Candlesticks

Our heroine sources her data from Oanda, a retail Forex broker that exposes a REST API for historical and streaming price data. Oanda provides candlestick (OHLCV) data: for each time interval, the open, high, low, and close prices for both the bid and ask sides of the market, plus volume.

A candlestick for EUR/USD at hourly granularity looks roughly like this in Oanda's API response:

```json
{
  "time": "1700000000",
  "complete": true,
  "volume": 1842,
  "bid": { "o": "1.09001", "h": "1.09187", "l": "1.08994", "c": "1.09142" },
  "ask": { "o": "1.09015", "h": "1.09201", "l": "1.09008", "c": "1.09156" }
}
```

The `complete` flag matters: an incomplete candlestick means the interval hasn't closed yet. Including incomplete candles in a training dataset would introduce look-ahead bias. The pipeline filters them out.

## The Database Decision: Why InfluxDB

The data is time series. Native time series. And our heroine really digs working with time series information. Therefore she wanted a database system equally time series-native.

She considered using a relational database first. PostgreSQL can store time-series data; it will accept a row per candle, and one can index on timestamp. It works.

But the relational approach fights the basic data model.

Forex OHLCV data has a specific shape: it is append-only (you never update a closed candle), it is almost always queried by time range, and it has a fixed set of metadata that naturally groups rows—`instrument` (e.g., currency pairs like EUR/USD and GBP/JPY) and `time granularity` (M15, H1, D). In relational terms these function as filter columns, not join columns. Every interesting query starts with something akin to "give me all EUR/USD one-hour candles between timestamps X and Y."

InfluxDB is designed for exactly this operation. It organizes data into **measurements** (think: table), **tags** (indexed metadata—`instrument` and `granularity` here), and **fields** (the actual values—bid/ask OHLC prices plus volume). InfluxDB's query language, Flux, expresses time-range scans and tag filters in readable lines:

```flux
from(bucket: "forex")
  |> range(start: 2024-01-02T00:00:00Z)
  |> filter(fn: (r) => r.instrument == "EUR/USD")
  |> filter(fn: (r) => r.granularity == "H1")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
```

Running this query produces the following example output:

![Flux-Query-Example](images/Flux-Query.png)

The same query in SQL would require a WHERE clause, a timestamp index hint, and if you want the bid and ask fields as columns rather than rows, a pivot or a set of self-joins. InfluxDB makes the common case simple.

There is also a practical consideration: InfluxDB has built-in retention policies, downsampling tasks, and a time-series aware data model that handles the `max(timestamp)` query—e.g., "what is the most recent candlestick already stored for this instrument?"—extremely efficiently. Our heroine uses this query during every pipeline run to determine where to resume ingestion. In a relational database this procedure would require a table scan or a carefully maintained index. In InfluxDB it is a first-class operation.

## Pipeline Architecture

The pipeline has three moving parts: the Oanda API client to pull updated Forex price data from the broker, a validation layer to ensure only quality content gets inserted into the database, and finally the InfluxDB insert itself. Prefect coordinates them. Here is the shape of it:

```
Oanda REST API
      ↓
CandlestickETL.fit()
      ↓  (raw dict → structured record)
CandlestickRecord  [Pydantic validation]
      ↓  (to_influx_dict())
InfluxDbTool.insert_dictionary_list()
      ↓
InfluxDB
```

Each layer has a single responsibility. The ETL class fetches and normalises raw API responses. Then the Pydantic model validates the shape of each record and produces the dict structure that InfluxDB expects. Finally the InfluxDB tool handles the write. Nothing crosses those boundaries.

## Fetching Data: CandlestickETL

The Oanda API returns up to 5,000 candles per request. Our heroine's `CandlestickETL` class walks backward through time in 5,000-candle windows until it reaches the most recent timestamp already stored in InfluxDB, then writes only the new records. On first run it fetches from 2010 (near the beginning of available Oanda data). On subsequent runs it resumes from where it left off.

A `tenacity` retry decorator handles transient network failures cleanly:

```python
@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def _fetch_from_api(self, url: str) -> dict:
    r = requests.get(url, headers=self.headers)
    r.raise_for_status()
    return r.json()
```

Five attempts, two seconds between each, re-raise on final failure so Prefect can record the task as failed rather than hanging.

## Validation: CandlestickRecord

Before anything touches the database, each candle passes through a Pydantic model:

```python
class CandlestickRecord(BaseModel):
    instrument: str
    granularity: str
    volume: int
    complete: bool
    bid_open: float
    bid_high: float
    bid_low: float
    bid_close: float
    ask_open: float
    ask_high: float
    ask_low: float
    ask_close: float
    timestamp: int

    TAGS: ClassVar[frozenset[str]] = frozenset({'instrument', 'granularity'})
    MEASUREMENT: ClassVar[str] = 'candlestick'

    def to_influx_dict(self) -> dict:
        data = self.model_dump()
        result = {
            'measurement': self.MEASUREMENT,
            'tags': {},
            'fields': {},
            'time': data.pop('timestamp'),
        }
        for key, value in data.items():
            if key in self.TAGS:
                result['tags'][key] = value
            else:
                result['fields'][key] = value
        return result
```

`CandlestickRecord` is the only place in the entire pipeline where the schema is enforced. If the Oanda API ever returns a malformed response, or if a code change accidentally drops a field, the `ValidationError` is raised here—at the write boundary—rather than written silently to the database as a null. The `to_influx_dict()` method then produces exactly the structure `InfluxDbTool` expects: a measurement name, a tag dict, a field dict, and a timestamp.

This separation is visible in the knowledge graph of our heroine's codebase. The graph clusters `CandlestickRecord` and its test suite into their own community, separate from the InfluxDB tool community—because `CandlestickRecord` never touches the database client. It only produces a dict. The client lives elsewhere.

![graphify community graph, Communities 4 (CandlestickETL Core), 17 (Candlestick Pydantic Model & Tests), and 35 (InfluxDB Tool) highlighted and annotated](forex-etl-graph-1.png)
*Three separate communities in the knowledge graph. CandlestickRecord sits between the ETL core and the database tool but shares no edges with either — it only produces a Python dictionary.*

## Secrets: The PEP 562 Pattern

The InfluxDB credentials live in AWS Secrets Manager under the key `Forex/InfluxDbPassword`. Fetching them at module import time—the naive approach—means credentials are retrieved before `main()` runs, before any argument parsing, and potentially in contexts (test runs, linting, CI) where AWS access is neither available nor desired.

Our heroine therefore uses Python's PEP 562 module `__getattr__` to make the fetch lazy:

```python
# database_config.py
import json
import functools
from python_tools_and_shortcuts.aws.secrets_manager import get_secret

_SECRET_NAME = 'Forex/InfluxDbPassword'
_KEYS = frozenset(['INFLUXDB_URL', 'INFLUXDB_TOKEN', 'INFLUXDB_ORG', 'INFLUXDB_BUCKET'])

@functools.lru_cache(maxsize=None)
def _load_secret():
    return json.loads(get_secret(_SECRET_NAME))

def __getattr__(name):
    if name in _KEYS:
        return _load_secret()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
```

When any part of the codebase executes `from database_config import INFLUXDB_URL`, Python calls `__getattr__('INFLUXDB_URL')` at access time, not at import time. `_load_secret()` is decorated with `@functools.lru_cache`, so the boto3 call to Secrets Manager happens exactly once per process lifetime. Subsequent accesses return the cached result.

The `lru_cache` has a subtle implication for long-running Prefect deployments: if the Prefect worker process stays alive between flow runs, the secret is fetched once on the first run and cached forever. Credential rotation does not take effect until the worker is restarted. This is intentional—it avoids Secrets Manager rate limits and per-run latency—but worth knowing.

The pattern is largely invisible to static analysis tools, including the knowledge graph visualizer our heroine uses. The graph correctly shows `__getattr__()` calling `_load_secret()`, but shows no edge from `_load_secret()` to the `get_secret()` function in the AWS tools package. Two reasons: the secrets utility file (`secrets_manager.py`) is excluded by the graph tool's sensitive-filename heuristic, and the PEP 562 dispatch itself—module attribute access—does not look like a function call to an AST parser.

![Forex ETL Graph 2](forex-etl-graph-2.png)
*The graph ends at _load_secret(). The boto3 → Secrets Manager hop is invisible: the utility file is excluded by the sensitive-filename filter, and PEP 562 module attribute dispatch doesn't register as a call edge in static analysis.*


The invisibility is, in a way, the point. A design that is opaque to static analysis is also opaque to casual inspection of the codebase. Credentials that are never bound to a module-level variable, never written to `os.environ`, and never fetched until the moment they are needed are credentials that are harder to accidentally log, serialize, or expose.

## Orchestration: Prefect

The pipeline originally ran as cron job calling a Python class directly. This worked, but provided no visibility into failures, no retry policy at the workflow level, and no run history. Our heroine improved this situation by replacing the cron job with a Prefect flow:

![Prefect-Run](images/prefect-run.png)

The complete candlestick fetch flow consists of three tasks and a flow function:

```python
@task(name='check-market-open')
def check_market_open_task() -> bool:
    logger = get_run_logger()
    open_ = is_market_open()
    logger.info('Market is %s', 'open' if open_ else 'closed')
    return open_

@task(name='fetch-candlestick-data', retries=3, retry_delay_seconds=30)
def fetch_candlestick_data(config_file: str, instrument: str, granularity: str) -> list[dict]:
    ifc = _make_ifc()
    etl = CandlestickETL(instrument, granularity, config_file, ifc)
    etl.fit()
    return etl.to_influx_list

@task(name='insert-to-influxdb')
def insert_to_influxdb(records: list[dict]) -> None:
    if not records:
        return
    ifc = _make_ifc()
    ifc.insert_dictionary_list(records, ALLOWED_TAGS, ALLOWED_FIELDS, INFLUXDB_BUCKET)

@flow(name='forex-candlestick-etl', log_prints=True)
def candlestick_flow(config_file: str, instrument: str, granularity: str) -> None:
    if not check_market_open_task():
        return
    records = fetch_candlestick_data(config_file, instrument, granularity)
    insert_to_influxdb(records)
```

A few design choices prove worth noting:

**The market-hours gate.** Forex markets close Friday at 5pm Eastern (U.S.) and reopen Sunday at 5pm. The `check_market_open_task()` evaluates this and returns early if the market is closed, so the flow can be scheduled aggressively (every 15 minutes on weekdays) without making API calls during downtime.

**`_make_ifc()` is called inside tasks, not passed as a parameter.** Prefect serialises task parameters so they can be stored, retried, and displayed in the UI. `InfluxDbTool` holds a live HTTP connection; it cannot be pickled. Constructing it inside each task that needs it is the correct pattern—each task gets a fresh, serialisable call with only the credentials (fetched lazily from Secrets Manager) as its inputs.

**Retries live at two levels.** The `@retry` tenacity decorator on `_fetch_from_api()` handles transient network errors within a single task execution (five attempts, two seconds apart). The `retries=3, retry_delay_seconds=30` on the Prefect task handles failures that exhaust the tenacity budget—for example, a prolonged Oanda outage. Two layers, two timescales.

The flow's data path is fully traceable in the knowledge graph:

![Flow-as-Text](forex-etl-graph-3.png)

![Flow-as-Graph](forex-etl-graph-4.png)
*The complete data path as the knowledge graph sees it. Both the fetch and insert tasks construct InfluxDbTool independently via _make_ifc(), which is why the graph shows two separate edges from different tasks to the same InfluxDbTool node.*

## The Forward Fill

Forex markets do not produce a candle for every interval. If no trades occur during a 15-minute window, Oanda returns nothing for that window. However, machine learning models do not work well with gaps in their input sequences.

The `ForwardFillInator` class addresses this. It pulls the stored candlestick data from InfluxDB, constructs a complete time grid for the instrument's trading hours (excluding weekends and the Friday-close to Sunday-open gap), left-joins the actual data onto the grid, and forward-fills any NaN rows. The result is a gapless sequence of OHLCV records at regular intervals, suitable for feeding directly into a model.

The market-hours logic that governs the time grid is the same `is_market_open_at_time()` function used by the pipeline's market gate—a single source of truth for what counts as a trading interval, used both to decide whether to run and to decide which rows to include in the forward-filled output.

Forward-filling plays another critical role: preventing "lookaheads" from polluting any forecasting models created from the data. The operation prevents database users from accidentally utilizing future data that would not be available in a real-world scenario. By contrast, had our heroine employed interpolation to fill gaps and then built a forecasting model on such data, when faced with a gap at the time of forecast creation she would need information unavailable at that time because it hadn't occurred yet.

## What the Knowledge Graph Reveals

Our heroine uses a knowledge graph tool to visualize the architecture of her codebase. The graph clusters code into communities by structural similarity and shared dependencies. Three observations regarding the code prove easier to see in this visualization than in the code itself:

**Separation of concerns is firmly established.** The Pydantic model (`CandlestickRecord`) and its test suite cluster into their own community, with no shared edges to the InfluxDB tool community. This confirms that the schema enforcement layer is genuinely decoupled from the write layer—not just by intention in the code, but by the absence of any dependency path between them.

**The Prefect flow is the glue.** In the graph, `_make_ifc()` is the node that connects the flow community to the InfluxDB tool community. It appears twice—once in the candlestick flow, once in the forward-fill flow—and both instances reference `InfluxDbTool`. Everything upstream of `_make_ifc()` is pure business logic. Everything downstream is infrastructure. The factory function is the seam.

**The secrets path has two invisible edges.** The graph shows `__getattr__()` calling `_load_secret()`, but nothing after that. The AWS Secrets Manager call is absent. This is not an error in the graph—it is a faithful representation of what static analysis can and cannot see. The PEP 562 dispatch is too dynamic for an AST parser, and the secrets utility file is excluded by the graph tool's own security filter. The gap in the graph mirrors the gap that would exist in any static security audit of the codebase.

![forex-etl-graph-5](forex-etl-graph-5.png)
*The full portfolio knowledge graph.*

## Next Steps

Our heroine's next few tasks will actually use this data. The pipeline is plumbing—the interesting work will be what she builds on top of it. Upcoming posts will cover feature engineering on the forward-filled OHLCV sequences, the construction of a backtesting framework, and eventually the development of a systematic trading strategy that runs without manual intervention.

## AI Use Statement

The Forex ETL pipeline described above was originally developed by the author from scratch. She then worked collaboratively with Claude Code review the design, fix bugs, and modernize the architecture. During that process our heroine employed graphify to generate the knowledge graph that facilitated Claude Code's analysis, thereby reducing overall token burden.

Because Claude Code became so familiar with the pipeline codebase and knowledge graph, our heroine decided to see how well it drafted a blog post about the pipeline. This turned out pretty well; she kept the draft's basic structure but significantly adjusted the wording to better fit her writing style. 

## Tags

forex
forecasting
time series
InfluxDB
ETL
Prefect
data science
data engineering
AI
machine learning
Python
time series database
database
PostgreSQL
SQL
AWS
Claude Code
graphify
