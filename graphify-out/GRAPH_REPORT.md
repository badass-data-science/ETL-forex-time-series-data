# Graph Report - .  (2026-07-27)

## Corpus Check
- 46 files · ~14,391 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 340 nodes · 601 edges · 28 communities (24 shown, 4 thin omitted)
- Extraction: 84% EXTRACTED · 16% INFERRED · 0% AMBIGUOUS · INFERRED: 98 edges (avg confidence: 0.7)
- Token cost: 0 input · 34,369 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Data Model & Schema Layer|Data Model & Schema Layer]]
- [[_COMMUNITY_Swap-Rate ETL|Swap-Rate ETL]]
- [[_COMMUNITY_Candlestick & Forward-Fill Flows|Candlestick & Forward-Fill Flows]]
- [[_COMMUNITY_Economic Calendar ETL (Dormant)|Economic Calendar ETL (Dormant)]]
- [[_COMMUNITY_Repo Conventions & Refactor Rationale|Repo Conventions & Refactor Rationale]]
- [[_COMMUNITY_Market-Hours Gate|Market-Hours Gate]]
- [[_COMMUNITY_Forward-Fill DST Logic|Forward-Fill DST Logic]]
- [[_COMMUNITY_Positioning ETL (Dormant)|Positioning ETL (Dormant)]]
- [[_COMMUNITY_Candlestick ETL|Candlestick ETL]]
- [[_COMMUNITY_Forward-Fill DST Tests|Forward-Fill DST Tests]]
- [[_COMMUNITY_Positioning Flow & InfluxDB Tool|Positioning Flow & InfluxDB Tool]]
- [[_COMMUNITY_Secrets-Isolation Tests|Secrets-Isolation Tests]]
- [[_COMMUNITY_InfluxDB Secret Config|InfluxDB Secret Config]]
- [[_COMMUNITY_Finnhub Secret Config|Finnhub Secret Config]]
- [[_COMMUNITY_Scheduled Deployment Entrypoint|Scheduled Deployment Entrypoint]]
- [[_COMMUNITY_Project Root|Project Root]]

## God Nodes (most connected - your core abstractions)
1. `ForwardFillInator` - 24 edges
2. `SwapRateETL` - 21 edges
3. `SwapRateRecord` - 21 edges
4. `EconomicCalendarEventRecord` - 20 edges
5. `CandlestickRecord` - 19 edges
6. `PositioningBucketRecord` - 19 edges
7. `CandlestickETL` - 18 edges
8. `PositioningETL` - 18 edges
9. `InfluxDbTool` - 16 edges
10. `is_market_open_at_time()` - 14 edges

## Surprising Connections (you probably didn't know these)
- `is_forward_filled Tag Semantics` --rationale_for--> `ForwardFilledCandlestickRecord`  [EXTRACTED]
  AGENTS.md → src/forex/etl/models.py
- `CandlestickETL` --references--> `Oanda REST API`  [EXTRACTED]
  src/forex/etl/CandlestickETL.py → README.md
- `Why SwapRateETL Bridges Data Model, Flow, and Positioning Communities` --references--> `CandlestickETL`  [EXTRACTED]
  graphify-out/memory/query_20260727_204958_why_does_swaprateetl_connect_swap_rate_etl_to_data.md → src/forex/etl/CandlestickETL.py
- `Positioning-Bucket Pipeline (blocked)` --references--> `PositioningETL`  [EXTRACTED]
  README.md → src/forex/etl/PositioningETL.py
- `SwapRateETL` --references--> `Oanda REST API`  [EXTRACTED]
  src/forex/etl/SwapRateETL.py → README.md

## Import Cycles
- None detected.

## Communities (28 total, 4 thin omitted)

### Community 0 - "Data Model & Schema Layer"
Cohesion: 0.07
Nodes (21): BaseModel, forex-ML (downstream project), forex-strategy (downstream project), InfluxDB, CandlestickRecord, EconomicCalendarEventRecord, ForwardFilledCandlestickRecord, PositioningBucketRecord (+13 more)

### Community 1 - "Swap-Rate ETL"
Cohesion: 0.09
Nodes (25): Dormant Pipelines Policy, Finnhub API, EconomicCalendarETL, Pure transform, kept separate from any HTTP call so it's directly testable     a, Pulls scheduled economic calendar events (release time, country, impact,     act, _records_from_calendar_response(), economic_calendar_flow(), fetch_economic_calendar() (+17 more)

### Community 2 - "Candlestick & Forward-Fill Flows"
Cohesion: 0.09
Nodes (21): BaseException, _is_not_client_error(), True unless `exc` is an HTTPError with a 4xx status -- those are     determinist, Pure transform, kept separate from any HTTP call so it's directly testable     a, Pulls per-instrument long/short financing (swap/rollover) rates from OANDA's, The financing-rate endpoint is scoped under /v3/accounts/{accountID}/         in, OANDA rejects the ENTIRE batched request (a single HTTP 404,         `INSTRUMENT, _records_from_instruments_response() (+13 more)

### Community 3 - "Economic Calendar ETL (Dormant)"
Cohesion: 0.13
Nodes (13): DataFrame, datetime, Series, TestTimezoneObject, _make_hourly_frame(), _make_local_time_frame(), Regression coverage for a real, confirmed-in-production bug: H4/D candles     ar, This is exactly what the pre-fix call order (account_for_holiday_market_ (+5 more)

### Community 4 - "Repo Conventions & Refactor Rationale"
Cohesion: 0.11
Nodes (22): CHANGELOG Update Convention, AGENTS.md Contributor Guide, is_forward_filled Tag Semantics, Lazy Secret Loading Pattern, src-layout Package Convention, forex.util Self-Contained Utilities, ci/vendor/ Workaround Removed, pyproject.toml (+14 more)

### Community 5 - "Market-Hours Gate"
Cohesion: 0.12
Nodes (23): candlestick_batch_flow(), candlestick_flow(), check_market_open_task(), fetch_candlestick_data(), insert_to_influxdb(), _make_ifc(), Prefect flows: fetch Oanda candlesticks → InfluxDB.  Single instrument (ad-hoc):, Run candlestick_flow for each instrument sequentially. (+15 more)

### Community 6 - "Forward-Fill DST Logic"
Cohesion: 0.15
Nodes (13): Oanda REST API, PositioningETL, Pure transform, kept separate from any HTTP call so it's directly testable     a, Pulls OANDA's per-instrument order-book and position-book snapshots (the     lat, _records_from_book_response(), Why SwapRateETL Bridges Data Model, Flow, and Positioning Communities, Positioning-Bucket Pipeline (blocked), test_compute_positioning_fetches_both_books_for_every_instrument() (+5 more)

### Community 7 - "Positioning ETL (Dormant)"
Cohesion: 0.13
Nodes (7): DST-Aware Grid Logic Gotcha, ndarray, compute_df_all_time_diff_market_open(), ForwardFillInator, # TODO: replace with an explicit holiday calendar to also drop/flag extended, Every timestamp a candle is expected to exist at, from `mn` to `mx`.          Bu, DST-Aware Expected-Bar Grid Fix

### Community 8 - "Candlestick ETL"
Cohesion: 0.17
Nodes (3): CandlestickETL, is_market_open(), CandlestickPipeline

### Community 9 - "Forward-Fill DST Tests"
Cohesion: 0.17
Nodes (7): fetch_swap_rates(), insert_swap_rates_to_influxdb(), _make_ifc(), Prefect flow: fetch OANDA per-instrument financing (swap/rollover) rates → Influ, No market-hours gate (unlike candlestick_flow) -- financing rates are an     acc, swap_rate_flow(), InfluxDbTool

### Community 10 - "Positioning Flow & InfluxDB Tool"
Cohesion: 0.36
Nodes (3): is_market_open_at_time(), _at(), TestMarketHours

### Community 11 - "Secrets-Isolation Tests"
Cohesion: 0.20
Nodes (3): Regression test for a real bug: importing these modules must never trigger AWS S, influxdb_bucket used to default directly to INFLUXDB_BUCKET (evaluated once, test_candlestick_pipeline_default_bucket_is_not_a_frozen_default_value()

## Knowledge Gaps
- **8 isolated node(s):** `etl-forex-time-series-data`, `src-layout Package Convention`, `Swap-Rate Pipeline`, `Market-Hours Gate`, `ci/vendor/ Workaround Removed` (+3 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **4 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `InfluxDB` connect `Data Model & Schema Layer` to `Candlestick ETL`, `Repo Conventions & Refactor Rationale`, `Positioning ETL (Dormant)`?**
  _High betweenness centrality (0.155) - this node is a cross-community bridge._
- **Why does `SwapRateETL` connect `Candlestick & Forward-Fill Flows` to `Data Model & Schema Layer`, `Forward-Fill DST Tests`, `Forward-Fill DST Logic`?**
  _High betweenness centrality (0.145) - this node is a cross-community bridge._
- **Why does `ForwardFillInator` connect `Positioning ETL (Dormant)` to `Data Model & Schema Layer`, `Economic Calendar ETL (Dormant)`, `Market-Hours Gate`?**
  _High betweenness centrality (0.142) - this node is a cross-community bridge._
- **Are the 5 inferred relationships involving `ForwardFillInator` (e.g. with `forward_fill_task()` and `ForwardFilledCandlestickRecord`) actually correct?**
  _`ForwardFillInator` has 5 INFERRED edges - model-reasoned connections that need verification._
- **Are the 7 inferred relationships involving `SwapRateETL` (e.g. with `SwapRateRecord` and `fetch_swap_rates()`) actually correct?**
  _`SwapRateETL` has 7 INFERRED edges - model-reasoned connections that need verification._
- **Are the 14 inferred relationships involving `SwapRateRecord` (e.g. with `SwapRateETL` and `.make_the_influxdb_dict()`) actually correct?**
  _`SwapRateRecord` has 14 INFERRED edges - model-reasoned connections that need verification._
- **Are the 15 inferred relationships involving `EconomicCalendarEventRecord` (e.g. with `EconomicCalendarETL` and `.make_the_influxdb_dict()`) actually correct?**
  _`EconomicCalendarEventRecord` has 15 INFERRED edges - model-reasoned connections that need verification._