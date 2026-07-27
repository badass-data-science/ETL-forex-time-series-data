# Graph Report - .  (2026-07-27)

## Corpus Check
- 52 files · ~17,357 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 367 nodes · 739 edges · 28 communities (26 shown, 2 thin omitted)
- Extraction: 84% EXTRACTED · 16% INFERRED · 0% AMBIGUOUS · INFERRED: 119 edges (avg confidence: 0.7)
- Token cost: 0 input · 29,124 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Data Model & Schema Layer|Data Model & Schema Layer]]
- [[_COMMUNITY_InfluxDbTool & Candlestick Pipeline|InfluxDbTool & Candlestick Pipeline]]
- [[_COMMUNITY_Repo Docs, Shared Config & Dormant Pipelines|Repo Docs, Shared Config & Dormant Pipelines]]
- [[_COMMUNITY_Market-Hours Gate & DST Regression Tests|Market-Hours Gate & DST Regression Tests]]
- [[_COMMUNITY_Swap-Rate ETL|Swap-Rate ETL]]
- [[_COMMUNITY_Candlestick ETL|Candlestick ETL]]
- [[_COMMUNITY_Prefect Flow Orchestration|Prefect Flow Orchestration]]
- [[_COMMUNITY_Positioning ETL (Dormant)|Positioning ETL (Dormant)]]
- [[_COMMUNITY_ForwardFillInator Core Logic|ForwardFillInator Core Logic]]
- [[_COMMUNITY_Economic Calendar ETL (Dormant)|Economic Calendar ETL (Dormant)]]
- [[_COMMUNITY_Secrets-Isolation Tests|Secrets-Isolation Tests]]
- [[_COMMUNITY_Scheduled Deployment Entrypoint|Scheduled Deployment Entrypoint]]
- [[_COMMUNITY_Project Root|Project Root]]

## God Nodes (most connected - your core abstractions)
1. `CandlestickETL` - 33 edges
2. `ForwardFillInator` - 28 edges
3. `InfluxDbTool` - 27 edges
4. `SwapRateETL` - 24 edges
5. `CandlestickRecord` - 20 edges
6. `SwapRateRecord` - 20 edges
7. `PositioningETL` - 19 edges
8. `EconomicCalendarEventRecord` - 19 edges
9. `PositioningBucketRecord` - 18 edges
10. `CandlestickPipeline` - 15 edges

## Surprising Connections (you probably didn't know these)
- `candlestick_batch_flow()` --shares_data_with--> `TRACKED_INSTRUMENTS`  [INFERRED]
  src/forex/flows/candlestick_flow.py → README.md
- `Why ForwardFillInator Bridges Data Model, Tests, and Flow Communities` --references--> `forward_fill_task()`  [EXTRACTED]
  graphify-out/memory/query_20260727_204957_why_does_forwardfillinator_connect_forward_fill_ds.md → src/forex/flows/forward_fill_flow.py
- `Why SwapRateETL Bridges Data Model, Flow, and Positioning Communities` --references--> `CandlestickETL`  [EXTRACTED]
  graphify-out/memory/query_20260727_204958_why_does_swaprateetl_connect_swap_rate_etl_to_data.md → src/forex/etl/CandlestickETL.py
- `Why SwapRateETL Bridges Data Model, Flow, and Positioning Communities` --references--> `SwapRateRecord`  [EXTRACTED]
  graphify-out/memory/query_20260727_204958_why_does_swaprateetl_connect_swap_rate_etl_to_data.md → src/forex/etl/models.py
- `Why SwapRateETL Bridges Data Model, Flow, and Positioning Communities` --references--> `PositioningETL`  [EXTRACTED]
  graphify-out/memory/query_20260727_204958_why_does_swaprateetl_connect_swap_rate_etl_to_data.md → src/forex/etl/PositioningETL.py

## Import Cycles
- None detected.

## Communities (28 total, 2 thin omitted)

### Community 0 - "Data Model & Schema Layer"
Cohesion: 0.08
Nodes (18): BaseModel, CandlestickRecord, EconomicCalendarEventRecord, MeasurementRecord, PositioningBucketRecord, A scheduled economic calendar event (Finnhub) -- release time, country,     impa, One price bucket from an OANDA order-book or position-book snapshot --     aggre, Per-instrument long/short financing (swap/rollover) rate -- an account-level (+10 more)

### Community 1 - "InfluxDbTool & Candlestick Pipeline"
Cohesion: 0.07
Nodes (10): python_tools_and_shortcuts (former private repo dependency), CandlestickPipeline, Point, Series, _FakeWriteApi, Regression guard for the exact bug the source comment describes: a         non-p, TestInsertDictionaryList, TestTimeColumnToUnixEpochS (+2 more)

### Community 2 - "Repo Docs, Shared Config & Dormant Pipelines"
Cohesion: 0.11
Nodes (36): 65% coverage threshold rationale, DST-aware expected-bar grid fix, economic_calendar_flow dormant (cost-blocked), dropna()/forward-fill ordering bug fix, graphify knowledge graph of codebase, is_forward_filled tagging convention, Keep a Changelog format, Lazy secret-loading __getattr__ pattern (+28 more)

### Community 3 - "Market-Hours Gate & DST Regression Tests"
Cohesion: 0.11
Nodes (15): datetime, is_market_open(), is_market_open_at_time(), _at(), TestMarketHours, TestTimezoneObject, _make_hourly_frame(), _make_local_time_frame() (+7 more)

### Community 4 - "Swap-Rate ETL"
Cohesion: 0.12
Nodes (18): DataFrame, CandlestickETL, _candle(), _FakeIfc, _oanda_and_influx_env(), CandlestickETL builds URLs/queries from oanda_config.OANDA_SERVER and     databa, Minimal stand-in for InfluxDbTool -- only the one method CandlestickETL     actu, One OANDA-shaped candle -- 'bid'/'ask' sub-dicts of o/h/l/c, matching the     re (+10 more)

### Community 5 - "Candlestick ETL"
Cohesion: 0.10
Nodes (20): BaseException, swap-rate pipeline simplicity (no ETL/pipeline/QA hierarchy), _is_not_client_error(), True unless `exc` is an HTTPError with a 4xx status -- those are     determinist, Pure transform, kept separate from any HTTP call so it's directly testable     a, Pulls per-instrument long/short financing (swap/rollover) rates from OANDA's, The financing-rate endpoint is scoped under /v3/accounts/{accountID}/         in, OANDA rejects the ENTIRE batched request (a single HTTP 404,         `INSTRUMENT (+12 more)

### Community 6 - "Prefect Flow Orchestration"
Cohesion: 0.11
Nodes (24): candlestick_batch_flow(), candlestick_flow(), check_market_open_task(), fetch_candlestick_data(), insert_to_influxdb(), Prefect flows: fetch Oanda candlesticks → InfluxDB.  Single instrument (ad-hoc):, Run candlestick_flow for each instrument sequentially., TRACKED_INSTRUMENTS (+16 more)

### Community 7 - "Positioning ETL (Dormant)"
Cohesion: 0.14
Nodes (12): PositioningETL, Pure transform, kept separate from any HTTP call so it's directly testable     a, Pulls OANDA's per-instrument order-book and position-book snapshots (the     lat, _records_from_book_response(), get_oanda_headers(), Oanda REST API, test_compute_positioning_fetches_both_books_for_every_instrument(), test_fit_produces_valid_influxdb_dicts() (+4 more)

### Community 8 - "ForwardFillInator Core Logic"
Cohesion: 0.18
Nodes (6): ForwardFilledCandlestickRecord, Why ForwardFillInator Bridges Data Model, Tests, and Flow Communities, ndarray, ForwardFillInator, # TODO: replace with an explicit holiday calendar to also drop/flag extended, Every timestamp a candle is expected to exist at, from `mn` to `mx`.          Bu

### Community 9 - "Economic Calendar ETL (Dormant)"
Cohesion: 0.17
Nodes (11): EconomicCalendarETL, Pure transform, kept separate from any HTTP call so it's directly testable     a, Pulls scheduled economic calendar events (release time, country, impact,     act, _records_from_calendar_response(), Finnhub API, test_compute_calendar_events_populates_records_from_the_api_response(), test_fit_produces_valid_influxdb_dicts_and_omits_null_fields(), test_records_from_calendar_response_defaults_missing_impact_and_unit() (+3 more)

### Community 10 - "Secrets-Isolation Tests"
Cohesion: 0.17
Nodes (5): Regression test for a real bug: importing these modules must never require the c, influxdb_bucket used to default directly to INFLUXDB_BUCKET (evaluated once, The lazy __getattr__ pattern must raise AttributeError for an unset     required, test_candlestick_pipeline_default_bucket_is_not_a_frozen_default_value(), test_missing_env_var_raises_attribute_error_not_key_error()

## Knowledge Gaps
- **2 isolated node(s):** `etl-forex-time-series-data`, `Semantic Versioning`
  These have ≤1 connection - possible missing edges or undocumented components.
- **2 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `InfluxDbTool` connect `InfluxDbTool & Candlestick Pipeline` to `ForwardFillInator Core Logic`, `Repo Docs, Shared Config & Dormant Pipelines`, `Swap-Rate ETL`, `Prefect Flow Orchestration`?**
  _High betweenness centrality (0.153) - this node is a cross-community bridge._
- **Why does `CandlestickETL` connect `Swap-Rate ETL` to `Data Model & Schema Layer`, `InfluxDbTool & Candlestick Pipeline`, `Repo Docs, Shared Config & Dormant Pipelines`, `Prefect Flow Orchestration`, `Positioning ETL (Dormant)`?**
  _High betweenness centrality (0.148) - this node is a cross-community bridge._
- **Why does `SwapRateETL` connect `Candlestick ETL` to `Data Model & Schema Layer`, `Repo Docs, Shared Config & Dormant Pipelines`, `Prefect Flow Orchestration`, `Positioning ETL (Dormant)`?**
  _High betweenness centrality (0.131) - this node is a cross-community bridge._
- **Are the 15 inferred relationships involving `CandlestickETL` (e.g. with `CandlestickRecord` and `InfluxDbTool`) actually correct?**
  _`CandlestickETL` has 15 INFERRED edges - model-reasoned connections that need verification._
- **Are the 6 inferred relationships involving `ForwardFillInator` (e.g. with `forward_fill_task()` and `ForwardFilledCandlestickRecord`) actually correct?**
  _`ForwardFillInator` has 6 INFERRED edges - model-reasoned connections that need verification._
- **Are the 7 inferred relationships involving `InfluxDbTool` (e.g. with `CandlestickETL` and `CandlestickPipeline`) actually correct?**
  _`InfluxDbTool` has 7 INFERRED edges - model-reasoned connections that need verification._
- **Are the 7 inferred relationships involving `SwapRateETL` (e.g. with `SwapRateRecord` and `fetch_swap_rates()`) actually correct?**
  _`SwapRateETL` has 7 INFERRED edges - model-reasoned connections that need verification._