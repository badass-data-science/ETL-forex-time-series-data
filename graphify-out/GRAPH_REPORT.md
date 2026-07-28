# Graph Report - .  (2026-07-28)

## Corpus Check
- 12 files · ~51,972 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 418 nodes · 730 edges · 34 communities (31 shown, 3 thin omitted)
- Extraction: 78% EXTRACTED · 22% INFERRED · 0% AMBIGUOUS · INFERRED: 163 edges (avg confidence: 0.74)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Data Model & Schema Layer|Data Model & Schema Layer]]
- [[_COMMUNITY_Swap-Rate ETL & GraphBlog Narrative|Swap-Rate ETL & Graph/Blog Narrative]]
- [[_COMMUNITY_Market-Hours Gate & DST Regression|Market-Hours Gate & DST Regression]]
- [[_COMMUNITY_InfluxDbTool & Tests|InfluxDbTool & Tests]]
- [[_COMMUNITY_Candlestick ETL|Candlestick ETL]]
- [[_COMMUNITY_Prefect Flow Orchestration & make_ifc()|Prefect Flow Orchestration & make_ifc()]]
- [[_COMMUNITY_ForwardFillInator Core Logic|ForwardFillInator Core Logic]]
- [[_COMMUNITY_Knowledge Graph Screenshot & Legend|Knowledge Graph Screenshot & Legend]]
- [[_COMMUNITY_Economic Calendar ETL (Dormant)|Economic Calendar ETL (Dormant)]]
- [[_COMMUNITY_Positioning ETL (Dormant)|Positioning ETL (Dormant)]]
- [[_COMMUNITY_InfluxDbTool & Candlestick Pipeline|InfluxDbTool & Candlestick Pipeline]]
- [[_COMMUNITY_Candlestick Flow & Changelog Convention|Candlestick Flow & Changelog Convention]]
- [[_COMMUNITY_Dormant Pipelines & PositioningSwap-Rate Buckets|Dormant Pipelines & Positioning/Swap-Rate Buckets]]
- [[_COMMUNITY_Env-Var Secrets & Repo Layout|Env-Var Secrets & Repo Layout]]
- [[_COMMUNITY_Flux Query Example|Flux Query Example]]
- [[_COMMUNITY_CI Workflow|CI Workflow]]
- [[_COMMUNITY_Scheduled Deployment Entrypoint|Scheduled Deployment Entrypoint]]
- [[_COMMUNITY_Pipeline Flow Diagram|Pipeline Flow Diagram]]
- [[_COMMUNITY_Project Root|Project Root]]

## God Nodes (most connected - your core abstractions)
1. `CandlestickETL` - 36 edges
2. `ForwardFillInator` - 32 edges
3. `InfluxDbTool` - 31 edges
4. `SwapRateETL` - 27 edges
5. `CandlestickRecord` - 20 edges
6. `SwapRateRecord` - 19 edges
7. `EconomicCalendarEventRecord` - 19 edges
8. `PositioningETL` - 18 edges
9. `PositioningBucketRecord` - 18 edges
10. `CandlestickPipeline` - 15 edges

## Surprising Connections (you probably didn't know these)
- `Candlestick ETL community (31 nodes, yellow)` --references--> `CandlestickETL`  [INFERRED]
  documentation/images/knowledge_graph_screenshot.png → src/forex/etl/CandlestickETL.py
- `Positioning ETL (Dormant) community (23 nodes, pink)` --references--> `PositioningETL`  [INFERRED]
  documentation/images/knowledge_graph_screenshot.png → src/forex/etl/PositioningETL.py
- `Swap-Rate ETL community (32 nodes, green)` --references--> `SwapRateETL`  [INFERRED]
  documentation/images/knowledge_graph_screenshot.png → src/forex/etl/SwapRateETL.py
- `ForwardFillInator Core Logic community (19 nodes, brown)` --references--> `ForwardFillInator`  [INFERRED]
  documentation/images/knowledge_graph_screenshot.png → src/forex/etl/pipelines/ForwardFillInator.py
- `InfluxDB ('economic-calendar-event')` --conceptually_related_to--> `InfluxDbTool`  [INFERRED]
  documentation/images/pipeline_flow_diagram.png → src/forex/util/influxdb_tool.py

## Import Cycles
- None detected.

## Communities (34 total, 3 thin omitted)

### Community 0 - "Data Model & Schema Layer"
Cohesion: 0.07
Nodes (21): MeasurementRecord Subclassing Convention, BaseModel, MeasurementRecord Base Class Added, CandlestickRecord, EconomicCalendarEventRecord, MeasurementRecord, PositioningBucketRecord, A scheduled economic calendar event (Finnhub) -- release time, country,     impa (+13 more)

### Community 1 - "Swap-Rate ETL & Graph/Blog Narrative"
Cohesion: 0.07
Nodes (32): graphify-out Knowledge Graph & Community Caveats, BaseException, graphify Knowledge Graph Added (v0.0.2), AI Use Statement (Claude Code + graphify collaboration), What the Knowledge Graph Reveals, _is_not_client_error(), True unless `exc` is an HTTPError with a 4xx status -- those are     determinist, Pure transform, kept separate from any HTTP call so it's directly testable     a (+24 more)

### Community 2 - "Market-Hours Gate & DST Regression"
Cohesion: 0.11
Nodes (14): datetime, is_market_open_at_time(), _at(), TestMarketHours, TestTimezoneObject, _make_hourly_frame(), _make_local_time_frame(), Regression coverage for a real, confirmed-in-production bug: H4/D candles     ar (+6 more)

### Community 3 - "InfluxDbTool & Tests"
Cohesion: 0.09
Nodes (10): InfluxDbTool snake_case Parameter Rename, Why InfluxDB Over Relational DB, Point, Series, _FakeWriteApi, Regression guard for the exact bug the source comment describes: a         non-p, TestInsertDictionaryList, TestTimeColumnToUnixEpochS (+2 more)

### Community 4 - "Candlestick ETL"
Cohesion: 0.12
Nodes (20): DataFrame, CandlestickETL Fetch/Retry Narrative, CandlestickETL, Idempotent and Resumable Flow Design, _candle(), _FakeIfc, _oanda_and_influx_env(), CandlestickETL builds URLs/queries from oanda_config.OANDA_SERVER and     databa (+12 more)

### Community 5 - "Prefect Flow Orchestration & make_ifc()"
Cohesion: 0.10
Nodes (25): Intentionally Dormant Pipelines (calendar/positioning), make_ifc() Single Construction Site Convention, make_ifc() Consolidation Added, Market-Hours Gate (check_market_open_task), Prefect Flow/Task Orchestration Design, make_ifc(), economic_calendar_flow(), fetch_economic_calendar() (+17 more)

### Community 6 - "ForwardFillInator Core Logic"
Cohesion: 0.13
Nodes (12): DST-Aware Expected-Bar Grid, is_forward_filled Tag Semantics, Forward-Fill Lookahead Prevention Narrative, ForwardFilledCandlestickRecord, Why ForwardFillInator Bridges Data Model, Tests, and Flow Communities, ndarray, ForwardFillInator, # TODO: replace with an explicit holiday calendar to also drop/flag extended (+4 more)

### Community 7 - "Knowledge Graph Screenshot & Legend"
Cohesion: 0.08
Nodes (18): Candlestick ETL community (31 nodes, yellow), Community Legend Panel (28 communities, node/edge counts), Data Model & Schema Layer community (60 nodes, blue), ForwardFillInator Core Logic community (19 nodes, brown), InfluxDbTool & Candlestick Pipeline community (42 nodes, orange), Market-Hours Gate & DST Regre... community (40 nodes, teal), Package Init Marker singleton communities (repeated 1-node communities, likely __init__.py files), Positioning ETL (Dormant) community (23 nodes, pink) (+10 more)

### Community 8 - "Economic Calendar ETL (Dormant)"
Cohesion: 0.12
Nodes (16): EconomicCalendarETL, Pure transform, kept separate from any HTTP call so it's directly testable     a, Pulls scheduled economic calendar events (release time, country, impact,     act, _records_from_calendar_response(), Economic Calendar ETL (Dormant) community (18 nodes, gray), EconomicCalendarETL (fetches upcoming scheduled economic release events), EconomicCalendarEventRecord (Pydantic schema), Finnhub API (NOT Oanda -- separate provider/credential) (+8 more)

### Community 9 - "Positioning ETL (Dormant)"
Cohesion: 0.15
Nodes (11): PositioningETL, Pure transform, kept separate from any HTTP call so it's directly testable     a, Pulls OANDA's per-instrument order-book and position-book snapshots (the     lat, _records_from_book_response(), get_oanda_headers(), test_compute_positioning_fetches_both_books_for_every_instrument(), test_fit_produces_valid_influxdb_dicts(), test_get_order_book_and_position_book_build_the_expected_urls() (+3 more)

### Community 10 - "InfluxDbTool & Candlestick Pipeline"
Cohesion: 0.17
Nodes (9): is_market_open(), CandlestickPipeline (orchestrates ETL, QA, InfluxDB write), CandlestickRecord (Pydantic schema), ForwardFilledCandlestickRecord (Pydantic; forward-filled schema), ForwardFillInator (fills market-closed gaps with last known price), InfluxDB ('candlestick'), InfluxDB ('forward-filled candlestick'), Annotation: ForwardFillInator tags each bar is_forward_filled True/False; DST-aware expected-bar grid (local wall-clock time, not fixed UTC step) (+1 more)

### Community 11 - "Candlestick Flow & Changelog Convention"
Cohesion: 0.24
Nodes (12): CHANGELOG.md Update Convention, XAU_USD Removal from TRACKED_INSTRUMENTS, candlestick_batch_flow(), candlestick_flow(), check_market_open_task(), fetch_candlestick_data(), insert_to_influxdb(), Prefect flows: fetch Oanda candlesticks → InfluxDB.  Single instrument (ad-hoc): (+4 more)

### Community 12 - "Dormant Pipelines & Positioning/Swap-Rate Buckets"
Cohesion: 0.20
Nodes (10): CandlestickETL (fetches, retries, validates), InfluxDB ('positioning-bucket'), InfluxDB ('swap-rate'), Annotation: Positioning status not currently ingested -- OANDA discontinued orderBook/positionBook endpoints for retail accounts, now enterprise-only; code complete and unit-tested, Annotation: swap-rate is a single current snapshot, not a historical time series -- no ETL/pipeline/QA class hierarchy needed, Oanda REST API, PositioningBucketRecord (Pydantic schema), PositioningETL (fetches order-book + position-book snapshots) (+2 more)

### Community 13 - "Env-Var Secrets & Repo Layout"
Cohesion: 0.22
Nodes (9): forex Package (src-layout), Lazy Secret Loading via Module __getattr__, No External Private-Repo Dependency (forex/util), AttributeError-not-KeyError Config Fix, AWS Secrets Manager Replaced with Env Vars, Lint/Format/Type-Check/Coverage CI Job, test_secrets_isolation.py Regression Test Added, src/forex/ Layout Reorganization (v0.0.2) (+1 more)

### Community 14 - "Flux Query Example"
Cohesion: 0.47
Nodes (6): "candlestick" measurement, EUR/USD H1 candlestick query result table, "forex" InfluxDB bucket, Flux pivot() function, Flux Query: forex bucket EUR/USD H1 pivot, Flux Query Screenshot (InfluxDB Data Explorer)

### Community 15 - "CI Workflow"
Cohesion: 0.67
Nodes (3): CI Workflow (ci.yml), CI lint job (ruff/mypy), CI test job (pytest matrix)

## Ambiguous Edges - Review These
- `candlestick_flow()` → `Prefect Flow Orchestration community (28 nodes, purple)`  [AMBIGUOUS]
  documentation/images/knowledge_graph_screenshot.png · relation: conceptually_related_to

## Knowledge Gaps
- **28 isolated node(s):** `CI lint job (ruff/mypy)`, `CI test job (pytest matrix)`, `etl-forex-time-series-data`, `Flux Query Screenshot (InfluxDB Data Explorer)`, `Graphify Knowledge Graph Visualization (Self-Portrait of Repo)` (+23 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `candlestick_flow()` and `Prefect Flow Orchestration community (28 nodes, purple)`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `InfluxDbTool` connect `InfluxDbTool & Tests` to `Swap-Rate ETL & Graph/Blog Narrative`, `Candlestick ETL`, `Prefect Flow Orchestration & make_ifc()`, `ForwardFillInator Core Logic`, `Knowledge Graph Screenshot & Legend`, `Economic Calendar ETL (Dormant)`, `InfluxDbTool & Candlestick Pipeline`, `Dormant Pipelines & Positioning/Swap-Rate Buckets`?**
  _High betweenness centrality (0.191) - this node is a cross-community bridge._
- **Why does `CandlestickETL` connect `Candlestick ETL` to `Data Model & Schema Layer`, `Swap-Rate ETL & Graph/Blog Narrative`, `Market-Hours Gate & DST Regression`, `InfluxDbTool & Tests`, `Prefect Flow Orchestration & make_ifc()`, `Knowledge Graph Screenshot & Legend`, `InfluxDbTool & Candlestick Pipeline`, `Candlestick Flow & Changelog Convention`, `Dormant Pipelines & Positioning/Swap-Rate Buckets`?**
  _High betweenness centrality (0.186) - this node is a cross-community bridge._
- **Why does `SwapRateETL` connect `Swap-Rate ETL & Graph/Blog Narrative` to `Data Model & Schema Layer`, `Candlestick Flow & Changelog Convention`, `Dormant Pipelines & Positioning/Swap-Rate Buckets`, `Knowledge Graph Screenshot & Legend`?**
  _High betweenness centrality (0.145) - this node is a cross-community bridge._
- **Are the 16 inferred relationships involving `CandlestickETL` (e.g. with `CandlestickRecord` and `InfluxDbTool`) actually correct?**
  _`CandlestickETL` has 16 INFERRED edges - model-reasoned connections that need verification._
- **Are the 8 inferred relationships involving `ForwardFillInator` (e.g. with `forward_fill_task()` and `ForwardFillInator Core Logic community (19 nodes, brown)`) actually correct?**
  _`ForwardFillInator` has 8 INFERRED edges - model-reasoned connections that need verification._
- **Are the 14 inferred relationships involving `InfluxDbTool` (e.g. with `Why InfluxDB Over Relational DB` and `CandlestickETL`) actually correct?**
  _`InfluxDbTool` has 14 INFERRED edges - model-reasoned connections that need verification._