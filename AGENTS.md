# AGENTS.md

Guidance for AI coding assistants (and future human contributors) working in
this repo. See `README.md` for the full architecture writeup; this file is
the fast-start version plus the gotchas that aren't obvious from the code.

## What this is

Fetches OHLCV candlestick, swap-rate, economic-calendar, and positioning data
from the Oanda REST API (economic calendar comes from Finnhub instead) and
writes it to InfluxDB via Prefect flows.

## Layout

```
src/forex/       # the installable package (import as `forex.*`)
├── critical_timezone.py
├── etl/         # fetch/transform classes + Pydantic schemas (models.py)
├── flows/       # Prefect flow wrappers around etl/
├── oanda/       # Oanda API auth/config helpers
├── eda/         # exploratory-analysis config constants
└── util/        # in-house InfluxDB client, time constants
                 # (no external private-repo dependency — see below)
tests/           # pytest, no network/AWS/InfluxDB required to run
pyproject.toml   # single source of truth for version + dependencies
CHANGELOG.md     # Keep a Changelog; headers track pyproject.toml's version
```

This is a **src-layout, PyPI-publishable package** (`pip install -e ".[dev]"`,
`python -m build`). Every internal import uses the installed package name:
`from forex.etl.models import CandlestickRecord`, never a relative path or a
sys.path hack.

## Running tests

```
pip install -e ".[dev]"
pytest tests -v
```

No credentials, network access, or running InfluxDB instance needed — the
whole suite mocks external calls. CI (`.github/workflows/ci.yml`) runs this
exact sequence on Python 3.11 and 3.12 for every push/PR.

## Linting, formatting, and type checking

```
ruff check src tests           # lint
ruff format --check src tests  # format check (ruff format src tests to fix)
mypy src/forex tests           # type check
```

All three are configured in `pyproject.toml` (`[tool.ruff]`, `[tool.mypy]`)
and run in CI as a separate `lint` job. `line-length = 120`, not the
ruff/black default of 88 — this codebase's comments and docstrings are
deliberately dense/explanatory, and reflowing them to 88 columns would make
them harder to read, not easier. `SIM103` is disabled for the same
readability reason (see `critical_timezone.py`'s guard clauses).
`quote-style = "single"` in `[tool.ruff.format]` matches the codebase's
existing convention rather than rewriting every string literal to double
quotes. Mypy uses `ignore_missing_imports` rather than pinning
`pandas-stubs` — pandas/numpy interop typing is noisy with real false
positives (ExtensionArray vs ndarray, Index bitwise ops) that aren't worth
chasing in a repo this size. A couple of `# type: ignore[arg-type]` comments
remain where a third-party library's own stubs are imprecise (Prefect's
`to_deployment()` sync/async overload in `serve.py`; `influxdb_client`'s
`WritePrecision` being a plain `str` constant at runtime but a stricter
Literal in `write()`'s signature) — both documented inline at the point of
use.

## Test coverage

```
pytest tests --cov=forex --cov-report=term-missing
```

`[tool.coverage.report]` sets `fail_under = 65`, reflecting the current
honest baseline (~70%): Prefect flow/task wrappers and `serve.py` are thin
orchestration glue over already-unit-tested ETL classes, and mocking
Prefect's decorators just to hit a coverage number isn't worth it — they're
exercised via `test_secrets_isolation.py`'s import-time checks and manual/
staging runs instead. The threshold exists to catch regressions in the parts
that *are* unit tested (all the ETL classes, `models.py`, `InfluxDbTool`,
`ForwardFillInator`), not to chase 100% on glue code.

## Conventions and gotchas

- **Config modules lazy-load secrets.** `etl/config/database_config.py`,
  `etl/config/finnhub_config.py`, and `oanda/config/oanda_config.py` all
  resolve environment variables via a module-level `__getattr__`, only on
  attribute access, and raise `AttributeError` (never a bare `KeyError`) for
  an unset required var, so `hasattr()`/`getattr(x, name, default)`/pytest's
  `monkeypatch.setattr(..., raising=False)` all still work correctly. Always
  reference them as `database_config.INFLUXDB_URL` (fresh resolution each
  time), **never** `from database_config import INFLUXDB_URL` — the latter
  freezes the resolved value into the importing module's namespace at import
  time, permanently, with no way to swap it later (including during `pytest`
  collection). See `tests/test_secrets_isolation.py` for the
  regression test and the real incident this guards against.
- **No external private-repo dependency.** `forex/util/` is a self-contained,
  in-house copy of the InfluxDB client and time-unit constants this project
  needs — not a vendored copy of someone else's package. Credentials come
  from environment variables (see the lazy-config bullet above), not a
  secrets-manager client. Don't reintroduce an import from outside this repo
  for these; add to `forex/util/` instead.
- **`CHANGELOG.md` must be updated whenever code changes.** Add an entry
  under `[Unreleased]` (Keep a Changelog categories: Added/Changed/Fixed/
  Removed) describing the change. Version headers correspond to
  `pyproject.toml`'s `version` field — don't invent a new version number
  without also bumping `pyproject.toml`.
- **Two pipelines are intentionally not live**: `economic_calendar_flow`
  (Finnhub's free tier 403s on the calendar endpoint) and `positioning_flow`
  (Oanda discontinued the order-book/position-book endpoints for retail
  accounts). Both are fully implemented and tested — don't "fix" them by
  silently swallowing the error; they're dormant on cost/access, not broken.
  See `README.md`'s Architecture section for the full story before touching
  either.
- **DST-aware grid logic in `ForwardFillInator`** is fixed but subtle: H4/D
  candles anchor to local wall-clock time, not a fixed UTC offset. Read the
  comment block in `README.md` (search "DST-aware expected-bar grid") before
  changing anything in `compute_df_all_time_diff_market_open` or the grid
  construction.
- **`is_forward_filled`** distinguishes real bars from imputed ones — a
  forward-filled bar has zero return/volatility by construction. Don't strip
  this tag or treat forward-filled bars as equivalent to real data anywhere
  downstream.
- **Every Pydantic record model subclasses `MeasurementRecord`**
  (`etl/models.py`) for `to_influx_dict()`. Adding a new measurement means
  declaring `TAGS`/`MEASUREMENT`/`FIELDS` on a `MeasurementRecord` subclass,
  not hand-writing another tag/field/time split — that duplication existed
  identically across all 5 models before it was consolidated.
- **`forex.flows._common.make_ifc()` is the one place `InfluxDbTool` gets
  constructed from `database_config`.** Every flow module imports it rather
  than redefining its own `_make_ifc()` — that was duplicated 5x before
  consolidation. Add new flows by importing `make_ifc`, not by copy-pasting
  the helper again.
- **`graphify-out/` holds a knowledge graph of this codebase**
  ([graphify](https://github.com/safishamsi/graphify)). Only `graph.json`,
  `graph.html`, and `GRAPH_REPORT.md` are tracked (see `.gitignore`) —
  cache/, cost.json, manifest.json, memory/, and reflections/ are local
  working state, regenerated by `/graphify . --update`. Treat community
  *labels* in the graph as structurally-derived groupings, not authoritative
  architecture: e.g. `swap_rate_flow.py` clusters with the candlestick/
  forward-fill flows purely because all `flows/*.py` share Prefect-wrapper
  boilerplate, even though `README.md` explicitly calls swap-rate an
  architecturally simpler, different kind of pipeline. Cross-check a
  community assignment against `README.md`/this file before treating it as
  a design claim.
  Two more caveats found while tracing bridge nodes after the
  professional-review `--update`: (1) `CandlestickETL`'s bridge to
  `SwapRateETL` in the graph is doc-citation-only (both are mentioned in
  `README.md`, but never call each other) — don't read it as a code
  dependency. (2) The same real entity can appear as two separate graph
  nodes when both AST and semantic extraction touch it — e.g. `make_ifc()`
  exists once from the real code (`_common.py`) and once from a semantic
  node created purely because `AGENTS.md`'s prose names it. Harmless, but
  don't be surprised by an apparent duplicate. Also: **community *IDs* are
  not stable across `--update` runs** — Louvain re-clusters from scratch
  each time, so community 3 today is not the same grouping as community 3
  before the last update. Re-derive labels from actual member content after
  every update rather than assuming an ID's label still fits. (3) Diagram/
  screenshot images (`documentation/images/*.png`) get their own semantic
  extraction pass and can produce nodes that reference the real code
  entities they depict — e.g. `knowledge_graph_screenshot.png`'s legend
  entries pointing back at `CandlestickETL`, `InfluxDbTool`, etc. These are
  self-referential (the graph depicting itself) and harmless, but they
  inflate a node's apparent community fan-out — don't read "N communities
  reference this class" as N genuine architectural relationships without
  checking whether some of those edges originate from an image of the graph
  itself. As of the last `--update`, the graph has 418 nodes, 730 edges, and
  34 communities — these numbers drift on every update; don't hardcode them
  elsewhere without re-checking `graphify-out/GRAPH_REPORT.md`.
