# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Version headers below correspond to the `version` field in `pyproject.toml`.

## [Unreleased]

### Changed
- `README.md` restructured for a dual audience — functional documentation
  plus a hiring-manager-facing pass demonstrating ETL fluency (Engineering
  highlights, Architecture walkthrough with an embedded PlantUML pipeline-flow
  diagram, an environment-variable Prerequisites table, and an embedded
  screenshot of the codebase's own `graphify` knowledge graph). DST-handling
  is described as a verified, confirmed design property throughout — not
  framed as a bug that was fixed.
- `documentation/blog-post.md` brought back in sync with the current code,
  README, and knowledge graph: replaced all broken `forex-etl-graph-*.png`
  references (an old, never-committed graph screenshot) with
  `documentation/images/knowledge_graph_screenshot.png`; rewrote the
  Pydantic-model, secrets, and Prefect-orchestration sections to match the
  current `MeasurementRecord`/`make_ifc()`/env-var code instead of the
  pre-refactor AWS Secrets Manager and `config_file`-based implementation;
  added `is_forward_filled`/DST-awareness to the forward-fill section.
- `graphify-out/` knowledge graph incrementally updated
  (`/graphify . --update`) to pick up the README/blog-post rewrite and the
  `XAU_USD` removal below: 367→418 nodes, 739→730 edges, 28→34 communities.
  New image nodes for `knowledge_graph_screenshot.png` and
  `pipeline_flow_diagram.png` now reference the real ETL classes they
  depict.

### Removed
- `XAU_USD` (gold) from `TRACKED_INSTRUMENTS` (`candlestick_flow.py`) and the
  mirrored `instrument_list` in `eda_config.py` — this project tracks forex
  pairs only; gold was a commodity CFD included to test whether a different
  asset class carried more signal, but doesn't belong in a forex-only story.
  13 instruments tracked now, down from 14; `README.md` updated to match
  throughout.
- `OANDA_DATE_TIME_FORMAT` environment variable — Oanda's
  `Accept-Datetime-Format` header value is always `'unix'`, so it's now
  hard-coded in `forex/oanda/headers.py` instead of being read from the
  environment. No longer a required env var in `oanda_config.py` or
  `README.md`'s Prerequisites table.
- `SwapRateETL.get_account_id()`'s `/v3/accounts` auto-resolution fallback —
  `OANDA_ACCOUNT_ID` is now a required env var (`oanda_config.py`), so
  `get_account_id()` just returns it directly instead of falling back to an
  extra API call when unset. `README.md`'s Prerequisites table updated to
  match.

## [0.0.3]

### Added
- Linting (`ruff check`), formatting (`ruff format`), type checking
  (`mypy`), and coverage (`pytest-cov`), all configured in `pyproject.toml`
  and run in CI as a separate `lint` job. `ruff`'s line length is set to 120
  (not the default 88) and quote style kept single (not ruff's double-quote
  default) to match this codebase's deliberately dense comments/docstrings
  and existing string style rather than reformatting every line for its own
  sake; `SIM103` is disabled where inlining would hurt readability (see
  `critical_timezone.py`'s guard clauses). Coverage is gated at 65%
  (current: ~70%) — Prefect flow/task orchestration wrappers are
  intentionally not chased to 100%; see `AGENTS.md`.
- README badges (CI status, Python version, license).
- Dedicated test coverage for `CandlestickETL` (`tests/test_candlestick_etl.py`)
  and `InfluxDbTool` (`tests/test_influxdb_tool.py`) — previously the only
  ETL class and the only util module with zero direct test coverage.
  A regression test for the `AttributeError`/`KeyError` config-loading fix
  was also added (`test_secrets_isolation.py`).
- `MeasurementRecord`, a shared base class in `models.py` for
  `to_influx_dict()` — every one of the 5 Pydantic record models previously
  reimplemented an identical tag/field/time split by hand.
- `forex/flows/_common.py`'s `make_ifc()` — the identical `_make_ifc()`
  helper was duplicated across all 5 flow modules; now defined once.

### Changed
- `InfluxDbTool` (owned by this repo, not vendored) renamed its
  SCREAMING_CASE constructor/method parameters to standard snake_case
  (`INFLUXDB_URL` → `url`, `ALLOWED_TAGS` → `allowed_tags`, etc.) and picked
  up full type hints throughout.
- `CandlestickETL.make_the_InfluxDB_dict()` renamed to
  `make_the_influxdb_dict()` for consistency with every other ETL class's
  identically-named method.
- `ifc` parameters (`CandlestickETL`, `CandlestickPipeline`,
  `ForwardFillInator`) are now typed as `InfluxDbTool` (or `InfluxDbTool |
  None` where tests legitimately construct one without a live connection),
  closing a real gap where mypy couldn't check these call sites at all.

### Fixed
- A handful of real issues `ruff`/`mypy` surfaced: two dead imports
  (`is_market_open_at_time` in `ForwardFillInator.py`, `pytest` in
  `test_critical_timezone.py`), a `zip()` without `strict=`, an imprecise
  `frozenset[str]`-vs-`set` type hint on `CandlestickPipeline.__init__`, and
  a `plt.yticks` call passing `bool` labels instead of `str`.
- `InfluxDbTool.validate_point` was missing `@staticmethod` despite having
  no `self` and always being called as `InfluxDbTool.validate_point(...)` —
  latent bug waiting for someone to call it on an instance instead.
- `InfluxDbTool.insert_dictionary_list`'s `write_precision_str` argument was
  never actually passed through to `validate_point` (which always used its
  own default), so a caller overriding write precision would get a mismatch
  between each point's embedded precision and the batch write's declared
  precision. Dormant in practice — nothing in this repo currently overrides
  the default — but fixed as part of the `InfluxDbTool` cleanup.

## [0.0.2]

### Added
- `AGENTS.md`, orienting notes for AI coding assistants and future
  contributors (layout, test invocation, and repo-specific gotchas like the
  lazy secret-loading pattern and the dormant economic-calendar/positioning
  pipelines). Linked from the top of `README.md`.
- A [graphify](https://github.com/safishamsi/graphify) knowledge graph of the
  codebase: `graphify-out/graph.json` (raw graph), `graphify-out/graph.html`
  (interactive browser view), and `graphify-out/GRAPH_REPORT.md` (audit
  report with god nodes, cross-community bridges, and suggested questions).
  Working state (cache, cost tracker, manifest, work-memory) stays local —
  see `.gitignore`.

### Changed
- Reorganized the package into a PyPI-publication-friendly `src/forex/` layout
  (was `etl/`, `flows/`, `oanda/`, `eda/`, `critical_timezone.py` at the repo
  root), with `__init__.py` added to every package directory.
- `pyproject.toml` now declares a full build (`hatchling`, wheel packaging,
  license, classifiers, project URLs); the package installs and builds as a
  real wheel (`pip install -e ".[dev]"`, `python -m build`).
- CI installs the package itself (`pip install -e ".[dev]"`) instead of a
  separate requirements file, and no longer needs the "checkout to a `forex/`
  directory" workaround now that the package is genuinely importable once
  installed.
- Ported the three modules this project used from the private
  `python_tools_and_shortcuts` repo (`get_secret`, `InfluxDbTool`,
  `time_conversions`) into `src/forex/util/`. The package no longer depends
  on that repo at all, in CI or otherwise; the old CI-only `ci/vendor/`
  workaround is removed as a result.
- Replaced AWS Secrets Manager with plain environment variables as the
  source of credentials. `database_config` and `finnhub_config` now read
  `INFLUXDB_URL`/`INFLUXDB_TOKEN`/`INFLUXDB_ORG`/`INFLUXDB_BUCKET` and
  `FINNHUB_API_KEY` directly from `os.environ`, keeping the same lazy
  module-level `__getattr__` access pattern (`database_config.INFLUXDB_URL`,
  never a top-level `from ... import`) so nothing is resolved just by
  importing a module.
- Replaced the `$OANDA_CONFIG_FILE`-pointed JSON config with individual
  environment variables: `OANDA_SERVER`, `OANDA_TOKEN`,
  `OANDA_DATE_TIME_FORMAT`, and optional `OANDA_ACCOUNT_ID`. New
  `oanda/config/oanda_config.py` lazy-loads them with the same
  `__getattr__` pattern as `database_config`/`finnhub_config`.
  `CandlestickETL`, `SwapRateETL`, `PositioningETL`, `CandlestickPipeline`,
  and every flow that took a `config_file`/`OANDA_CONFIG_FILE` parameter no
  longer accept or need one. `get_oanda_headers()` now takes no arguments.
- Fixed a latent bug in the lazy `__getattr__` config pattern (affects
  `database_config`, `finnhub_config`, and the new `oanda_config`): an
  unset required env var now raises `AttributeError`, not a bare
  `KeyError` — the previous behavior silently broke `hasattr()`,
  `getattr(x, name, default)`, and pytest's `monkeypatch.setattr(...,
  raising=False)`, all of which rely on catching `AttributeError`
  specifically to mean "not found."

### Removed
- `requirements.txt` / `requirements-dev.txt`, superseded by
  `pyproject.toml`'s `dependencies` / `optional-dependencies.dev`.
- `ci/vendor/`, no longer needed now that `forex.util` carries these modules
  directly.
- `src/forex/util/secrets_manager.py` and the `boto3` dependency — no longer
  needed now that credentials come from environment variables instead of AWS
  Secrets Manager.
- The `$OANDA_CONFIG_FILE`-loading code path (file open + `json.load`) in
  `CandlestickETL`, `SwapRateETL`, `PositioningETL`, and `serve.py` — Oanda
  credentials are read directly from environment variables now.

## [0.0.1]

### Added
- GitHub Actions CI pipeline running pytest on Python 3.11 and 3.12 for every
  push and pull request (`.github/workflows/ci.yml`).
- `requirements.txt` / `requirements-dev.txt` dependency manifests.
- `pyproject.toml` project metadata, the source of truth for the version
  number used in this file.
