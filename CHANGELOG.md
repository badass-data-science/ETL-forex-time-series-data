# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Version headers below correspond to the `version` field in `pyproject.toml`.

## [Unreleased]

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
