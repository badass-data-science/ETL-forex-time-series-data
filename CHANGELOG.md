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

### Removed
- `requirements.txt` / `requirements-dev.txt`, superseded by
  `pyproject.toml`'s `dependencies` / `optional-dependencies.dev`.
- `ci/vendor/`, no longer needed now that `forex.util` carries these modules
  directly.
- `src/forex/util/secrets_manager.py` and the `boto3` dependency — no longer
  needed now that credentials come from environment variables instead of AWS
  Secrets Manager.

## [0.0.1]

### Added
- GitHub Actions CI pipeline running pytest on Python 3.11 and 3.12 for every
  push and pull request (`.github/workflows/ci.yml`).
- `requirements.txt` / `requirements-dev.txt` dependency manifests.
- `pyproject.toml` project metadata, the source of truth for the version
  number used in this file.
