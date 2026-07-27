# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Version headers below correspond to the `version` field in `pyproject.toml`.

## [Unreleased]

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

### Removed
- `requirements.txt` / `requirements-dev.txt`, superseded by
  `pyproject.toml`'s `dependencies` / `optional-dependencies.dev`.

## [0.0.1]

### Added
- GitHub Actions CI pipeline running pytest on Python 3.11 and 3.12 for every
  push and pull request (`.github/workflows/ci.yml`).
- `requirements.txt` / `requirements-dev.txt` dependency manifests.
- `pyproject.toml` project metadata, the source of truth for the version
  number used in this file.
