# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Version headers below correspond to the `version` field in `pyproject.toml`.

## [Unreleased]

## [0.0.1]

### Added
- GitHub Actions CI pipeline running pytest on Python 3.11 and 3.12 for every
  push and pull request (`.github/workflows/ci.yml`).
- `requirements.txt` / `requirements-dev.txt` dependency manifests.
- `pyproject.toml` project metadata, the source of truth for the version
  number used in this file.
