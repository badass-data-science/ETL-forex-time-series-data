"""Regression test for a real bug: importing these modules must never trigger AWS
Secrets Manager resolution on its own -- only actually calling a function that needs
a live connection should.

database_config lazy-loads credentials via a module-level __getattr__ triggered on
attribute access. `from database_config import INFLUXDB_URL` at another module's top
level fires that trigger immediately, at IMPORT time, binding the resolved secret
into that module's namespace once, permanently, for the life of the process --
merely importing (or collecting, via pytest) any of these modules used to be enough
to eagerly resolve real credentials, and no later test monkeypatch of
database_config.get_secret could undo an already-executed import. Discovered via a
downstream consumer (forex-ML) whose "flaky" integration test turned out to be
silently querying a real production InfluxDB instead of its intended local Docker
container, because this eager import had already frozen the real credentials.

The fix: reference database_config as a module and resolve attributes fresh at the
point of use, rather than importing the values themselves (or using them as
mutable-default-style class/function default parameter values, which are evaluated
once at definition time -- the same bug wearing a different hat).
"""

from __future__ import annotations


def test_candlestick_flow_does_not_freeze_secrets_at_import_time():
    from forex.flows import candlestick_flow

    for name in ("INFLUXDB_URL", "INFLUXDB_TOKEN", "INFLUXDB_ORG", "INFLUXDB_BUCKET"):
        assert not hasattr(candlestick_flow, name)
    assert hasattr(candlestick_flow, "database_config")


def test_forward_fill_flow_does_not_freeze_secrets_at_import_time():
    from forex.flows import forward_fill_flow

    for name in ("INFLUXDB_URL", "INFLUXDB_TOKEN", "INFLUXDB_ORG", "INFLUXDB_BUCKET"):
        assert not hasattr(forward_fill_flow, name)
    assert hasattr(forward_fill_flow, "database_config")


def test_swap_rate_flow_does_not_freeze_secrets_at_import_time():
    from forex.flows import swap_rate_flow

    for name in ("INFLUXDB_URL", "INFLUXDB_TOKEN", "INFLUXDB_ORG", "INFLUXDB_BUCKET"):
        assert not hasattr(swap_rate_flow, name)
    assert hasattr(swap_rate_flow, "database_config")


def test_candlestick_etl_does_not_freeze_secrets_at_import_time():
    from forex.etl import CandlestickETL

    assert not hasattr(CandlestickETL, "INFLUXDB_BUCKET")
    assert hasattr(CandlestickETL, "database_config")


def test_candlestick_pipeline_default_bucket_is_not_a_frozen_default_value():
    """influxdb_bucket used to default directly to INFLUXDB_BUCKET (evaluated once
    at class-definition/import time, the same bug in a different shape -- a mutable
    default parameter value). It must now default to None and resolve lazily inside
    __init__ instead."""
    import inspect

    from forex.etl.pipelines.CandlestickPipeline import CandlestickPipeline

    signature = inspect.signature(CandlestickPipeline.__init__)
    assert signature.parameters["influxdb_bucket"].default is None
