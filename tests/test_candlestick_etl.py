from __future__ import annotations

import pandas as pd
import pytest

from forex.etl.CandlestickETL import CandlestickETL
from forex.etl.config import database_config
from forex.oanda.config import oanda_config


@pytest.fixture(autouse=True)
def _oanda_and_influx_env(monkeypatch):
    """CandlestickETL builds URLs/queries from oanda_config.OANDA_LIVE_SERVER and
    database_config.INFLUXDB_BUCKET even when the test only cares about pure
    dataframe logic -- set both for every test in this file rather than
    repeating it per-test."""
    monkeypatch.setattr(oanda_config, 'OANDA_LIVE_SERVER', 'https://example.test', raising=False)
    monkeypatch.setattr(database_config, 'INFLUXDB_BUCKET', 'test-bucket', raising=False)


def _candle(time_s: int, complete: bool = True, bid_o: str = '1.1000', ask_o: str = '1.1002') -> dict:
    """One OANDA-shaped candle -- 'bid'/'ask' sub-dicts of o/h/l/c, matching the
    real /v3/instruments/{instrument}/candles response CandlestickETL parses."""
    return {
        'complete': complete,
        'volume': 100,
        'time': f'{time_s}.000000000',
        'bid': {'o': bid_o, 'h': '1.1010', 'l': '1.0990', 'c': '1.1005'},
        'ask': {'o': ask_o, 'h': '1.1012', 'l': '1.0992', 'c': '1.1007'},
    }


def test_get_instrument_candlesticks_builds_the_expected_url(monkeypatch):
    monkeypatch.setattr(oanda_config, 'OANDA_LIVE_SERVER', 'https://example.test', raising=False)
    etl = CandlestickETL('EUR/USD', 'H1', ifc=None, count=500, price_types='BA')

    captured = {}

    def fake_fetch(url):
        captured['url'] = url
        return {'candles': []}

    monkeypatch.setattr(etl, '_fetch_from_api', fake_fetch)
    etl.get_instrument_candlesticks(1_700_000_000)

    assert captured['url'] == (
        'https://example.test/v3/instruments/EUR_USD/candles?count=500&price=BA&granularity=H1&to=1700000000'
    )


def test_compute_candle_features_paginates_until_a_short_page(monkeypatch):
    """OANDA pages backward in time: a full page (== count) means there might be
    more history; a short page (< count) means this is the last one."""
    etl = CandlestickETL('EUR/USD', 'H1', ifc=None, count=2)
    etl.start_time = 0  # accept everything -- pagination isn't testing the cutoff here

    pages = [
        [_candle(1_700_010_000), _candle(1_700_000_000)],  # full page (== count) -> keep paging
        [_candle(1_699_990_000)],  # short page (< count) -> stop
    ]
    calls = []

    def fake_fetch(url):
        calls.append(url)
        return {'candles': pages[len(calls) - 1]}

    monkeypatch.setattr(etl, '_fetch_from_api', fake_fetch)
    etl.compute_candle_features()

    assert len(calls) == 2
    assert len(etl.insert_many_list) == 3
    times = sorted(c['time'] for c in etl.insert_many_list)
    assert times == [1_699_990_000, 1_700_000_000, 1_700_010_000]


def test_compute_candle_features_flattens_bid_ask_and_tags_instrument(monkeypatch):
    etl = CandlestickETL('EUR/USD', 'H1', ifc=None, count=10)
    etl.start_time = 0

    monkeypatch.setattr(etl, '_fetch_from_api', lambda url: {'candles': [_candle(1_700_000_000)]})
    etl.compute_candle_features()

    candle = etl.insert_many_list[0]
    assert candle['instrument'] == 'EUR/USD'
    assert candle['granularity'] == 'H1'
    assert candle['bid_o'] == 1.1000
    assert candle['ask_o'] == 1.1002
    assert 'bid' not in candle and 'ask' not in candle  # sub-dicts flattened away


def test_compute_candle_features_drops_incomplete_candles_when_keep_complete_only(monkeypatch):
    etl = CandlestickETL('EUR/USD', 'H1', ifc=None, count=10, keep_complete_only=True)
    etl.start_time = 0

    monkeypatch.setattr(
        etl,
        '_fetch_from_api',
        lambda url: {
            'candles': [_candle(1_700_000_000, complete=True), _candle(1_700_003_600, complete=False)],
        },
    )
    etl.compute_candle_features()

    assert len(etl.insert_many_list) == 1
    assert etl.insert_many_list[0]['complete'] is True


def test_qa_raises_on_duplicate_timestamps():
    etl = CandlestickETL('EUR/USD', 'H1', ifc=None)
    etl.df = pd.DataFrame({'time': [1, 1, 2], 'instrument': ['EUR/USD'] * 3})

    try:
        etl.qa()
    except AssertionError as exc:
        assert 'Duplicate timestamps' in str(exc)
    else:
        raise AssertionError('expected qa() to raise on duplicate timestamps')


def test_clean_up_dataframe_renames_columns_to_the_schema_models_expect():
    etl = CandlestickETL('EUR/USD', 'H1', ifc=None)
    etl.df = pd.DataFrame(
        {
            'time': [1_700_000_000],
            'time_iso': ['2023-11-14T22:13:20+00:00'],
            'bid_o': [1.1000],
            'bid_l': [1.0990],
            'bid_h': [1.1010],
            'bid_c': [1.1005],
            'ask_o': [1.1002],
            'ask_l': [1.0992],
            'ask_h': [1.1012],
            'ask_c': [1.1007],
        }
    )
    etl.clean_up_dataframe()

    assert 'time_iso' not in etl.df.columns
    assert 'timestamp' in etl.df.columns and 'time' not in etl.df.columns
    assert set(etl.df.columns) >= {
        'bid_open',
        'bid_low',
        'bid_high',
        'bid_close',
        'ask_open',
        'ask_low',
        'ask_high',
        'ask_close',
    }


def test_make_the_influxdb_dict_produces_a_valid_candlestick_record():
    etl = CandlestickETL('EUR/USD', 'H1', ifc=None)
    etl.df = pd.DataFrame(
        {
            'instrument': ['EUR/USD'],
            'granularity': ['H1'],
            'timestamp': [1_700_000_000],
            'volume': [100],
            'complete': [True],
            'bid_open': [1.1000],
            'bid_high': [1.1010],
            'bid_low': [1.0990],
            'bid_close': [1.1005],
            'ask_open': [1.1002],
            'ask_high': [1.1012],
            'ask_low': [1.0992],
            'ask_close': [1.1007],
        }
    )
    etl.make_the_influxdb_dict()

    assert len(etl.to_influx_list) == 1
    d = etl.to_influx_list[0]
    assert d['measurement'] == 'candlestick'
    assert d['tags'] == {'instrument': 'EUR/USD', 'granularity': 'H1'}
    assert d['time'] == 1_700_000_000
    assert d['fields']['bid_open'] == 1.1000


def test_get_max_previous_time_resumes_from_the_stored_max(monkeypatch):
    etl = CandlestickETL('EUR/USD', 'H1', ifc=_FakeIfc(pd.DataFrame({'unix_epoch_s': [1_700_000_000]})))
    etl.get_max_previous_time()
    assert etl.start_time == 1_700_000_000


def test_get_max_previous_time_keeps_default_when_no_prior_data(monkeypatch):
    etl = CandlestickETL('EUR/USD', 'H1', ifc=_FakeIfc(pd.DataFrame({'unix_epoch_s': []})))
    default_start = etl.start_time
    etl.get_max_previous_time()
    assert etl.start_time == default_start


class _FakeIfc:
    """Minimal stand-in for InfluxDbTool -- only the one method CandlestickETL
    actually calls on it for get_max_previous_time()."""

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def run_flux_query_on_forex_database_and_get_dataframe(self, query: str) -> pd.DataFrame:
        return self._df
