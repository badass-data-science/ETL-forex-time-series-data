import pytest
from pydantic import ValidationError

from forex.etl.models import CandlestickRecord

_BASE = dict(
    instrument='EUR/USD',
    granularity='H1',
    volume=100,
    complete=True,
    bid_open=1.1000,
    bid_high=1.1050,
    bid_low=1.0950,
    bid_close=1.1020,
    ask_open=1.1002,
    ask_high=1.1052,
    ask_low=1.0952,
    ask_close=1.1022,
    timestamp=1_700_000_000,
)


class TestCandlestickRecord:
    def test_valid_record_instantiates(self):
        rec = CandlestickRecord(**_BASE)
        assert rec.instrument == 'EUR/USD'
        assert rec.granularity == 'H1'

    def test_missing_field_raises(self):
        bad = {k: v for k, v in _BASE.items() if k != 'volume'}
        with pytest.raises(ValidationError):
            CandlestickRecord(**bad)

    def test_int_coercion(self):
        rec = CandlestickRecord(**{**_BASE, 'volume': '42'})
        assert rec.volume == 42
        assert isinstance(rec.volume, int)

    def test_float_coercion(self):
        rec = CandlestickRecord(**{**_BASE, 'bid_open': '1.1234'})
        assert abs(rec.bid_open - 1.1234) < 1e-9


class TestToInfluxDict:
    def _dict(self, **overrides):
        return CandlestickRecord(**{**_BASE, **overrides}).to_influx_dict()

    def test_measurement_name(self):
        assert self._dict()['measurement'] == 'candlestick'

    def test_tag_keys(self):
        assert set(self._dict()['tags']) == {'instrument', 'granularity'}

    def test_tag_values(self):
        d = self._dict()
        assert d['tags']['instrument'] == 'EUR/USD'
        assert d['tags']['granularity'] == 'H1'

    def test_time_field(self):
        assert self._dict()['time'] == 1_700_000_000

    def test_timestamp_not_in_fields_or_tags(self):
        d = self._dict()
        assert 'timestamp' not in d['fields']
        assert 'timestamp' not in d['tags']

    def test_ohlc_in_fields(self):
        d = self._dict()
        for col in ('bid_open', 'bid_high', 'bid_low', 'bid_close',
                    'ask_open', 'ask_high', 'ask_low', 'ask_close'):
            assert col in d['fields'], f'{col} missing from fields'

    def test_tags_not_duplicated_in_fields(self):
        d = self._dict()
        assert 'instrument' not in d['fields']
        assert 'granularity' not in d['fields']

    def test_volume_and_complete_in_fields(self):
        d = self._dict()
        assert 'volume' in d['fields']
        assert 'complete' in d['fields']
