import pytest
from pydantic import ValidationError

from forex.etl.models import CandlestickRecord, EconomicCalendarEventRecord, PositioningBucketRecord, SwapRateRecord

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


_SWAP_BASE = dict(
    instrument='EUR/USD',
    long_rate=-0.0067,
    short_rate=-0.0038,
    timestamp=1_700_000_000,
)


class TestSwapRateRecord:
    def test_valid_record_instantiates(self):
        rec = SwapRateRecord(**_SWAP_BASE)
        assert rec.instrument == 'EUR/USD'
        assert rec.long_rate == -0.0067

    def test_missing_field_raises(self):
        bad = {k: v for k, v in _SWAP_BASE.items() if k != 'short_rate'}
        with pytest.raises(ValidationError):
            SwapRateRecord(**bad)

    def test_float_coercion(self):
        rec = SwapRateRecord(**{**_SWAP_BASE, 'long_rate': '-0.0067'})
        assert abs(rec.long_rate - (-0.0067)) < 1e-9


class TestSwapRateToInfluxDict:
    def _dict(self, **overrides):
        return SwapRateRecord(**{**_SWAP_BASE, **overrides}).to_influx_dict()

    def test_measurement_name(self):
        assert self._dict()['measurement'] == 'swap-rate'

    def test_tag_keys_do_not_include_granularity(self):
        # unlike CandlestickRecord, swap rates are an account-level snapshot, not
        # tied to any candle granularity
        assert set(self._dict()['tags']) == {'instrument'}

    def test_time_field(self):
        assert self._dict()['time'] == 1_700_000_000

    def test_rates_in_fields_not_tags(self):
        d = self._dict()
        assert 'long_rate' in d['fields']
        assert 'short_rate' in d['fields']
        assert 'long_rate' not in d['tags']


_CALENDAR_BASE = dict(
    country='US',
    impact='high',
    event='Non-Farm Payrolls',
    estimate=200000.0,
    prev=199000.0,
    unit='K',
    timestamp=1_700_000_000,
)


class TestEconomicCalendarEventRecord:
    def test_valid_record_instantiates(self):
        rec = EconomicCalendarEventRecord(**_CALENDAR_BASE)
        assert rec.event == 'Non-Farm Payrolls'
        assert rec.actual is None  # not provided -- defaults to None

    def test_missing_field_raises(self):
        bad = {k: v for k, v in _CALENDAR_BASE.items() if k != 'event'}
        with pytest.raises(ValidationError):
            EconomicCalendarEventRecord(**bad)

    def test_actual_defaults_to_none_when_omitted(self):
        rec = EconomicCalendarEventRecord(**_CALENDAR_BASE)
        assert rec.actual is None

    def test_actual_can_be_set_for_a_past_event(self):
        rec = EconomicCalendarEventRecord(**{**_CALENDAR_BASE, 'actual': 175000.0})
        assert rec.actual == 175000.0


class TestEconomicCalendarToInfluxDict:
    def _dict(self, **overrides):
        return EconomicCalendarEventRecord(**{**_CALENDAR_BASE, **overrides}).to_influx_dict()

    def test_measurement_name(self):
        assert self._dict()['measurement'] == 'economic-calendar-event'

    def test_tag_keys(self):
        assert set(self._dict()['tags']) == {'country', 'impact', 'event'}

    def test_time_field(self):
        assert self._dict()['time'] == 1_700_000_000

    def test_none_actual_is_omitted_from_fields_not_written_as_null(self):
        d = self._dict()  # actual not provided in _CALENDAR_BASE -- defaults to None
        assert 'actual' not in d['fields']

    def test_provided_values_appear_in_fields(self):
        d = self._dict(actual=175000.0)
        assert d['fields']['actual'] == 175000.0
        assert d['fields']['estimate'] == 200000.0
        assert d['fields']['prev'] == 199000.0
        assert d['fields']['unit'] == 'K'


_POSITIONING_BASE = dict(
    instrument='EUR/USD',
    book_type='order',
    bucket_price=1.0990,
    long_count_percent=0.1234,
    short_count_percent=0.0456,
    timestamp=1_700_000_000,
)


class TestPositioningBucketRecord:
    def test_valid_record_instantiates(self):
        rec = PositioningBucketRecord(**_POSITIONING_BASE)
        assert rec.instrument == 'EUR/USD'
        assert rec.book_type == 'order'

    def test_missing_field_raises(self):
        bad = {k: v for k, v in _POSITIONING_BASE.items() if k != 'bucket_price'}
        with pytest.raises(ValidationError):
            PositioningBucketRecord(**bad)

    def test_float_coercion(self):
        rec = PositioningBucketRecord(**{**_POSITIONING_BASE, 'bucket_price': '1.0990'})
        assert abs(rec.bucket_price - 1.0990) < 1e-9


class TestPositioningToInfluxDict:
    def _dict(self, **overrides):
        return PositioningBucketRecord(**{**_POSITIONING_BASE, **overrides}).to_influx_dict()

    def test_measurement_name(self):
        assert self._dict()['measurement'] == 'positioning-bucket'

    def test_tag_keys(self):
        assert set(self._dict()['tags']) == {'instrument', 'book_type'}

    def test_time_field(self):
        assert self._dict()['time'] == 1_700_000_000

    def test_bucket_values_in_fields_not_tags(self):
        d = self._dict()
        assert 'bucket_price' in d['fields']
        assert 'long_count_percent' in d['fields']
        assert 'short_count_percent' in d['fields']
        assert 'bucket_price' not in d['tags']
