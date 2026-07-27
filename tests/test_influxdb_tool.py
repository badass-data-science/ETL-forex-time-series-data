from __future__ import annotations

import pandas as pd
import pytest

from forex.util.influxdb_tool import InfluxDbTool

_ALLOWED_TAGS = frozenset({'instrument'})
_ALLOWED_FIELDS = {'volume': int, 'price': float}


class TestTimeColumnToUnixEpochS:
    def test_converts_naive_strings_assumed_utc(self):
        result = InfluxDbTool._time_column_to_unix_epoch_s(pd.Series(['2023-11-14 22:13:20']))
        assert result.iloc[0] == 1_700_000_000

    def test_converts_tz_aware_timestamps(self):
        series = pd.Series([pd.Timestamp('2023-11-14 22:13:20', tz='UTC')])
        result = InfluxDbTool._time_column_to_unix_epoch_s(series)
        assert result.iloc[0] == 1_700_000_000

    def test_handles_microsecond_precision_without_a_1000x_error(self):
        """Regression guard for the exact bug the source comment describes: a
        non-pivoted Flux result can parse to datetime64[us, UTC] rather than
        [ns, UTC] -- dividing an already-microsecond int64 by 10**9 (instead of
        upcasting to ns first) would silently produce a value 1000x too small."""
        series = pd.Series([pd.Timestamp('2023-11-14 22:13:20', tz='UTC')]).astype('datetime64[us, UTC]')
        result = InfluxDbTool._time_column_to_unix_epoch_s(series)
        assert result.iloc[0] == 1_700_000_000


class TestValidatePoint:
    def test_valid_point_builds_successfully(self):
        p = InfluxDbTool.validate_point(
            'candlestick',
            {'instrument': 'EUR/USD'},
            {'volume': 100, 'price': 1.1},
            _ALLOWED_TAGS,
            _ALLOWED_FIELDS,
            1_700_000_000,
        )
        assert p is not None

    def test_unexpected_tag_raises_value_error(self):
        with pytest.raises(ValueError, match='Unexpected tag'):
            InfluxDbTool.validate_point(
                'candlestick',
                {'instrument': 'EUR/USD', 'extra': 'x'},
                {'volume': 100},
                _ALLOWED_TAGS,
                _ALLOWED_FIELDS,
                1_700_000_000,
            )

    def test_unexpected_field_raises_value_error(self):
        with pytest.raises(ValueError, match='Unexpected field'):
            InfluxDbTool.validate_point(
                'candlestick',
                {'instrument': 'EUR/USD'},
                {'volume': 100, 'bogus': 1},
                _ALLOWED_TAGS,
                _ALLOWED_FIELDS,
                1_700_000_000,
            )

    def test_wrong_field_type_raises_type_error(self):
        with pytest.raises(TypeError, match='must be int'):
            InfluxDbTool.validate_point(
                'candlestick',
                {'instrument': 'EUR/USD'},
                {'volume': '100'},
                _ALLOWED_TAGS,
                _ALLOWED_FIELDS,
                1_700_000_000,
            )


class _FakeWriteApi:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def write(self, bucket, record, write_precision):
        self.calls.append({'bucket': bucket, 'record': record, 'write_precision': write_precision})


class TestInsertDictionaryList:
    def _make_tool(self, monkeypatch) -> tuple[InfluxDbTool, _FakeWriteApi]:
        tool = InfluxDbTool('https://example.test', 'token', 'org')
        fake_write_api = _FakeWriteApi()
        monkeypatch.setattr(tool.client, 'write_api', lambda write_options=None: fake_write_api)
        return tool, fake_write_api

    def test_writes_all_records_to_the_given_bucket(self, monkeypatch):
        tool, fake_write_api = self._make_tool(monkeypatch)
        records = [
            {'measurement': 'candlestick', 'tags': {'instrument': 'EUR/USD'}, 'fields': {'volume': 1}, 'time': 1},
            {'measurement': 'candlestick', 'tags': {'instrument': 'GBP/USD'}, 'fields': {'volume': 2}, 'time': 2},
        ]
        tool.insert_dictionary_list(records, _ALLOWED_TAGS, {'volume': int}, 'my-bucket')

        assert len(fake_write_api.calls) == 1
        assert fake_write_api.calls[0]['bucket'] == 'my-bucket'
        assert len(fake_write_api.calls[0]['record']) == 2

    def test_batches_writes_at_batch_size(self, monkeypatch):
        tool, fake_write_api = self._make_tool(monkeypatch)
        records = [
            {'measurement': 'candlestick', 'tags': {'instrument': 'EUR/USD'}, 'fields': {'volume': i}, 'time': i}
            for i in range(5)
        ]
        tool.insert_dictionary_list(records, _ALLOWED_TAGS, {'volume': int}, 'my-bucket', batch_size=2)

        assert len(fake_write_api.calls) == 3  # 2 + 2 + 1
        assert [len(c['record']) for c in fake_write_api.calls] == [2, 2, 1]

    def test_invalid_record_raises_before_any_write(self, monkeypatch):
        tool, fake_write_api = self._make_tool(monkeypatch)
        records = [
            {'measurement': 'candlestick', 'tags': {'instrument': 'EUR/USD'}, 'fields': {'bogus': 1}, 'time': 1},
        ]
        with pytest.raises(ValueError, match='Unexpected field'):
            tool.insert_dictionary_list(records, _ALLOWED_TAGS, {'volume': int}, 'my-bucket')
        assert fake_write_api.calls == []
