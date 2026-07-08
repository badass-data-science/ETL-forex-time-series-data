from __future__ import annotations

import datetime

from forex.etl.PositioningETL import PositioningETL, _records_from_book_response

_ORDER_BOOK_RESPONSE = {
    'orderBook': {
        'instrument': 'EUR_USD',
        'time': '2024-01-05T12:00:00Z',
        'price': '1.10000',
        'bucketWidth': '0.0005',
        'buckets': [
            {'price': '1.0990', 'longCountPercent': '0.1234', 'shortCountPercent': '0.0456'},
            {'price': '1.0995', 'longCountPercent': '0.2000', 'shortCountPercent': '0.1000'},
        ],
    },
}

_POSITION_BOOK_RESPONSE = {
    'positionBook': {
        'instrument': 'EUR_USD',
        'time': '2024-01-05T12:00:00Z',
        'price': '1.10000',
        'bucketWidth': '0.0005',
        'buckets': [
            {'price': '1.0990', 'longCountPercent': '0.3000', 'shortCountPercent': '0.2000'},
        ],
    },
}


def test_records_from_book_response_parses_order_book_buckets():
    records = _records_from_book_response(_ORDER_BOOK_RESPONSE, 'orderBook', 'order')

    assert len(records) == 2
    assert records[0]['instrument'] == 'EUR/USD'  # underscore -> slash, same as candlesticks
    assert records[0]['book_type'] == 'order'
    assert records[0]['bucket_price'] == 1.0990
    assert records[0]['long_count_percent'] == 0.1234
    assert records[0]['short_count_percent'] == 0.0456
    expected_ts = int(datetime.datetime(2024, 1, 5, 12, 0, 0, tzinfo=datetime.timezone.utc).timestamp())
    assert records[0]['timestamp'] == expected_ts


def test_records_from_book_response_parses_position_book_buckets():
    records = _records_from_book_response(_POSITION_BOOK_RESPONSE, 'positionBook', 'position')

    assert len(records) == 1
    assert records[0]['book_type'] == 'position'
    assert records[0]['long_count_percent'] == 0.3000


def test_records_from_book_response_handles_no_buckets():
    empty = {'orderBook': {**_ORDER_BOOK_RESPONSE['orderBook'], 'buckets': []}}
    assert _records_from_book_response(empty, 'orderBook', 'order') == []


def test_compute_positioning_fetches_both_books_for_every_instrument(monkeypatch):
    etl = PositioningETL(['EUR/USD', 'USD/JPY'], config_file='unused')
    etl.config = {'server': 'https://example.test'}

    calls = []

    def fake_order_book(instrument):
        calls.append(('order', instrument))
        return _ORDER_BOOK_RESPONSE

    def fake_position_book(instrument):
        calls.append(('position', instrument))
        return _POSITION_BOOK_RESPONSE

    monkeypatch.setattr(etl, 'get_order_book', fake_order_book)
    monkeypatch.setattr(etl, 'get_position_book', fake_position_book)
    etl.compute_positioning()

    assert calls == [
        ('order', 'EUR_USD'), ('position', 'EUR_USD'),
        ('order', 'USD_JPY'), ('position', 'USD_JPY'),
    ]
    assert len(etl.records) == (2 + 1) * 2  # 2 order buckets + 1 position bucket, per instrument


def test_get_order_book_and_position_book_build_the_expected_urls(monkeypatch):
    etl = PositioningETL(['EUR/USD'], config_file='unused')
    etl.config = {'server': 'https://example.test'}

    captured = {}

    def fake_fetch(url):
        captured['url'] = url
        return _ORDER_BOOK_RESPONSE

    monkeypatch.setattr(etl, '_fetch_from_api', fake_fetch)
    etl.get_order_book('EUR_USD')
    assert captured['url'] == 'https://example.test/v3/instruments/EUR_USD/orderBook'

    monkeypatch.setattr(etl, '_fetch_from_api', fake_fetch)
    etl.get_position_book('EUR_USD')
    assert captured['url'] == 'https://example.test/v3/instruments/EUR_USD/positionBook'


def test_fit_produces_valid_influxdb_dicts(monkeypatch, tmp_path):
    config_file = tmp_path / 'oanda_config.json'
    config_file.write_text('{"server": "https://example.test", "token": "fake", "oanda_date_time_format": "UNIX"}')
    etl = PositioningETL(['EUR/USD'], config_file=str(config_file))

    monkeypatch.setattr(etl, 'get_order_book', lambda instrument: _ORDER_BOOK_RESPONSE)
    monkeypatch.setattr(etl, 'get_position_book', lambda instrument: _POSITION_BOOK_RESPONSE)
    etl.fit()

    assert len(etl.to_influx_list) == 3
    d = etl.to_influx_list[0]
    assert d['measurement'] == 'positioning-bucket'
    assert d['tags'] == {'instrument': 'EUR/USD', 'book_type': 'order'}
    assert 'bucket_price' in d['fields']
    assert 'long_count_percent' in d['fields']
    assert 'short_count_percent' in d['fields']
