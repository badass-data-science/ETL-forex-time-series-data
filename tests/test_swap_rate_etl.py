from __future__ import annotations

from forex.etl.SwapRateETL import SwapRateETL, _records_from_instruments_response


def test_records_from_instruments_response_converts_underscore_to_slash_and_parses_rates():
    rj = {
        'instruments': [
            {'name': 'EUR_USD', 'financing': {'longRate': '-0.0067', 'shortRate': '-0.0038'}},
            {'name': 'USD_JPY', 'financing': {'longRate': '0.0012', 'shortRate': '-0.0091'}},
        ],
    }
    records = _records_from_instruments_response(rj, timestamp=1_700_000_000)

    assert records == [
        {'instrument': 'EUR/USD', 'long_rate': -0.0067, 'short_rate': -0.0038, 'timestamp': 1_700_000_000},
        {'instrument': 'USD/JPY', 'long_rate': 0.0012, 'short_rate': -0.0091, 'timestamp': 1_700_000_000},
    ]


def test_records_from_instruments_response_handles_an_empty_list():
    assert _records_from_instruments_response({'instruments': []}, timestamp=0) == []


def test_get_account_id_uses_config_value_when_present():
    etl = SwapRateETL(['EUR/USD'], config_file='unused')
    etl.config = {'server': 'https://example.test', 'account_id': '001-001-1234567-001'}
    assert etl.get_account_id() == '001-001-1234567-001'


def test_get_account_id_resolves_via_api_when_absent(monkeypatch):
    etl = SwapRateETL(['EUR/USD'], config_file='unused')
    etl.config = {'server': 'https://example.test'}

    calls = []

    def fake_fetch(url):
        calls.append(url)
        return {'accounts': [{'id': '001-001-7654321-001'}, {'id': '001-001-9999999-002'}]}

    monkeypatch.setattr(etl, '_fetch_from_api', fake_fetch)

    assert etl.get_account_id() == '001-001-7654321-001'  # first account, if more than one exists
    assert calls == ['https://example.test/v3/accounts']


def test_compute_swap_rates_populates_records_from_the_api_response(monkeypatch):
    etl = SwapRateETL(['EUR/USD', 'USD/JPY'], config_file='unused')
    etl.config = {'server': 'https://example.test', 'account_id': '001-001-1234567-001'}

    def fake_fetch(url):
        assert url == (
            'https://example.test/v3/accounts/001-001-1234567-001/instruments?instruments=EUR_USD,USD_JPY'
        )
        return {
            'instruments': [
                {'name': 'EUR_USD', 'financing': {'longRate': '-0.0067', 'shortRate': '-0.0038'}},
                {'name': 'USD_JPY', 'financing': {'longRate': '0.0012', 'shortRate': '-0.0091'}},
            ],
        }

    monkeypatch.setattr(etl, '_fetch_from_api', fake_fetch)
    etl.compute_swap_rates()

    assert len(etl.records) == 2
    assert etl.records[0]['instrument'] == 'EUR/USD'


def test_fit_produces_valid_influxdb_dicts(monkeypatch, tmp_path):
    config_file = tmp_path / 'oanda_config.json'
    config_file.write_text(
        '{"server": "https://example.test", "token": "fake", '
        '"oanda_date_time_format": "UNIX", "account_id": "001-001-1234567-001"}'
    )
    etl = SwapRateETL(['EUR/USD'], config_file=str(config_file))

    def fake_fetch(url):
        return {'instruments': [{'name': 'EUR_USD', 'financing': {'longRate': '-0.0067', 'shortRate': '-0.0038'}}]}

    monkeypatch.setattr(etl, '_fetch_from_api', fake_fetch)
    etl.fit()

    assert len(etl.to_influx_list) == 1
    d = etl.to_influx_list[0]
    assert d['measurement'] == 'swap-rate'
    assert d['tags'] == {'instrument': 'EUR/USD'}
    assert d['fields']['long_rate'] == -0.0067
    assert d['fields']['short_rate'] == -0.0038
