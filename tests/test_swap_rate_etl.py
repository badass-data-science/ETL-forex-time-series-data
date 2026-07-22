from __future__ import annotations

import requests

from forex.etl.SwapRateETL import SwapRateETL, _records_from_instruments_response


def _http_error(status_code: int, text: str) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    response._content = text.encode()
    return requests.HTTPError(response=response)


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


def test_get_instrument_financing_falls_back_to_per_instrument_when_batch_404s(monkeypatch):
    """Regression test for a real, confirmed-live bug: OANDA rejects the ENTIRE
    batched request with a 404 if even one requested instrument isn't tradeable
    on the account (confirmed directly: XAU_USD 404s 'INSTRUMENT_NOT_TRADEABLE'
    on a practice account not provisioned for commodities, while every other
    instrument in the same batch returns 200 individually). Without a fallback,
    one not-yet-tradeable instrument would silently block collecting real rates
    for everything else in the batch too."""
    etl = SwapRateETL(['EUR/USD', 'XAU/USD', 'USD/JPY'], config_file='unused')
    etl.config = {'server': 'https://example.test', 'account_id': '001-001-1234567-001'}

    calls = []

    def fake_fetch(url):
        calls.append(url)
        if 'instruments=EUR_USD,XAU_USD,USD_JPY' in url:
            raise _http_error(404, '{"errorCode":"INSTRUMENT_NOT_TRADEABLE"}')
        if url.endswith('instruments=EUR_USD'):
            return {'instruments': [{'name': 'EUR_USD', 'financing': {'longRate': '-0.0067', 'shortRate': '-0.0038'}}]}
        if url.endswith('instruments=XAU_USD'):
            raise _http_error(404, '{"errorCode":"INSTRUMENT_NOT_TRADEABLE"}')
        if url.endswith('instruments=USD_JPY'):
            return {'instruments': [{'name': 'USD_JPY', 'financing': {'longRate': '0.0012', 'shortRate': '-0.0091'}}]}
        raise AssertionError(f'unexpected url: {url}')

    monkeypatch.setattr(etl, '_fetch_from_api', fake_fetch)
    rj = etl.get_instrument_financing()

    names = {entry['name'] for entry in rj['instruments']}
    assert names == {'EUR_USD', 'USD_JPY'}  # XAU_USD skipped, not silently dropping the other two
    assert len(calls) == 4  # 1 batch attempt + 3 per-instrument fallback calls


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
