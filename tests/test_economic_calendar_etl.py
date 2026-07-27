from __future__ import annotations

import datetime

from forex.etl.EconomicCalendarETL import EconomicCalendarETL, _records_from_calendar_response


def test_records_from_calendar_response_parses_a_future_event_with_no_actual_yet():
    rj = {
        'economicCalendar': [
            {
                'event': 'Non-Farm Payrolls', 'country': 'US', 'impact': 'high',
                'estimate': 200000, 'prev': 199000, 'unit': 'K',
                'time': '2024-01-05 13:30:00',
            },
        ],
    }
    records = _records_from_calendar_response(rj)

    assert len(records) == 1
    r = records[0]
    assert r['event'] == 'Non-Farm Payrolls'
    assert r['country'] == 'US'
    assert r['impact'] == 'high'
    assert r['actual'] is None  # hasn't happened yet
    assert r['estimate'] == 200000
    assert r['prev'] == 199000
    assert r['unit'] == 'K'
    expected_ts = int(datetime.datetime(2024, 1, 5, 13, 30, 0, tzinfo=datetime.timezone.utc).timestamp())
    assert r['timestamp'] == expected_ts


def test_records_from_calendar_response_parses_a_past_event_with_an_actual_value():
    rj = {
        'economicCalendar': [
            {
                'event': 'CPI', 'country': 'US', 'impact': 'high',
                'actual': 3.1, 'estimate': 3.0, 'prev': 3.2, 'unit': '%',
                'time': '2023-12-12 13:30:00',
            },
        ],
    }
    records = _records_from_calendar_response(rj)
    assert records[0]['actual'] == 3.1


def test_records_from_calendar_response_defaults_missing_impact_and_unit():
    rj = {'economicCalendar': [{'event': 'Some Release', 'country': 'DE', 'time': '2024-01-05 08:00:00'}]}
    records = _records_from_calendar_response(rj)
    assert records[0]['impact'] == 'unknown'
    assert records[0]['unit'] == ''


def test_records_from_calendar_response_handles_an_empty_calendar():
    assert _records_from_calendar_response({'economicCalendar': []}) == []
    assert _records_from_calendar_response({}) == []  # key missing entirely


def test_compute_calendar_events_populates_records_from_the_api_response(monkeypatch):
    etl = EconomicCalendarETL(api_key='fake-key')

    def fake_fetch(from_date, to_date):
        assert from_date == '2024-01-01'
        assert to_date == '2024-01-15'
        return {
            'economicCalendar': [
                {'event': 'FOMC Rate Decision', 'country': 'US', 'impact': 'high', 'time': '2024-01-05 19:00:00'},
            ],
        }

    monkeypatch.setattr(etl, '_fetch_from_api', fake_fetch)
    etl.compute_calendar_events('2024-01-01', '2024-01-15')

    assert len(etl.records) == 1
    assert etl.records[0]['event'] == 'FOMC Rate Decision'


def test_fit_produces_valid_influxdb_dicts_and_omits_null_fields(monkeypatch):
    etl = EconomicCalendarETL(api_key='fake-key')

    def fake_fetch(from_date, to_date):
        return {
            'economicCalendar': [
                {
                    'event': 'Non-Farm Payrolls', 'country': 'US', 'impact': 'high',
                    'estimate': 200000, 'time': '2024-01-05 13:30:00',
                },
            ],
        }

    monkeypatch.setattr(etl, '_fetch_from_api', fake_fetch)
    etl.fit('2024-01-01', '2024-01-15')

    assert len(etl.to_influx_list) == 1
    d = etl.to_influx_list[0]
    assert d['measurement'] == 'economic-calendar-event'
    assert d['tags'] == {'event': 'Non-Farm Payrolls', 'country': 'US', 'impact': 'high'}
    assert d['fields']['estimate'] == 200000
    assert 'actual' not in d['fields']  # None -- omitted, not written as null
    assert 'prev' not in d['fields']
