import datetime
import logging

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from forex.etl.models import EconomicCalendarEventRecord

logger = logging.getLogger(__name__)

BASE_URL = 'https://finnhub.io/api/v1/calendar/economic'


def _records_from_calendar_response(rj: dict) -> list[dict]:
    """Pure transform, kept separate from any HTTP call so it's directly testable
    against a synthetic Finnhub-shaped response. Field names (`event`/`country`/
    `impact`/`actual`/`estimate`/`prev`/`unit`/`time`) match Finnhub's
    /calendar/economic schema. `time` is UTC ("YYYY-MM-DD HH:MM:SS", per Finnhub's
    documented format) -- parsed as such explicitly rather than relying on any
    local/system timezone."""
    records = []
    for entry in rj.get('economicCalendar', []):
        time_dt = datetime.datetime.strptime(entry['time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=datetime.timezone.utc)
        records.append({
            'event': entry['event'],
            'country': entry['country'],
            'impact': entry.get('impact') or 'unknown',
            'actual': entry.get('actual'),
            'estimate': entry.get('estimate'),
            'prev': entry.get('prev'),
            'unit': entry.get('unit') or '',
            'timestamp': int(time_dt.timestamp()),
        })
    return records


class EconomicCalendarETL:
    """Pulls scheduled economic calendar events (release time, country, impact,
    actual/estimate/previous values) from Finnhub. Unlike CandlestickETL, this is a
    forward-looking pull over a date range, not an incremental backfill from the
    last-seen timestamp -- the whole point is having known-in-advance event times
    available before they happen, and re-pulling the same window is cheap and
    naturally idempotent (later runs pick up newly-published `actual` values for
    events that already occurred)."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _fetch_from_api(self, from_date: str, to_date: str) -> dict:
        r = requests.get(BASE_URL, params={'token': self.api_key, 'from': from_date, 'to': to_date})
        r.raise_for_status()
        return r.json()

    def compute_calendar_events(self, from_date: str, to_date: str) -> None:
        rj = self._fetch_from_api(from_date, to_date)
        self.records = _records_from_calendar_response(rj)
        logger.info('Fetched %d calendar events from %s to %s', len(self.records), from_date, to_date)

    def make_the_influxdb_dict(self) -> None:
        self.to_influx_list = [EconomicCalendarEventRecord(**r).to_influx_dict() for r in self.records]

    def fit(self, from_date: str, to_date: str) -> None:
        self.compute_calendar_events(from_date, to_date)
        self.make_the_influxdb_dict()
