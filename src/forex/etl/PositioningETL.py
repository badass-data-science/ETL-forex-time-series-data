import datetime
import json
import logging

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from forex.etl.models import PositioningBucketRecord
from forex.oanda.headers import get_oanda_headers

logger = logging.getLogger(__name__)


def _records_from_book_response(rj: dict, response_key: str, book_type: str) -> list[dict]:
    """Pure transform, kept separate from any HTTP call so it's directly testable
    against a synthetic OANDA-shaped response. `response_key` is "orderBook" or
    "positionBook" -- the top-level key OANDA nests the actual book under;
    `book_type` is the short tag value ("order"/"position") stored alongside each
    bucket."""
    book = rj[response_key]
    instrument = book['instrument'].replace('_', '/')
    time_dt = datetime.datetime.strptime(book['time'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc)
    timestamp = int(time_dt.timestamp())

    return [
        {
            'instrument': instrument,
            'book_type': book_type,
            'bucket_price': float(bucket['price']),
            'long_count_percent': float(bucket['longCountPercent']),
            'short_count_percent': float(bucket['shortCountPercent']),
            'timestamp': timestamp,
        }
        for bucket in book['buckets']
    ]


class PositioningETL:
    """Pulls OANDA's per-instrument order-book and position-book snapshots (the
    latest available -- no historical `time=` param is passed) for every
    instrument given. Unlike CandlestickETL, this is a single current snapshot per
    instrument per book type, not a historical time series -- no backfill/
    windowing logic needed, same as SwapRateETL."""

    def __init__(self, instruments: list[str], config_file: str) -> None:
        self.instruments = [i.replace('/', '_') for i in instruments]
        self.config_file = config_file

    def get_headers(self) -> None:
        with open(self.config_file) as f:
            self.config = json.load(f)
        self.headers = get_oanda_headers(self.config)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _fetch_from_api(self, url: str) -> dict:
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return r.json()

    def get_order_book(self, instrument: str) -> dict:
        return self._fetch_from_api(self.config['server'] + '/v3/instruments/' + instrument + '/orderBook')

    def get_position_book(self, instrument: str) -> dict:
        return self._fetch_from_api(self.config['server'] + '/v3/instruments/' + instrument + '/positionBook')

    def compute_positioning(self) -> None:
        self.records: list[dict] = []
        for instrument in self.instruments:
            order_book = self.get_order_book(instrument)
            self.records.extend(_records_from_book_response(order_book, 'orderBook', 'order'))
            position_book = self.get_position_book(instrument)
            self.records.extend(_records_from_book_response(position_book, 'positionBook', 'position'))
        logger.info('Fetched %d positioning buckets across %d instruments', len(self.records), len(self.instruments))

    def make_the_influxdb_dict(self) -> None:
        self.to_influx_list = [PositioningBucketRecord(**r).to_influx_dict() for r in self.records]

    def fit(self) -> None:
        self.get_headers()
        self.compute_positioning()
        self.make_the_influxdb_dict()
