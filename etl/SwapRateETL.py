import datetime
import json
import logging
from zoneinfo import ZoneInfo

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from forex.etl.models import SwapRateRecord
from forex.oanda.headers import get_oanda_headers

logger = logging.getLogger(__name__)


def _records_from_instruments_response(rj: dict, timestamp: int) -> list[dict]:
    """Pure transform, kept separate from any HTTP call so it's directly testable
    against a synthetic OANDA-shaped response. Field names (`financing.longRate`/
    `shortRate`) match OANDA's v20 `/v3/accounts/{accountID}/instruments` schema."""
    return [
        {
            'instrument': entry['name'].replace('_', '/'),
            'long_rate': float(entry['financing']['longRate']),
            'short_rate': float(entry['financing']['shortRate']),
            'timestamp': timestamp,
        }
        for entry in rj['instruments']
    ]


class SwapRateETL:
    """Pulls per-instrument long/short financing (swap/rollover) rates from OANDA's
    v20 API. Unlike CandlestickETL, this is a single current snapshot per instrument,
    not a historical time series -- no backfill/windowing logic needed."""

    TIMEZONE_NAME = 'America/Toronto'  # matches CandlestickETL/ForwardFillInator

    def __init__(self, instruments: list[str], config_file: str) -> None:
        self.instruments = [i.replace('/', '_') for i in instruments]
        self.config_file = config_file
        self.timezone = ZoneInfo(self.TIMEZONE_NAME)

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

    def get_account_id(self) -> str:
        """The financing-rate endpoint is scoped under /v3/accounts/{accountID}/
        instruments. Uses config['account_id'] if present (the Oanda config JSON is
        documented in the README as having this key, though no other code in this
        repo currently reads it); otherwise resolves it via /v3/accounts, assuming
        one account per API token -- the same implicit assumption the rest of this
        pipeline already makes via a single Bearer token. Takes the first account if
        more than one exists."""
        account_id = self.config.get('account_id')
        if account_id:
            return account_id
        rj = self._fetch_from_api(self.config['server'] + '/v3/accounts')
        return rj['accounts'][0]['id']

    def get_instrument_financing(self) -> dict:
        account_id = self.get_account_id()
        url = (
            self.config['server']
            + '/v3/accounts/' + account_id
            + '/instruments?instruments=' + ','.join(self.instruments)
        )
        return self._fetch_from_api(url)

    def compute_swap_rates(self) -> None:
        rj = self.get_instrument_financing()
        timestamp = int(datetime.datetime.now(tz=self.timezone).timestamp())
        self.records = _records_from_instruments_response(rj, timestamp)
        logger.info('Fetched swap rates for %d instruments', len(self.records))

    def make_the_influxdb_dict(self) -> None:
        self.to_influx_list = [SwapRateRecord(**r).to_influx_dict() for r in self.records]

    def fit(self) -> None:
        self.get_headers()
        self.compute_swap_rates()
        self.make_the_influxdb_dict()
