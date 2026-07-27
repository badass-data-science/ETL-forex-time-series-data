import datetime
import json
import logging
from zoneinfo import ZoneInfo

import requests
from tenacity import retry, retry_if_exception, retry_if_exception_type, stop_after_attempt, wait_fixed

from forex.etl.models import SwapRateRecord
from forex.oanda.headers import get_oanda_headers

logger = logging.getLogger(__name__)


def _is_not_client_error(exc: BaseException) -> bool:
    """True unless `exc` is an HTTPError with a 4xx status -- those are
    deterministic (bad instrument, bad auth), not worth 5 retries at 2s apiece
    for the same guaranteed outcome."""
    if not isinstance(exc, requests.HTTPError) or exc.response is None:
        return True
    return not (400 <= exc.response.status_code < 500)


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
        # 4xx responses (bad instrument, bad auth, etc.) are deterministic --
        # retrying them 5 times just wastes 4x30s per failure for the same
        # outcome. Only transient conditions (5xx, connection resets, timeouts)
        # are worth a retry; a 4xx should surface immediately.
        stop=stop_after_attempt(5),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(requests.RequestException) & retry_if_exception(_is_not_client_error),
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

    def _instruments_url(self, account_id: str, instruments: list[str]) -> str:
        return (
            self.config['server']
            + '/v3/accounts/' + account_id
            + '/instruments?instruments=' + ','.join(instruments)
        )

    def get_instrument_financing(self) -> dict:
        """OANDA rejects the ENTIRE batched request (a single HTTP 404,
        `INSTRUMENT_NOT_TRADEABLE`) if even one requested instrument isn't
        tradeable on this account -- confirmed directly (XAU_USD 404s on a
        practice account not provisioned for commodity trading, even though
        every other instrument in the same batch returns 200 individually).
        Falls back to one-request-per-instrument on that specific failure, so
        a single not-yet-tradeable instrument doesn't silently block collecting
        real rates for everything else. The common case (every instrument
        tradeable) stays a single batched request."""
        account_id = self.get_account_id()
        try:
            return self._fetch_from_api(self._instruments_url(account_id, self.instruments))
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 404:
                raise
            logger.warning(
                'Batched instruments request failed (%s) -- falling back to one request per instrument',
                exc.response.text,
            )

        all_instruments: list[dict] = []
        for instrument in self.instruments:
            try:
                rj = self._fetch_from_api(self._instruments_url(account_id, [instrument]))
                all_instruments.extend(rj['instruments'])
            except requests.HTTPError as exc:
                logger.warning('Skipping %s: %s', instrument, exc.response.text if exc.response is not None else exc)
        return {'instruments': all_instruments}

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
