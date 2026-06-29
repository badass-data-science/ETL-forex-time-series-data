import datetime
import json
import logging
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from forex.etl.config.database_config import INFLUXDB_BUCKET
from forex.etl.models import CandlestickRecord
from forex.oanda.config.price_type_map import price_type_map
from forex.oanda.headers import get_oanda_headers

logger = logging.getLogger(__name__)


class CandlestickETL:

    TIMEZONE_NAME = 'America/Toronto'  # canonical timezone for this pipeline; must match critical_timezone.py and ForwardFillInator

    def __init__(
        self,
        instrument: str,
        granularity: str,
        config_file: str,
        ifc,
        measurement_name: str = CandlestickRecord.MEASUREMENT,
        error_retry_interval: int = 2,
        max_retries: int = 5,
        keep_complete_only: bool = True,
        count: int = 5000,
        price_types: str = 'BA',
    ) -> None:
        self.config_file = config_file
        self.instrument = instrument.replace('/', '_')
        self.granularity = granularity
        self.ifc = ifc

        self.count = count
        self.price_types = price_types
        self.error_retry_interval = error_retry_interval
        self.max_retries = max_retries
        self.keep_complete_only = keep_complete_only
        self.measurement_name = measurement_name

        self.price_type_list = [price_type_map[q] for q in self.price_types]

        self.timezone = ZoneInfo(self.TIMEZONE_NAME)
        self.start_time = int(datetime.datetime(2009, 12, 31, 23, 59, 59, tzinfo=self.timezone).timestamp())
        self.end_date_original = int(datetime.datetime.now(tz=self.timezone).timestamp())

    def get_headers(self) -> None:
        with open(self.config_file) as f:
            self.config = json.load(f)
        self.headers = get_oanda_headers(self.config)

    def get_max_previous_time(self) -> None:
        instrument_str = self.instrument.replace('_', '/')
        query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: 0)
          |> filter(fn: (r) => r._measurement == "candlestick")
          |> filter(fn: (r) => r.instrument == "{instrument_str}")
          |> filter(fn: (r) => r.granularity == "{self.granularity}")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> keep(columns: ["granularity", "instrument", "_time"])
          |> group(columns: ["granularity", "instrument"])
          |> max(column: "_time")
        '''
        df_max_time = self.ifc.run_flux_query_on_forex_database_and_get_dataframe(query)
        if len(df_max_time.index) > 0:
            self.start_time = int(df_max_time['unix_epoch_s'].iloc[0])
            logger.info(
                'Resuming from %s',
                datetime.datetime.fromtimestamp(self.start_time, tz=self.timezone),
            )

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

    def get_instrument_candlesticks(self, end_date: float) -> dict:
        url = (
            self.config['server']
            + '/v3/instruments/' + self.instrument
            + '/candles?count=' + str(self.count)
            + '&price=' + self.price_types
            + '&granularity=' + self.granularity
            + '&to=' + str(end_date)
        )
        logger.debug('Fetching candlesticks to %s', end_date)
        return self._fetch_from_api(url)

    def compute_candle_features(self) -> None:
        finished = False
        end_date = self.end_date_original
        self.insert_many_list: list[dict] = []

        while not finished:
            rj = self.get_instrument_candlesticks(end_date)
            candlesticks = rj['candles']

            date_list = []
            for candle in candlesticks:
                candle['instrument'] = self.instrument.replace('_', '/')
                candle['granularity'] = self.granularity
                candle['time'] = int(float(candle['time']))
                time_dt = datetime.datetime.fromtimestamp(candle['time'], tz=self.timezone)
                candle['time_iso'] = time_dt.isoformat()

                for price_type in self.price_type_list:
                    for component in candle[price_type]:
                        candle[f'{price_type}_{component}'] = float(candle[price_type][component])
                for price_type in self.price_type_list:
                    del candle[price_type]

                if not self.keep_complete_only or candle['complete']:
                    self.insert_many_list.append(candle)

                date_list.append(candle['time'])

            if len(date_list) < self.count or min(date_list) < self.start_time:
                finished = True
            else:
                end_date = min(date_list) - 0.1

        logger.info('Fetched %d candlesticks for %s %s', len(self.insert_many_list), self.instrument, self.granularity)

    def create_dataframe(self) -> None:
        self.df = (
            pd.DataFrame(self.insert_many_list)
            .sort_values(by=['instrument', 'time'])
            .reset_index(drop=True)
        )
        self.df = self.df[self.df['time'] >= int(self.start_time)].reset_index(drop=True)

        self.time_filtered_df = self.df[self.df['time'] > self.start_time].sort_values(by=['time']).copy()
        self.to_insert = self.time_filtered_df.to_dict(orient='records')

    def qa(self) -> None:
        duplicates = self.df.duplicated(subset=['time'])
        assert not duplicates.any(), f'Duplicate timestamps: {self.df[duplicates]["time"].tolist()}'

    def clean_up_dataframe(self) -> None:
        rename_map = {
            f'{pt}_{old}': f'{pt}_{new}'
            for pt in self.price_type_list
            for old, new in zip(['o', 'l', 'h', 'c'], ['open', 'low', 'high', 'close'])
        }
        self.df.rename(columns=rename_map, inplace=True)
        self.df.drop(columns=['time_iso'], inplace=True)
        self.df.rename(columns={'time': 'timestamp'}, inplace=True)

    def make_the_InfluxDB_dict(self) -> None:
        self.to_influx_list = [
            CandlestickRecord(**r).to_influx_dict()
            for r in self.df.to_dict(orient='records')
        ]
        logger.info('Prepared %d records for InfluxDB', len(self.to_influx_list))

    def fit(self) -> None:
        self.get_headers()
        self.get_max_previous_time()
        self.compute_candle_features()
        self.create_dataframe()
        self.qa()
        self.clean_up_dataframe()
        self.make_the_InfluxDB_dict()
