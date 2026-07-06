import logging
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from forex.critical_timezone import is_market_open_at_time
from forex.eda.eda_config.eda_config import granularity_to_seconds_map
from forex.etl.models import ForwardFilledCandlestickRecord

logger = logging.getLogger(__name__)


class ForwardFillInator:

    candlestick_part_list = ['open', 'low', 'high', 'close']

    def __init__(
        self,
        instrument: str,
        granularity: str,
        ifc,
        influxdb_bucket: str = 'forex',
        cutoff_timestamp: int = 1420088160,
        critical_timezone_str: str = 'America/Toronto',
    ) -> None:
        self.instrument = instrument
        self.granularity = granularity
        self.ifc = ifc
        self.INFLUXDB_BUCKET = influxdb_bucket
        self.cutoff_timestamp = cutoff_timestamp
        self.critical_timezone_str = critical_timezone_str
        self.critical_timezone = ZoneInfo(critical_timezone_str)

    def pull_data(self) -> None:
        query = f'''
            start_s = {self.cutoff_timestamp}
            from(bucket: "{self.INFLUXDB_BUCKET}")
              |> range(start: time(v: int(v: start_s) * 1000000000))
              |> filter(fn: (r) => r._measurement == "candlestick")
              |> filter(fn: (r) => r.granularity == "{self.granularity}")
              |> filter(fn: (r) => r.instrument == "{self.instrument}")
              |> pivot(
                  rowKey: ["_time"],
                  columnKey: ["_field"],
                  valueColumn: "_value"
              )
              |> drop(columns: ["_start", "_stop", "_measurement"])
        '''
        df = self.ifc.run_flux_query_on_forex_database_and_get_dataframe(query)
        df = df[df['complete']]
        df.drop(columns=['complete', 'instrument', 'granularity'], inplace=True)
        df.sort_values(by=['unix_epoch_s'], inplace=True)
        self.df = df.copy()
        logger.info('Pulled %d rows for %s %s', len(self.df), self.instrument, self.granularity)

    def perform_mid_calculations(self) -> None:
        for cp in self.candlestick_part_list:
            self.df[f'mid_{cp}'] = self.df[[f'ask_{cp}', f'bid_{cp}']].mean(axis=1)

    def perform_spread_calculations(self) -> None:
        for cp in self.candlestick_part_list:
            self.df[f'spread_{cp}'] = self.df[f'ask_{cp}'] - self.df[f'bid_{cp}']

    def perform_time_calculations(self, cut_friday_hour_17: bool = True) -> None:
        df = self.df[self.df['unix_epoch_s'] >= self.cutoff_timestamp].copy()

        df['datetime'] = (
            pd.to_datetime(df['unix_epoch_s'], unit='s', utc=True)
            .dt.tz_convert(self.critical_timezone_str)
        )

        df['weekday'] = df['datetime'].dt.weekday
        df['hour'] = df['datetime'].dt.hour
        df['minute'] = df['datetime'].dt.minute

        if cut_friday_hour_17:
            df = df[~((df['weekday'] == 4) & (df['hour'] == 17))].copy()

        df.sort_values(by=['unix_epoch_s'], inplace=True)
        df['lagged_unix_epoch_s'] = df['unix_epoch_s'].shift(1)
        df.dropna(inplace=True)
        df['lagged_unix_epoch_s'] = df['lagged_unix_epoch_s'].astype(np.int64)
        df['diff_unix_epoch_s'] = df['unix_epoch_s'] - df['lagged_unix_epoch_s']

        self.df = df.reset_index(drop=True)

    def weekday_hour_qa(self) -> None:
        self.df_weekday_hour_agg = self.df.groupby(['weekday', 'hour'])['unix_epoch_s'].agg('count').reset_index()

    def compute_df_all_time_diff_market_open(self) -> None:
        mn = np.min(self.df['unix_epoch_s'])
        mx = np.max(self.df['unix_epoch_s'])
        step = granularity_to_seconds_map[self.granularity]
        unix_epoch_s_array = np.arange(mn, mx + step, step)

        df = pd.DataFrame({'unix_epoch_s': unix_epoch_s_array})

        df['datetime_test'] = (
            pd.to_datetime(df['unix_epoch_s'], unit='s', utc=True)
            .dt.tz_convert(self.critical_timezone_str)
        )

        df['weekday_test'] = df['datetime_test'].dt.weekday
        df['hour_test'] = df['datetime_test'].dt.hour
        df['minute_test'] = df['datetime_test'].dt.minute

        # Vectorised market-open flag — equivalent to is_market_open_at_time row-by-row
        df['is_market_open'] = ~(
            ((df['weekday_test'] == 4) & (df['hour_test'] >= 17)) |
            (df['weekday_test'] == 5) |
            ((df['weekday_test'] == 6) & (df['hour_test'] < 17))
        )

        df = pd.merge(df, self.df, on=['unix_epoch_s'], how='left')

        # Captured here, right after the merge and before any QA/drop/ffill steps
        # touch the frame, so it reflects genuinely-missing candles regardless of
        # what happens to the other columns downstream (ffill only fills NaNs in the
        # OHLCV columns; this one is never null, so ffill leaves it untouched).
        df['is_forward_filled'] = df['volume'].isna()

        # QA: time columns must agree between the full grid and the pulled data
        df_to_test = df.dropna()
        assert np.min(np.int8((df_to_test['datetime_test'] == df_to_test['datetime']).values)) == 1
        assert np.min(np.int8((df_to_test['weekday_test'] == df_to_test['weekday']).values)) == 1
        assert np.min(np.int8((df_to_test['hour_test'] == df_to_test['hour']).values)) == 1
        assert np.min(np.int8((df_to_test['minute_test'] == df_to_test['minute']).values)) == 1

        df.drop(columns=['datetime', 'weekday', 'hour', 'minute'], inplace=True)
        df.rename(columns={
            'datetime_test': 'datetime',
            'weekday_test': 'weekday',
            'hour_test': 'hour',
            'minute_test': 'minute',
        }, inplace=True)
        df.drop(columns=['lagged_unix_epoch_s', 'diff_unix_epoch_s'], inplace=True)

        df = df[df['is_market_open']].drop(columns=['is_market_open']).copy()

        df.sort_values(by=['unix_epoch_s'], inplace=True)
        df['lagged_unix_epoch_s'] = df['unix_epoch_s'].shift(1)
        df.dropna(subset=['lagged_unix_epoch_s'], inplace=True)
        df['diff_unix_epoch_s'] = df['unix_epoch_s'] - df['lagged_unix_epoch_s']

        df_to_test = df.groupby(['diff_unix_epoch_s'])['volume'].agg('count').reset_index()
        df_to_test['diff_unix_epoch_s'] = df_to_test['diff_unix_epoch_s'].astype(np.int64)
        df_to_test.rename(columns={'volume': 'count'}, inplace=True)
        self.df_all_time_diff_market_open_agg_test = df_to_test

        df['lagged_unix_epoch_s'] = df['lagged_unix_epoch_s'].astype('int64')
        df['diff_unix_epoch_s'] = df['diff_unix_epoch_s'].astype('int64')
        df['volume'] = df['volume'].astype('Int64')

        self.df_all_time_diff_market_open = df.reset_index(drop=True)
        logger.info(
            'Full market-open grid: %d rows (%d non-null)',
            len(self.df_all_time_diff_market_open),
            self.df_all_time_diff_market_open['volume'].notna().sum(),
        )

    def forward_fill_it(self) -> None:
        df = self.df_all_time_diff_market_open.sort_values(by=['unix_epoch_s'])
        df.ffill(inplace=True)
        self.df_all_time_diff_market_open_forward_filled = df.copy()
        logger.info('Forward-filled %d rows', len(self.df_all_time_diff_market_open_forward_filled))

    def account_for_holiday_market_closure(self) -> None:
        # Drops rows still NaN after forward-filling -- these are leading rows before
        # any real candle exists yet to forward-fill from (ffill only propagates
        # forward in time, so it cannot fill a gap with nothing before it).
        #
        # Previously this ran BEFORE forward_fill_it() and called plain dropna() on
        # the pre-ffill frame, which drops every row with ANY missing OHLCV value --
        # i.e. every gap, not just holiday closures. That made forward_fill_it()'s
        # ffill() a no-op: there was nothing left with a NaN by the time it ran.
        # TODO: replace with an explicit holiday calendar to also drop/flag extended
        # multi-day closures (e.g. Christmas week) that ffill currently just bridges
        # over with a stale price instead of excluding.
        self.df_all_time_diff_market_open_forward_filled.dropna(inplace=True)

    def make_the_influxdb_dict(self) -> None:
        columns = [
            'instrument', 'granularity', 'timestamp',
            'mid_open', 'mid_high', 'mid_low', 'mid_close', 'spread_close',
            'volume', 'is_forward_filled',
        ]
        df = (
            self.df_all_time_diff_market_open_forward_filled
            .rename(columns={'unix_epoch_s': 'timestamp'})
            .assign(instrument=self.instrument, granularity=self.granularity)
        )
        self.to_influx_list = [
            ForwardFilledCandlestickRecord(**r).to_influx_dict()
            for r in df[columns].to_dict(orient='records')
        ]
        logger.info('Prepared %d forward-filled records for InfluxDB', len(self.to_influx_list))

    def fit(self) -> None:
        self.pull_data()
        self.perform_mid_calculations()
        self.perform_spread_calculations()
        self.perform_time_calculations()
        self.weekday_hour_qa()
        self.compute_df_all_time_diff_market_open()
        self.forward_fill_it()
        self.account_for_holiday_market_closure()
        self.make_the_influxdb_dict()

    def plot_NaNs_vs_time(self) -> None:
        x = self.df_all_time_diff_market_open['datetime'].values
        y = np.int8((~self.df_all_time_diff_market_open['volume'].isna()).values)
        plt.figure(figsize=[10, 3])
        plt.plot(x, y, '.', color='magenta')
        plt.yticks([0, 1], [True, False])
        plt.ylim([-0.4, 1.4])
        plt.ylabel('NaN')
        plt.title('Is data point NaN?')
        plt.tight_layout()
        plt.show()
        plt.close()
