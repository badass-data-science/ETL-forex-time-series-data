import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from forex.critical_timezone import is_market_open_at_time
from forex.eda.eda_config.eda_config import granularity_to_seconds_map

class ForwardFillInator():

    candlestick_part_list = ['open', 'low', 'high', 'close']
    
    def __init__(
        self,
        instrument,
        granularity,
        ifc,
        INFLUXDB_BUCKET = 'forex',
        cutoff_timestamp = 1420088160,
        critical_timezone_str = 'America/New_York',
    ):
        self.instrument = instrument
        self.granularity = granularity
        self.ifc = ifc
        self.INFLUXDB_BUCKET = INFLUXDB_BUCKET
        self.cutoff_timestamp = cutoff_timestamp
        self.critical_timezone_str = critical_timezone_str
    
    def pull_data(self):
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
        df.drop(columns = ['complete', 'instrument', 'granularity'], inplace = True)
        df.sort_values(by = ['unix_epoch_s'], inplace = True)
        
        self.df = df.copy()
        del(df)

    def perform_mid_calculations(self):
        for cp in self.candlestick_part_list:
            self.df['mid_' + cp] = self.df[['ask_' + cp, 'bid_' + cp]].mean(axis = 1)

    def perform_spread_calculations(self):
        for cp in self.candlestick_part_list:
            self.df['spread_' + cp] = self.df['ask_' + cp] - self.df['bid_' + cp]

    def perform_time_calculations(self, cut_Friday_hour_17 = True):
        df = self.df[self.df['unix_epoch_s'] >= self.cutoff_timestamp].copy()

        df['datetime'] = (
            pd.to_datetime(df['unix_epoch_s'], unit='s', utc=True)
            .dt.tz_convert(self.critical_timezone_str)
        )

        # use an apply here if possible
        df['weekday'] = [x.weekday() for x in df['datetime']]
        df['hour'] = [x.hour for x in df['datetime']]
        df['minute'] = [x.minute for x in df['datetime']]

        if cut_Friday_hour_17:
            df = df[~((df['weekday'] == 4) & (df['hour'] == 17))].copy()
            df = df.copy()
        
        df.sort_values(by = ['unix_epoch_s'])
        df['lagged_unix_epoch_s'] = df['unix_epoch_s'].shift(1)
        df.dropna(inplace = True)  # reconcider this in terms of using a more pandas-like type change
        df['lagged_unix_epoch_s'] = np.int64(df['lagged_unix_epoch_s'])  # use a more pandas-like type change
        df['diff_unix_epoch_s'] = df['unix_epoch_s'] - df['lagged_unix_epoch_s']
        
        self.df = df.copy().reset_index().drop(columns = ['index'])
        del(df)

    def weekday_hour_qa(self):
        self.df_weekday_hour_agg = self.df.groupby(['weekday', 'hour'])['unix_epoch_s'].agg('count').reset_index()

    def compute_df_all_time_diff_market_open(self):
        
        #
        # produce an array/DataFrame containing the full range of dates
        #
        # we'll want to revisit the "granularity_to_seconds_map" dictionary
        #
        mn = np.min(self.df['unix_epoch_s'])
        mx = np.max(self.df['unix_epoch_s'])
        unix_epoch_s_array = np.arange(
            mn,
            mx + granularity_to_seconds_map[self.granularity],
            granularity_to_seconds_map[self.granularity],
        )
        df = pd.DataFrame({'unix_epoch_s' : unix_epoch_s_array})

        # compute the datetime from the full unix epoch time data
        df['datetime_test'] = (
            pd.to_datetime(df['unix_epoch_s'], unit='s', utc=True)
            .dt.tz_convert(self.critical_timezone_str)
        )

        # try an apply command here or see if pandas has native commands for this
        df['weekday_test'] = [x.weekday() for x in df['datetime_test']]
        df['hour_test'] = [x.hour for x in df['datetime_test']]
        df['minute_test'] = [x.minute for x in df['datetime_test']]
        
        # use an apply command
        df['is_market_open'] = [is_market_open_at_time(x) for x in df['datetime_test']]

        # join our previous data to full date range
        df = pd.merge(df, self.df, on = ['unix_epoch_s'], how = 'left')

        # QA
        df_to_test = df.dropna()
        assert np.min(np.int8((df_to_test['datetime_test'] == df_to_test['datetime']).values)) == 1
        assert np.min(np.int8((df_to_test['weekday_test'] == df_to_test['weekday']).values)) == 1
        assert np.min(np.int8((df_to_test['hour_test'] == df_to_test['hour']).values)) == 1
        assert np.min(np.int8((df_to_test['minute_test'] == df_to_test['minute']).values)) == 1

        # drop and rename some columns
        df.drop(columns = ['datetime', 'weekday', 'hour', 'minute'], inplace = True)
        df.rename(
            columns = {'datetime_test' : 'datetime', 'weekday_test' : 'weekday', 'hour_test' : 'hour', 'minute_test' : 'minute'},
            inplace = True,
        )
        df.drop(columns = ['lagged_unix_epoch_s', 'diff_unix_epoch_s'], inplace = True)

        # keep only rows when the market is open
        df = df[df['is_market_open']].copy()
        df.drop(columns = ['is_market_open'], inplace = True)

        # prepare for QA
        df.sort_values(by = ['unix_epoch_s'], inplace = True)
        df['lagged_unix_epoch_s'] = df['unix_epoch_s'].shift(1)
        df.dropna(subset = ['lagged_unix_epoch_s'], inplace = True)
        df['diff_unix_epoch_s'] = df['unix_epoch_s'] - df['lagged_unix_epoch_s']
        
        # QA
        df_to_test = df.groupby(['diff_unix_epoch_s'])['volume'].agg('count').reset_index()
        df_to_test['diff_unix_epoch_s'] = np.int64(df_to_test['diff_unix_epoch_s'].values)
        df_to_test.rename(columns = {'volume' : 'count'}, inplace = True)
        self.df_all_time_diff_market_open_agg_test = df_to_test

        # finalize data types
        df['lagged_unix_epoch_s'] = df['lagged_unix_epoch_s'].astype('int64')
        df['diff_unix_epoch_s'] = df['diff_unix_epoch_s'].astype('int64')
        df['volume'] = df['volume'].astype('Int64') # note capitalization for nullable type

        # save results to class
        self.df_all_time_diff_market_open = df.copy()
        del(df)

    #
    # We'll come up with a more robust method in the near future
    #
    def account_for_holiday_market_closure(self):
        self.df_all_time_diff_market_open.dropna(inplace = True)
    
    def forward_fill_it(self):
        df = self.df_all_time_diff_market_open.copy()
        df.sort_values(by = ['unix_epoch_s'], inplace = True)  # "insurance"
        df.ffill(inplace = True)
        self.df_all_time_diff_market_open_forward_filled = df.copy()
        del(df)

    def fit(self):
        self.perform_mid_calculations()
        self.perform_spread_calculations()
        self.perform_time_calculations()
        self.weekday_hour_qa()
        self.compute_df_all_time_diff_market_open()
        self.account_for_holiday_market_closure()
        self.forward_fill_it()

    def plot_NaNs_vs_time(self):
        x = self.df_all_time_diff_market_open['datetime'].values
        y = np.int8((~(self.df_all_time_diff_market_open['volume'].isna())).values)
        plt.figure(figsize = [10, 3])
        plt.plot(x, y, '.', color = 'magenta')
        plt.yticks([0, 1], [True, False])
        plt.ylim([-0.4, 1.4])
        plt.ylabel('NaN')
        plt.title('Is data point NaN?')
        plt.tight_layout()
        plt.show()
        plt.close()
