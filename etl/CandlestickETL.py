import json
import time
import datetime
import pytz
import requests
import pandas as pd

from forex.etl.config.database_config import INFLUXDB_BUCKET
from forex.etl.config.schema_config import schema_measurement_name, ALLOWED_TAGS, ALLOWED_FIELDS

from forex.oanda.config.price_type_map import price_type_map
from forex.oanda.headers import get_oanda_headers


class CandlestickETL():

    #
    # constructor
    #
    def __init__(
        self,
        
        instrument,
        granularity,
        config_file,
        ifc,
    
        measurement_name = schema_measurement_name,
        error_retry_interval = 2,
        max_retries = 5,
        keep_complete_only = True,
        count = 5000,
        price_types = 'BA',
    ):
        
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

        self.fields_list = list(ALLOWED_FIELDS.keys())
        self.tags_list = ALLOWED_TAGS
        
        #
        # initialize (hard-coded)
        #
        self.timezone_name = 'America/Toronto'   # Don't change this!
        self.timezone = pytz.timezone(self.timezone_name)
        self.start_time = int(self.timezone.localize(datetime.datetime(2009, 12, 31, 23, 59, 59)).timestamp())
        
        #
        # other initialization
        #
        self.price_type_list = [price_type_map[q] for q in self.price_types]
        self.end_date_original = int(time.mktime(datetime.datetime.now().timetuple()))
        

    #
    # get headers from the configuration
    #
    def get_headers(self):
        with open(self.config_file) as f:
            self.config = json.load(f)
        self.headers = get_oanda_headers(self.config)

    #
    # figure out the maximum time in the database
    # per instrument and granularity
    #
    def get_max_previous_time(self):
        
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
            self.start_time = int(df_max_time['unix_epoch_s'][0]) #.to_pydatetime().timestamp())
    
    #
    # request forex price/volume candlesticks from Oanda
    #
    def get_instrument_candlesticks(self, end_date):
        url = (
            self.config['server']
            + '/v3/instruments/' + self.instrument
            + '/candles?count=' + str(self.count)
            + '&price=' + self.price_types
            + '&granularity=' + self.granularity
            + '&to=' + str(end_date)
        )
        
        for attempt in range(self.max_retries):
            try:
                r = requests.get(url, headers=self.headers)
                break
            except requests.RequestException:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.error_retry_interval)
        
        rj = r.json()
        return rj

    #
    # compute additional forex candlestick features
    #
    def compute_candle_features(self):
        
        finished = False
        end_date = self.end_date_original

        self.insert_many_list = []

        # loop through the timestamp ranges for each set of n=count values
        while not finished:

            # retrieve the instrument candlesticks from the Oanda server
            rj = self.get_instrument_candlesticks(end_date) # instrument, count, price_types, granularity, end_date)        
            candlesticks = rj['candles']

            #
            # deal with timestamps and time-related content
            #
            date_list = []
            for candle in candlesticks:
                candle['instrument'] = self.instrument.replace('_', '/')
                candle['granularity'] = self.granularity
                candle['time'] = int(float(candle['time']))
                time_dt = datetime.datetime.fromtimestamp(candle['time'], tz = self.timezone)
                candle['time_iso'] = time_dt.isoformat()

                for price_type in self.price_type_list:
                    for candlestick_component in candle[price_type].keys():
                        candle[price_type + '_' + candlestick_component] = float(candle[price_type][candlestick_component])
                    
                for price_type in self.price_type_list:
                    del(candle[price_type])

                if self.keep_complete_only:
                    if candle['complete']:    
                        self.insert_many_list.append(candle)
                else:
                    self.insert_many_list.append(candle)

                date_list.append(candle['time'])

            # Are we done?
            if (len(date_list) < self.count) or (min(date_list) < self.start_time):
                finished = True
            else:
                # prepare for the next iteration
                end_date = min(date_list) - 0.1

    #
    # Create a dataframe
    #
    def create_dataframe(self):
        self.df = pd.DataFrame(self.insert_many_list).sort_values(by = ['instrument', 'time'])
        self.df = self.df[self.df['time'] >= int(self.start_time)]
        self.df = self.df.reset_index().drop(columns = ['index']).copy()

        self.time_filtered_df = self.df[self.df['time'] > self.start_time].sort_values(by = ['time']).copy()
        self.to_insert = self.time_filtered_df.to_dict(orient = 'records')

    #
    # clean up teh dataframe
    #
    def clean_up_dataframe(self):
        new_name_dict = {}
        for price_type in self.price_type_list:
            for old_name, new_name in zip(['o', 'l', 'h', 'c'], ['open', 'low', 'high', 'close']):
                new_name_dict[price_type + '_' + old_name] = price_type + '_' + new_name
        self.df.rename(columns = new_name_dict, inplace = True)
        self.df.drop(columns = ['time_iso'], inplace = True)

        self.df.rename(columns = {'time' : 'timestamp'}, inplace = True)
        
        self.df['instrument'] = self.df['instrument'].replace('_', '/')

    #
    # compute the dictionary used to populate the InfluxDB bucket
    #
    def make_the_InfluxDB_dict(self):
      
        # parallelize this later?
        
        self.to_influx_list = []
        for r in self.df.to_dict(orient = 'records'):
            record_result = {'measurement' : self.measurement_name, 'fields' : {}, 'tags' : {}}
            for key in r.keys():
                key_str = str(key) # not sure this is necessary

                # make these lists variables
                if key_str in self.tags_list:
                    record_result['tags'][key_str] = r[key_str]

                elif key_str in self.fields_list:
                    record_result['fields'][key_str] = r[key_str]

                elif key_str in ['timestamp']:
                    record_result['time'] = r[key_str]

                else:
                    raise ValueError(
                        f"Unexpected field '{key_str}' is not in ALLOWED_FIELDS, ALLOWED_TAGS, or 'timestamp'. "
                        "Update schema_config if this field is intentional."
                    )
            self.to_influx_list.append(record_result)

    #
    # QA
    #
    def qa(self):
        assert len(self.df.index) == len(self.df[['time']].drop_duplicates())
        assert len(self.df.index) == len(self.df['time'].unique())

    #
    # Run everything
    #
    def fit(self):
        self.get_headers()
        self.get_max_previous_time()
        self.compute_candle_features()
        self.create_dataframe()
        self.qa()
        self.clean_up_dataframe()
        self.make_the_InfluxDB_dict()
