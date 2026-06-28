import numpy as np
import datetime
import pprint as pp

from forex.etl.config.schema_config import ALLOWED_TAGS, ALLOWED_FIELDS
from forex.etl.config.database_config import INFLUXDB_BUCKET
from forex.etl.CandlestickETL import CandlestickETL
from forex.critical_timezone import is_market_open

class CandlestickPipeline():
    def __init__(
        self,
        config_file,
        instrument,
        granularity,
        ifc,
        ALLOWED_TAGS = ALLOWED_TAGS,
        ALLOWED_FIELDS = ALLOWED_FIELDS,
        INFLUXDB_BUCKET = INFLUXDB_BUCKET,
        run_test_query = False,
    ):
        self.config_file = config_file
        self.instrument = instrument
        self.granularity = granularity
        self.ifc = ifc
        self.run_test_query = run_test_query
        self.ALLOWED_TAGS = ALLOWED_TAGS
        self.ALLOWED_FIELDS = ALLOWED_FIELDS
        self.INFLUXDB_BUCKET = INFLUXDB_BUCKET
    
    def test_whether_market_is_open(self):
        self.run_it = is_market_open()
        if not self.run_it:
            print()
            print('Market is closed.')
            print()
        
    def retrieve_candlestick_data(self):
        self.cs = CandlestickETL(self.instrument, self.granularity, self.config_file, self.ifc)
        self.cs.fit()

    def qa(self, n = 3):
        print(self.cs.start_time)
        print(datetime.datetime.fromtimestamp(self.cs.start_time))
        print()
        print(len(self.cs.to_influx_list))
        print()
        pp.pprint(self.cs.to_influx_list[0:n])
        print()

    def insert_data(self):
        self.ifc.insert_dictionary_list(
            self.cs.to_influx_list,
            self.ALLOWED_TAGS,
            self.ALLOWED_FIELDS,
            self.INFLUXDB_BUCKET,
        )

    def test_query(self):
        query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: 0)
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
        print(df)
        
        df_agg = df.groupby(['granularity', 'instrument'])['unix_epoch_s'].agg('count')
        print(df_agg)
        
        test = df.groupby(['granularity', 'instrument', 'unix_epoch_s'])['instrument'].agg('count').to_numpy()
        assert int(np.max(test)) == 1
        
    def fit(self):
        self.test_whether_market_is_open()
        if self.run_it:
            self.retrieve_candlestick_data()
            self.qa()
            self.insert_data()
        if self.run_test_query:
            self.test_query()
