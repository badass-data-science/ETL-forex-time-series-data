import datetime
import logging
import pprint

import numpy as np

from forex.critical_timezone import is_market_open
from forex.etl.CandlestickETL import CandlestickETL
from forex.etl.config import database_config
from forex.etl.models import CandlestickRecord

logger = logging.getLogger(__name__)


class CandlestickPipeline:

    def __init__(
        self,
        config_file: str,
        instrument: str,
        granularity: str,
        ifc,
        allowed_tags: set = CandlestickRecord.TAGS,
        allowed_fields: dict = CandlestickRecord.FIELDS,
        influxdb_bucket: str | None = None,
        run_test_query: bool = False,
    ) -> None:
        self.config_file = config_file
        self.instrument = instrument
        self.granularity = granularity
        self.ifc = ifc
        self.run_test_query = run_test_query
        self.ALLOWED_TAGS = allowed_tags
        self.ALLOWED_FIELDS = allowed_fields
        # database_config.INFLUXDB_BUCKET is resolved here, not as the parameter's
        # default value -- a default value is evaluated once at class-definition
        # (i.e. import) time, which would trigger database_config's lazy AWS Secrets
        # Manager load merely by importing this module, same bug class as importing
        # `from database_config import X` at module top level.
        self.INFLUXDB_BUCKET = influxdb_bucket if influxdb_bucket is not None else database_config.INFLUXDB_BUCKET

    def test_whether_market_is_open(self) -> None:
        self.run_it = is_market_open()
        if not self.run_it:
            logger.info('Market is closed — skipping run')

    def retrieve_candlestick_data(self) -> None:
        self.cs = CandlestickETL(self.instrument, self.granularity, self.config_file, self.ifc)
        self.cs.fit()

    def qa(self, n: int = 3) -> None:
        logger.info('start_time: %s (%s)', self.cs.start_time, datetime.datetime.fromtimestamp(self.cs.start_time))
        logger.info('record count: %d', len(self.cs.to_influx_list))
        logger.debug('first %d records:\n%s', n, pprint.pformat(self.cs.to_influx_list[:n]))

    def insert_data(self) -> None:
        self.ifc.insert_dictionary_list(
            self.cs.to_influx_list,
            self.ALLOWED_TAGS,
            self.ALLOWED_FIELDS,
            self.INFLUXDB_BUCKET,
        )
        logger.info('Inserted %d records for %s %s', len(self.cs.to_influx_list), self.instrument, self.granularity)

    def test_query(self) -> None:
        query = f'''
            from(bucket: "{self.INFLUXDB_BUCKET}")
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
        logger.info('\n%s', df)

        df_agg = df.groupby(['granularity', 'instrument'])['unix_epoch_s'].agg('count')
        logger.info('\n%s', df_agg)

        test = df.groupby(['granularity', 'instrument', 'unix_epoch_s'])['instrument'].agg('count').to_numpy()
        assert int(np.max(test)) == 1

    def fit(self) -> None:
        self.test_whether_market_is_open()
        if self.run_it:
            self.retrieve_candlestick_data()
            self.qa()
            self.insert_data()
        if self.run_test_query:
            self.test_query()
