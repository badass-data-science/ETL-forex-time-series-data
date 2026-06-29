"""
Prefect flow: fetch Oanda candlesticks → InfluxDB.

Deploy:
    python -m forex.flows.candlestick_flow

Run ad-hoc:
    prefect run -m forex.flows.candlestick_flow \
        --param config_file=/path/to/config.json \
        --param instrument=EUR/USD \
        --param granularity=H1
"""

from prefect import flow, task, get_run_logger

from forex.critical_timezone import is_market_open
from forex.etl.CandlestickETL import CandlestickETL
from forex.etl.config.database_config import INFLUXDB_BUCKET, INFLUXDB_ORG, INFLUXDB_TOKEN, INFLUXDB_URL
from forex.etl.config.schema_config import ALLOWED_FIELDS, ALLOWED_TAGS
from python_tools_and_shortcuts.databases.influxdb.InfluxDbTool import InfluxDbTool


def _make_ifc() -> InfluxDbTool:
    return InfluxDbTool(INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG)


@task(name='check-market-open')
def check_market_open_task() -> bool:
    logger = get_run_logger()
    open_ = is_market_open()
    logger.info('Market is %s', 'open' if open_ else 'closed')
    return open_


@task(name='fetch-candlestick-data', retries=3, retry_delay_seconds=30)
def fetch_candlestick_data(config_file: str, instrument: str, granularity: str) -> list[dict]:
    logger = get_run_logger()
    ifc = _make_ifc()
    etl = CandlestickETL(instrument, granularity, config_file, ifc)
    etl.fit()
    logger.info('Fetched %d records for %s %s', len(etl.to_influx_list), instrument, granularity)
    return etl.to_influx_list


@task(name='insert-to-influxdb')
def insert_to_influxdb(records: list[dict]) -> None:
    logger = get_run_logger()
    if not records:
        logger.info('No new records to insert')
        return
    ifc = _make_ifc()
    ifc.insert_dictionary_list(records, ALLOWED_TAGS, ALLOWED_FIELDS, INFLUXDB_BUCKET)
    logger.info('Inserted %d records', len(records))


@flow(name='forex-candlestick-etl', log_prints=True)
def candlestick_flow(config_file: str, instrument: str, granularity: str) -> None:
    if not check_market_open_task():
        return
    records = fetch_candlestick_data(config_file, instrument, granularity)
    insert_to_influxdb(records)


if __name__ == '__main__':
    candlestick_flow.serve(name='forex-candlestick-etl')
