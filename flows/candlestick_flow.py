"""
Prefect flows: fetch Oanda candlesticks → InfluxDB.

Single pair (ad-hoc):
    python -m forex.flows.candlestick_flow

All major pairs on a schedule:
    python -m forex.flows.serve
"""

from prefect import flow, task, get_run_logger

from forex.critical_timezone import is_market_open
from forex.etl.CandlestickETL import CandlestickETL
from forex.etl.config import database_config
from forex.etl.models import CandlestickRecord
from python_tools_and_shortcuts.databases.influxdb.InfluxDbTool import InfluxDbTool


def _make_ifc() -> InfluxDbTool:
    # database_config lazy-loads credentials via a module-level __getattr__ triggered
    # on attribute access -- accessed here as database_config.X (not `from
    # database_config import X` at module top level) so that merely importing this
    # module (e.g. via pytest collecting an unrelated test file) never touches AWS
    # Secrets Manager; only actually calling _make_ifc() does.
    return InfluxDbTool(database_config.INFLUXDB_URL, database_config.INFLUXDB_TOKEN, database_config.INFLUXDB_ORG)


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
    ifc.insert_dictionary_list(records, CandlestickRecord.TAGS, CandlestickRecord.FIELDS, database_config.INFLUXDB_BUCKET)
    logger.info('Inserted %d records', len(records))


MAJOR_PAIRS: list[str] = [
    'EUR_USD', 'USD_JPY', 'GBP_USD', 'USD_CHF',
    'USD_CAD', 'AUD_USD', 'NZD_USD',
]


@flow(name='forex-candlestick-etl', log_prints=True)
def candlestick_flow(config_file: str, instrument: str, granularity: str) -> None:
    if not check_market_open_task():
        return
    records = fetch_candlestick_data(config_file, instrument, granularity)
    insert_to_influxdb(records)


@flow(name='forex-candlestick-batch', log_prints=True)
def candlestick_batch_flow(
    config_file: str,
    granularity: str,
    instruments: list[str] = MAJOR_PAIRS,
) -> None:
    """Run candlestick_flow for each instrument sequentially."""
    for instrument in instruments:
        candlestick_flow(config_file=config_file, instrument=instrument, granularity=granularity)


if __name__ == '__main__':
    candlestick_flow.serve(name='forex-candlestick-etl')
