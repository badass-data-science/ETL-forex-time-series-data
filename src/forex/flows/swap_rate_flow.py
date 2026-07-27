"""
Prefect flow: fetch OANDA per-instrument financing (swap/rollover) rates → InfluxDB.

Single run (ad-hoc):
    python -m forex.flows.swap_rate_flow

All major pairs on a schedule:
    python -m forex.flows.serve
"""

from prefect import flow, task, get_run_logger

from forex.etl.SwapRateETL import SwapRateETL
from forex.etl.config import database_config
from forex.etl.models import SwapRateRecord
from forex.flows.candlestick_flow import TRACKED_INSTRUMENTS
from python_tools_and_shortcuts.databases.influxdb.InfluxDbTool import InfluxDbTool


def _make_ifc() -> InfluxDbTool:
    # See candlestick_flow._make_ifc for why this is a function, not a module-level
    # constant -- database_config lazy-loads secrets on attribute access.
    return InfluxDbTool(database_config.INFLUXDB_URL, database_config.INFLUXDB_TOKEN, database_config.INFLUXDB_ORG)


@task(name='fetch-swap-rates', retries=3, retry_delay_seconds=30)
def fetch_swap_rates(config_file: str, instruments: list[str]) -> list[dict]:
    logger = get_run_logger()
    etl = SwapRateETL(instruments, config_file)
    etl.fit()
    logger.info('Fetched swap rates for %d instruments', len(etl.to_influx_list))
    return etl.to_influx_list


@task(name='insert-swap-rates-to-influxdb', retries=3, retry_delay_seconds=30)
def insert_swap_rates_to_influxdb(records: list[dict]) -> None:
    logger = get_run_logger()
    if not records:
        logger.info('No swap rate records to insert')
        return
    ifc = _make_ifc()
    ifc.insert_dictionary_list(records, SwapRateRecord.TAGS, SwapRateRecord.FIELDS, database_config.INFLUXDB_BUCKET)
    logger.info('Inserted %d swap rate records', len(records))


@flow(name='forex-swap-rate-etl', log_prints=True)
def swap_rate_flow(config_file: str, instruments: list[str] = TRACKED_INSTRUMENTS) -> None:
    """No market-hours gate (unlike candlestick_flow) -- financing rates are an
    account-level daily snapshot, not tied to candle formation, and OANDA continues
    to serve this endpoint outside trading hours."""
    records = fetch_swap_rates(config_file, instruments)
    insert_swap_rates_to_influxdb(records)


if __name__ == '__main__':
    swap_rate_flow.serve(name='forex-swap-rate-etl')
