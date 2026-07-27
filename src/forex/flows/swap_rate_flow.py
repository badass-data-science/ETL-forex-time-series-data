"""
Prefect flow: fetch OANDA per-instrument financing (swap/rollover) rates → InfluxDB.

Single run (ad-hoc):
    python -m forex.flows.swap_rate_flow

All major pairs on a schedule:
    python -m forex.flows.serve
"""

from prefect import flow, get_run_logger, task

from forex.etl.config import database_config
from forex.etl.models import SwapRateRecord
from forex.etl.SwapRateETL import SwapRateETL
from forex.flows._common import make_ifc
from forex.flows.candlestick_flow import TRACKED_INSTRUMENTS


@task(name='fetch-swap-rates', retries=3, retry_delay_seconds=30)
def fetch_swap_rates(instruments: list[str]) -> list[dict]:
    logger = get_run_logger()
    etl = SwapRateETL(instruments)
    etl.fit()
    logger.info('Fetched swap rates for %d instruments', len(etl.to_influx_list))
    return etl.to_influx_list


@task(name='insert-swap-rates-to-influxdb', retries=3, retry_delay_seconds=30)
def insert_swap_rates_to_influxdb(records: list[dict]) -> None:
    logger = get_run_logger()
    if not records:
        logger.info('No swap rate records to insert')
        return
    ifc = make_ifc()
    ifc.insert_dictionary_list(records, SwapRateRecord.TAGS, SwapRateRecord.FIELDS, database_config.INFLUXDB_BUCKET)
    logger.info('Inserted %d swap rate records', len(records))


@flow(name='forex-swap-rate-etl', log_prints=True)
def swap_rate_flow(instruments: list[str] = TRACKED_INSTRUMENTS) -> None:
    """No market-hours gate (unlike candlestick_flow) -- financing rates are an
    account-level daily snapshot, not tied to candle formation, and OANDA continues
    to serve this endpoint outside trading hours."""
    records = fetch_swap_rates(instruments)
    insert_swap_rates_to_influxdb(records)


if __name__ == '__main__':
    swap_rate_flow.serve(name='forex-swap-rate-etl')
