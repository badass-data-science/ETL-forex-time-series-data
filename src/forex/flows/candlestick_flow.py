"""
Prefect flows: fetch Oanda candlesticks → InfluxDB.

Single instrument (ad-hoc):
    python -m forex.flows.candlestick_flow

All tracked instruments on a schedule:
    python -m forex.flows.serve
"""

from prefect import flow, get_run_logger, task

from forex.critical_timezone import is_market_open
from forex.etl.CandlestickETL import CandlestickETL
from forex.etl.config import database_config
from forex.etl.models import CandlestickRecord
from forex.flows._common import make_ifc


@task(name='check-market-open')
def check_market_open_task() -> bool:
    logger = get_run_logger()
    open_ = is_market_open()
    logger.info('Market is %s', 'open' if open_ else 'closed')
    return open_


@task(name='fetch-candlestick-data', retries=3, retry_delay_seconds=30)
def fetch_candlestick_data(instrument: str, granularity: str) -> list[dict]:
    logger = get_run_logger()
    ifc = make_ifc()
    etl = CandlestickETL(instrument, granularity, ifc)
    etl.fit()
    logger.info('Fetched %d records for %s %s', len(etl.to_influx_list), instrument, granularity)
    return etl.to_influx_list


@task(name='insert-to-influxdb', retries=3, retry_delay_seconds=30)
def insert_to_influxdb(records: list[dict]) -> None:
    logger = get_run_logger()
    if not records:
        logger.info('No new records to insert')
        return
    ifc = make_ifc()
    ifc.insert_dictionary_list(
        records,
        CandlestickRecord.TAGS,
        CandlestickRecord.FIELDS,
        database_config.INFLUXDB_BUCKET,
    )
    logger.info('Inserted %d records', len(records))


# Not just "major pairs" any more (2026-07-14): XAU_USD is a commodity CFD, not a
# currency pair, and the crosses below aren't majors -- both added to explore
# whether less USD-major-crowded markets carry more signal (see forex-ML's
# README). Renamed from MAJOR_PAIRS to reflect that, since it's the single
# instrument list forward_fill_flow/swap_rate_flow/positioning_flow all default to.
TRACKED_INSTRUMENTS: list[str] = [
    'EUR_USD',
    'USD_JPY',
    'GBP_USD',
    'USD_CHF',
    'USD_CAD',
    'AUD_USD',
    'NZD_USD',
    'XAU_USD',
    'GBP_JPY',
    'EUR_JPY',
    'AUD_JPY',
    'EUR_GBP',
    'AUD_NZD',
    'EUR_CHF',
]


@flow(name='forex-candlestick-etl', log_prints=True)
def candlestick_flow(instrument: str, granularity: str) -> None:
    if not check_market_open_task():
        return
    records = fetch_candlestick_data(instrument, granularity)
    insert_to_influxdb(records)


@flow(name='forex-candlestick-batch', log_prints=True)
def candlestick_batch_flow(
    granularity: str,
    instruments: list[str] = TRACKED_INSTRUMENTS,
) -> None:
    """Run candlestick_flow for each instrument sequentially."""
    for instrument in instruments:
        candlestick_flow(instrument=instrument, granularity=granularity)


if __name__ == '__main__':
    candlestick_flow.serve(name='forex-candlestick-etl')
