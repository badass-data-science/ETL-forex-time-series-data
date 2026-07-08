"""
Prefect flow: fetch OANDA per-instrument order-book/position-book snapshots →
InfluxDB.

Single run (ad-hoc):
    python -m forex.flows.positioning_flow

All major pairs on a schedule:
    python -m forex.flows.serve
"""

from prefect import flow, task, get_run_logger

from forex.etl.PositioningETL import PositioningETL
from forex.etl.config import database_config
from forex.etl.models import PositioningBucketRecord
from forex.flows.candlestick_flow import MAJOR_PAIRS
from python_tools_and_shortcuts.databases.influxdb.InfluxDbTool import InfluxDbTool


def _make_ifc() -> InfluxDbTool:
    # See candlestick_flow._make_ifc for why this is a function, not a module-level
    # constant -- database_config lazy-loads secrets on attribute access.
    return InfluxDbTool(database_config.INFLUXDB_URL, database_config.INFLUXDB_TOKEN, database_config.INFLUXDB_ORG)


@task(name='fetch-positioning', retries=3, retry_delay_seconds=30)
def fetch_positioning(config_file: str, instruments: list[str]) -> list[dict]:
    logger = get_run_logger()
    etl = PositioningETL(instruments, config_file)
    etl.fit()
    logger.info('Fetched %d positioning buckets', len(etl.to_influx_list))
    return etl.to_influx_list


@task(name='insert-positioning-to-influxdb')
def insert_positioning_to_influxdb(records: list[dict]) -> None:
    logger = get_run_logger()
    if not records:
        logger.info('No positioning records to insert')
        return
    ifc = _make_ifc()
    ifc.insert_dictionary_list(
        records, PositioningBucketRecord.TAGS, PositioningBucketRecord.FIELDS, database_config.INFLUXDB_BUCKET,
    )
    logger.info('Inserted %d positioning records', len(records))


@flow(name='forex-positioning-etl', log_prints=True)
def positioning_flow(config_file: str, instruments: list[str] = MAJOR_PAIRS) -> None:
    """No market-hours gate -- OANDA continues to serve its most recent order-book/
    position-book snapshot even outside trading hours (it just won't have updated
    recently)."""
    records = fetch_positioning(config_file, instruments)
    insert_positioning_to_influxdb(records)


if __name__ == '__main__':
    positioning_flow.serve(name='forex-positioning-etl')
