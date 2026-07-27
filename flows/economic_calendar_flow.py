"""
Prefect flow: fetch upcoming economic calendar events (Finnhub) → InfluxDB.

Single run (ad-hoc):
    python -m forex.flows.economic_calendar_flow

All scheduled on a cron:
    python -m forex.flows.serve
"""

import datetime

from prefect import flow, task, get_run_logger

from forex.etl.EconomicCalendarETL import EconomicCalendarETL
from forex.etl.config import database_config, finnhub_config
from forex.etl.models import EconomicCalendarEventRecord
from python_tools_and_shortcuts.databases.influxdb.InfluxDbTool import InfluxDbTool


def _make_ifc() -> InfluxDbTool:
    # See candlestick_flow._make_ifc for why this is a function, not a module-level
    # constant -- database_config lazy-loads secrets on attribute access.
    return InfluxDbTool(database_config.INFLUXDB_URL, database_config.INFLUXDB_TOKEN, database_config.INFLUXDB_ORG)


@task(name='fetch-economic-calendar', retries=3, retry_delay_seconds=30)
def fetch_economic_calendar(from_date: str, to_date: str) -> list[dict]:
    logger = get_run_logger()
    etl = EconomicCalendarETL(finnhub_config.FINNHUB_API_KEY)
    etl.fit(from_date, to_date)
    logger.info('Fetched %d calendar events for %s to %s', len(etl.to_influx_list), from_date, to_date)
    return etl.to_influx_list


@task(name='insert-economic-calendar-to-influxdb', retries=3, retry_delay_seconds=30)
def insert_economic_calendar_to_influxdb(records: list[dict]) -> None:
    logger = get_run_logger()
    if not records:
        logger.info('No calendar events to insert')
        return
    ifc = _make_ifc()
    ifc.insert_dictionary_list(
        records, EconomicCalendarEventRecord.TAGS, EconomicCalendarEventRecord.FIELDS, database_config.INFLUXDB_BUCKET,
    )
    logger.info('Inserted %d calendar events', len(records))


@flow(name='forex-economic-calendar-etl', log_prints=True)
def economic_calendar_flow(days_ahead: int = 14) -> None:
    """Pulls a rolling window of events from today through `days_ahead` days out,
    rather than a single fixed date -- the whole point is having known-in-advance
    event times on hand before they happen. Re-pulling the same window on each
    scheduled run is cheap and naturally idempotent (see EconomicCalendarETL)."""
    today = datetime.date.today()
    from_date = today.isoformat()
    to_date = (today + datetime.timedelta(days=days_ahead)).isoformat()
    records = fetch_economic_calendar(from_date, to_date)
    insert_economic_calendar_to_influxdb(records)


if __name__ == '__main__':
    economic_calendar_flow.serve(name='forex-economic-calendar-etl')
