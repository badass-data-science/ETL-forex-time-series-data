"""
Prefect flow: forward-fill stored candlestick gaps → InfluxDB.

Deploy:
    python -m forex.flows.forward_fill_flow

Run ad-hoc:
    prefect run -m forex.flows.forward_fill_flow \
        --param instrument=EUR/USD \
        --param granularity=H1
"""

from prefect import flow, task, get_run_logger

from forex.etl.config.database_config import INFLUXDB_BUCKET, INFLUXDB_ORG, INFLUXDB_TOKEN, INFLUXDB_URL
from forex.etl.models import ForwardFilledCandlestickRecord
from forex.etl.pipelines.ForwardFillInator import ForwardFillInator
from python_tools_and_shortcuts.databases.influxdb.InfluxDbTool import InfluxDbTool


def _make_ifc() -> InfluxDbTool:
    return InfluxDbTool(INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG)


@task(name='forward-fill-candlesticks', retries=3, retry_delay_seconds=30)
def forward_fill_task(instrument: str, granularity: str) -> list[dict]:
    logger = get_run_logger()
    ifc = _make_ifc()
    ff = ForwardFillInator(instrument, granularity, ifc, influxdb_bucket=INFLUXDB_BUCKET)
    ff.fit()
    logger.info(
        'Forward-filled %s %s: %d rows (%d forward-filled)',
        instrument,
        granularity,
        len(ff.df_all_time_diff_market_open_forward_filled),
        int(ff.df_all_time_diff_market_open_forward_filled['is_forward_filled'].sum()),
    )
    return ff.to_influx_list


@task(name='insert-forward-filled-to-influxdb')
def insert_to_influxdb(records: list[dict]) -> None:
    logger = get_run_logger()
    if not records:
        logger.info('No forward-filled records to insert')
        return
    ifc = _make_ifc()
    ifc.insert_dictionary_list(
        records, ForwardFilledCandlestickRecord.TAGS, ForwardFilledCandlestickRecord.FIELDS, INFLUXDB_BUCKET,
    )
    logger.info('Inserted %d forward-filled records', len(records))


@flow(name='forex-forward-fill', log_prints=True)
def forward_fill_flow(instrument: str, granularity: str) -> None:
    records = forward_fill_task(instrument, granularity)
    insert_to_influxdb(records)


if __name__ == '__main__':
    forward_fill_flow.serve(name='forex-forward-fill')
