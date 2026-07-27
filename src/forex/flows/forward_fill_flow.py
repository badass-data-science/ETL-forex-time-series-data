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

from forex.etl.config import database_config
from forex.etl.models import ForwardFilledCandlestickRecord
from forex.etl.pipelines.ForwardFillInator import ForwardFillInator
from forex.flows.candlestick_flow import TRACKED_INSTRUMENTS
from python_tools_and_shortcuts.databases.influxdb.InfluxDbTool import InfluxDbTool


def _make_ifc() -> InfluxDbTool:
    # database_config lazy-loads credentials via a module-level __getattr__ triggered
    # on attribute access -- accessed here as database_config.X (not `from
    # database_config import X` at module top level) so that merely importing this
    # module (e.g. via pytest collecting an unrelated test file) never touches AWS
    # Secrets Manager; only actually calling _make_ifc() does.
    return InfluxDbTool(database_config.INFLUXDB_URL, database_config.INFLUXDB_TOKEN, database_config.INFLUXDB_ORG)


@task(name='forward-fill-candlesticks', retries=3, retry_delay_seconds=30)
def forward_fill_task(instrument: str, granularity: str) -> list[dict]:
    logger = get_run_logger()
    ifc = _make_ifc()
    ff = ForwardFillInator(instrument, granularity, ifc, influxdb_bucket=database_config.INFLUXDB_BUCKET)
    ff.fit()
    logger.info(
        'Forward-filled %s %s: %d rows (%d forward-filled)',
        instrument,
        granularity,
        len(ff.df_all_time_diff_market_open_forward_filled),
        int(ff.df_all_time_diff_market_open_forward_filled['is_forward_filled'].sum()),
    )
    return ff.to_influx_list


@task(name='insert-forward-filled-to-influxdb', retries=3, retry_delay_seconds=30)
def insert_to_influxdb(records: list[dict]) -> None:
    logger = get_run_logger()
    if not records:
        logger.info('No forward-filled records to insert')
        return
    ifc = _make_ifc()
    ifc.insert_dictionary_list(
        records, ForwardFilledCandlestickRecord.TAGS, ForwardFilledCandlestickRecord.FIELDS,
        database_config.INFLUXDB_BUCKET,
    )
    logger.info('Inserted %d forward-filled records', len(records))


@flow(name='forex-forward-fill', log_prints=True)
def forward_fill_flow(instrument: str, granularity: str) -> None:
    records = forward_fill_task(instrument, granularity)
    insert_to_influxdb(records)


@flow(name='forex-forward-fill-batch', log_prints=True)
def forward_fill_batch_flow(
    granularity: str,
    instruments: list[str] | None = None,
) -> None:
    """Run forward_fill_flow for each instrument sequentially, mirroring
    candlestick_flow.py's candlestick_batch_flow. `instruments` defaults to
    TRACKED_INSTRUMENTS converted from its underscore form ('EUR_USD', the Oanda API
    convention) to the slash form InfluxDB actually stores the instrument tag as
    ('EUR/USD') -- see CandlestickETL.compute_candle_features.
    """
    logger = get_run_logger()
    pairs = instruments if instruments is not None else [p.replace('_', '/') for p in TRACKED_INSTRUMENTS]
    for instrument in pairs:
        logger.info('Forward-filling %s %s', instrument, granularity)
        forward_fill_flow(instrument=instrument, granularity=granularity)


if __name__ == '__main__':
    forward_fill_flow.serve(name='forex-forward-fill')
