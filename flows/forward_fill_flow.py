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
from forex.etl.pipelines.ForwardFillInator import ForwardFillInator
from python_tools_and_shortcuts.databases.influxdb.InfluxDbTool import InfluxDbTool


def _make_ifc() -> InfluxDbTool:
    return InfluxDbTool(INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG)


@task(name='forward-fill-candlesticks')
def forward_fill_task(instrument: str, granularity: str) -> None:
    logger = get_run_logger()
    ifc = _make_ifc()
    ff = ForwardFillInator(instrument, granularity, ifc, influxdb_bucket=INFLUXDB_BUCKET)
    ff.fit()
    logger.info(
        'Forward-filled %s %s: %d rows',
        instrument,
        granularity,
        len(ff.df_all_time_diff_market_open_forward_filled),
    )


@flow(name='forex-forward-fill', log_prints=True)
def forward_fill_flow(instrument: str, granularity: str) -> None:
    forward_fill_task(instrument, granularity)


if __name__ == '__main__':
    forward_fill_flow.serve(name='forex-forward-fill')
