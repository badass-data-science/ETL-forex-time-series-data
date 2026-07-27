from forex.etl.config import database_config
from forex.util.influxdb_tool import InfluxDbTool


def make_ifc() -> InfluxDbTool:
    # database_config lazy-loads credentials from environment variables via a
    # module-level __getattr__ triggered on attribute access -- accessed here as
    # database_config.X (not `from database_config import X` at module top level)
    # so that merely importing this module (e.g. via pytest collecting an unrelated
    # test file) never requires the env vars to be set; only actually calling
    # make_ifc() does.
    return InfluxDbTool(database_config.INFLUXDB_URL, database_config.INFLUXDB_TOKEN, database_config.INFLUXDB_ORG)
