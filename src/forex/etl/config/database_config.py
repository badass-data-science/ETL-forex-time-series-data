import os

_KEYS = frozenset(['INFLUXDB_URL', 'INFLUXDB_TOKEN', 'INFLUXDB_ORG', 'INFLUXDB_BUCKET'])

def __getattr__(name):
    if name in _KEYS:
        return os.environ[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
