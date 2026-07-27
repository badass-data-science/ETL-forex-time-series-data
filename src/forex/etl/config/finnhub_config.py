import os

_KEYS = frozenset(['FINNHUB_API_KEY'])

def __getattr__(name):
    if name in _KEYS:
        return os.environ[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
