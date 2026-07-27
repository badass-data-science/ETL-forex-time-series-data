import json
import functools

from python_tools_and_shortcuts.aws.secrets_manager import get_secret

_SECRET_NAME = 'Forex/FinnhubApiKey'
_KEYS = frozenset(['FINNHUB_API_KEY'])

@functools.lru_cache(maxsize=None)
def _load_secret():
    return json.loads(get_secret(_SECRET_NAME))

def __getattr__(name):
    if name in _KEYS:
        return _load_secret()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
