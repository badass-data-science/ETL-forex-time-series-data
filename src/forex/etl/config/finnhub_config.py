import os

_KEYS = frozenset(['FINNHUB_API_KEY'])


def __getattr__(name):
    if name in _KEYS:
        try:
            return os.environ[name]
        except KeyError:
            raise AttributeError(f'environment variable {name!r} is not set') from None
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
