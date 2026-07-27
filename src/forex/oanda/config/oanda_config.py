import os

_REQUIRED_KEYS = frozenset(['OANDA_SERVER', 'OANDA_TOKEN', 'OANDA_DATE_TIME_FORMAT'])
_OPTIONAL_KEYS = frozenset(['OANDA_ACCOUNT_ID'])


def __getattr__(name):
    if name in _REQUIRED_KEYS:
        try:
            return os.environ[name]
        except KeyError:
            raise AttributeError(f'environment variable {name!r} is not set') from None
    if name in _OPTIONAL_KEYS:
        return os.environ.get(name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
