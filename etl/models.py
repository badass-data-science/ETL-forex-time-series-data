from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel


class CandlestickRecord(BaseModel):
    # Tags
    instrument: str
    granularity: str
    # Fields
    volume: int
    complete: bool
    bid_open: float
    bid_high: float
    bid_low: float
    bid_close: float
    ask_open: float
    ask_high: float
    ask_low: float
    ask_close: float
    # Time
    timestamp: int

    TAGS: ClassVar[frozenset[str]] = frozenset({'instrument', 'granularity'})
    MEASUREMENT: ClassVar[str] = 'candlestick'
    FIELDS: ClassVar[dict[str, type]] = {
        'volume': int,
        'complete': bool,
        'bid_open': float,
        'bid_high': float,
        'bid_low': float,
        'bid_close': float,
        'ask_open': float,
        'ask_high': float,
        'ask_low': float,
        'ask_close': float,
    }

    def to_influx_dict(self) -> dict:
        data = self.model_dump()
        result: dict = {
            'measurement': self.MEASUREMENT,
            'tags': {},
            'fields': {},
            'time': data.pop('timestamp'),
        }
        for key, value in data.items():
            if key in self.TAGS:
                result['tags'][key] = value
            else:
                result['fields'][key] = value
        return result


class SwapRateRecord(BaseModel):
    """Per-instrument long/short financing (swap/rollover) rate -- an account-level
    daily snapshot, not tied to a candle granularity, so unlike CandlestickRecord
    there's no `granularity` tag here."""

    # Tags
    instrument: str
    # Fields
    long_rate: float
    short_rate: float
    # Time
    timestamp: int

    TAGS: ClassVar[frozenset[str]] = frozenset({'instrument'})
    MEASUREMENT: ClassVar[str] = 'swap-rate'
    FIELDS: ClassVar[dict[str, type]] = {
        'long_rate': float,
        'short_rate': float,
    }

    def to_influx_dict(self) -> dict:
        data = self.model_dump()
        result: dict = {
            'measurement': self.MEASUREMENT,
            'tags': {},
            'fields': {},
            'time': data.pop('timestamp'),
        }
        for key, value in data.items():
            if key in self.TAGS:
                result['tags'][key] = value
            else:
                result['fields'][key] = value
        return result


class EconomicCalendarEventRecord(BaseModel):
    """A scheduled economic calendar event (Finnhub) -- release time, country,
    impact level, and actual/estimate/previous values if available. Not part of
    OANDA's API; a separate provider/credential (see config/finnhub_config.py).
    `actual`/`estimate`/`prev` are the whole point of pulling this data BEFORE it
    happens: a future-scheduled event has no `actual` yet (and possibly no
    `estimate` either), so these are optional and simply omitted from `fields`
    rather than written as null -- see `to_influx_dict` below."""

    # Tags
    country: str
    impact: str
    # `event` (e.g. "Non-Farm Payrolls", "CPI", "FOMC Rate Decision") is a tag, not a
    # field, despite higher cardinality than instrument/granularity/country/impact --
    # it's a bounded, recurring set of named releases (not free text), and being able
    # to filter/group by event name is exactly the point of ingesting this data.
    event: str
    # Fields
    actual: float | None = None
    estimate: float | None = None
    prev: float | None = None
    unit: str = ''
    # Time
    timestamp: int

    TAGS: ClassVar[frozenset[str]] = frozenset({'country', 'impact', 'event'})
    MEASUREMENT: ClassVar[str] = 'economic-calendar-event'
    FIELDS: ClassVar[dict[str, type]] = {
        'actual': float,
        'estimate': float,
        'prev': float,
        'unit': str,
    }

    def to_influx_dict(self) -> dict:
        data = self.model_dump()
        result: dict = {
            'measurement': self.MEASUREMENT,
            'tags': {},
            'fields': {},
            'time': data.pop('timestamp'),
        }
        for key, value in data.items():
            if key in self.TAGS:
                result['tags'][key] = value
            elif value is not None:
                result['fields'][key] = value
        return result


class ForwardFilledCandlestickRecord(BaseModel):
    # Tags
    instrument: str
    granularity: str
    # Fields
    mid_open: float
    mid_high: float
    mid_low: float
    mid_close: float
    spread_close: float
    volume: float
    is_forward_filled: bool
    # Time
    timestamp: int

    TAGS: ClassVar[frozenset[str]] = frozenset({'instrument', 'granularity'})
    MEASUREMENT: ClassVar[str] = 'forward-filled candlestick'
    FIELDS: ClassVar[dict[str, type]] = {
        'mid_open': float,
        'mid_high': float,
        'mid_low': float,
        'mid_close': float,
        'spread_close': float,
        'volume': float,
        'is_forward_filled': bool,
    }

    def to_influx_dict(self) -> dict:
        data = self.model_dump()
        result: dict = {
            'measurement': self.MEASUREMENT,
            'tags': {},
            'fields': {},
            'time': data.pop('timestamp'),
        }
        for key, value in data.items():
            if key in self.TAGS:
                result['tags'][key] = value
            else:
                result['fields'][key] = value
        return result
