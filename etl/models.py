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
