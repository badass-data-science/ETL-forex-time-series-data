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
