from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel


class MeasurementRecord(BaseModel):
    """Base class for every InfluxDB-bound record in this pipeline. Subclasses
    declare TAGS/MEASUREMENT/FIELDS and get to_influx_dict() for free -- every
    subclass previously reimplemented the identical tag/field/time split by
    hand, keyed only off its own TAGS set.

    `omit_none_fields`: if True (EconomicCalendarEventRecord only), a field
    whose value is None is dropped from `fields` entirely rather than written
    as a null -- the point of a future-scheduled event is that `actual` (and
    sometimes `estimate`) genuinely doesn't exist yet."""

    TAGS: ClassVar[frozenset[str]]
    MEASUREMENT: ClassVar[str]
    # FIELDS duplicates what Pydantic already knows via model_fields; kept as an
    # explicit, hand-maintained dict rather than derived because InfluxDbTool.
    # validate_point() needs it as a plain runtime dict of {name: type} for schema
    # checks against raw dicts that were never Pydantic models in the first place
    # (see validate_point's ALLOWED_FIELDS parameter) -- deriving it from
    # model_fields would need to exclude TAGS by convention anyway, so the
    # explicitness isn't costing much for a repo this size.
    FIELDS: ClassVar[dict[str, type]]
    omit_none_fields: ClassVar[bool] = False

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
            elif not (self.omit_none_fields and value is None):
                result['fields'][key] = value
        return result


class CandlestickRecord(MeasurementRecord):
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


class SwapRateRecord(MeasurementRecord):
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


class EconomicCalendarEventRecord(MeasurementRecord):
    """A scheduled economic calendar event (Finnhub) -- release time, country,
    impact level, and actual/estimate/previous values if available. Not part of
    OANDA's API; a separate provider/credential (see config/finnhub_config.py).
    `actual`/`estimate`/`prev` are the whole point of pulling this data BEFORE it
    happens: a future-scheduled event has no `actual` yet (and possibly no
    `estimate` either), so these are optional and simply omitted from `fields`
    rather than written as null -- see `omit_none_fields` on MeasurementRecord."""

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
    omit_none_fields: ClassVar[bool] = True


class PositioningBucketRecord(MeasurementRecord):
    """One price bucket from an OANDA order-book or position-book snapshot --
    aggregated retail positioning data. Reachable via the same v20 API/token
    already used for candlesticks (just a different path suffix), unlike swap
    rates (needed an account ID) or the economic calendar (a separate provider).

    Stored per-bucket rather than collapsed into a single "overall % long/short"
    summary stat: OANDA's per-bucket longCountPercent/shortCountPercent
    normalization isn't something to silently reinterpret here, and a downstream
    consumer can compute whatever aggregate it actually needs (near-price
    concentration, distance-weighted, etc.) directly from the raw buckets. Real
    responses can carry on the order of a hundred+ buckets per instrument per book
    type per snapshot -- a genuine storage/cardinality cost worth being aware of,
    unlike every other measurement in this pipeline."""

    # Tags
    instrument: str
    book_type: str  # "order" or "position"
    # Fields
    bucket_price: float
    long_count_percent: float
    short_count_percent: float
    # Time
    timestamp: int

    TAGS: ClassVar[frozenset[str]] = frozenset({'instrument', 'book_type'})
    MEASUREMENT: ClassVar[str] = 'positioning-bucket'
    FIELDS: ClassVar[dict[str, type]] = {
        'bucket_price': float,
        'long_count_percent': float,
        'short_count_percent': float,
    }


class ForwardFilledCandlestickRecord(MeasurementRecord):
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
