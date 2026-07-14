from __future__ import annotations

import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from forex.etl.pipelines.ForwardFillInator import ForwardFillInator

TZ = ZoneInfo('America/Toronto')


def _make_hourly_frame(start: datetime.datetime, n: int, missing_index: int | None = None) -> pd.DataFrame:
    timestamps = [start + datetime.timedelta(hours=i) for i in range(n)]
    df = pd.DataFrame({
        'unix_epoch_s': [int(t.timestamp()) for t in timestamps],
        'volume': [100.0 + i for i in range(n)],
        'mid_open': [1.10 + 0.0001 * i for i in range(n)],
        'mid_high': [1.101 + 0.0001 * i for i in range(n)],
        'mid_low': [1.099 + 0.0001 * i for i in range(n)],
        'mid_close': [1.1005 + 0.0001 * i for i in range(n)],
        'spread_close': [0.0002 for _ in range(n)],
    })
    if missing_index is not None:
        df = df.drop(index=missing_index).reset_index(drop=True)
    return df


def _run_through_forward_fill(df: pd.DataFrame, granularity: str = 'H1') -> ForwardFillInator:
    inator = ForwardFillInator('EUR/USD', granularity, ifc=None, cutoff_timestamp=0)
    inator.df = df
    inator.perform_time_calculations()
    inator.weekday_hour_qa()
    inator.compute_df_all_time_diff_market_open()
    inator.forward_fill_it()
    inator.account_for_holiday_market_closure()
    return inator


def _run_full_fit(df: pd.DataFrame, granularity: str = 'H1') -> ForwardFillInator:
    inator = _run_through_forward_fill(df, granularity)
    inator.make_the_influxdb_dict()
    return inator


class TestIsForwardFilledFlag:
    def test_missing_bar_is_flagged_and_real_bars_are_not(self):
        start = datetime.datetime(2024, 1, 2, 9, 0, tzinfo=TZ)  # Tuesday
        assert start.weekday() == 1  # clear of the Friday-17:00 / weekend cuts
        df = _make_hourly_frame(start, n=10, missing_index=5)

        inator = _run_through_forward_fill(df)
        result = (
            inator.df_all_time_diff_market_open_forward_filled
            .sort_values('unix_epoch_s').reset_index(drop=True)
        )

        missing_ts = int((start + datetime.timedelta(hours=5)).timestamp())
        assert result['is_forward_filled'].sum() == 1
        flagged_row = result[result['unix_epoch_s'] == missing_ts].iloc[0]
        assert bool(flagged_row['is_forward_filled'])

        other_rows = result[result['unix_epoch_s'] != missing_ts]
        assert not other_rows['is_forward_filled'].any()

    def test_forward_fill_actually_propagates_the_previous_value(self):
        """This is exactly what the pre-fix call order (account_for_holiday_market_
        closure() before forward_fill_it(), dropping every gap via a bare dropna()
        before ffill ever ran) prevented -- the filled row would have stayed NaN
        (and then been silently dropped) instead of inheriting the previous value."""
        start = datetime.datetime(2024, 1, 2, 9, 0, tzinfo=TZ)
        df = _make_hourly_frame(start, n=10, missing_index=5)

        inator = _run_through_forward_fill(df)
        result = (
            inator.df_all_time_diff_market_open_forward_filled
            .sort_values('unix_epoch_s').reset_index(drop=True)
        )

        missing_ts = int((start + datetime.timedelta(hours=5)).timestamp())
        prev_ts = int((start + datetime.timedelta(hours=4)).timestamp())

        filled_volume = result.loc[result['unix_epoch_s'] == missing_ts, 'volume'].iloc[0]
        prev_volume = result.loc[result['unix_epoch_s'] == prev_ts, 'volume'].iloc[0]
        assert filled_volume == prev_volume
        assert result['volume'].isna().sum() == 0

    def test_no_missing_bars_means_nothing_forward_filled(self):
        start = datetime.datetime(2024, 1, 2, 9, 0, tzinfo=TZ)
        df = _make_hourly_frame(start, n=10, missing_index=None)

        inator = _run_through_forward_fill(df)
        result = inator.df_all_time_diff_market_open_forward_filled

        assert not result['is_forward_filled'].any()
        assert result['volume'].isna().sum() == 0


def _make_local_time_frame(timestamps: list[datetime.datetime]) -> pd.DataFrame:
    n = len(timestamps)
    return pd.DataFrame({
        'unix_epoch_s': [int(t.timestamp()) for t in timestamps],
        'volume': [100.0 + i for i in range(n)],
        'mid_open': [1.10 + 0.0001 * i for i in range(n)],
        'mid_high': [1.101 + 0.0001 * i for i in range(n)],
        'mid_low': [1.099 + 0.0001 * i for i in range(n)],
        'mid_close': [1.1005 + 0.0001 * i for i in range(n)],
        'spread_close': [0.0002 for _ in range(n)],
    })


class TestDstAwareGrid:
    """Regression coverage for a real, confirmed-in-production bug: H4/D candles
    are anchored to a local time-of-day (the same 5pm America/New_York-style
    rollover convention used elsewhere in this pipeline), which shifts by
    exactly one hour, in UTC terms, at every DST transition. The old grid
    (`np.arange(mn, mx + step, step)`, a fixed number of UTC seconds forever)
    silently fell out of alignment with real data once that happened -- checked
    directly against real EUR/USD history, ~66% of H4/D rows were misaligned,
    with the very first bad H4 row landing exactly on 2010-03-14, the date the
    US switched to Daylight Time that year. H1/M15 (anchored to fixed
    UTC-hour/quarter-hour marks) were never affected."""

    def test_h4_grid_survives_a_spring_forward_transition(self):
        # Two throwaway buffer bars first: both perform_time_calculations() and
        # compute_df_all_time_diff_market_open() each drop the first row of
        # whatever they're given (nothing to compute a lagged-timestamp diff
        # against) -- real, pre-existing, inconsequential-in-production
        # behavior (production always pulls from a fixed, years-old cutoff
        # timestamp, so this only ever discards one row near the very start of
        # the entire history). Unrelated to what this test checks, so absorb
        # it with buffer rows rather than let it eat into the real assertions.
        buffer_timestamps = [
            datetime.datetime(2024, 3, 6, 9, 0, tzinfo=TZ),
            datetime.datetime(2024, 3, 7, 9, 0, tzinfo=TZ),
        ]
        # Real bars at fixed LOCAL wall-clock hours, on the Friday before and
        # the Monday after the March 2024 US spring-forward (Sun March 10,
        # 2am EST -> 3am EDT), skipping the closed weekend in between like real
        # data does. Friday is restricted to hours before the 5pm market close
        # (a 9pm Friday bar isn't real data -- the market's shut by then, and
        # correctly excluded by the existing market-hours filter, unrelated to
        # what this test checks); Monday, fully open all day, gets the full set.
        friday_hours = [1, 5, 9, 13]
        monday_hours = [1, 5, 9, 13, 21]
        subject_timestamps = (
            [datetime.datetime(2024, 3, 8, h, 0, tzinfo=TZ) for h in friday_hours]
            + [datetime.datetime(2024, 3, 11, h, 0, tzinfo=TZ) for h in monday_hours]
        )
        df = _make_local_time_frame(buffer_timestamps + subject_timestamps)

        inator = _run_through_forward_fill(df, granularity='H4')
        result = inator.df_all_time_diff_market_open_forward_filled

        subject_ts_set = {int(t.timestamp()) for t in subject_timestamps}
        result_timestamps = set(result['unix_epoch_s'])
        assert subject_ts_set <= result_timestamps, "a real bar went missing from the grid entirely"
        for ts in subject_ts_set:
            row = result.loc[result['unix_epoch_s'] == ts].iloc[0]
            assert not row['is_forward_filled'], (
                f"real bar at {ts} was wrongly treated as missing/forward-filled across the DST transition"
            )

    def test_daily_grid_survives_a_spring_forward_transition(self):
        # Two throwaway buffer bars first -- see the H4 test above for why.
        buffer_timestamps = [
            datetime.datetime(2024, 3, 4, 9, 0, tzinfo=TZ),
            datetime.datetime(2024, 3, 5, 9, 0, tzinfo=TZ),
        ]
        # One bar per real trading day at a fixed local time, spanning the
        # same March 2024 spring-forward.
        days = [
            datetime.date(2024, 3, 6), datetime.date(2024, 3, 7), datetime.date(2024, 3, 8),
            datetime.date(2024, 3, 11), datetime.date(2024, 3, 12),
        ]
        subject_timestamps = [datetime.datetime(d.year, d.month, d.day, 9, 0, tzinfo=TZ) for d in days]
        df = _make_local_time_frame(buffer_timestamps + subject_timestamps)

        inator = _run_through_forward_fill(df, granularity='D')
        result = inator.df_all_time_diff_market_open_forward_filled

        subject_ts_set = {int(t.timestamp()) for t in subject_timestamps}
        result_timestamps = set(result['unix_epoch_s'])
        assert subject_ts_set <= result_timestamps, "a real bar went missing from the grid entirely"
        for ts in subject_ts_set:
            row = result.loc[result['unix_epoch_s'] == ts].iloc[0]
            assert not row['is_forward_filled'], (
                f"real bar at {ts} was wrongly treated as missing/forward-filled across the DST transition"
            )


class TestMakeTheInfluxDbDict:
    def test_records_carry_the_forward_filled_flag_and_expected_schema(self):
        start = datetime.datetime(2024, 1, 2, 9, 0, tzinfo=TZ)
        df = _make_hourly_frame(start, n=10, missing_index=5)

        inator = _run_full_fit(df, granularity='H1')
        missing_ts = int((start + datetime.timedelta(hours=5)).timestamp())

        by_time = {r['time']: r for r in inator.to_influx_list}
        # 10 rows, minus the intentional gap at hour 5, minus the very first
        # surviving row (perform_time_calculations always drops it: its
        # lagged_unix_epoch_s is NaN with nothing before it, unrelated to this test)
        assert len(by_time) == 8
        assert by_time[missing_ts]['fields']['is_forward_filled'] is True
        assert all(
            r['fields']['is_forward_filled'] is False for t, r in by_time.items() if t != missing_ts
        )

        sample = next(iter(by_time.values()))
        assert sample['measurement'] == 'forward-filled candlestick'
        assert sample['tags'] == {'instrument': 'EUR/USD', 'granularity': 'H1'}
        assert set(sample['fields']) == {
            'mid_open', 'mid_high', 'mid_low', 'mid_close', 'spread_close', 'volume', 'is_forward_filled',
        }
