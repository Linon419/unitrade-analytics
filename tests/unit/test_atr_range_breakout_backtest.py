from datetime import datetime
from zoneinfo import ZoneInfo

from scripts.backtest_atr_range_breakout import (
    Bar,
    Window,
    _make_window,
    backtest_3m_breakout_ema_exit,
    backtest_atr_range_breakout,
 )


def test_backtest_creates_trade_and_forced_end_close_when_no_post_window_bar():
    # Build synthetic 1h bars with a clear breakout and no extra bar after window end.
    # We use tiny lengths so indicators are available quickly.
    tz = ZoneInfo("UTC")

    def ts(h: int) -> int:
        return int(datetime(2026, 1, 1, h, 0, 0, tzinfo=tz).timestamp() * 1000)

    bars = [
        Bar(ts(0), 10, 10.2, 9.8, 10.0),
        Bar(ts(1), 10, 10.2, 9.8, 10.0),
        Bar(ts(2), 10, 10.2, 9.8, 10.0),
        # Breakout bar: close above HH + k*ATR (k=0 for simplicity)
        Bar(ts(3), 10, 11.0, 9.9, 11.0),
        # Entry bar
        Bar(ts(4), 11.0, 11.1, 10.5, 10.8),
        # Last bar inside window; no bar after end boundary
        Bar(ts(5), 10.8, 10.9, 10.4, 10.6),
        Bar(ts(6), 10.6, 10.7, 10.21, 10.5),
    ]

    window = Window(
        start_utc=datetime(2026, 1, 1, 0, 0, 0, tzinfo=tz),
        end_utc=datetime(2026, 1, 1, 7, 0, 0, tzinfo=tz),
    )

    res = backtest_atr_range_breakout(
        bars,
        window=window,
        lookback_n=2,
        atr_len=2,
        k=0.0,  # pure HH/LL breakout
        max_hold_bars=24,
    )

    assert "error" not in res
    assert res["stats"]["count"] == 1
    t = res["trades"][0]
    assert t.side == "long"
    assert t.exit_reason == "end"


def test_make_window_supports_duration_hours_cross_day():
    w = _make_window("2026-01-05", "Australia/Sydney", "05:00", duration_hours=24)
    assert (w.end_utc - w.start_utc).total_seconds() == 24 * 60 * 60


def test_make_window_rejects_end_and_duration_both_set():
    try:
        _make_window("2026-01-05", "Australia/Sydney", "05:00", end_hhmm="10:00", duration_hours=24)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_3m_breakout_entry_and_ema_exit():
    tz = ZoneInfo("UTC")

    def ts_1h(h: int) -> int:
        return int(datetime(2026, 1, 1, h, 0, 0, tzinfo=tz).timestamp() * 1000)

    # 1H bars: make upper level = HH of previous 2 bars (k=0), available at bar index 2.
    bars_1h = [
        Bar(ts_1h(0), 10, 10.2, 9.8, 10.0),
        Bar(ts_1h(1), 10, 10.2, 9.8, 10.0),
        Bar(ts_1h(2), 10, 10.2, 9.8, 10.0),
        Bar(ts_1h(3), 10, 10.2, 9.8, 10.0),
    ]

    # 3m bars start right after 03:00 (so latest completed 1H is bar[2], closing at 03:00).
    base_3m = int(datetime(2026, 1, 1, 3, 0, 0, tzinfo=tz).timestamp() * 1000)
    step_3m = 3 * 60 * 1000

    bars_3m = [
        # Breakout close above 10.2 -> enter next 3m open.
        Bar(base_3m + 0 * step_3m, 10.1, 10.35, 10.05, 10.30),
        # Entry bar (open at 03:03)
        Bar(base_3m + 1 * step_3m, 10.31, 10.40, 10.20, 10.35),
        # Close below EMA(3) after some decay -> exit next open.
        Bar(base_3m + 2 * step_3m, 10.20, 10.25, 10.00, 10.05),
        Bar(base_3m + 3 * step_3m, 10.00, 10.05, 9.90, 9.95),
    ]

    window = Window(
        start_utc=datetime(2026, 1, 1, 3, 0, 0, tzinfo=tz),
        end_utc=datetime(2026, 1, 1, 3, 30, 0, tzinfo=tz),
    )

    res = backtest_3m_breakout_ema_exit(
        bars_1h=bars_1h,
        bars_3m=bars_3m,
        window=window,
        lookback_n=2,
        atr_len=2,
        k=0.0,
        ema_len=3,
    )

    assert "error" not in res
    assert res["stats"]["count"] == 1
    t = res["trades"][0]
    assert t.side == "long"
    assert t.exit_reason == "ema200"
