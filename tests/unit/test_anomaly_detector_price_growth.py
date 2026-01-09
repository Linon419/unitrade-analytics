import asyncio

import pytest

from unitrade.scanner.signal_detector.anomaly_detector import AnomalyConfig, AnomalyDetector


@pytest.mark.asyncio
async def test_detect_breakout_triggers_with_price_growth_and_one_other_factor():
    cfg = AnomalyConfig(
        timeframes=["15m"],
        require_ema200_breakout=False,
        # Make OI/volume hard to trigger so we can isolate price growth + net inflow.
        oi_change_threshold=0.50,
        rvol_threshold=10.0,
        rvol_lookback=3,
        net_inflow_ratio_threshold=0.50,
        price_growth_lookback_bars=3,
        price_growth_min_gain_pct=0.01,  # 1%
        price_growth_require_above_ema200=True,
    )
    det = AnomalyDetector(cfg)

    sent = []
    stored = []

    async def fake_check_cooldown(symbol: str, timeframe: str) -> bool:
        return True

    async def fake_send_alert(signal) -> None:
        sent.append(signal)

    async def fake_store_signal(signal) -> None:
        stored.append(signal)

    det._check_cooldown = fake_check_cooldown
    det._send_alert = fake_send_alert
    det._store_signal = fake_store_signal

    cache = {
        "closes": [100.0, 100.5, 100.7, 101.0],  # +1.0% over last 3 bars
        "volumes": [10.0, 10.0, 10.0, 10.0],
        "ema200_prev": 99.0,
        "ema200": 99.0,
    }

    await det._detect_breakout(
        symbol="TESTUSDT",
        timeframe="15m",
        close_price=101.0,
        prev_close=100.7,
        volume=10.0,
        net_inflow=0.0,
        net_inflow_ratio=0.60,  # net inflow spike true
        current_oi=101.0,
        old_oi=100.0,
        cache=cache,
    )

    assert len(sent) == 1
    assert len(stored) == 1


@pytest.mark.asyncio
async def test_detect_breakout_triggers_with_price_growth_alone():
    cfg = AnomalyConfig(
        timeframes=["15m"],
        require_ema200_breakout=False,
        # Make other factors hard to trigger so we can isolate price growth.
        oi_change_threshold=0.50,
        rvol_threshold=10.0,
        rvol_lookback=3,
        net_inflow_ratio_threshold=0.50,
        price_growth_lookback_bars=3,
        price_growth_min_gain_pct=0.01,  # 1%
        price_growth_require_above_ema200=True,
    )
    det = AnomalyDetector(cfg)

    sent = []
    stored = []

    async def fake_check_cooldown(symbol: str, timeframe: str) -> bool:
        return True

    async def fake_send_alert(signal) -> None:
        sent.append(signal)

    async def fake_store_signal(signal) -> None:
        stored.append(signal)

    det._check_cooldown = fake_check_cooldown
    det._send_alert = fake_send_alert
    det._store_signal = fake_store_signal

    cache = {
        "closes": [100.0, 100.5, 100.7, 101.0],  # +1.0% over last 3 bars
        "volumes": [10.0, 10.0, 10.0, 10.0],
        "ema200_prev": 99.0,
        "ema200": 99.0,
    }

    await det._detect_breakout(
        symbol="TESTUSDT",
        timeframe="15m",
        close_price=101.0,
        prev_close=100.7,
        volume=10.0,
        net_inflow=0.0,
        net_inflow_ratio=0.10,  # net inflow spike false
        current_oi=101.0,
        old_oi=100.0,
        cache=cache,
    )

    assert len(sent) == 1
    assert len(stored) == 1
