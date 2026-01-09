from unitrade.scanner.big_trade_monitor import BigTradeMonitor, BigTradeMonitorConfig, compute_dynamic_threshold
from unitrade.scanner.signal_detector.calculators import TradeData


def test_compute_dynamic_threshold_uses_avg_multiplier_over_min():
    threshold, avg = compute_dynamic_threshold(
        100_000_000.0,
        10_000,
        min_notional=50_000.0,
        avg_multiplier=200.0,
    )
    assert avg == 10_000.0
    assert threshold == 2_000_000.0


def test_compute_dynamic_threshold_falls_back_to_min_on_bad_inputs():
    threshold, avg = compute_dynamic_threshold(
        0.0,
        0,
        min_notional=50_000.0,
        avg_multiplier=200.0,
    )
    assert avg == 0.0
    assert threshold == 50_000.0


def test_build_message_has_side_and_pre_block():
    monitor = BigTradeMonitor(
        BigTradeMonitorConfig(
            telegram_bot_token="t",
            telegram_chat_id="c",
        )
    )
    monitor._avg_trade_notional["BTCUSDT"] = 10_000.0
    monitor._quote_volume_24h["BTCUSDT"] = 100_000_000.0
    monitor._trade_count_24h["BTCUSDT"] = 10_000

    trade = TradeData(
        symbol="BTCUSDT",
        price=50_000.0,
        quantity=2.0,
        quote_volume=100_000.0,
        is_buyer_maker=False,  # taker buy => Ask Side
        timestamp=1_700_000_000.0,
    )

    msg = monitor._build_message(trade, threshold=2_000_000.0, oi=None)
    assert "Ask Side" in msg
    assert "<pre>" in msg and "</pre>" in msg

