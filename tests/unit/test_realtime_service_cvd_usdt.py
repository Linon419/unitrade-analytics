"""
测试 realtime_service 多周期 CVD 统一为 USDT 口径，同时保留 qty 口径。
"""

from decimal import Decimal

from unitrade.analytics.trade import TradeSide, TradeTick
from unitrade.web.realtime_service import MultiTimeframeAggregator


def _make_trade(symbol: str, side: TradeSide, price: str, qty: str) -> TradeTick:
    return TradeTick(
        exchange="binance",
        symbol=symbol,
        trade_id="1",
        price=Decimal(price),
        quantity=Decimal(qty),
        side=side,
        timestamp=1,
        received_at=1,
    )


def test_realtime_cvd_is_usdt_and_qty_is_exposed():
    agg = MultiTimeframeAggregator("BTCUSDT", db_manager=None, max_buckets=10)

    # Buy 2 @ 100 => +200 USDT, +2 qty
    agg.update_trade(_make_trade("BTCUSDT", TradeSide.BUY, "100", "2"))
    m = agg.get_timeframe_metrics("1m")
    assert m["current"]["cvd"] == 200.0
    assert m["current"]["cvd_qty"] == 2.0
    assert m["current"]["buy_volume"] == 200.0
    assert m["current"]["buy_volume_qty"] == 2.0

    # Sell 1 @ 110 => -110 USDT, -1 qty (net +90 USDT, +1 qty)
    agg.update_trade(_make_trade("BTCUSDT", TradeSide.SELL, "110", "1"))
    m = agg.get_timeframe_metrics("1m")
    assert m["current"]["cvd"] == 90.0
    assert m["current"]["cvd_qty"] == 1.0
    assert m["current"]["sell_volume"] == 110.0
    assert m["current"]["sell_volume_qty"] == 1.0

    assert m["cumulative_cvd"] == 90.0
    assert m["cumulative_cvd_qty"] == 1.0
