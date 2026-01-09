"""
测试 FundFlowTracker / FundFlowDB 的资金流口径与聚合逻辑
"""

from datetime import datetime, timedelta

from unitrade.tracker.fund_flow import FundFlowDB, FlowSnapshot, FundFlowTracker, FundFlowConfig


def test_fundflowdb_hourly_flow_uses_deltas(tmp_path):
    db_path = tmp_path / "fundflow.db"
    db = FundFlowDB(str(db_path))

    symbol = "BTCUSDT"
    base = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)

    # hour 1 (base hour): 2 snapshots
    db.save_snapshot(
        FlowSnapshot(
            symbol=symbol,
            timestamp=base + timedelta(minutes=0),
            buy_volume=100.0,
            sell_volume=40.0,
            cvd=60.0,
            price=50000.0,
            trade_count=10,
        )
    )
    db.save_snapshot(
        FlowSnapshot(
            symbol=symbol,
            timestamp=base + timedelta(minutes=30),
            buy_volume=150.0,
            sell_volume=70.0,
            cvd=80.0,
            price=50010.0,
            trade_count=20,
        )
    )

    # hour 2 (base+1h): 2 snapshots
    db.save_snapshot(
        FlowSnapshot(
            symbol=symbol,
            timestamp=base + timedelta(hours=1, minutes=0),
            buy_volume=200.0,
            sell_volume=90.0,
            cvd=110.0,
            price=50020.0,
            trade_count=30,
        )
    )
    db.save_snapshot(
        FlowSnapshot(
            symbol=symbol,
            timestamp=base + timedelta(hours=1, minutes=30),
            buy_volume=240.0,
            sell_volume=110.0,
            cvd=130.0,
            price=50025.0,
            trade_count=40,
        )
    )

    rows = db.get_hourly_flow(symbol, hours=6)
    assert len(rows) >= 2

    # 最新小时在前；取最后一条应对应 base 的小时
    oldest = rows[-1]
    assert oldest["buy"] == 50.0
    assert oldest["sell"] == 30.0
    assert oldest["net_flow"] == 20.0
    assert oldest["trade_count"] == 10


def test_fundflowdb_daily_flow_uses_deltas(tmp_path):
    db_path = tmp_path / "fundflow.db"
    db = FundFlowDB(str(db_path))

    symbol = "BTCUSDT"
    day1 = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    day2 = day1 + timedelta(days=1)

    # Day 1 (2 snapshots)
    db.save_snapshot(FlowSnapshot(symbol, day1 + timedelta(hours=1), 100.0, 50.0, 50.0, 50000.0, 10))
    db.save_snapshot(FlowSnapshot(symbol, day1 + timedelta(hours=23), 180.0, 90.0, 90.0, 50100.0, 25))

    # Day 2 (2 snapshots)
    db.save_snapshot(FlowSnapshot(symbol, day2 + timedelta(hours=1), 20.0, 10.0, 10.0, 50200.0, 3))
    db.save_snapshot(FlowSnapshot(symbol, day2 + timedelta(hours=12), 70.0, 40.0, 30.0, 50300.0, 12))

    rows = db.get_daily_flow(symbol, days=7)
    assert len(rows) >= 2

    # 最新日期在前：day2 的 diff = (70-20, 40-10, 30-10, 12-3)
    newest = rows[0]
    assert newest["buy"] == 50.0
    assert newest["sell"] == 30.0
    assert newest["net_flow"] == 20.0
    assert newest["trade_count"] == 9


def test_fundflowtracker_rollover_keeps_first_trade():
    cfg = FundFlowConfig(symbols=["BTCUSDT"])
    tracker = FundFlowTracker(cfg)

    symbol = "BTCUSDT"
    tracker._accumulators = {
        symbol: {
            "buy_volume": 123.0,
            "sell_volume": 456.0,
            "trade_count": 7,
            "last_price": 0.0,
            "day_start": (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0),
        }
    }

    # m=false -> taker buy
    tracker._handle_trade({"s": symbol, "q": "1", "p": "100", "m": False})

    stats = tracker.get_current_stats(symbol)
    assert stats is not None
    assert stats["buy_volume"] == 100.0
    assert stats["sell_volume"] == 0.0
    assert stats["trade_count"] == 1
    assert stats["cvd"] == 100.0
