"""
测试 OrderBook 和 OBI 计算
"""

import pytest
from decimal import Decimal

from unitrade.analytics.orderbook import LocalOrderBook, OrderBookMetrics


class TestLocalOrderBook:
    """测试本地订单簿"""
    
    def test_apply_snapshot(self):
        """测试应用快照"""
        ob = LocalOrderBook("BTCUSDT", "binance")
        
        bids = [["50000", "1.0"], ["49990", "2.0"], ["49980", "3.0"]]
        asks = [["50010", "1.5"], ["50020", "2.5"], ["50030", "3.5"]]
        
        ob.apply_snapshot(bids, asks, 12345)
        
        assert ob.is_initialized
        assert ob.last_update_id == 12345
        assert len(ob.bids) == 3
        assert len(ob.asks) == 3
    
    def test_apply_delta(self):
        """测试应用增量更新"""
        ob = LocalOrderBook("BTCUSDT", "binance")
        
        # 初始快照
        bids = [["50000", "1.0"]]
        asks = [["50010", "1.0"]]
        ob.apply_snapshot(bids, asks, 100)
        
        # 增量更新
        delta_bids = [["49990", "2.0"], ["50000", "0"]]  # 添加49990，删除50000
        delta_asks = [["50010", "1.5"]]  # 修改50010
        
        result = ob.apply_delta(delta_bids, delta_asks, 101)
        
        assert result is True
        assert Decimal("50000") not in ob.bids
        assert ob.bids[Decimal("49990")] == Decimal("2.0")
        assert ob.asks[Decimal("50010")] == Decimal("1.5")
    
    def test_calculate_obi(self):
        """测试 OBI 计算"""
        ob = LocalOrderBook("BTCUSDT", "binance")
        
        # 买盘强势: bid_volume > ask_volume
        bids = [["50000", "10.0"]]  # bid_volume = 10
        asks = [["50010", "5.0"]]   # ask_volume = 5
        ob.apply_snapshot(bids, asks, 100)
        
        obi = ob.calculate_obi(levels=10)
        
        # OBI = (10 - 5) / (10 + 5) = 5 / 15 = 0.333...
        assert obi > 0
        assert abs(obi - Decimal("0.333333333333333333333333333")) < Decimal("0.01")
    
    def test_obi_balanced(self):
        """测试 OBI 平衡状态"""
        ob = LocalOrderBook("BTCUSDT", "binance")
        
        bids = [["50000", "10.0"]]
        asks = [["50010", "10.0"]]
        ob.apply_snapshot(bids, asks, 100)
        
        obi = ob.calculate_obi(levels=10)
        
        # OBI = (10 - 10) / (10 + 10) = 0
        assert obi == Decimal("0")
    
    def test_obi_sell_pressure(self):
        """测试 OBI 卖盘压力"""
        ob = LocalOrderBook("BTCUSDT", "binance")
        
        bids = [["50000", "5.0"]]   # bid_volume = 5
        asks = [["50010", "15.0"]]  # ask_volume = 15
        ob.apply_snapshot(bids, asks, 100)
        
        obi = ob.calculate_obi(levels=10)
        
        # OBI = (5 - 15) / (5 + 15) = -10 / 20 = -0.5
        assert obi < 0
        assert obi == Decimal("-0.5")
    
    def test_get_metrics(self):
        """测试获取指标"""
        ob = LocalOrderBook("BTCUSDT", "binance")
        
        bids = [["50000", "1.0"]]
        asks = [["50010", "1.0"]]
        ob.apply_snapshot(bids, asks, 100)
        
        metrics = ob.get_metrics()
        
        assert metrics is not None
        assert isinstance(metrics, OrderBookMetrics)
        assert metrics.mid_price == Decimal("50005")
        assert metrics.spread == Decimal("10")
        assert metrics.best_bid == Decimal("50000")
        assert metrics.best_ask == Decimal("50010")
    
    def test_empty_orderbook(self):
        """测试空订单簿"""
        ob = LocalOrderBook("BTCUSDT", "binance")
        
        assert not ob.is_initialized
        assert ob.calculate_obi() == Decimal("0")
        assert ob.get_metrics() is None
        assert ob.best_bid_price is None
        assert ob.best_ask_price is None
