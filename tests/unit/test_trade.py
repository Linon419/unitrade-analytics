"""
测试 Trade 标准化和 CVD 计算
"""

import pytest
from decimal import Decimal

from unitrade.analytics.trade import (
    TradeTick, TradeSide, TradeAnalytics, TradeNormalizer
)


class TestTradeNormalizer:
    """测试成交数据标准化"""
    
    def test_from_binance_buy(self):
        """测试 Binance 买入订单标准化"""
        # m=false 表示 Taker 是买方
        event = {
            "e": "aggTrade",
            "s": "BTCUSDT",
            "a": 12345,
            "p": "50000.00",
            "q": "1.5",
            "T": 1699999999999,
            "m": False  # Taker 买入
        }
        
        trade = TradeNormalizer.from_binance(event)
        
        assert trade.exchange == "binance"
        assert trade.symbol == "BTCUSDT"
        assert trade.trade_id == "12345"
        assert trade.price == Decimal("50000.00")
        assert trade.quantity == Decimal("1.5")
        assert trade.side == TradeSide.BUY
        assert trade.timestamp == 1699999999999
    
    def test_from_binance_sell(self):
        """测试 Binance 卖出订单标准化"""
        # m=true 表示买方是 Maker，所以 Taker 是卖方
        event = {
            "e": "aggTrade",
            "s": "ETHUSDT",
            "a": 99999,
            "p": "3000.00",
            "q": "10.0",
            "T": 1699999999999,
            "m": True  # Taker 卖出
        }
        
        trade = TradeNormalizer.from_binance(event)
        
        assert trade.side == TradeSide.SELL
        assert trade.symbol == "ETHUSDT"
    
    def test_from_bybit_buy(self):
        """测试 Bybit 买入订单标准化"""
        trade_data = {
            "s": "BTCUSDT",
            "S": "Buy",
            "v": "2.0",
            "p": "50000.00",
            "i": "trade123",
            "T": 1699999999999
        }
        
        trade = TradeNormalizer.from_bybit(trade_data)
        
        assert trade.exchange == "bybit"
        assert trade.side == TradeSide.BUY
        assert trade.quantity == Decimal("2.0")
    
    def test_from_bybit_sell(self):
        """测试 Bybit 卖出订单标准化"""
        trade_data = {
            "s": "BTCUSDT",
            "S": "Sell",
            "v": "1.0",
            "p": "49500.00",
            "i": "trade456",
            "T": 1699999999999
        }
        
        trade = TradeNormalizer.from_bybit(trade_data)
        
        assert trade.side == TradeSide.SELL
    
    def test_notional_calculation(self):
        """测试成交额计算"""
        trade = TradeTick(
            exchange="binance",
            symbol="BTCUSDT",
            trade_id="123",
            price=Decimal("50000"),
            quantity=Decimal("2"),
            side=TradeSide.BUY,
            timestamp=1699999999999,
            received_at=1699999999999
        )
        
        assert trade.notional == Decimal("100000")  # 50000 * 2


class TestTradeAnalytics:
    """测试成交分析"""
    
    def test_cvd_buy(self):
        """测试买入对 CVD 的影响"""
        analytics = TradeAnalytics("BTCUSDT")
        
        trade = TradeTick(
            exchange="binance",
            symbol="BTCUSDT",
            trade_id="1",
            price=Decimal("50000"),
            quantity=Decimal("1.0"),
            side=TradeSide.BUY,
            timestamp=1699999999999,
            received_at=1699999999999
        )
        
        analytics.process_trade(trade)
        
        assert analytics.calculate_cvd() == Decimal("1.0")
    
    def test_cvd_sell(self):
        """测试卖出对 CVD 的影响"""
        analytics = TradeAnalytics("BTCUSDT")
        
        trade = TradeTick(
            exchange="binance",
            symbol="BTCUSDT",
            trade_id="1",
            price=Decimal("50000"),
            quantity=Decimal("2.0"),
            side=TradeSide.SELL,
            timestamp=1699999999999,
            received_at=1699999999999
        )
        
        analytics.process_trade(trade)
        
        assert analytics.calculate_cvd() == Decimal("-2.0")
    
    def test_cvd_cumulative(self):
        """测试 CVD 累积计算"""
        analytics = TradeAnalytics("BTCUSDT")
        
        # 买入 3
        analytics.process_trade(TradeTick(
            exchange="binance", symbol="BTCUSDT", trade_id="1",
            price=Decimal("50000"), quantity=Decimal("3.0"),
            side=TradeSide.BUY, timestamp=1, received_at=1
        ))
        
        # 卖出 1
        analytics.process_trade(TradeTick(
            exchange="binance", symbol="BTCUSDT", trade_id="2",
            price=Decimal("50000"), quantity=Decimal("1.0"),
            side=TradeSide.SELL, timestamp=2, received_at=2
        ))
        
        # 买入 2
        analytics.process_trade(TradeTick(
            exchange="binance", symbol="BTCUSDT", trade_id="3",
            price=Decimal("50000"), quantity=Decimal("2.0"),
            side=TradeSide.BUY, timestamp=3, received_at=3
        ))
        
        # CVD = 3 - 1 + 2 = 4
        assert analytics.calculate_cvd() == Decimal("4.0")
    
    def test_get_metrics(self):
        """测试获取指标"""
        analytics = TradeAnalytics("BTCUSDT")
        
        analytics.process_trade(TradeTick(
            exchange="binance", symbol="BTCUSDT", trade_id="1",
            price=Decimal("50000"), quantity=Decimal("1.0"),
            side=TradeSide.BUY, timestamp=1699999999999, received_at=1699999999999
        ))
        
        metrics = analytics.get_metrics()
        
        assert metrics["symbol"] == "BTCUSDT"
        assert metrics["cvd"] == 1.0
        assert metrics["buy_volume"] == 1.0
        assert metrics["sell_volume"] == 0.0
        assert metrics["trade_count"] == 1
    
    def test_reset_session(self):
        """测试会话重置"""
        analytics = TradeAnalytics("BTCUSDT")
        
        analytics.process_trade(TradeTick(
            exchange="binance", symbol="BTCUSDT", trade_id="1",
            price=Decimal("50000"), quantity=Decimal("5.0"),
            side=TradeSide.BUY, timestamp=1, received_at=1
        ))
        
        assert analytics.calculate_cvd() == Decimal("5.0")
        
        analytics.reset_session()
        
        assert analytics.calculate_cvd() == Decimal("0")
