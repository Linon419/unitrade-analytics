"""
测试 Open Interest 分析和市场状态检测
"""

import pytest
from decimal import Decimal

from unitrade.analytics.open_interest import (
    OpenInterestAnalyzer, OIMetrics, MarketRegime
)


class TestOpenInterestAnalyzer:
    """测试 OI 分析器"""
    
    def test_update_oi(self):
        """测试 OI 更新"""
        analyzer = OpenInterestAnalyzer("BTCUSDT")
        
        analyzer.update_oi(Decimal("10000"), 1000)
        analyzer.update_oi(Decimal("10500"), 2000)
        
        metrics = analyzer.get_metrics("binance")
        
        assert metrics is not None
        assert metrics.current_oi == Decimal("10500")
    
    def test_oi_delta(self):
        """测试 OI Delta 计算"""
        analyzer = OpenInterestAnalyzer("BTCUSDT")
        
        # 添加多个数据点
        analyzer.update_oi(Decimal("10000"), 0)
        analyzer.update_oi(Decimal("10100"), 1000)
        analyzer.update_oi(Decimal("10300"), 2000)
        
        delta, pct = analyzer.calculate_oi_delta(window_minutes=5)
        
        # Delta = 10300 - 10000 = 300
        assert delta == Decimal("300")
        # Pct = 300 / 10000 * 100 = 3%
        assert abs(pct - 3.0) < 0.01
    
    def test_long_build_regime(self):
        """测试 LONG_BUILD 市场状态"""
        analyzer = OpenInterestAnalyzer("BTCUSDT")
        
        # 模拟价格上涨 + OI 上涨
        # 需要多个数据点来满足窗口
        base_ts = 0
        for i in range(10):
            analyzer.update_oi(Decimal("10000") + Decimal(str(i * 100)), base_ts + i * 1000)
            analyzer.update_price(Decimal("50000") + Decimal(str(i * 100)), base_ts + i * 1000)
        
        regime = analyzer.detect_regime()
        
        # 价格上涨 + OI 上涨 = LONG_BUILD
        assert regime == MarketRegime.LONG_BUILD
    
    def test_short_build_regime(self):
        """测试 SHORT_BUILD 市场状态"""
        analyzer = OpenInterestAnalyzer("BTCUSDT")
        
        # 模拟价格下跌 + OI 上涨
        base_ts = 0
        for i in range(10):
            analyzer.update_oi(Decimal("10000") + Decimal(str(i * 100)), base_ts + i * 1000)
            analyzer.update_price(Decimal("50000") - Decimal(str(i * 100)), base_ts + i * 1000)
        
        regime = analyzer.detect_regime()
        
        # 价格下跌 + OI 上涨 = SHORT_BUILD
        assert regime == MarketRegime.SHORT_BUILD
    
    def test_long_unwind_regime(self):
        """测试 LONG_UNWIND 市场状态"""
        analyzer = OpenInterestAnalyzer("BTCUSDT")
        
        # 模拟价格下跌 + OI 下跌
        base_ts = 0
        for i in range(10):
            analyzer.update_oi(Decimal("10000") - Decimal(str(i * 100)), base_ts + i * 1000)
            analyzer.update_price(Decimal("50000") - Decimal(str(i * 100)), base_ts + i * 1000)
        
        regime = analyzer.detect_regime()
        
        # 价格下跌 + OI 下跌 = LONG_UNWIND
        assert regime == MarketRegime.LONG_UNWIND
    
    def test_short_cover_regime(self):
        """测试 SHORT_COVER 市场状态"""
        analyzer = OpenInterestAnalyzer("BTCUSDT")
        
        # 模拟价格上涨 + OI 下跌
        base_ts = 0
        for i in range(10):
            analyzer.update_oi(Decimal("10000") - Decimal(str(i * 100)), base_ts + i * 1000)
            analyzer.update_price(Decimal("50000") + Decimal(str(i * 100)), base_ts + i * 1000)
        
        regime = analyzer.detect_regime()
        
        # 价格上涨 + OI 下跌 = SHORT_COVER
        assert regime == MarketRegime.SHORT_COVER
    
    def test_neutral_regime(self):
        """测试 NEUTRAL 市场状态"""
        analyzer = OpenInterestAnalyzer("BTCUSDT")
        
        # 模拟小幅变化
        base_ts = 0
        for i in range(5):
            analyzer.update_oi(Decimal("10000") + Decimal(str(i)), base_ts + i * 1000)
            analyzer.update_price(Decimal("50000") + Decimal(str(i)), base_ts + i * 1000)
        
        regime = analyzer.detect_regime()
        
        # 变化太小 = NEUTRAL
        assert regime == MarketRegime.NEUTRAL
    
    def test_get_metrics(self):
        """测试获取指标"""
        analyzer = OpenInterestAnalyzer("BTCUSDT")
        
        analyzer.update_oi(Decimal("10000"), 0)
        analyzer.update_oi(Decimal("10500"), 1000)
        analyzer.update_price(Decimal("50000"), 0)
        analyzer.update_price(Decimal("50500"), 1000)
        
        metrics = analyzer.get_metrics("binance")
        
        assert metrics is not None
        assert isinstance(metrics, OIMetrics)
        assert metrics.symbol == "BTCUSDT"
        assert metrics.exchange == "binance"
        assert metrics.current_oi == Decimal("10500")


class TestMarketRegime:
    """测试市场状态枚举"""
    
    def test_regime_values(self):
        """测试状态值"""
        assert MarketRegime.LONG_BUILD.value == "long_build"
        assert MarketRegime.SHORT_BUILD.value == "short_build"
        assert MarketRegime.LONG_UNWIND.value == "long_unwind"
        assert MarketRegime.SHORT_COVER.value == "short_cover"
        assert MarketRegime.NEUTRAL.value == "neutral"
