"""
测试信号生成器
"""

import pytest
from datetime import datetime

from unitrade.strategy.signal_generator import (
    SignalGenerator,
    TradingSignal,
    SignalType,
    SignalReason,
)


class TestSignalGenerator:
    """测试信号生成器"""
    
    @pytest.fixture
    def generator(self):
        return SignalGenerator()
    
    def test_strong_long_signal(self, generator):
        """测试强多信号"""
        signal = generator.generate_signal(
            symbol="BTCUSDT",
            exchange="binance",
            obi=0.5,  # 强买盘
            cvd=2000,  # 正 CVD
            oi_delta_pct=5.0,  # OI 上升
            market_regime="long_build",  # 多头建仓
            volatility=30,
            funding_rate=-0.02,  # 负 funding
            price_change_pct=2.0,
        )
        
        assert signal.signal_type in (SignalType.STRONG_LONG, SignalType.LONG)
        assert signal.strength > 0.5
        assert SignalReason.OBI_BULLISH in signal.reasons
        assert SignalReason.OI_LONG_BUILD in signal.reasons
    
    def test_strong_short_signal(self, generator):
        """测试强空信号"""
        signal = generator.generate_signal(
            symbol="BTCUSDT",
            exchange="binance",
            obi=-0.5,  # 强卖盘
            cvd=-2000,  # 负 CVD
            oi_delta_pct=5.0,  # OI 上升
            market_regime="short_build",  # 空头建仓
            volatility=30,
            funding_rate=0.02,  # 正 funding
            price_change_pct=-2.0,
        )
        
        assert signal.signal_type in (SignalType.STRONG_SHORT, SignalType.SHORT)
        assert signal.strength > 0.5
        assert SignalReason.OBI_BEARISH in signal.reasons
        assert SignalReason.OI_SHORT_BUILD in signal.reasons
    
    def test_neutral_signal(self, generator):
        """测试中性信号"""
        signal = generator.generate_signal(
            symbol="BTCUSDT",
            exchange="binance",
            obi=0.1,  # 中性
            cvd=100,  # 小 CVD
            oi_delta_pct=0.5,  # 小变化
            market_regime="neutral",
            volatility=20,
        )
        
        assert signal.signal_type == SignalType.NEUTRAL
        assert signal.strength == 0.0
    
    def test_cvd_divergence_bull(self, generator):
        """测试 CVD 看多背离"""
        signal = generator.generate_signal(
            symbol="ETHUSDT",
            exchange="binance",
            obi=0.1,
            cvd=1500,  # 正 CVD
            oi_delta_pct=1.0,
            market_regime="neutral",
            volatility=25,
            price_change_pct=-3.0,  # 价格下跌
        )
        
        assert SignalReason.CVD_DIVERGENCE_BULL in signal.reasons
    
    def test_cvd_divergence_bear(self, generator):
        """测试 CVD 看空背离"""
        signal = generator.generate_signal(
            symbol="ETHUSDT",
            exchange="binance",
            obi=-0.1,
            cvd=-1500,  # 负 CVD
            oi_delta_pct=1.0,
            market_regime="neutral",
            volatility=25,
            price_change_pct=3.0,  # 价格上涨
        )
        
        assert SignalReason.CVD_DIVERGENCE_BEAR in signal.reasons
    
    def test_squeeze_potential_short(self, generator):
        """测试空头挤压潜力"""
        signal = generator.generate_signal(
            symbol="PEPEUSDT",
            exchange="binance",
            obi=0.4,  # 增加买盘压力
            cvd=1500,  # 增加 CVD
            oi_delta_pct=10.0,  # OI 大幅上升
            market_regime="neutral",  # 使用中性避免抵消
            volatility=40,
            funding_rate=-0.05,  # 负 funding (空头付费)
        )
        
        # 检测到挤压潜力
        assert SignalReason.SQUEEZE_POTENTIAL in signal.reasons
    
    def test_high_volatility_filter(self, generator):
        """测试高波动率过滤"""
        # 正常波动率
        signal1 = generator.generate_signal(
            symbol="BTCUSDT",
            exchange="binance",
            obi=0.5,
            cvd=2000,
            oi_delta_pct=5.0,
            market_regime="long_build",
            volatility=30,
        )
        
        # 高波动率
        signal2 = generator.generate_signal(
            symbol="BTCUSDT",
            exchange="binance",
            obi=0.5,
            cvd=2000,
            oi_delta_pct=5.0,
            market_regime="long_build",
            volatility=100,  # 极高波动
        )
        
        # 高波动率应该降低信号强度
        assert signal2.strength < signal1.strength
    
    def test_signal_history(self, generator):
        """测试信号历史"""
        # 生成多个信号
        for i in range(5):
            generator.generate_signal(
                symbol="BTCUSDT",
                exchange="binance",
                obi=0.1 * i,
                cvd=100 * i,
                oi_delta_pct=1.0,
                market_regime="neutral",
            )
        
        history = generator.get_signal_history("binance", "BTCUSDT", limit=3)
        
        assert len(history) == 3
    
    def test_active_signals(self, generator):
        """测试活跃信号获取"""
        # 生成一个强信号
        generator.generate_signal(
            symbol="BTCUSDT",
            exchange="binance",
            obi=0.6,
            cvd=3000,
            oi_delta_pct=8.0,
            market_regime="long_build",
            funding_rate=-0.03,
        )
        
        # 生成一个弱信号
        generator.generate_signal(
            symbol="ETHUSDT",
            exchange="binance",
            obi=0.1,
            cvd=100,
            oi_delta_pct=0.5,
            market_regime="neutral",
        )
        
        active = generator.get_active_signals(min_strength=0.5)
        
        # 只有强信号应该在活跃列表
        assert len(active) >= 1
        assert all(s.strength >= 0.5 for s in active)
    
    def test_signal_to_dict(self, generator):
        """测试信号序列化"""
        signal = generator.generate_signal(
            symbol="BTCUSDT",
            exchange="binance",
            obi=0.4,
            cvd=1000,
            oi_delta_pct=2.0,
            market_regime="long_build",
        )
        
        d = signal.to_dict()
        
        assert d["symbol"] == "BTCUSDT"
        assert d["exchange"] == "binance"
        assert "signal" in d
        assert "strength" in d
        assert "reasons" in d
        assert "metrics" in d
        assert "timestamp" in d
