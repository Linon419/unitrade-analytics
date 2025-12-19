"""
测试 Short Squeeze Scanner
"""

import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, patch, MagicMock

from unitrade.scanner.squeeze_scanner import (
    BinanceScanner,
    ScannerConfig,
    OISpikeAlert,
)


class TestScannerConfig:
    """测试扫描器配置"""
    
    def test_default_config(self):
        """测试默认配置"""
        config = ScannerConfig()
        
        assert config.auto_top_n == 100
        assert config.spike_threshold == 1.2
        assert config.max_concurrent_requests == 10
        assert "BTCUSDT" in config.ignore_list
        assert "ETHUSDT" in config.ignore_list
    
    def test_custom_config(self):
        """测试自定义配置"""
        config = ScannerConfig(
            auto_top_n=50,
            spike_threshold=1.15,
            extra_whitelist=["PEPEUSDT"],
            ignore_list=["BTCUSDT"],
        )
        
        assert config.auto_top_n == 50
        assert config.spike_threshold == 1.15
        assert config.extra_whitelist == ["PEPEUSDT"]
        assert config.ignore_list == ["BTCUSDT"]


class TestOISpikeAlert:
    """测试 OI 飙升警报"""
    
    def test_alert_creation(self):
        """测试创建警报"""
        alert = OISpikeAlert(
            symbol="VOXELUSDT",
            oi_spike_pct=25.5,
            current_oi=1000000,
            ma_short=120,
            ma_long=100,
            funding_rate=-0.0001,
            current_price=0.1234,
            price_change_pct=5.2,
        )
        
        assert alert.symbol == "VOXELUSDT"
        assert alert.oi_spike_pct == 25.5
        assert alert.funding_rate == -0.0001
    
    def test_telegram_message(self):
        """测试 Telegram 消息格式"""
        alert = OISpikeAlert(
            symbol="VOXELUSDT",
            oi_spike_pct=25.5,
            current_oi=1000000,
            ma_short=120,
            ma_long=100,
            funding_rate=-0.0001,
            current_price=0.1234,
            price_change_pct=5.2,
        )
        
        message = alert.to_telegram_message()
        
        assert "OI SURGE DETECTED" in message
        assert "VOXEL" in message
        assert "+25.5%" in message
        assert "-0.0100%" in message  # funding rate
        assert "0.1234" in message  # price
        assert "+5.20%" in message  # price change
    
    def test_telegram_message_without_optional_fields(self):
        """测试没有可选字段的消息"""
        alert = OISpikeAlert(
            symbol="TESTUSDT",
            oi_spike_pct=10.0,
            current_oi=500000,
            ma_short=110,
            ma_long=100,
        )
        
        message = alert.to_telegram_message()
        
        assert "OI SURGE DETECTED" in message
        assert "N/A" in message  # funding rate and price change


class TestBinanceScanner:
    """测试 Binance 扫描器"""
    
    @pytest.fixture
    def scanner(self):
        """创建扫描器实例"""
        config = ScannerConfig(
            auto_top_n=50,
            ignore_list=["BTCUSDT"],
            spike_threshold=1.1,
        )
        return BinanceScanner(config)
    
    def test_scanner_creation(self, scanner):
        """测试扫描器创建"""
        assert scanner.config.auto_top_n == 50
        assert scanner.config.spike_threshold == 1.1
    
    @pytest.mark.asyncio
    async def test_scanner_start_stop(self, scanner):
        """测试启动和停止"""
        await scanner.start()
        
        assert scanner._session is not None
        assert scanner._semaphore is not None
        
        await scanner.stop()
        
        # Session should be closed
        assert scanner._session.closed


class TestOIDetection:
    """测试 OI 检测逻辑"""
    
    def test_spike_detection_formula(self):
        """测试飙升检测公式"""
        # 模拟 OI 数据
        oi_values = [100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                     100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                     100, 100, 100, 100, 100, 100, 100, 120, 125, 130]
        
        ma_short_periods = 3
        ma_long_periods = 30
        spike_threshold = 1.2
        
        # 计算 MA
        ma_short = sum(oi_values[-ma_short_periods:]) / ma_short_periods
        ma_long = sum(oi_values) / len(oi_values)
        
        # MA_Short = (120 + 125 + 130) / 3 = 125
        assert abs(ma_short - 125) < 0.01
        
        # MA_Long ≈ 103.5
        expected_ma_long = sum(oi_values) / 30
        assert abs(ma_long - expected_ma_long) < 0.01
        
        # 检测飙升
        threshold = ma_long * spike_threshold
        is_spike = ma_short > threshold
        
        # 125 > 103.5 * 1.2 = 124.2 → True
        assert is_spike is True
    
    def test_no_spike_detection(self):
        """测试无飙升情况"""
        # 平稳的 OI 数据
        oi_values = [100] * 30
        
        ma_short = sum(oi_values[-3:]) / 3
        ma_long = sum(oi_values) / 30
        
        assert ma_short == 100
        assert ma_long == 100
        
        threshold = ma_long * 1.2
        is_spike = ma_short > threshold
        
        assert is_spike is False
    
    def test_spike_percentage_calculation(self):
        """测试飙升百分比计算"""
        ma_short = 150
        ma_long = 100
        
        spike_pct = ((ma_short - ma_long) / ma_long) * 100
        
        assert spike_pct == 50.0  # 50% spike
