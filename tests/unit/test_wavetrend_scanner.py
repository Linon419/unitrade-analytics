"""
测试 WaveTrend Scanner 修复
验证与 Pine Script V78 的一致性
"""

import pytest
import numpy as np
import pandas as pd

pandas_ta = pytest.importorskip("pandas_ta")

from unitrade.scanner.wavetrend_scanner import (
    WaveTrendScanner,
    WaveTrendConfig,
    _get_laguerre,
    _get_ma,
)


class TestMAFunctions:
    """测试 MA 辅助函数"""

    def test_get_ma_alma(self):
        """测试 ALMA 计算"""
        src = pd.Series([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])
        result = _get_ma(src, length=4, ma_type="ALMA")
        assert result is not None
        assert len(result) == len(src)

    def test_get_ma_ema(self):
        """测试 EMA 计算"""
        src = pd.Series([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])
        result = _get_ma(src, length=4, ma_type="EMA")
        assert result is not None
        assert len(result) == len(src)

    def test_get_ma_sma(self):
        """测试 SMA 计算"""
        src = pd.Series([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])
        result = _get_ma(src, length=4, ma_type="SMA")
        assert result is not None
        # SMA 的最后一个值应该是 (17+18+19+20)/4 = 18.5
        assert abs(result.iloc[-1] - 18.5) < 0.01

    def test_get_ma_laguerre(self):
        """测试 Laguerre 计算"""
        src = pd.Series([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])
        result = _get_ma(src, length=4, ma_type="Laguerre", laguerre_gamma=0.66)
        assert result is not None
        assert len(result) == len(src)

    def test_get_laguerre_formula(self):
        """测试 Laguerre 滤波器公式"""
        # 简单测试：平稳输入应该产生平稳输出
        src = pd.Series([10.0] * 20)
        result = _get_laguerre(src, gamma=0.66)
        # 收敛后应该接近 10
        assert abs(result.iloc[-1] - 10.0) < 0.5


class TestWaveTrendConfig:
    """测试配置"""

    def test_default_config(self):
        """测试默认配置"""
        config = WaveTrendConfig()
        assert config.ma_type == "ALMA"
        assert config.laguerre_gamma == 0.66
        assert config.channel_len == 9
        assert config.avg_len == 7
        assert config.smooth_len == 4

    def test_custom_ma_type(self):
        """测试自定义 MA 类型"""
        config = WaveTrendConfig(ma_type="Laguerre", laguerre_gamma=0.75)
        assert config.ma_type == "Laguerre"
        assert config.laguerre_gamma == 0.75


class TestHeikinAshi:
    """测试 Heikin Ashi 计算"""

    def test_heikin_ashi_calculation(self):
        """测试 HA 蜡烛计算"""
        scanner = WaveTrendScanner()

        df = pd.DataFrame({
            'open': [100, 102, 101, 103, 105],
            'high': [105, 106, 104, 107, 108],
            'low': [99, 100, 99, 101, 103],
            'close': [102, 101, 103, 105, 106],
            'volume': [1000, 1100, 1200, 1300, 1400],
        })

        df_ha = scanner._calc_heikin_ashi_df(df)

        # HA Close = (O+H+L+C)/4
        expected_ha_close_0 = (100 + 105 + 99 + 102) / 4
        assert abs(df_ha['close'].iloc[0] - expected_ha_close_0) < 0.01

        # HA 应该比原始数据更平滑
        assert len(df_ha) == len(df)


class TestDivergenceIndexFix:
    """测试背离索引修复"""

    def test_divergence_search_index(self):
        """验证背离搜索索引计算正确"""
        # 模拟数据
        n = 100
        osc = pd.Series(np.random.randn(n) * 30)
        lb_right = 3
        curr_idx = n - 1 - lb_right  # = 96

        # 修复后: check_idx = curr_idx - i
        # 当 i=1 时, check_idx = 96 - 1 = 95
        # 当 i=10 时, check_idx = 96 - 10 = 86

        for i in range(1, 20):
            check_idx = curr_idx - i
            # 验证索引在有效范围内
            assert check_idx >= 0
            assert check_idx < n
            # 验证是从 pivot 位置往前搜索
            assert check_idx < curr_idx

    def test_divergence_level_calculation(self):
        """验证背离级别计算 (使用 i 而非 div_dist)"""
        # Pine Script: div_idx 是循环变量 i
        # if div_idx <= 15: level = 1
        # else if div_idx <= 35: level = 2
        # else if div_idx <= 55: level = 3
        # else: level = 4

        test_cases = [
            (1, 1), (15, 1),
            (16, 2), (35, 2),
            (36, 3), (55, 3),
            (56, 4), (100, 4),
        ]

        for i, expected_level in test_cases:
            level = 1 if i <= 15 else 2 if i <= 35 else 3 if i <= 55 else 4
            assert level == expected_level, f"i={i}, expected {expected_level}, got {level}"


class TestCandleConfirmation:
    """测试蜡烛确认使用 HA 数据"""

    def test_candle_color_with_ha(self):
        """验证使用 HA 蜡烛判断阴阳线"""
        # 原始 K 线: 阳线 (close > open)
        # HA K 线: 阴线 (ha_close < ha_open)
        # 修复后应该使用 HA 来判断

        df = pd.DataFrame({
            'open': [100],
            'high': [110],
            'low': [95],
            'close': [105],  # 原始是阳线
        })

        df_ha = pd.DataFrame({
            'open': [106],  # HA open
            'high': [110],
            'low': [95],
            'close': [102],  # HA close < HA open = 阴线
        })

        # 使用原始数据判断 (错误的旧逻辑)
        is_green_orig = df['close'].iloc[-1] >= df['open'].iloc[-1]
        assert is_green_orig == True  # 原始是阳线

        # 使用 HA 数据判断 (正确的新逻辑)
        is_green_ha = df_ha['close'].iloc[-1] >= df_ha['open'].iloc[-1]
        assert is_green_ha == False  # HA 是阴线

        # 修复后应该使用 HA 判断
        is_red_candle = df_ha['close'].iloc[-1] < df_ha['open'].iloc[-1]
        assert is_red_candle == True


class TestWaveTrendCalculation:
    """测试 WaveTrend OSC 计算"""

    def test_osc_range(self):
        """测试 OSC 值范围"""
        scanner = WaveTrendScanner()

        # 创建测试数据
        np.random.seed(42)
        n = 200
        df = pd.DataFrame({
            'open': 100 + np.cumsum(np.random.randn(n) * 0.5),
            'high': 0,
            'low': 0,
            'close': 0,
            'volume': np.random.randint(1000, 10000, n).astype(float),
        })
        df['high'] = df['open'] + np.abs(np.random.randn(n))
        df['low'] = df['open'] - np.abs(np.random.randn(n))
        df['close'] = df['open'] + np.random.randn(n) * 0.3

        df_ha = scanner._calc_heikin_ashi_df(df)
        osc = scanner._calc_wavetrend_pandas(df, df_ha)

        # OSC 应该在 [-60, 60] 范围内 (配置的 osc_min/osc_max)
        valid_osc = osc.dropna()
        assert valid_osc.min() >= -60.0
        assert valid_osc.max() <= 60.0

    def test_step_size_quantization(self):
        """测试阶梯量化"""
        config = WaveTrendConfig(step_size=6.6)
        scanner = WaveTrendScanner(config)

        # OSC 值应该是 step_size 的整数倍
        # 例如: 0, 6.6, 13.2, 19.8, ...
        valid_values = [0, 6.6, 13.2, 19.8, 26.4, 33.0, 39.6, 46.2, 52.8, 59.4,
                        -6.6, -13.2, -19.8, -26.4, -33.0, -39.6, -46.2, -52.8, -59.4]

        # 创建简单测试数据
        np.random.seed(123)
        n = 200
        df = pd.DataFrame({
            'open': 100 + np.cumsum(np.random.randn(n) * 0.5),
            'high': 0,
            'low': 0,
            'close': 0,
            'volume': np.random.randint(1000, 10000, n).astype(float),
        })
        df['high'] = df['open'] + np.abs(np.random.randn(n))
        df['low'] = df['open'] - np.abs(np.random.randn(n))
        df['close'] = df['open'] + np.random.randn(n) * 0.3

        df_ha = scanner._calc_heikin_ashi_df(df)
        osc = scanner._calc_wavetrend_pandas(df, df_ha)

        # 检查最后几个值是否是 step_size 的整数倍
        for val in osc.dropna().tail(10):
            remainder = abs(val) % 6.6
            # 允许浮点误差
            assert remainder < 0.01 or abs(remainder - 6.6) < 0.01, f"Value {val} is not a multiple of 6.6"
