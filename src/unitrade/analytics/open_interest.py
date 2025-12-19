"""
持仓量分析与市场状态检测

功能:
1. 计算 OI Delta (5 分钟变化)
2. 检测市场状态 (Regime)
3. 对齐高频成交与低频 OI 数据
"""

import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Optional, Tuple


class MarketRegime(Enum):
    """
    市场状态 - 基于价格与 OI 的关系
    
    经典解读:
    - LONG_BUILD: 价格↑ + OI↑ = 新多头入场，趋势延续
    - SHORT_BUILD: 价格↓ + OI↑ = 新空头入场，趋势延续
    - LONG_UNWIND: 价格↓ + OI↓ = 多头平仓，趋势结束
    - SHORT_COVER: 价格↑ + OI↓ = 空头平仓，反弹
    - NEUTRAL: 变化不明显
    """
    LONG_BUILD = "long_build"       # 多头建仓
    SHORT_BUILD = "short_build"     # 空头建仓
    LONG_UNWIND = "long_unwind"     # 多头平仓
    SHORT_COVER = "short_cover"     # 空头回补
    NEUTRAL = "neutral"


@dataclass
class OIMetrics:
    """OI 指标"""
    exchange: str
    symbol: str
    current_oi: Decimal
    oi_delta_5m: Decimal
    oi_delta_pct_5m: float
    price_change_5m: float
    regime: MarketRegime
    timestamp: int
    
    def to_dict(self) -> dict:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "current_oi": str(self.current_oi),
            "oi_delta_5m": str(self.oi_delta_5m),
            "oi_delta_pct_5m": self.oi_delta_pct_5m,
            "price_change_5m": self.price_change_5m,
            "regime": self.regime.value,
            "timestamp": self.timestamp,
        }


class OpenInterestAnalyzer:
    """
    持仓量分析器
    
    功能:
    1. 计算 OI Delta (5 分钟变化)
    2. 检测市场状态 (Regime)
    3. 对齐高频成交与低频 OI 数据
    """
    
    # 状态判定阈值
    OI_CHANGE_THRESHOLD = 0.5  # 0.5% OI 变化
    PRICE_CHANGE_THRESHOLD = 0.1  # 0.1% 价格变化
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        
        # OI 历史 (5 分钟窗口，假设 5 秒采样 = 60 个点)
        self._oi_history: deque = deque(maxlen=60)
        
        # 价格历史 (用于关联分析)
        self._price_history: deque = deque(maxlen=60)
        
        # 当前 OI
        self._current_oi: Optional[Decimal] = None
        self._current_price: Optional[Decimal] = None
    
    def update_oi(self, oi: Decimal, timestamp: int) -> None:
        """更新 OI 数据"""
        self._current_oi = oi
        self._oi_history.append({
            "oi": oi,
            "timestamp": timestamp
        })
    
    def update_price(self, price: Decimal, timestamp: int) -> None:
        """
        更新价格数据
        
        用于对齐高频成交与低频 OI:
        - 成交数据: 毫秒级
        - OI 数据: 秒级 (Bybit) 或 5 秒级 (Binance 轮询)
        
        策略: 每 5 秒采样一次价格，与 OI 频率对齐
        """
        self._current_price = price
        
        # 5 秒采样
        if self._price_history:
            last_ts = self._price_history[-1]["timestamp"]
            if timestamp - last_ts < 5000:  # 5 秒内
                return
        
        self._price_history.append({
            "price": price,
            "timestamp": timestamp
        })
    
    def calculate_oi_delta(self, window_minutes: int = 5) -> Tuple[Decimal, float]:
        """
        计算 OI Delta
        
        返回: (绝对变化, 百分比变化)
        """
        if len(self._oi_history) < 2:
            return Decimal("0"), 0.0
        
        current = self._oi_history[-1]
        
        # 查找窗口起点
        window_ms = window_minutes * 60 * 1000
        baseline_oi = None
        
        for entry in self._oi_history:
            if current["timestamp"] - entry["timestamp"] >= window_ms:
                baseline_oi = entry["oi"]
                break
        
        if baseline_oi is None:
            baseline_oi = self._oi_history[0]["oi"]
        
        delta = current["oi"] - baseline_oi
        pct_change = float(delta / baseline_oi * 100) if baseline_oi else 0.0
        
        return delta, pct_change
    
    def calculate_price_change(self, window_minutes: int = 5) -> float:
        """计算价格变化百分比"""
        if len(self._price_history) < 2:
            return 0.0
        
        current = self._price_history[-1]
        window_ms = window_minutes * 60 * 1000
        baseline_price = None
        
        for entry in self._price_history:
            if current["timestamp"] - entry["timestamp"] >= window_ms:
                baseline_price = entry["price"]
                break
        
        if baseline_price is None:
            baseline_price = self._price_history[0]["price"]
        
        return float((current["price"] - baseline_price) / baseline_price * 100)
    
    def detect_regime(self) -> MarketRegime:
        """
        检测市场状态
        
        逻辑:
        1. 计算 5 分钟 OI 变化
        2. 计算 5 分钟价格变化
        3. 根据两者关系判定状态
        """
        _, oi_pct = self.calculate_oi_delta(5)
        price_pct = self.calculate_price_change(5)
        
        # 变化太小，视为中性
        if abs(oi_pct) < self.OI_CHANGE_THRESHOLD and \
           abs(price_pct) < self.PRICE_CHANGE_THRESHOLD:
            return MarketRegime.NEUTRAL
        
        # 判定状态
        oi_up = oi_pct > self.OI_CHANGE_THRESHOLD
        oi_down = oi_pct < -self.OI_CHANGE_THRESHOLD
        price_up = price_pct > self.PRICE_CHANGE_THRESHOLD
        price_down = price_pct < -self.PRICE_CHANGE_THRESHOLD
        
        if price_up and oi_up:
            return MarketRegime.LONG_BUILD
        elif price_down and oi_up:
            return MarketRegime.SHORT_BUILD
        elif price_down and oi_down:
            return MarketRegime.LONG_UNWIND
        elif price_up and oi_down:
            return MarketRegime.SHORT_COVER
        else:
            return MarketRegime.NEUTRAL
    
    def get_metrics(self, exchange: str) -> Optional[OIMetrics]:
        """获取当前 OI 指标"""
        if not self._current_oi:
            return None
        
        oi_delta, oi_pct = self.calculate_oi_delta(5)
        price_change = self.calculate_price_change(5)
        regime = self.detect_regime()
        
        return OIMetrics(
            exchange=exchange,
            symbol=self.symbol,
            current_oi=self._current_oi,
            oi_delta_5m=oi_delta,
            oi_delta_pct_5m=oi_pct,
            price_change_5m=price_change,
            regime=regime,
            timestamp=int(time.time() * 1000)
        )
