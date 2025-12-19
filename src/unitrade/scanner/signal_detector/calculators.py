"""
Signal Calculators - RVOL, NetFlow, Rebound 计算器

高频计算优化:
- 使用 numpy 向量化运算
- 增量计算避免重复查询
- 内存缓存热点数据
"""

import asyncio
import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class TradeData:
    """交易数据"""
    symbol: str
    price: float
    quantity: float
    quote_volume: float  # 成交额 (USDT)
    is_buyer_maker: bool  # True = 主动卖出, False = 主动买入
    timestamp: float
    
    @property
    def is_buy(self) -> bool:
        """是否为主动买入"""
        return not self.is_buyer_maker


@dataclass
class KlineData:
    """K线数据"""
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    taker_buy_volume: float
    taker_buy_quote_volume: float
    timestamp: float
    is_closed: bool = False


@dataclass
class SignalResult:
    """信号计算结果"""
    symbol: str
    price: float
    price_change_pct: float
    
    # RVOL
    rvol: float  # 量能倍数
    current_volume: float
    avg_volume: float
    
    # NetFlow
    net_flow: float  # 净流入 (USDT)
    buy_volume: float
    sell_volume: float
    
    # Rebound
    rebound_pct: float  # 反弹幅度
    rebound_days: int   # 从N天前低点
    low_price: float
    
    # Meta
    timestamp: float = field(default_factory=time.time)
    
    @property
    def should_alert(self) -> bool:
        """是否应该报警"""
        return self.rvol >= 2.0 or abs(self.net_flow) >= 100000 or self.rebound_pct >= 5.0
    
    def format_message(self) -> str:
        """格式化报警消息"""
        # #SOMI - ₮0.2761 (4.30%) 启动 , 量能3.1x , 净流入28.12万 , 自6天前低点反弹31.73%
        base = self.symbol.replace("USDT", "")
        
        # 趋势启动/下挫判定（更严格的“启动”语义）
        up_trend = (
            self.price_change_pct >= 1.0
            and self.rvol >= 2.0
            and self.rebound_pct >= 3.0
        )
        down_trend = (
            self.price_change_pct <= -1.0
            and self.rvol >= 2.0
            and self.rebound_pct >= 3.0
        )

        if up_trend:
            trend = "🟢"
            action = "趋势启动"
        elif down_trend:
            trend = "🔴"
            action = "趋势下挫"
        elif self.price_change_pct > 0:
            trend = "🟢"
            action = "上涨"
        else:
            trend = "🔴"
            action = "回落"
        
        # 净流入人性化
        if abs(self.net_flow) >= 1e6:
            flow_str = f"{self.net_flow/1e6:.2f}百万"
        elif abs(self.net_flow) >= 1e4:
            flow_str = f"{self.net_flow/1e4:.2f}万"
        else:
            flow_str = f"{self.net_flow:.0f}"
        
        parts = [
            f"#{base} - ₮{self.price:.4f} ({self.price_change_pct:+.2f}%) {action}",
        ]
        
        details = []
        if self.rvol >= 1.5:
            details.append(f"量能{self.rvol:.1f}x")
        if abs(self.net_flow) >= 50000:
            details.append(f"净流入{flow_str}")
        if self.rebound_pct >= 3.0:
            details.append(f"自{self.rebound_days}天前低点反弹{self.rebound_pct:.2f}%")
        
        if details:
            parts.append(" | ".join(details))
        
        return "\n".join(parts)


class RVOLCalculator:
    """
    相对成交量计算器 (RVOL)
    
    RVOL = 当前周期成交量 / 过去N周期平均成交量
    
    实现:
    - 使用滑动窗口存储历史成交量
    - 支持增量更新 (每分钟)
    """
    
    def __init__(self, window_minutes: int = 60):
        self.window_minutes = window_minutes
        # 内存缓存: {symbol: [(timestamp, volume), ...]}
        self._cache: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
        # 当前分钟累计
        self._current_minute: Dict[str, float] = defaultdict(float)
        self._current_minute_ts: Dict[str, int] = {}
    
    def add_trade(self, trade: TradeData):
        """处理逐笔交易"""
        symbol = trade.symbol
        current_min = int(trade.timestamp // 60)
        
        # 检查是否进入新分钟
        if symbol in self._current_minute_ts:
            if self._current_minute_ts[symbol] < current_min:
                # 保存上一分钟数据
                last_min = self._current_minute_ts[symbol]
                vol = self._current_minute[symbol]
                self._cache[symbol].append((last_min * 60, vol))
                
                # 清理过期数据
                cutoff = (current_min - self.window_minutes) * 60
                self._cache[symbol] = [
                    (ts, v) for ts, v in self._cache[symbol] if ts >= cutoff
                ]
                
                # 重置当前分钟
                self._current_minute[symbol] = 0
        
        self._current_minute_ts[symbol] = current_min
        self._current_minute[symbol] += trade.quote_volume
    
    def calculate(self, symbol: str) -> Tuple[float, float, float]:
        """
        计算 RVOL
        
        Returns: (rvol, current_volume, avg_volume)
        """
        current_vol = self._current_minute.get(symbol, 0)
        history = self._cache.get(symbol, [])
        
        if not history:
            return (1.0, current_vol, current_vol) if current_vol > 0 else (0, 0, 0)
        
        avg_vol = sum(v for _, v in history) / len(history)
        
        if avg_vol == 0:
            return (0, current_vol, 0)
        
        rvol = current_vol / avg_vol
        return (rvol, current_vol, avg_vol)

    def calculate_window(self, symbol: str, window_minutes: int) -> Tuple[float, float, float]:
        """
        Calculate RVOL for a rolling window (in minutes).

        Returns: (rvol, window_volume, expected_volume)
        """
        if window_minutes <= 0:
            return (0.0, 0.0, 0.0)

        current_vol = self._current_minute.get(symbol, 0.0)
        history = self._cache.get(symbol, [])

        # Sum volume for the last N minutes (including current minute)
        current_min = self._current_minute_ts.get(symbol)
        if current_min is None:
            current_min = int(time.time() // 60)

        cutoff_ts = (current_min - window_minutes + 1) * 60
        window_vol = current_vol + sum(v for ts, v in history if ts >= cutoff_ts)

        # Estimate average per-minute volume from historical data (accounting for missing minutes)
        if not history:
            expected = window_vol
            return ((window_vol / expected) if expected > 0 else 0.0, window_vol, expected)

        minute_marks = [int(ts // 60) for ts, _ in history]
        span_minutes = max(minute_marks) - min(minute_marks) + 1
        span_minutes = max(span_minutes, 1)
        avg_per_min = sum(v for _, v in history) / span_minutes

        expected = avg_per_min * window_minutes
        if expected <= 0:
            return (0.0, window_vol, 0.0)

        return (window_vol / expected, window_vol, expected)


class NetFlowCalculator:
    """
    资金净流入计算器
    
    NetFlow = Σ(主动买入成交额) - Σ(主动卖出成交额)
    
    基于 aggTrade 的 is_buyer_maker 字段:
    - is_buyer_maker = True  → 主动卖出 (Sell)
    - is_buyer_maker = False → 主动买入 (Buy)
    """
    
    def __init__(self, window_minutes: int = 5):
        self.window_minutes = window_minutes
        # 内存累计: {symbol: {buy_vol, sell_vol, start_ts}}
        self._flow: Dict[str, Dict] = defaultdict(lambda: {
            "buy_vol": 0.0, 
            "sell_vol": 0.0,
            "start_ts": time.time()
        })
    
    def add_trade(self, trade: TradeData):
        """处理逐笔交易"""
        symbol = trade.symbol
        flow = self._flow[symbol]
        
        # 检查窗口是否过期
        if time.time() - flow["start_ts"] > self.window_minutes * 60:
            # 重置
            flow["buy_vol"] = 0.0
            flow["sell_vol"] = 0.0
            flow["start_ts"] = time.time()
        
        if trade.is_buy:
            flow["buy_vol"] += trade.quote_volume
        else:
            flow["sell_vol"] += trade.quote_volume
    
    def calculate(self, symbol: str) -> Tuple[float, float, float]:
        """
        计算净流入
        
        Returns: (net_flow, buy_vol, sell_vol)
        """
        flow = self._flow.get(symbol, {})
        buy = flow.get("buy_vol", 0)
        sell = flow.get("sell_vol", 0)
        return (buy - sell, buy, sell)
    
    def reset(self, symbol: str):
        """重置币种"""
        if symbol in self._flow:
            self._flow[symbol] = {
                "buy_vol": 0.0,
                "sell_vol": 0.0,
                "start_ts": time.time()
            }


class ReboundCalculator:
    """
    反弹幅度计算器
    
    Rebound% = (当前价 - N日最低价) / N日最低价 * 100
    
    优化:
    - 缓存历史最低价 (每小时更新)
    - 支持多个时间周期 [1, 3, 7, 14, 30天]
    """
    
    def __init__(self, periods: List[int] = None):
        self.periods = periods or [1, 3, 7, 14, 30]
        # 低点缓存: {symbol: {days: price}}
        self._lows: Dict[str, Dict[int, float]] = defaultdict(dict)
        self._last_update: Dict[str, float] = {}
    
    def set_lows(self, symbol: str, lows: Dict[int, float]):
        """设置历史低点"""
        self._lows[symbol] = lows
        self._last_update[symbol] = time.time()
    
    def get_lows(self, symbol: str) -> Dict[int, float]:
        """获取历史低点"""
        return self._lows.get(symbol, {})
    
    def needs_update(self, symbol: str, max_age_seconds: int = 3600) -> bool:
        """是否需要更新低点数据"""
        last = self._last_update.get(symbol, 0)
        return time.time() - last > max_age_seconds
    
    def calculate(self, symbol: str, current_price: float) -> Tuple[float, int, float]:
        """
        计算反弹幅度
        
        Returns: (rebound_pct, days, low_price)
        找到最大反弹的周期
        """
        lows = self._lows.get(symbol, {})
        
        if not lows or current_price <= 0:
            return (0, 0, 0)
        
        max_rebound = 0
        best_days = 0
        best_low = current_price
        
        for days, low in lows.items():
            if low and low > 0:
                rebound = (current_price - low) / low * 100
                if rebound > max_rebound:
                    max_rebound = rebound
                    best_days = days
                    best_low = low
        
        return (max_rebound, best_days, best_low)


class CompositeCalculator:
    """
    组合计算器 - 封装所有指标计算
    """
    
    def __init__(self, config: Optional[Dict] = None):
        config = config or {}
        
        self.rvol = RVOLCalculator(
            window_minutes=config.get("rvol_window", 60)
        )
        self.flow = NetFlowCalculator(
            window_minutes=config.get("flow_window", 5)
        )
        self.rebound = ReboundCalculator(
            periods=config.get("rebound_periods", [1, 3, 7, 14, 30])
        )
        
        # 价格缓存
        self._prices: Dict[str, float] = {}
        self._price_changes: Dict[str, float] = {}
    
    def process_trade(self, trade: TradeData):
        """处理交易数据"""
        self.rvol.add_trade(trade)
        self.flow.add_trade(trade)
        self._prices[trade.symbol] = trade.price
    
    def process_kline(self, kline: KlineData):
        """处理 K 线数据"""
        symbol = kline.symbol
        self._prices[symbol] = kline.close
        
        # 计算涨跌幅
        if kline.open > 0:
            self._price_changes[symbol] = (kline.close - kline.open) / kline.open * 100
    
    def calculate_all(self, symbol: str) -> Optional[SignalResult]:
        """计算所有指标"""
        price = self._prices.get(symbol, 0)
        if price <= 0:
            return None
        
        rvol, cur_vol, avg_vol = self.rvol.calculate(symbol)
        net_flow, buy_vol, sell_vol = self.flow.calculate(symbol)
        rebound_pct, rebound_days, low_price = self.rebound.calculate(symbol, price)
        
        return SignalResult(
            symbol=symbol,
            price=price,
            price_change_pct=self._price_changes.get(symbol, 0),
            rvol=rvol,
            current_volume=cur_vol,
            avg_volume=avg_vol,
            net_flow=net_flow,
            buy_volume=buy_vol,
            sell_volume=sell_vol,
            rebound_pct=rebound_pct,
            rebound_days=rebound_days,
            low_price=low_price
        )
