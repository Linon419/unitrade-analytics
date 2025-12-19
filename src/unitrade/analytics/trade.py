"""
成交数据标准化与分析

功能:
1. 标准化 Binance/Bybit 成交数据
2. 计算 CVD (Cumulative Volume Delta)
3. 计算已实现波动率
"""

import math
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional


class TradeSide(Enum):
    """成交方向 - 基于 Taker"""
    BUY = "buy"    # Taker 买入 (lifted ask)
    SELL = "sell"  # Taker 卖出 (hit bid)


@dataclass
class TradeTick:
    """
    标准化成交记录
    
    核心字段说明:
    - side: Taker 的方向
      - BUY: 主动买入，推高价格
      - SELL: 主动卖出，压低价格
    """
    exchange: str
    symbol: str
    trade_id: str
    price: Decimal
    quantity: Decimal
    side: TradeSide
    timestamp: int  # Exchange timestamp (ms)
    received_at: int  # Local timestamp (ms)
    
    @property
    def notional(self) -> Decimal:
        """成交额"""
        return self.price * self.quantity
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "trade_id": self.trade_id,
            "price": str(self.price),
            "quantity": str(self.quantity),
            "side": self.side.value,
            "notional": str(self.notional),
            "timestamp": self.timestamp,
            "received_at": self.received_at,
        }


class TradeAnalytics:
    """
    成交分析引擎
    
    计算指标:
    1. CVD (Cumulative Volume Delta) - 累积成交量差
    2. Realized Volatility - 已实现波动率
    """
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        
        # CVD 跟踪
        self._cvd: Decimal = Decimal("0")
        self._cvd_history: deque = deque(maxlen=1000)
        
        # 波动率计算
        self._price_history: deque = deque(maxlen=60)  # 1 分钟 @ 1 秒采样
        self._last_price_sample: float = 0
        
        # 成交统计
        self._buy_volume: Decimal = Decimal("0")
        self._sell_volume: Decimal = Decimal("0")
        self._buy_notional: Decimal = Decimal("0")
        self._sell_notional: Decimal = Decimal("0")
        self._trade_count: int = 0
        
        # 最新价格
        self._last_price: Optional[Decimal] = None
    
    def process_trade(self, trade: TradeTick) -> Dict:
        """
        处理单笔成交，更新所有指标
        
        返回当前指标快照
        """
        # 更新 CVD
        if trade.side == TradeSide.BUY:
            self._cvd += trade.quantity
            self._buy_volume += trade.quantity
            self._buy_notional += trade.notional
        else:
            self._cvd -= trade.quantity
            self._sell_volume += trade.quantity
            self._sell_notional += trade.notional
        
        self._trade_count += 1
        self._last_price = trade.price
        
        # 记录 CVD 历史
        self._cvd_history.append({
            "cvd": self._cvd,
            "price": trade.price,
            "timestamp": trade.timestamp
        })
        
        # 更新价格历史 (用于波动率)
        now = time.time()
        if now - self._last_price_sample >= 1.0:  # 1 秒采样
            self._price_history.append(float(trade.price))
            self._last_price_sample = now
        
        return self.get_metrics()
    
    def calculate_cvd(self) -> Decimal:
        """
        获取当前 CVD
        
        CVD (Cumulative Volume Delta):
        - 正值: 累积净买入，看涨信号
        - 负值: 累积净卖出，看跌信号
        
        用途:
        - CVD 上升 + 价格上升 = 强势上涨
        - CVD 下降 + 价格上升 = 弱势上涨 (潜在反转)
        """
        return self._cvd
    
    def calculate_realized_volatility(self, window_seconds: int = 60) -> Optional[float]:
        """
        计算已实现波动率 (1 分钟滚动窗口)
        
        方法: 对数收益率的标准差 * sqrt(年化因子)
        
        公式:
        1. r_i = ln(P_i / P_{i-1})  # 对数收益
        2. σ = std(r_1, r_2, ..., r_n)  # 标准差
        3. 年化: σ * sqrt(525600)  # 分钟数/年
        
        返回: 年化波动率百分比
        """
        if len(self._price_history) < 10:
            return None
        
        prices = list(self._price_history)
        
        # 计算对数收益率
        returns = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                r = math.log(prices[i] / prices[i-1])
                returns.append(r)
        
        if len(returns) < 5:
            return None
        
        # 标准差
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        std_dev = math.sqrt(variance)
        
        # 年化 (假设 1 秒采样，年化到分钟级)
        # 1 年 = 365 * 24 * 60 = 525600 分钟
        annualized = std_dev * math.sqrt(525600)
        
        return annualized * 100  # 百分比
    
    def get_metrics(self) -> Dict:
        """获取当前所有指标"""
        return {
            "symbol": self.symbol,
            "cvd": float(self._cvd),
            "buy_volume": float(self._buy_volume),
            "sell_volume": float(self._sell_volume),
            "buy_notional": float(self._buy_notional),
            "sell_notional": float(self._sell_notional),
            "net_volume": float(self._buy_volume - self._sell_volume),
            "trade_count": self._trade_count,
            "realized_volatility": self.calculate_realized_volatility(),
            "last_price": float(self._last_price) if self._last_price else None,
            "timestamp": int(time.time() * 1000)
        }
    
    def reset_session(self) -> None:
        """重置会话统计 (如每日重置)"""
        self._cvd = Decimal("0")
        self._buy_volume = Decimal("0")
        self._sell_volume = Decimal("0")
        self._buy_notional = Decimal("0")
        self._sell_notional = Decimal("0")
        self._trade_count = 0
    
    def get_cvd_history(self) -> List[Dict]:
        """获取 CVD 历史"""
        return list(self._cvd_history)


class TradeNormalizer:
    """
    成交数据标准化器
    
    关键: 正确映射 Taker 方向
    """
    
    @staticmethod
    def from_binance(event: Dict) -> TradeTick:
        """
        标准化 Binance aggTrade
        
        Binance "m" (isBuyerMaker) 字段:
        - m = true: 买方是 Maker → 卖方是 Taker → SELL
        - m = false: 买方是 Taker → BUY
        
        ⚠️ 注意: 逻辑是反的!
        """
        # m=true 表示买方挂单，所以成交是卖方主动吃单
        side = TradeSide.SELL if event["m"] else TradeSide.BUY
        
        return TradeTick(
            exchange="binance",
            symbol=event["s"],
            trade_id=str(event["a"]),
            price=Decimal(event["p"]),
            quantity=Decimal(event["q"]),
            side=side,
            timestamp=event["T"],
            received_at=int(time.time() * 1000)
        )
    
    @staticmethod
    def from_bybit(trade_data: Dict) -> TradeTick:
        """
        标准化 Bybit publicTrade
        
        Bybit "S" (Side) 字段:
        - "Buy": Taker 买入
        - "Sell": Taker 卖出
        
        直接映射，无需转换
        """
        side = TradeSide.BUY if trade_data["S"] == "Buy" else TradeSide.SELL
        
        return TradeTick(
            exchange="bybit",
            symbol=trade_data["s"],
            trade_id=trade_data["i"],
            price=Decimal(trade_data["p"]),
            quantity=Decimal(trade_data["v"]),
            side=side,
            timestamp=trade_data["T"],
            received_at=int(time.time() * 1000)
        )
