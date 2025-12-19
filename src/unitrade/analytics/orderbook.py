"""
订单簿管理与 OBI 计算

功能:
1. 维护实时本地订单簿
2. 计算 OBI (Order Book Imbalance)
3. 支持 Binance diffDepth 和 Bybit snapshot/delta
"""

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional, Tuple
from collections import deque

from sortedcontainers import SortedDict


@dataclass
class OrderBookMetrics:
    """订单簿指标"""
    symbol: str
    exchange: str
    mid_price: Decimal
    spread: Decimal
    spread_bps: Decimal  # 基点
    obi: Decimal  # Order Book Imbalance
    bid_depth_10: Decimal  # Top 10 买盘深度 (价值)
    ask_depth_10: Decimal  # Top 10 卖盘深度 (价值)
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    timestamp: int = 0


class LocalOrderBook:
    """
    本地订单簿维护与指标计算
    
    功能:
    1. 维护实时订单簿
    2. 计算 OBI (Order Book Imbalance)
    3. 支持 Binance diffDepth 和 Bybit snapshot/delta
    """
    
    def __init__(self, symbol: str, exchange: str):
        self.symbol = symbol
        self.exchange = exchange
        
        # 订单簿数据 (SortedDict for O(log n) operations)
        # bids: 价格从高到低 (买方希望最高价优先)
        # asks: 价格从低到高 (卖方希望最低价优先)
        self.bids: SortedDict = SortedDict()  # price -> qty
        self.asks: SortedDict = SortedDict()  # price -> qty
        
        # 同步状态
        self.last_update_id: int = 0
        self.sequence: int = 0
        self._initialized: bool = False
        
        # Binance 同步缓冲
        self._buffer: List[dict] = []
        self._syncing: bool = False
        
        # 指标历史
        self._obi_history: deque = deque(maxlen=100)
    
    @property
    def is_initialized(self) -> bool:
        return self._initialized
    
    def apply_snapshot(self, bids: List, asks: List, update_id: int) -> None:
        """
        应用全量快照
        
        Args:
            bids: [[price, qty], ...] 买单列表
            asks: [[price, qty], ...] 卖单列表
            update_id: 更新序号
        """
        self.bids.clear()
        self.asks.clear()
        
        for price, qty in bids:
            p, q = Decimal(str(price)), Decimal(str(qty))
            if q > 0:
                self.bids[p] = q
        
        for price, qty in asks:
            p, q = Decimal(str(price)), Decimal(str(qty))
            if q > 0:
                self.asks[p] = q
        
        self.last_update_id = update_id
        self._initialized = True
    
    def apply_delta(self, bids: List, asks: List, update_id: int) -> bool:
        """
        应用增量更新
        
        规则: qty=0 表示删除该价位
        
        Returns:
            是否成功应用更新
        """
        if not self._initialized:
            return False
        
        for price, qty in bids:
            p, q = Decimal(str(price)), Decimal(str(qty))
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q
        
        for price, qty in asks:
            p, q = Decimal(str(price)), Decimal(str(qty))
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q
        
        self.last_update_id = update_id
        return True
    
    def calculate_obi(self, levels: int = 10) -> Decimal:
        """
        计算 Order Book Imbalance
        
        公式: OBI = (Bid_Volume - Ask_Volume) / (Bid_Volume + Ask_Volume)
        
        范围: [-1, 1]
        - OBI > 0: 买盘压力大
        - OBI < 0: 卖盘压力大
        - OBI = 0: 平衡
        
        Args:
            levels: 计算深度，默认 Top 10
        """
        if not self.bids or not self.asks:
            return Decimal("0")
        
        # 获取 Top N levels
        # bids: 最高 N 个买价 (从高到低)
        top_bids = list(self.bids.items())[-levels:]
        # asks: 最低 N 个卖价 (从低到高)
        top_asks = list(self.asks.items())[:levels]
        
        bid_volume = sum(qty for _, qty in top_bids)
        ask_volume = sum(qty for _, qty in top_asks)
        
        total_volume = bid_volume + ask_volume
        if total_volume == 0:
            return Decimal("0")
        
        obi = (bid_volume - ask_volume) / total_volume
        
        # 记录历史
        self._obi_history.append({
            "obi": obi,
            "timestamp": int(time.time() * 1000)
        })
        
        return obi
    
    def get_depth(self, levels: int = 10) -> Tuple[Decimal, Decimal]:
        """
        获取买卖深度 (价值)
        
        Returns:
            (bid_depth, ask_depth) 以报价货币计价的深度
        """
        if not self.bids or not self.asks:
            return Decimal("0"), Decimal("0")
        
        top_bids = list(self.bids.items())[-levels:]
        top_asks = list(self.asks.items())[:levels]
        
        bid_depth = sum(p * q for p, q in top_bids)
        ask_depth = sum(p * q for p, q in top_asks)
        
        return bid_depth, ask_depth
    
    def get_metrics(self) -> Optional[OrderBookMetrics]:
        """获取当前订单簿指标"""
        if not self.bids or not self.asks:
            return None
        
        # 最佳买卖价
        best_bid = self.bids.peekitem(-1)  # (price, qty)
        best_ask = self.asks.peekitem(0)
        
        mid_price = (best_bid[0] + best_ask[0]) / 2
        spread = best_ask[0] - best_bid[0]
        spread_bps = (spread / mid_price) * 10000 if mid_price > 0 else Decimal("0")
        
        obi = self.calculate_obi(10)
        
        # Top 10 深度
        bid_depth, ask_depth = self.get_depth(10)
        
        return OrderBookMetrics(
            symbol=self.symbol,
            exchange=self.exchange,
            mid_price=mid_price,
            spread=spread,
            spread_bps=spread_bps,
            obi=obi,
            bid_depth_10=bid_depth,
            ask_depth_10=ask_depth,
            best_bid=best_bid[0],
            best_ask=best_ask[0],
            timestamp=int(time.time() * 1000)
        )
    
    @property
    def best_bid_price(self) -> Optional[Decimal]:
        """最佳买价"""
        if not self.bids:
            return None
        return self.bids.peekitem(-1)[0]
    
    @property
    def best_ask_price(self) -> Optional[Decimal]:
        """最佳卖价"""
        if not self.asks:
            return None
        return self.asks.peekitem(0)[0]
    
    @property
    def mid_price(self) -> Optional[Decimal]:
        """中间价"""
        bid = self.best_bid_price
        ask = self.best_ask_price
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2
    
    def get_obi_history(self) -> List[dict]:
        """获取 OBI 历史"""
        return list(self._obi_history)
    
    def clear(self) -> None:
        """清空订单簿"""
        self.bids.clear()
        self.asks.clear()
        self._initialized = False
        self.last_update_id = 0
