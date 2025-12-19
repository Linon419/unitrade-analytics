"""
订单 ID 管理器

ClientOrderId 格式: {PREFIX}-{ALGO_ID}-{UUID8}

示例: UT-MM01-a1b2c3d4

组成:
- PREFIX: "UT" (UniTrade 标识)
- ALGO_ID: 策略 ID (最多 6 字符)
- UUID8: UUID 前 8 位 (防碰撞)

限制:
- Binance: 最长 36 字符
- Bybit: 最长 36 字符
"""

import time
import uuid
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class OrderIdEntry:
    """订单 ID 映射条目"""
    client_order_id: str
    exchange_order_id: Optional[str] = None
    exchange: str = ""
    symbol: str = ""
    algo_id: str = ""
    created_at: int = 0


class OrderIdManager:
    """
    订单 ID 管理器
    
    功能:
    - 生成唯一的 ClientOrderId
    - 关联 ClientOrderId 与 ExchangeOrderId
    - 支持双向查询
    """
    
    PREFIX = "UT"
    MAX_LENGTH = 36
    
    def __init__(self):
        self._by_client_id: Dict[str, OrderIdEntry] = {}
        self._by_exchange_id: Dict[str, str] = {}
    
    def generate(self, algo_id: str, exchange: str, symbol: str) -> str:
        """生成新的 ClientOrderId"""
        uuid8 = uuid.uuid4().hex[:8]
        
        # 截断 algo_id 确保总长度不超限
        # 格式: UT-ALGO_ID-UUID8 = 2 + 1 + len(algo) + 1 + 8 = 12 + len(algo)
        max_algo_len = self.MAX_LENGTH - 12
        truncated_algo = algo_id[:max_algo_len]
        
        client_order_id = f"{self.PREFIX}-{truncated_algo}-{uuid8}"
        
        # 注册
        self._by_client_id[client_order_id] = OrderIdEntry(
            client_order_id=client_order_id,
            exchange=exchange,
            symbol=symbol,
            algo_id=algo_id,
            created_at=int(time.time() * 1000)
        )
        
        return client_order_id
    
    def register_exchange_id(self, client_order_id: str, exchange_order_id: str) -> None:
        """关联交易所订单 ID"""
        if client_order_id in self._by_client_id:
            self._by_client_id[client_order_id].exchange_order_id = exchange_order_id
            self._by_exchange_id[exchange_order_id] = client_order_id
    
    def get_by_client_id(self, client_order_id: str) -> Optional[OrderIdEntry]:
        """通过 ClientOrderId 查询"""
        return self._by_client_id.get(client_order_id)
    
    def get_by_exchange_id(self, exchange_order_id: str) -> Optional[OrderIdEntry]:
        """通过 ExchangeOrderId 查询"""
        client_id = self._by_exchange_id.get(exchange_order_id)
        return self._by_client_id.get(client_id) if client_id else None
    
    def remove(self, client_order_id: str) -> None:
        """移除订单 ID 映射 (订单完成后清理)"""
        if entry := self._by_client_id.pop(client_order_id, None):
            if entry.exchange_order_id:
                self._by_exchange_id.pop(entry.exchange_order_id, None)
    
    def cleanup_old(self, max_age_ms: int = 86400000) -> int:
        """清理超过指定时间的旧条目"""
        now = int(time.time() * 1000)
        to_remove = []
        
        for cid, entry in self._by_client_id.items():
            if now - entry.created_at > max_age_ms:
                to_remove.append(cid)
        
        for cid in to_remove:
            self.remove(cid)
        
        return len(to_remove)
