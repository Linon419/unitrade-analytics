"""
交易所网关基类

提供 WebSocket 连接的抽象接口和通用功能。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional, Set
import asyncio
import logging
import random

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    """WebSocket 连接状态"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATED = "authenticated"
    RECONNECTING = "reconnecting"
    FATAL = "fatal"


@dataclass
class StreamConfig:
    """单个数据流配置"""
    stream_type: str  # "orderbook", "trade", "ticker", "oi"
    symbol: str
    depth: Optional[int] = None  # For orderbook
    interval: Optional[str] = None  # For kline
    
    def to_binance_stream(self) -> str:
        """转换为 Binance 流名称"""
        symbol_lower = self.symbol.lower()
        if self.stream_type == "orderbook":
            return f"{symbol_lower}@depth@100ms"
        elif self.stream_type == "trade":
            return f"{symbol_lower}@aggTrade"
        elif self.stream_type == "ticker":
            return f"{symbol_lower}@ticker"
        elif self.stream_type == "liquidation":
            return f"{symbol_lower}@forceOrder"
        else:
            raise ValueError(f"Unknown stream type: {self.stream_type}")
    
    def to_bybit_topic(self) -> str:
        """转换为 Bybit 主题名称"""
        if self.stream_type == "orderbook":
            depth = self.depth or 50
            return f"orderbook.{depth}.{self.symbol}"
        elif self.stream_type == "trade":
            return f"publicTrade.{self.symbol}"
        elif self.stream_type == "ticker":
            return f"tickers.{self.symbol}"
        elif self.stream_type == "liquidation":
            return f"liquidation.{self.symbol}"
        else:
            raise ValueError(f"Unknown stream type: {self.stream_type}")


class AbstractExchangeGateway(ABC):
    """
    交易所网关基类
    
    提供:
    - WebSocket 连接管理
    - 心跳维护
    - 指数退避重连
    - 回调注册
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.state = ConnectionState.DISCONNECTED
        self._callbacks: Dict[str, Callable] = {}
        self._subscriptions: Set[str] = set()
        
        # 重连参数
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = config.get("reconnect_max_attempts", 10)
        self._base_delay = config.get("reconnect_base_delay", 1.0)
        self._max_delay = config.get("reconnect_max_delay", 60.0)
        
        # 心跳配置
        self._heartbeat_interval = config.get("heartbeat_interval", 30)
    
    @property
    def exchange_name(self) -> str:
        """交易所名称"""
        return self.config.get("name", "unknown")
    
    @abstractmethod
    async def connect(self) -> None:
        """建立 WebSocket 连接"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """断开连接"""
        pass
    
    @abstractmethod
    async def subscribe(self, streams: list) -> None:
        """订阅数据流"""
        pass
    
    @abstractmethod
    async def unsubscribe(self, streams: list) -> None:
        """取消订阅"""
        pass
    
    async def fetch_open_interest(self, symbol: str) -> Dict:
        """
        获取持仓量数据 (REST API)
        
        子类可选实现，用于不支持 OI WebSocket 推送的交易所
        """
        raise NotImplementedError("fetch_open_interest not implemented")
    
    def register_callback(self, event_type: str, callback: Callable) -> None:
        """
        注册事件回调
        
        Args:
            event_type: 事件类型 (on_orderbook, on_trade, on_ticker, on_open_interest)
            callback: 异步回调函数
        """
        self._callbacks[event_type] = callback
        logger.debug(f"Registered callback for {event_type}")
    
    def unregister_callback(self, event_type: str) -> None:
        """取消注册回调"""
        self._callbacks.pop(event_type, None)
    
    async def _dispatch(self, event_type: str, data: Any) -> None:
        """分发事件到回调"""
        if callback := self._callbacks.get(event_type):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
            except Exception as e:
                logger.error(f"Callback error for {event_type}: {e}")
    
    def _calculate_backoff(self) -> float:
        """
        计算指数退避延迟
        
        公式: min(base * 2^attempts + jitter, max)
        """
        delay = min(
            self._base_delay * (2 ** self._reconnect_attempts) + random.uniform(0, 1),
            self._max_delay
        )
        return delay
    
    async def _reconnect(self) -> bool:
        """
        执行重连
        
        Returns:
            是否成功重连
        """
        while self._reconnect_attempts < self._max_reconnect_attempts:
            self.state = ConnectionState.RECONNECTING
            delay = self._calculate_backoff()
            
            logger.info(
                f"{self.exchange_name}: Reconnect attempt {self._reconnect_attempts + 1}/"
                f"{self._max_reconnect_attempts}, waiting {delay:.1f}s"
            )
            
            await asyncio.sleep(delay)
            self._reconnect_attempts += 1
            
            try:
                await self.connect()
                
                # 重新订阅
                if self._subscriptions:
                    await self.subscribe(list(self._subscriptions))
                
                self._reconnect_attempts = 0
                logger.info(f"{self.exchange_name}: Reconnected successfully")
                return True
                
            except Exception as e:
                logger.error(f"{self.exchange_name}: Reconnect failed: {e}")
        
        self.state = ConnectionState.FATAL
        logger.critical(f"{self.exchange_name}: Max reconnect attempts reached")
        return False
    
    def _update_state(self, new_state: ConnectionState) -> None:
        """更新连接状态"""
        old_state = self.state
        self.state = new_state
        logger.info(f"{self.exchange_name}: State changed {old_state.value} -> {new_state.value}")
