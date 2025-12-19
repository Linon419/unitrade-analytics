"""
双策略速率限制器

Binance: 权重制速率限制
- 2400 权重/分钟 (USDT-M Futures)
- 不同端点权重不同

Bybit: 计数制速率限制
- 订单端点: 10 次/秒/symbol, 100 次/秒/总
- 公共端点: 120 次/秒
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class RateLimitState:
    """速率限制状态"""
    current_usage: int
    limit: int
    window_seconds: int
    blocked_until: float = 0


class BinanceRateLimiter:
    """
    Binance 权重制速率限制器
    
    规则:
    - 2400 权重/分钟 (USDT-M Futures)
    - 不同端点权重不同
    - 超限返回 HTTP 429 + Retry-After
    """
    
    ENDPOINT_WEIGHTS = {
        "/fapi/v1/order": 1,
        "/fapi/v1/batchOrders": 5,
        "/fapi/v1/allOpenOrders": 1,
        "/fapi/v1/depth": 10,  # 默认，实际根据 limit 变化
        "/fapi/v1/openInterest": 1,
        "/fapi/v1/account": 5,
        "/fapi/v1/positionRisk": 5,
        "/fapi/v2/account": 5,
        "/fapi/v2/positionRisk": 5,
    }
    
    def __init__(self, max_weight: int = 2400, window_seconds: int = 60):
        self.max_weight = max_weight
        self.window_seconds = window_seconds
        
        self._requests: deque = deque()  # (timestamp, weight)
        self._current_weight: int = 0
        self._blocked_until: float = 0
        self._lock = asyncio.Lock()
    
    async def acquire(self, endpoint: str, params: Optional[Dict] = None) -> None:
        """获取请求许可"""
        weight = self._get_weight(endpoint, params)
        
        async with self._lock:
            now = time.time()
            
            # 检查是否被封禁
            if now < self._blocked_until:
                wait_time = self._blocked_until - now
                await asyncio.sleep(wait_time)
            
            # 清理过期请求
            self._cleanup(now)
            
            # 预留 10% 缓冲
            effective_limit = int(self.max_weight * 0.9)
            
            # 等待直到有足够额度
            while self._current_weight + weight > effective_limit:
                if self._requests:
                    oldest = self._requests[0][0]
                    sleep_time = oldest + self.window_seconds - now + 0.1
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                    self._cleanup(time.time())
                else:
                    break
            
            # 记录请求
            self._requests.append((time.time(), weight))
            self._current_weight += weight
    
    def handle_429(self, retry_after: int) -> None:
        """处理 429 响应"""
        self._blocked_until = time.time() + retry_after
    
    def _cleanup(self, now: float) -> None:
        """清理过期请求"""
        cutoff = now - self.window_seconds
        while self._requests and self._requests[0][0] < cutoff:
            _, weight = self._requests.popleft()
            self._current_weight -= weight
    
    def _get_weight(self, endpoint: str, params: Optional[Dict]) -> int:
        """获取端点权重"""
        base_weight = self.ENDPOINT_WEIGHTS.get(endpoint, 1)
        
        # depth 端点根据 limit 调整
        if endpoint == "/fapi/v1/depth" and params:
            limit = params.get("limit", 500)
            if limit <= 100:
                return 5
            elif limit <= 500:
                return 10
            else:
                return 20
        
        return base_weight
    
    @property
    def current_usage(self) -> int:
        """当前使用的权重"""
        self._cleanup(time.time())
        return self._current_weight
    
    @property
    def available(self) -> int:
        """可用权重"""
        return self.max_weight - self.current_usage


class BybitRateLimiter:
    """
    Bybit 计数制速率限制器
    
    规则:
    - 订单端点: 10 次/秒/symbol, 100 次/秒/总
    - 公共端点: 120 次/秒
    """
    
    def __init__(self):
        self._global_requests: deque = deque()
        self._symbol_requests: Dict[str, deque] = {}
        self._blocked_until: float = 0
        self._lock = asyncio.Lock()
        
        self.global_limit = 100
        self.symbol_limit = 10
        self.window = 1.0  # 1 秒窗口
    
    async def acquire(self, endpoint: str, symbol: Optional[str] = None) -> None:
        """获取请求许可"""
        async with self._lock:
            now = time.time()
            
            if now < self._blocked_until:
                await asyncio.sleep(self._blocked_until - now)
            
            # 全局限制
            await self._check_limit(self._global_requests, self.global_limit, now)
            
            # 按 symbol 限制 (仅订单端点)
            if symbol and "/order" in endpoint:
                if symbol not in self._symbol_requests:
                    self._symbol_requests[symbol] = deque()
                await self._check_limit(self._symbol_requests[symbol], self.symbol_limit, now)
                self._symbol_requests[symbol].append(now)
            
            self._global_requests.append(now)
    
    async def _check_limit(self, queue: deque, limit: int, now: float) -> None:
        """检查并等待限制"""
        cutoff = now - self.window
        
        while queue and queue[0] < cutoff:
            queue.popleft()
        
        while len(queue) >= limit:
            sleep_time = queue[0] + self.window - now + 0.01
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            now = time.time()
            cutoff = now - self.window
            while queue and queue[0] < cutoff:
                queue.popleft()
    
    def handle_rate_limit(self, retry_after: float = 1.0) -> None:
        """处理速率限制"""
        self._blocked_until = time.time() + retry_after


class DualRateLimiter:
    """双策略速率限制器"""
    
    def __init__(self):
        self.binance = BinanceRateLimiter()
        self.bybit = BybitRateLimiter()
    
    async def acquire(self, exchange: str, endpoint: str, **kwargs) -> None:
        """获取请求许可"""
        if exchange == "binance":
            await self.binance.acquire(endpoint, kwargs.get("params"))
        elif exchange == "bybit":
            await self.bybit.acquire(endpoint, kwargs.get("symbol"))
        else:
            raise ValueError(f"Unknown exchange: {exchange}")
    
    def handle_rate_limit(self, exchange: str, retry_after: float = 1.0) -> None:
        """处理速率限制响应"""
        if exchange == "binance":
            self.binance.handle_429(int(retry_after))
        elif exchange == "bybit":
            self.bybit.handle_rate_limit(retry_after)
