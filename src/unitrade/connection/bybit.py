"""
Bybit V5 WebSocket 网关

支持 Bybit V5 Unified Account API:
- 公共频道: orderbook, trade, ticker (含 OI)
- 私有频道: 订单、仓位、账户
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional

import aiohttp

from .base import AbstractExchangeGateway, ConnectionState, StreamConfig

logger = logging.getLogger(__name__)


class BybitV5Gateway(AbstractExchangeGateway):
    """
    Bybit V5 统一账户网关
    
    特点:
    - 单一 WebSocket 支持所有数据类型
    - ticker 主题包含 Open Interest
    - 订阅格式统一: topic.params
    """
    
    PUBLIC_URL = "wss://stream.bybit.com/v5/public/linear"
    PRIVATE_URL = "wss://stream.bybit.com/v5/private"
    TESTNET_PUBLIC_URL = "wss://stream-testnet.bybit.com/v5/public/linear"
    TESTNET_PRIVATE_URL = "wss://stream-testnet.bybit.com/v5/private"
    REST_URL = "https://api.bybit.com"
    TESTNET_REST_URL = "https://api-testnet.bybit.com"
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.testnet = config.get("testnet", False)
        
        # URL 设置
        self.public_url = self.TESTNET_PUBLIC_URL if self.testnet else self.PUBLIC_URL
        self.private_url = self.TESTNET_PRIVATE_URL if self.testnet else self.PRIVATE_URL
        self.rest_url = self.TESTNET_REST_URL if self.testnet else self.REST_URL
        
        # WebSocket 连接
        self.public_ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.private_ws: Optional[aiohttp.ClientWebSocketResponse] = None
        
        # 会话
        self._session: Optional[aiohttp.ClientSession] = None
        
        # 任务
        self._public_task: Optional[asyncio.Task] = None
        self._private_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        
        # 运行状态
        self._running = False
    
    async def connect(self) -> None:
        """建立公共和私有 WebSocket 连接"""
        self._update_state(ConnectionState.CONNECTING)
        self._running = True
        
        self._session = aiohttp.ClientSession()
        
        # 公共频道
        self.public_ws = await self._session.ws_connect(
            self.public_url,
            heartbeat=20
        )
        
        self._public_task = asyncio.create_task(self._public_message_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())
        
        self._update_state(ConnectionState.CONNECTED)
        logger.info("Bybit: Public WebSocket connected")
    
    async def connect_private(self) -> None:
        """连接私有频道 (需要 API Key)"""
        api_key = self.config.get("api_key", "")
        api_secret = self.config.get("api_secret", "")
        
        if not api_key or not api_secret:
            raise ValueError("API key and secret required for private channel")
        
        self.private_ws = await self._session.ws_connect(
            self.private_url,
            heartbeat=20
        )
        
        # 认证
        await self._authenticate(api_key, api_secret)
        
        self._private_task = asyncio.create_task(self._private_message_loop())
        
        self._update_state(ConnectionState.AUTHENTICATED)
        logger.info("Bybit: Private WebSocket authenticated")
    
    async def _authenticate(self, api_key: str, api_secret: str) -> None:
        """私有频道认证"""
        expires = int((time.time() + 10) * 1000)
        signature = hmac.new(
            api_secret.encode("utf-8"),
            f"GET/realtime{expires}".encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        
        auth_payload = {
            "op": "auth",
            "args": [api_key, expires, signature]
        }
        
        await self.private_ws.send_json(auth_payload)
        
        # 等待认证响应
        msg = await self.private_ws.receive()
        if msg.type == aiohttp.WSMsgType.TEXT:
            data = json.loads(msg.data)
            if not data.get("success"):
                raise Exception(f"Authentication failed: {data}")
    
    async def disconnect(self) -> None:
        """断开所有连接"""
        self._running = False
        
        if self._public_task:
            self._public_task.cancel()
        if self._private_task:
            self._private_task.cancel()
        if self._ping_task:
            self._ping_task.cancel()
        
        if self.public_ws:
            await self.public_ws.close()
        if self.private_ws:
            await self.private_ws.close()
        if self._session:
            await self._session.close()
        
        self._update_state(ConnectionState.DISCONNECTED)
        logger.info("Bybit: All connections closed")
    
    async def subscribe(self, streams: list) -> None:
        """
        订阅 Bybit 数据流
        
        主题格式:
        - orderbook.{depth}.{symbol}: orderbook.50.BTCUSDT
        - publicTrade.{symbol}: publicTrade.BTCUSDT
        - tickers.{symbol}: tickers.BTCUSDT (包含 OI!)
        - liquidation.{symbol}: liquidation.BTCUSDT
        """
        topics = []
        
        for stream in streams:
            if isinstance(stream, StreamConfig):
                topic = stream.to_bybit_topic()
            else:
                topic = stream
            topics.append(topic)
            self._subscriptions.add(topic)
        
        if topics and self.public_ws:
            payload = {
                "op": "subscribe",
                "args": topics
            }
            await self.public_ws.send_json(payload)
            logger.debug(f"Bybit: Subscribed to {topics}")
    
    async def unsubscribe(self, streams: list) -> None:
        """取消订阅"""
        topics = []
        
        for stream in streams:
            if isinstance(stream, StreamConfig):
                topic = stream.to_bybit_topic()
            else:
                topic = stream
            topics.append(topic)
            self._subscriptions.discard(topic)
        
        if topics and self.public_ws:
            payload = {
                "op": "unsubscribe",
                "args": topics
            }
            await self.public_ws.send_json(payload)
    
    async def _public_message_loop(self) -> None:
        """公共频道消息处理"""
        while self._running:
            try:
                msg = await self.public_ws.receive()
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    
                    # 处理 pong 响应
                    if data.get("op") == "pong":
                        continue
                    
                    # 处理订阅确认
                    if data.get("success") is not None:
                        if not data.get("success"):
                            logger.error(f"Bybit subscription failed: {data}")
                        continue
                    
                    # 分发数据
                    topic = data.get("topic", "")
                    await self._dispatch_by_topic(topic, data)
                    
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    logger.warning("Bybit: Public WebSocket closed/error")
                    await self._reconnect_public()
                    break
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Bybit public WS error: {e}")
                await self._reconnect_public()
                break
    
    async def _private_message_loop(self) -> None:
        """私有频道消息处理"""
        while self._running and self.private_ws:
            try:
                msg = await self.private_ws.receive()
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    
                    topic = data.get("topic", "")
                    
                    if topic.startswith("order"):
                        if callback := self._callbacks.get("on_order"):
                            await self._safe_callback(callback, data)
                    elif topic.startswith("execution"):
                        if callback := self._callbacks.get("on_execution"):
                            await self._safe_callback(callback, data)
                    elif topic.startswith("position"):
                        if callback := self._callbacks.get("on_position"):
                            await self._safe_callback(callback, data)
                    elif topic.startswith("wallet"):
                        if callback := self._callbacks.get("on_wallet"):
                            await self._safe_callback(callback, data)
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Bybit private WS error: {e}")
                break
    
    async def _dispatch_by_topic(self, topic: str, data: Dict) -> None:
        """根据主题分发消息"""
        if topic.startswith("orderbook"):
            if callback := self._callbacks.get("on_orderbook"):
                await self._safe_callback(callback, data)
                
        elif topic.startswith("publicTrade"):
            if callback := self._callbacks.get("on_trade"):
                await self._safe_callback(callback, data)
                
        elif topic.startswith("tickers"):
            # tickers 包含 OI，需要分别处理
            if callback := self._callbacks.get("on_ticker"):
                await self._safe_callback(callback, data)
            if callback := self._callbacks.get("on_open_interest"):
                # 提取 OI 数据
                oi_data = self._extract_oi_from_ticker(data)
                if oi_data:
                    await self._safe_callback(callback, oi_data)
                    
        elif topic.startswith("liquidation"):
            if callback := self._callbacks.get("on_liquidation"):
                await self._safe_callback(callback, data)
    
    def _extract_oi_from_ticker(self, ticker_data: Dict) -> Optional[Dict]:
        """
        从 Bybit ticker 中提取 Open Interest
        
        Ticker 数据结构:
        {
            "topic": "tickers.BTCUSDT",
            "data": {
                "symbol": "BTCUSDT",
                "lastPrice": "50000",
                "openInterest": "12345.678",
                "openInterestValue": "617283900",
                "fundingRate": "0.0001",
                ...
            }
        }
        """
        data = ticker_data.get("data", {})
        
        if "openInterest" not in data:
            return None
        
        return {
            "exchange": "bybit",
            "symbol": data.get("symbol"),
            "open_interest": data.get("openInterest"),
            "open_interest_value": data.get("openInterestValue"),
            "timestamp": ticker_data.get("ts")
        }
    
    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全执行回调"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            logger.error(f"Callback error: {e}")
    
    async def _ping_loop(self) -> None:
        """
        Bybit 心跳维护
        
        规则: 必须每20秒发送一次 ping
        格式: {"op": "ping"}
        响应: {"op": "pong", ...}
        """
        while self._running:
            try:
                if self.public_ws and not self.public_ws.closed:
                    await self.public_ws.send_json({"op": "ping"})
                if self.private_ws and not self.private_ws.closed:
                    await self.private_ws.send_json({"op": "ping"})
                await asyncio.sleep(20)
            except asyncio.CancelledError:
                break
            except Exception:
                break
    
    async def _reconnect_public(self) -> None:
        """重连公共频道"""
        if not self._running:
            return
        
        logger.info("Bybit: Reconnecting public WebSocket...")
        
        # 保存订阅
        subscriptions = list(self._subscriptions)
        
        for attempt in range(self._max_reconnect_attempts):
            delay = self._calculate_backoff()
            await asyncio.sleep(delay)
            
            try:
                self.public_ws = await self._session.ws_connect(
                    self.public_url,
                    heartbeat=20
                )
                
                # 重新订阅
                if subscriptions:
                    await self.subscribe(subscriptions)
                
                self._public_task = asyncio.create_task(self._public_message_loop())
                logger.info("Bybit: Public WebSocket reconnected")
                return
                
            except Exception as e:
                logger.error(f"Bybit: Reconnect attempt {attempt + 1} failed: {e}")
        
        logger.critical("Bybit: Failed to reconnect public WebSocket")
    
    async def fetch_open_interest(self, symbol: str) -> Dict:
        """
        通过 REST API 获取持仓量
        
        Endpoint: GET /v5/market/open-interest
        """
        if not self._session:
            raise RuntimeError("Not connected")
        
        url = f"{self.rest_url}/v5/market/open-interest"
        params = {
            "category": "linear",
            "symbol": symbol
        }
        
        async with self._session.get(url, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("retCode") == 0:
                    return data.get("result", {})
                else:
                    raise Exception(f"API error: {data}")
            else:
                raise Exception(f"HTTP {resp.status}: {await resp.text()}")
