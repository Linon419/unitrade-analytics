"""
Binance WebSocket 连接池

处理 Binance USDT-M Futures WebSocket 连接，包括:
- 1024 流限制的分片处理
- 一致性哈希流分配
- 心跳维护
- 自动重连
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

import aiohttp

from .base import AbstractExchangeGateway, ConnectionState, StreamConfig

logger = logging.getLogger(__name__)


@dataclass
class BinanceWSConnection:
    """单个 Binance WebSocket 连接"""
    connection_id: int
    websocket: Optional[aiohttp.ClientWebSocketResponse] = None
    subscribed_streams: Set[str] = field(default_factory=set)
    max_streams: int = 1000  # 预留缓冲 (限制1024)
    last_pong: float = 0
    is_running: bool = False
    
    @property
    def available_slots(self) -> int:
        return self.max_streams - len(self.subscribed_streams)


class BinanceConnectionPool:
    """
    Binance WebSocket 连接池
    
    解决 1024 流限制:
    - 使用一致性哈希将流分配到不同连接
    - 自动扩展连接数
    - 每个连接独立心跳维护
    """
    
    STREAM_URL = "wss://fstream.binance.com/stream"
    TESTNET_URL = "wss://stream.binancefuture.com/stream"
    
    def __init__(
        self, 
        max_connections: int = 5,
        testnet: bool = False,
        callbacks: Optional[Dict[str, Callable]] = None
    ):
        self.max_connections = max_connections
        self.testnet = testnet
        self.base_url = self.TESTNET_URL if testnet else self.STREAM_URL
        
        self.connections: Dict[int, BinanceWSConnection] = {}
        self.stream_to_conn: Dict[str, int] = {}
        self._callbacks = callbacks or {}
        
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._message_tasks: Dict[int, asyncio.Task] = {}
        self._heartbeat_tasks: Dict[int, asyncio.Task] = {}
    
    def _get_shard_id(self, stream: str) -> int:
        """一致性哈希分配连接"""
        hash_val = int(hashlib.md5(stream.encode()).hexdigest(), 16)
        return hash_val % self.max_connections
    
    async def connect_all(self) -> None:
        """初始化所有连接"""
        if not self._session:
            self._session = aiohttp.ClientSession()
        
        for conn_id in range(self.max_connections):
            await self._create_connection(conn_id)
        
        logger.info(f"Binance: Initialized {self.max_connections} connections")
    
    async def _create_connection(self, conn_id: int) -> BinanceWSConnection:
        """创建单个 WebSocket 连接"""
        try:
            ws = await self._session.ws_connect(
                self.base_url,
                heartbeat=30,
                receive_timeout=60
            )
            
            conn = BinanceWSConnection(
                connection_id=conn_id,
                websocket=ws,
                last_pong=time.time(),
                is_running=True
            )
            self.connections[conn_id] = conn
            
            # 启动消息处理和心跳
            self._message_tasks[conn_id] = asyncio.create_task(
                self._message_loop(conn)
            )
            self._heartbeat_tasks[conn_id] = asyncio.create_task(
                self._heartbeat_loop(conn)
            )
            
            logger.debug(f"Binance: Connection {conn_id} established")
            return conn
            
        except Exception as e:
            logger.error(f"Binance: Failed to create connection {conn_id}: {e}")
            raise
    
    async def subscribe(self, streams: List[str]) -> None:
        """
        订阅数据流
        
        自动分配到合适的连接，批量订阅
        """
        async with self._lock:
            # 按连接分组
            conn_streams: Dict[int, List[str]] = {}
            
            for stream in streams:
                if stream in self.stream_to_conn:
                    continue  # 已订阅
                
                shard_id = self._get_shard_id(stream)
                conn_streams.setdefault(shard_id, []).append(stream)
            
            # 批量订阅到各连接
            for conn_id, stream_list in conn_streams.items():
                conn = self.connections.get(conn_id)
                if not conn or not conn.websocket:
                    conn = await self._create_connection(conn_id)
                
                # Binance 批量订阅限制: 每次最多200个
                for batch in self._chunk(stream_list, 200):
                    await self._send_subscribe(conn, batch)
    
    async def unsubscribe(self, streams: List[str]) -> None:
        """取消订阅"""
        async with self._lock:
            conn_streams: Dict[int, List[str]] = {}
            
            for stream in streams:
                if stream not in self.stream_to_conn:
                    continue
                
                conn_id = self.stream_to_conn[stream]
                conn_streams.setdefault(conn_id, []).append(stream)
            
            for conn_id, stream_list in conn_streams.items():
                conn = self.connections.get(conn_id)
                if conn and conn.websocket:
                    await self._send_unsubscribe(conn, stream_list)
    
    async def _send_subscribe(self, conn: BinanceWSConnection, streams: List[str]) -> None:
        """发送订阅请求"""
        payload = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": int(time.time() * 1000)
        }
        await conn.websocket.send_json(payload)
        conn.subscribed_streams.update(streams)
        
        for s in streams:
            self.stream_to_conn[s] = conn.connection_id
        
        logger.debug(f"Binance: Subscribed {len(streams)} streams on connection {conn.connection_id}")
    
    async def _send_unsubscribe(self, conn: BinanceWSConnection, streams: List[str]) -> None:
        """发送取消订阅请求"""
        payload = {
            "method": "UNSUBSCRIBE",
            "params": streams,
            "id": int(time.time() * 1000)
        }
        await conn.websocket.send_json(payload)
        conn.subscribed_streams.difference_update(streams)
        
        for s in streams:
            self.stream_to_conn.pop(s, None)
    
    async def _message_loop(self, conn: BinanceWSConnection) -> None:
        """消息处理循环"""
        while conn.is_running:
            try:
                msg = await conn.websocket.receive()
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await self._dispatch_message(data)
                    
                elif msg.type == aiohttp.WSMsgType.PONG:
                    conn.last_pong = time.time()
                    
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    logger.warning(f"Binance: Connection {conn.connection_id} closed/error")
                    await self._handle_disconnect(conn)
                    break
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Binance: Connection {conn.connection_id} error: {e}")
                await self._handle_disconnect(conn)
                break
    
    async def _dispatch_message(self, data: Dict[str, Any]) -> None:
        """分发消息到回调"""
        # 处理订阅确认
        if "result" in data:
            return
        
        # 获取流名称
        stream = data.get("stream", "")
        event_data = data.get("data", data)
        
        # 根据流类型分发
        if "@depth" in stream:
            if callback := self._callbacks.get("on_orderbook"):
                await callback(event_data)
        elif "@aggTrade" in stream:
            if callback := self._callbacks.get("on_trade"):
                await callback(event_data)
        elif "@ticker" in stream:
            if callback := self._callbacks.get("on_ticker"):
                await callback(event_data)
        elif "@forceOrder" in stream:
            if callback := self._callbacks.get("on_liquidation"):
                await callback(event_data)
    
    async def _heartbeat_loop(self, conn: BinanceWSConnection) -> None:
        """
        心跳维护
        
        Binance 规则:
        - 服务器每3分钟发送 ping
        - 必须在10分钟内响应 pong
        - 客户端也可主动发 ping
        """
        while conn.is_running and conn.websocket and not conn.websocket.closed:
            await asyncio.sleep(180)  # 3分钟
            
            try:
                await conn.websocket.ping()
            except Exception:
                break
            
            # 检查 pong 超时
            now = time.time()
            if now - conn.last_pong > 600:  # 10分钟无 pong
                logger.warning(f"Binance: Connection {conn.connection_id} pong timeout")
                await self._handle_disconnect(conn)
                break
    
    async def _handle_disconnect(self, conn: BinanceWSConnection) -> None:
        """处理断线重连"""
        logger.warning(f"Binance: Connection {conn.connection_id} disconnected, reconnecting...")
        
        # 保存订阅列表
        streams = list(conn.subscribed_streams)
        conn.subscribed_streams.clear()
        conn.is_running = False
        
        # 指数退避重连
        for attempt in range(10):
            delay = min(1 * (2 ** attempt), 60)
            await asyncio.sleep(delay)
            
            try:
                new_conn = await self._create_connection(conn.connection_id)
                # 重新订阅
                if streams:
                    for batch in self._chunk(streams, 200):
                        await self._send_subscribe(new_conn, batch)
                logger.info(f"Binance: Connection {conn.connection_id} reconnected")
                return
            except Exception as e:
                logger.error(f"Binance: Reconnect attempt {attempt + 1} failed: {e}")
        
        logger.critical(f"Binance: Connection {conn.connection_id} failed to reconnect")
    
    async def close(self) -> None:
        """关闭所有连接"""
        for conn in self.connections.values():
            conn.is_running = False
            if conn.websocket:
                await conn.websocket.close()
        
        for task in self._message_tasks.values():
            task.cancel()
        for task in self._heartbeat_tasks.values():
            task.cancel()
        
        if self._session:
            await self._session.close()
        
        logger.info("Binance: All connections closed")
    
    @staticmethod
    def _chunk(lst: List, size: int):
        """分块"""
        for i in range(0, len(lst), size):
            yield lst[i:i + size]


class BinanceGateway(AbstractExchangeGateway):
    """
    Binance USDT-M Futures 网关
    
    整合连接池和 REST API
    """
    
    REST_URL = "https://fapi.binance.com"
    TESTNET_REST_URL = "https://testnet.binancefuture.com"
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.testnet = config.get("testnet", False)
        self.rest_url = self.TESTNET_REST_URL if self.testnet else self.REST_URL
        
        self._pool: Optional[BinanceConnectionPool] = None
        self._rest_session: Optional[aiohttp.ClientSession] = None
    
    async def connect(self) -> None:
        """建立连接"""
        self._update_state(ConnectionState.CONNECTING)
        
        # 创建连接池
        self._pool = BinanceConnectionPool(
            max_connections=self.config.get("max_connections", 5),
            testnet=self.testnet,
            callbacks=self._callbacks
        )
        await self._pool.connect_all()
        
        # REST 会话
        self._rest_session = aiohttp.ClientSession()
        
        self._update_state(ConnectionState.CONNECTED)
    
    async def disconnect(self) -> None:
        """断开连接"""
        if self._pool:
            await self._pool.close()
        if self._rest_session:
            await self._rest_session.close()
        
        self._update_state(ConnectionState.DISCONNECTED)
    
    async def subscribe(self, streams: list) -> None:
        """订阅数据流"""
        if not self._pool:
            raise RuntimeError("Not connected")
        
        # 转换 StreamConfig 到流名称
        stream_names = []
        for s in streams:
            if isinstance(s, StreamConfig):
                stream_names.append(s.to_binance_stream())
                self._subscriptions.add(s.to_binance_stream())
            else:
                stream_names.append(s)
                self._subscriptions.add(s)
        
        await self._pool.subscribe(stream_names)
    
    async def unsubscribe(self, streams: list) -> None:
        """取消订阅"""
        if not self._pool:
            return
        
        stream_names = []
        for s in streams:
            if isinstance(s, StreamConfig):
                name = s.to_binance_stream()
                stream_names.append(name)
                self._subscriptions.discard(name)
            else:
                stream_names.append(s)
                self._subscriptions.discard(s)
        
        await self._pool.unsubscribe(stream_names)
    
    async def fetch_open_interest(self, symbol: str) -> Dict:
        """
        获取持仓量
        
        Endpoint: GET /fapi/v1/openInterest
        """
        if not self._rest_session:
            raise RuntimeError("Not connected")
        
        url = f"{self.rest_url}/fapi/v1/openInterest"
        async with self._rest_session.get(url, params={"symbol": symbol}) as resp:
            if resp.status == 200:
                return await resp.json()
            elif resp.status == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                raise Exception(f"Rate limited, retry after {retry_after}s")
            else:
                raise Exception(f"HTTP {resp.status}: {await resp.text()}")
    
    async def fetch_orderbook_snapshot(self, symbol: str, limit: int = 500) -> Dict:
        """
        获取订单簿快照
        
        用于初始化本地订单簿
        """
        if not self._rest_session:
            raise RuntimeError("Not connected")
        
        url = f"{self.rest_url}/fapi/v1/depth"
        async with self._rest_session.get(url, params={"symbol": symbol, "limit": limit}) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                raise Exception(f"HTTP {resp.status}: {await resp.text()}")
