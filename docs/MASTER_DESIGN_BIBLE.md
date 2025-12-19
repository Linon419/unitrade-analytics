# UniTrade Analytics Gateway: Master Design & Execution Bible

---

> Note: 本文档描述的是早期/规划中的架构（包含 Redis + TimescaleDB + Grafana 的 Hot/Cold Path）。当前仓库默认以 SQLite 为主，TimescaleDB/Grafana 相关实现已移除。

# PART 1: TECHNICAL DESIGN DOCUMENT

## High-Level Data Flow Pipeline

```mermaid
graph TB
    subgraph "External Data Sources"
        BIN_WS[Binance WebSocket<br/>depth/aggTrade/forceOrder]
        BIN_REST[Binance REST<br/>Open Interest Polling]
        BYB_WS[Bybit WebSocket<br/>orderbook/trade/ticker/OI]
    end

    subgraph "Connection Layer"
        subgraph "Binance Gateway"
            B_POOL[Connection Pool<br/>Sharded WS]
            B_OI_POLL[OI Poller<br/>5s interval]
        end
        
        subgraph "Bybit Gateway"
            BY_PUB[Public WS]
            BY_PRIV[Private WS]
        end
    end

    subgraph "Normalization Layer"
        OB_MGR[OrderBook Manager<br/>LocalOrderBook per symbol]
        TRADE_NORM[Trade Normalizer<br/>TradeTick schema]
        OI_SYNC[OI Synchronizer<br/>Align with trades]
    end

    subgraph "Analytics Engine"
        OBI[OBI Calculator<br/>Top 10 Bid/Ask Ratio]
        CVD[CVD Tracker<br/>Cumulative Volume Delta]
        VOL[Volatility Calculator<br/>1-min Rolling StdDev]
        OI_DELTA[OI Delta Analyzer<br/>5-min Change + Regime]
    end

    subgraph "Persistence Layer"
        REDIS[(Redis<br/>Real-time Pub/Sub)]
        TSDB[(TimescaleDB<br/>Historical Storage)]
    end

    subgraph "Consumers"
        STRAT[Strategy Engine]
        GRAFANA[Grafana Dashboard]
        BACKTEST[Backtesting System]
    end

    BIN_WS --> B_POOL
    BIN_REST --> B_OI_POLL
    BYB_WS --> BY_PUB & BY_PRIV

    B_POOL --> OB_MGR & TRADE_NORM
    B_OI_POLL --> OI_SYNC
    BY_PUB --> OB_MGR & TRADE_NORM & OI_SYNC

    OB_MGR --> OBI
    TRADE_NORM --> CVD & VOL
    OI_SYNC --> OI_DELTA

    OBI & CVD & VOL & OI_DELTA --> REDIS
    TRADE_NORM --> TSDB
    OBI & CVD & OI_DELTA --> TSDB

    REDIS --> STRAT & GRAFANA
    TSDB --> BACKTEST & GRAFANA
```

---

## 1. Connection Layer (The "Plumbing")

### 1.1 ExchangeGateway Class Hierarchy

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Callable, Optional, Set
from enum import Enum
from dataclasses import dataclass
import asyncio


class ConnectionState(Enum):
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


class AbstractExchangeGateway(ABC):
    """交易所网关基类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.state = ConnectionState.DISCONNECTED
        self._callbacks: Dict[str, Callable] = {}
        self._subscriptions: Set[str] = set()
        
        # 重连参数
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 10
        self._base_delay = 1.0
        self._max_delay = 60.0
    
    @abstractmethod
    async def connect(self) -> None:
        """建立连接"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """断开连接"""
        pass
    
    @abstractmethod
    async def subscribe(self, streams: list[StreamConfig]) -> None:
        """订阅数据流"""
        pass
    
    @abstractmethod
    async def fetch_open_interest(self, symbol: str) -> Dict:
        """获取持仓量数据"""
        pass
    
    def register_callback(self, event_type: str, callback: Callable) -> None:
        """注册回调函数"""
        self._callbacks[event_type] = callback
    
    def _calculate_backoff(self) -> float:
        """指数退避计算"""
        import random
        delay = min(
            self._base_delay * (2 ** self._reconnect_attempts) + random.uniform(0, 1),
            self._max_delay
        )
        return delay
```

### 1.2 WebSocket Optimization

#### Binance Connection Pool (1024流限制处理)

```python
import hashlib
from dataclasses import dataclass, field
from typing import Set, Dict
import aiohttp
import asyncio


@dataclass
class BinanceWSConnection:
    """单个Binance WebSocket连接"""
    connection_id: int
    websocket: Optional[aiohttp.ClientWebSocketResponse] = None
    subscribed_streams: Set[str] = field(default_factory=set)
    max_streams: int = 1000  # 预留24个缓冲
    last_pong: float = 0
    
    @property
    def available_slots(self) -> int:
        return self.max_streams - len(self.subscribed_streams)


class BinanceConnectionPool:
    """
    Binance WebSocket连接池
    
    解决1024流限制:
    - 使用一致性哈希将流分配到不同连接
    - 自动扩展连接数
    - 每个连接独立心跳维护
    """
    
    STREAM_URL = "wss://fstream.binance.com/stream"
    
    def __init__(self, max_connections: int = 5):
        self.max_connections = max_connections
        self.connections: Dict[int, BinanceWSConnection] = {}
        self.stream_to_conn: Dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
    
    def _get_shard_id(self, stream: str) -> int:
        """一致性哈希分配连接"""
        hash_val = int(hashlib.md5(stream.encode()).hexdigest(), 16)
        return hash_val % self.max_connections
    
    async def connect_all(self) -> None:
        """初始化所有连接"""
        self._session = aiohttp.ClientSession()
        
        for conn_id in range(self.max_connections):
            await self._create_connection(conn_id)
    
    async def _create_connection(self, conn_id: int) -> BinanceWSConnection:
        """创建单个WebSocket连接"""
        ws = await self._session.ws_connect(
            self.STREAM_URL,
            heartbeat=30,
            receive_timeout=60
        )
        
        conn = BinanceWSConnection(
            connection_id=conn_id,
            websocket=ws,
            last_pong=asyncio.get_event_loop().time()
        )
        self.connections[conn_id] = conn
        
        # 启动消息处理和心跳
        asyncio.create_task(self._message_loop(conn))
        asyncio.create_task(self._heartbeat_loop(conn))
        
        return conn
    
    async def subscribe(self, streams: list[str]) -> None:
        """
        订阅数据流
        
        自动分配到合适的连接，批量订阅
        """
        async with self._lock:
            # 按连接分组
            conn_streams: Dict[int, list[str]] = {}
            
            for stream in streams:
                if stream in self.stream_to_conn:
                    continue  # 已订阅
                
                shard_id = self._get_shard_id(stream)
                conn_streams.setdefault(shard_id, []).append(stream)
            
            # 批量订阅到各连接
            for conn_id, stream_list in conn_streams.items():
                conn = self.connections.get(conn_id)
                if not conn:
                    conn = await self._create_connection(conn_id)
                
                # Binance批量订阅限制: 每次最多200个
                for batch in self._chunk(stream_list, 200):
                    await self._send_subscribe(conn, batch)
    
    async def _send_subscribe(self, conn: BinanceWSConnection, streams: list[str]) -> None:
        """发送订阅请求"""
        payload = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": int(asyncio.get_event_loop().time() * 1000)
        }
        await conn.websocket.send_json(payload)
        conn.subscribed_streams.update(streams)
        
        for s in streams:
            self.stream_to_conn[s] = conn.connection_id
    
    async def _message_loop(self, conn: BinanceWSConnection) -> None:
        """消息处理循环"""
        while True:
            try:
                msg = await conn.websocket.receive()
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = msg.json()
                    await self._dispatch_message(data)
                    
                elif msg.type == aiohttp.WSMsgType.PONG:
                    conn.last_pong = asyncio.get_event_loop().time()
                    
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    await self._handle_disconnect(conn)
                    break
                    
            except Exception as e:
                logging.error(f"Connection {conn.connection_id} error: {e}")
                await self._handle_disconnect(conn)
                break
    
    async def _heartbeat_loop(self, conn: BinanceWSConnection) -> None:
        """
        心跳维护
        
        Binance规则:
        - 服务器每3分钟发送ping
        - 必须在10分钟内响应pong
        - 客户端也可主动发ping
        """
        while conn.websocket and not conn.websocket.closed:
            await asyncio.sleep(180)  # 3分钟
            
            try:
                await conn.websocket.ping()
            except Exception:
                break
            
            # 检查pong超时
            now = asyncio.get_event_loop().time()
            if now - conn.last_pong > 600:  # 10分钟无pong
                logging.warning(f"Connection {conn.connection_id} pong timeout")
                await self._handle_disconnect(conn)
                break
    
    async def _handle_disconnect(self, conn: BinanceWSConnection) -> None:
        """处理断线重连"""
        logging.warning(f"Connection {conn.connection_id} disconnected, reconnecting...")
        
        # 保存订阅列表
        streams = list(conn.subscribed_streams)
        conn.subscribed_streams.clear()
        
        # 指数退避重连
        for attempt in range(10):
            delay = min(1 * (2 ** attempt), 60)
            await asyncio.sleep(delay)
            
            try:
                new_conn = await self._create_connection(conn.connection_id)
                # 重新订阅
                if streams:
                    await self._send_subscribe(new_conn, streams)
                logging.info(f"Connection {conn.connection_id} reconnected")
                return
            except Exception as e:
                logging.error(f"Reconnect attempt {attempt + 1} failed: {e}")
        
        logging.critical(f"Connection {conn.connection_id} failed to reconnect")
    
    @staticmethod
    def _chunk(lst: list, size: int):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]
```

#### Bybit V5 统一主题订阅

```python
class BybitV5Gateway(AbstractExchangeGateway):
    """
    Bybit V5 统一账户网关
    
    特点:
    - 单一WebSocket支持所有数据类型
    - ticker主题包含Open Interest
    - 订阅格式统一: topic.params
    """
    
    PUBLIC_URL = "wss://stream.bybit.com/v5/public/linear"
    PRIVATE_URL = "wss://stream.bybit.com/v5/private"
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.public_ws = None
        self.private_ws = None
        self._message_handlers: Dict[str, Callable] = {}
    
    async def connect(self) -> None:
        """建立公共和私有WebSocket连接"""
        self._session = aiohttp.ClientSession()
        
        # 公共频道
        self.public_ws = await self._session.ws_connect(
            self.PUBLIC_URL,
            heartbeat=20
        )
        asyncio.create_task(self._public_message_loop())
        asyncio.create_task(self._ping_loop(self.public_ws, "public"))
        
        self.state = ConnectionState.CONNECTED
    
    async def subscribe(self, streams: list[StreamConfig]) -> None:
        """
        订阅Bybit数据流
        
        主题格式:
        - orderbook.{depth}.{symbol}: orderbook.50.BTCUSDT
        - publicTrade.{symbol}: publicTrade.BTCUSDT  
        - tickers.{symbol}: tickers.BTCUSDT (包含OI!)
        - liquidation.{symbol}: liquidation.BTCUSDT
        """
        topics = []
        
        for stream in streams:
            if stream.stream_type == "orderbook":
                depth = stream.depth or 50
                topics.append(f"orderbook.{depth}.{stream.symbol}")
            elif stream.stream_type == "trade":
                topics.append(f"publicTrade.{stream.symbol}")
            elif stream.stream_type == "ticker":
                # Bybit ticker 包含: lastPrice, openInterest, fundingRate等
                topics.append(f"tickers.{stream.symbol}")
            elif stream.stream_type == "liquidation":
                topics.append(f"liquidation.{stream.symbol}")
        
        if topics:
            payload = {
                "op": "subscribe",
                "args": topics
            }
            await self.public_ws.send_json(payload)
            self._subscriptions.update(topics)
    
    async def _public_message_loop(self) -> None:
        """公共频道消息处理"""
        while True:
            try:
                msg = await self.public_ws.receive()
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = msg.json()
                    
                    # 处理pong响应
                    if data.get("op") == "pong":
                        continue
                    
                    # 处理订阅确认
                    if data.get("success") is not None:
                        continue
                    
                    # 分发数据
                    topic = data.get("topic", "")
                    await self._dispatch_by_topic(topic, data)
                    
            except Exception as e:
                logging.error(f"Bybit public WS error: {e}")
                await self._reconnect_public()
                break
    
    async def _dispatch_by_topic(self, topic: str, data: Dict) -> None:
        """根据主题分发消息"""
        if topic.startswith("orderbook"):
            if "on_orderbook" in self._callbacks:
                await self._callbacks["on_orderbook"](data)
                
        elif topic.startswith("publicTrade"):
            if "on_trade" in self._callbacks:
                await self._callbacks["on_trade"](data)
                
        elif topic.startswith("tickers"):
            # tickers包含OI，需要分别处理
            if "on_ticker" in self._callbacks:
                await self._callbacks["on_ticker"](data)
            if "on_open_interest" in self._callbacks:
                # 提取OI数据
                oi_data = self._extract_oi_from_ticker(data)
                await self._callbacks["on_open_interest"](oi_data)
    
    def _extract_oi_from_ticker(self, ticker_data: Dict) -> Dict:
        """
        从Bybit ticker中提取Open Interest
        
        Ticker数据结构:
        {
            "topic": "tickers.BTCUSDT",
            "data": {
                "symbol": "BTCUSDT",
                "lastPrice": "50000",
                "openInterest": "12345.678",  # 持仓量
                "openInterestValue": "617283900",  # 持仓价值(USDT)
                "fundingRate": "0.0001",
                ...
            }
        }
        """
        data = ticker_data.get("data", {})
        return {
            "exchange": "bybit",
            "symbol": data.get("symbol"),
            "open_interest": data.get("openInterest"),
            "open_interest_value": data.get("openInterestValue"),
            "timestamp": ticker_data.get("ts")
        }
    
    async def _ping_loop(self, ws, name: str) -> None:
        """
        Bybit心跳维护
        
        规则: 必须每20秒发送一次ping
        格式: {"op": "ping"}
        响应: {"op": "pong", "args": [...]}
        """
        while ws and not ws.closed:
            try:
                await ws.send_json({"op": "ping"})
                await asyncio.sleep(20)
            except Exception:
                break
```

### 1.3 Open Interest 特殊处理

```python
class OpenInterestManager:
    """
    Open Interest 数据管理器
    
    关键差异:
    - Bybit: 通过tickers WebSocket实时推送OI
    - Binance: 需要REST轮询 (无实时WS推送)
    
    策略:
    - Bybit: 直接使用WS推送，延迟~100ms
    - Binance: REST轮询，默认5秒间隔，高频模式可降至1秒
    """
    
    def __init__(self):
        # 最新OI数据缓存
        self._oi_cache: Dict[str, Dict] = {}  # key: "exchange:symbol"
        
        # OI历史用于计算Delta
        self._oi_history: Dict[str, deque] = {}  # 保留5分钟数据
        
        # Binance轮询任务
        self._binance_poll_tasks: Dict[str, asyncio.Task] = {}
    
    async def start_binance_polling(
        self, 
        symbols: list[str], 
        interval: float = 5.0
    ) -> None:
        """
        启动Binance OI轮询
        
        Endpoint: GET /fapi/v1/openInterest
        参数: symbol
        返回: {"symbol": "BTCUSDT", "openInterest": "12345.678", "time": 1699999999999}
        
        注意:
        - 权重: 1
        - 限制: 2400权重/分钟，可支持2400次/分钟
        - 建议: 5秒轮询足够捕捉趋势变化
        """
        for symbol in symbols:
            task = asyncio.create_task(
                self._poll_binance_oi(symbol, interval)
            )
            self._binance_poll_tasks[symbol] = task
    
    async def _poll_binance_oi(self, symbol: str, interval: float) -> None:
        """轮询单个symbol的OI"""
        url = "https://fapi.binance.com/fapi/v1/openInterest"
        
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url, params={"symbol": symbol}) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            await self._process_oi_update(
                                exchange="binance",
                                symbol=symbol,
                                oi=Decimal(data["openInterest"]),
                                timestamp=data["time"]
                            )
                        elif resp.status == 429:
                            # 速率限制，增加等待时间
                            retry_after = int(resp.headers.get("Retry-After", 60))
                            logging.warning(f"Binance OI rate limited, waiting {retry_after}s")
                            await asyncio.sleep(retry_after)
                            continue
                            
                except Exception as e:
                    logging.error(f"Binance OI poll error for {symbol}: {e}")
                
                await asyncio.sleep(interval)
    
    async def on_bybit_oi(self, oi_data: Dict) -> None:
        """处理Bybit实时OI推送"""
        await self._process_oi_update(
            exchange="bybit",
            symbol=oi_data["symbol"],
            oi=Decimal(oi_data["open_interest"]),
            timestamp=oi_data["timestamp"]
        )
    
    async def _process_oi_update(
        self, 
        exchange: str, 
        symbol: str, 
        oi: Decimal, 
        timestamp: int
    ) -> None:
        """处理OI更新，存入缓存和历史"""
        key = f"{exchange}:{symbol}"
        
        # 更新缓存
        self._oi_cache[key] = {
            "exchange": exchange,
            "symbol": symbol,
            "open_interest": oi,
            "timestamp": timestamp
        }
        
        # 添加到历史
        if key not in self._oi_history:
            self._oi_history[key] = deque(maxlen=60)  # 5分钟 @ 5秒间隔
        
        self._oi_history[key].append({
            "oi": oi,
            "timestamp": timestamp
        })
        
        # 触发回调
        if self._on_oi_update:
            await self._on_oi_update(self._oi_cache[key])
    
    def get_oi_delta(self, exchange: str, symbol: str, window_minutes: int = 5) -> Optional[Decimal]:
        """
        计算OI变化量
        
        返回: 指定时间窗口内的OI变化
        """
        key = f"{exchange}:{symbol}"
        history = self._oi_history.get(key)
        
        if not history or len(history) < 2:
            return None
        
        current_oi = history[-1]["oi"]
        
        # 查找窗口起点
        window_ms = window_minutes * 60 * 1000
        current_ts = history[-1]["timestamp"]
        
        for entry in history:
            if current_ts - entry["timestamp"] >= window_ms:
                return current_oi - entry["oi"]
        
        # 数据不足，使用最早的数据
        return current_oi - history[0]["oi"]
```

---

## 2. Data Normalization & Analytics (The "Refinery")

### 2.1 OrderBook Management with OBI Calculation

```python
from decimal import Decimal
from dataclasses import dataclass
from sortedcontainers import SortedDict
from typing import Optional, List, Tuple
from collections import deque
import time


@dataclass
class OrderBookMetrics:
    """订单簿指标"""
    mid_price: Decimal
    spread: Decimal
    spread_bps: Decimal  # 基点
    obi: Decimal  # Order Book Imbalance
    bid_depth_10: Decimal  # Top 10 买盘深度
    ask_depth_10: Decimal  # Top 10 卖盘深度
    timestamp: int


class LocalOrderBook:
    """
    本地订单簿维护与指标计算
    
    功能:
    1. 维护实时订单簿
    2. 计算OBI (Order Book Imbalance)
    3. 支持Binance diffDepth和Bybit snapshot/delta
    """
    
    def __init__(self, symbol: str, exchange: str):
        self.symbol = symbol
        self.exchange = exchange
        
        # 订单簿数据 (SortedDict for O(log n) operations)
        self.bids: SortedDict = SortedDict()  # price -> qty
        self.asks: SortedDict = SortedDict()  # price -> qty
        
        # 同步状态
        self.last_update_id: int = 0
        self.sequence: int = 0
        self._initialized: bool = False
        
        # Binance同步缓冲
        self._buffer: List[Dict] = []
        self._syncing: bool = False
        
        # 指标历史
        self._obi_history: deque = deque(maxlen=100)
    
    def apply_snapshot(self, bids: List, asks: List, update_id: int) -> None:
        """应用全量快照"""
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
            levels: 计算深度，默认Top 10
        """
        if not self.bids or not self.asks:
            return Decimal("0")
        
        # 获取Top N levels
        top_bids = list(self.bids.items())[-levels:]  # 最高N个买价
        top_asks = list(self.asks.items())[:levels]   # 最低N个卖价
        
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
    
    def get_metrics(self) -> Optional[OrderBookMetrics]:
        """获取当前订单簿指标"""
        if not self.bids or not self.asks:
            return None
        
        best_bid = self.bids.peekitem(-1)  # (price, qty)
        best_ask = self.asks.peekitem(0)
        
        mid_price = (best_bid[0] + best_ask[0]) / 2
        spread = best_ask[0] - best_bid[0]
        spread_bps = (spread / mid_price) * 10000
        
        obi = self.calculate_obi(10)
        
        # Top 10 深度
        top_bids = list(self.bids.items())[-10:]
        top_asks = list(self.asks.items())[:10]
        bid_depth = sum(p * q for p, q in top_bids)
        ask_depth = sum(p * q for p, q in top_asks)
        
        return OrderBookMetrics(
            mid_price=mid_price,
            spread=spread,
            spread_bps=spread_bps,
            obi=obi,
            bid_depth_10=bid_depth,
            ask_depth_10=ask_depth,
            timestamp=int(time.time() * 1000)
        )
    
    @property
    def best_bid(self) -> Optional[Tuple[Decimal, Decimal]]:
        if not self.bids:
            return None
        return self.bids.peekitem(-1)
    
    @property
    def best_ask(self) -> Optional[Tuple[Decimal, Decimal]]:
        if not self.asks:
            return None
        return self.asks.peekitem(0)
```

### 2.2 Trade Normalization & CVD/Volatility Calculation

```python
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import List, Optional
from collections import deque
import math


class TradeSide(Enum):
    """成交方向 - 基于Taker"""
    BUY = "buy"    # Taker买入 (lifted ask)
    SELL = "sell"  # Taker卖出 (hit bid)


@dataclass
class TradeTick:
    """
    标准化成交记录
    
    核心字段说明:
    - side: Taker的方向
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
        self._price_history: deque = deque(maxlen=60)  # 1分钟 @ 1秒采样
        self._last_price_sample: float = 0
        
        # 成交统计
        self._buy_volume: Decimal = Decimal("0")
        self._sell_volume: Decimal = Decimal("0")
        self._trade_count: int = 0
    
    def process_trade(self, trade: TradeTick) -> Dict:
        """
        处理单笔成交，更新所有指标
        
        返回当前指标快照
        """
        # 更新CVD
        if trade.side == TradeSide.BUY:
            self._cvd += trade.quantity
            self._buy_volume += trade.quantity
        else:
            self._cvd -= trade.quantity
            self._sell_volume += trade.quantity
        
        self._trade_count += 1
        
        # 记录CVD历史
        self._cvd_history.append({
            "cvd": self._cvd,
            "price": trade.price,
            "timestamp": trade.timestamp
        })
        
        # 更新价格历史 (用于波动率)
        now = time.time()
        if now - self._last_price_sample >= 1.0:  # 1秒采样
            self._price_history.append(float(trade.price))
            self._last_price_sample = now
        
        return self.get_metrics()
    
    def calculate_cvd(self) -> Decimal:
        """
        获取当前CVD
        
        CVD (Cumulative Volume Delta):
        - 正值: 累积净买入，看涨信号
        - 负值: 累积净卖出，看跌信号
        
        用途:
        - CVD上升 + 价格上升 = 强势上涨
        - CVD下降 + 价格上升 = 弱势上涨 (潜在反转)
        """
        return self._cvd
    
    def calculate_realized_volatility(self, window_seconds: int = 60) -> Optional[float]:
        """
        计算已实现波动率 (1分钟滚动窗口)
        
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
        
        # 年化 (假设1秒采样，年化到分钟级)
        # 1年 = 365 * 24 * 60 = 525600 分钟
        annualized = std_dev * math.sqrt(525600)
        
        return annualized * 100  # 百分比
    
    def get_metrics(self) -> Dict:
        """获取当前所有指标"""
        return {
            "symbol": self.symbol,
            "cvd": float(self._cvd),
            "buy_volume": float(self._buy_volume),
            "sell_volume": float(self._sell_volume),
            "net_volume": float(self._buy_volume - self._sell_volume),
            "trade_count": self._trade_count,
            "realized_volatility": self.calculate_realized_volatility(),
            "timestamp": int(time.time() * 1000)
        }
    
    def reset_session(self) -> None:
        """重置会话统计 (如每日重置)"""
        self._cvd = Decimal("0")
        self._buy_volume = Decimal("0")
        self._sell_volume = Decimal("0")
        self._trade_count = 0


class TradeNormalizer:
    """
    成交数据标准化器
    
    关键: 正确映射Taker方向
    """
    
    @staticmethod
    def from_binance(event: Dict) -> TradeTick:
        """
        标准化Binance aggTrade
        
        Binance "m" (isBuyerMaker) 字段:
        - m = true: 买方是Maker → 卖方是Taker → SELL
        - m = false: 买方是Taker → BUY
        
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
        标准化Bybit publicTrade
        
        Bybit "S" (Side) 字段:
        - "Buy": Taker买入
        - "Sell": Taker卖出
        
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
```

### 2.3 Open Interest Analysis & Market Regime Detection

```python
from enum import Enum
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Tuple
from collections import deque


class MarketRegime(Enum):
    """
    市场状态 - 基于价格与OI的关系
    
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
    """OI指标"""
    exchange: str
    symbol: str
    current_oi: Decimal
    oi_delta_5m: Decimal
    oi_delta_pct_5m: float
    price_change_5m: float
    regime: MarketRegime
    timestamp: int


class OpenInterestAnalyzer:
    """
    持仓量分析器
    
    功能:
    1. 计算OI Delta (5分钟变化)
    2. 检测市场状态 (Regime)
    3. 对齐高频成交与低频OI数据
    """
    
    # 状态判定阈值
    OI_CHANGE_THRESHOLD = 0.005  # 0.5% OI变化
    PRICE_CHANGE_THRESHOLD = 0.001  # 0.1% 价格变化
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        
        # OI历史 (5分钟窗口，假设5秒采样 = 60个点)
        self._oi_history: deque = deque(maxlen=60)
        
        # 价格历史 (用于关联分析)
        self._price_history: deque = deque(maxlen=60)
        
        # 当前OI
        self._current_oi: Optional[Decimal] = None
        self._current_price: Optional[Decimal] = None
    
    def update_oi(self, oi: Decimal, timestamp: int) -> None:
        """更新OI数据"""
        self._current_oi = oi
        self._oi_history.append({
            "oi": oi,
            "timestamp": timestamp
        })
    
    def update_price(self, price: Decimal, timestamp: int) -> None:
        """
        更新价格数据
        
        用于对齐高频成交与低频OI:
        - 成交数据: 毫秒级
        - OI数据: 秒级 (Bybit) 或 5秒级 (Binance轮询)
        
        策略: 每5秒采样一次价格，与OI频率对齐
        """
        self._current_price = price
        
        # 5秒采样
        if self._price_history:
            last_ts = self._price_history[-1]["timestamp"]
            if timestamp - last_ts < 5000:  # 5秒内
                return
        
        self._price_history.append({
            "price": price,
            "timestamp": timestamp
        })
    
    def calculate_oi_delta(self, window_minutes: int = 5) -> Tuple[Decimal, float]:
        """
        计算OI Delta
        
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
        1. 计算5分钟OI变化
        2. 计算5分钟价格变化
        3. 根据两者关系判定状态
        """
        _, oi_pct = self.calculate_oi_delta(5)
        price_pct = self.calculate_price_change(5)
        
        # 变化太小，视为中性
        if abs(oi_pct) < self.OI_CHANGE_THRESHOLD * 100 and \
           abs(price_pct) < self.PRICE_CHANGE_THRESHOLD * 100:
            return MarketRegime.NEUTRAL
        
        # 判定状态
        oi_up = oi_pct > self.OI_CHANGE_THRESHOLD * 100
        oi_down = oi_pct < -self.OI_CHANGE_THRESHOLD * 100
        price_up = price_pct > self.PRICE_CHANGE_THRESHOLD * 100
        price_down = price_pct < -self.PRICE_CHANGE_THRESHOLD * 100
        
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
        """获取当前OI指标"""
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
```

### 2.4 Persistence Strategy

```python
import redis.asyncio as redis
import asyncpg
from typing import Dict, Any, List
import json


class DataPersister:
    """
    数据持久化管理器
    
    Hot Path (Redis):
    - 实时指标发布 (Pub/Sub)
    - 最新状态缓存 (Hash)
    - 延迟: < 1ms
    
    Cold Path (TimescaleDB):
    - 历史数据存储
    - 用于回测和分析
    - 批量写入优化
    """
    
    def __init__(self, redis_url: str, postgres_url: str):
        self.redis_url = redis_url
        self.postgres_url = postgres_url
        self._redis: Optional[redis.Redis] = None
        self._pg_pool: Optional[asyncpg.Pool] = None
        
        # 批量写入缓冲
        self._trade_buffer: List[TradeTick] = []
        self._metrics_buffer: List[Dict] = []
        self._buffer_size = 100
    
    async def connect(self) -> None:
        """建立连接"""
        # Redis
        self._redis = redis.from_url(self.redis_url)
        
        # PostgreSQL/TimescaleDB
        self._pg_pool = await asyncpg.create_pool(
            self.postgres_url,
            min_size=5,
            max_size=20
        )
        
        # 创建表 (如果不存在)
        await self._create_tables()
    
    async def _create_tables(self) -> None:
        """创建TimescaleDB表"""
        async with self._pg_pool.acquire() as conn:
            # 成交记录表
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    time TIMESTAMPTZ NOT NULL,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    trade_id TEXT NOT NULL,
                    price DECIMAL NOT NULL,
                    quantity DECIMAL NOT NULL,
                    side TEXT NOT NULL,
                    notional DECIMAL NOT NULL
                );
                
                -- 转换为Hypertable (TimescaleDB)
                SELECT create_hypertable('trades', 'time', 
                    if_not_exists => TRUE,
                    chunk_time_interval => INTERVAL '1 day'
                );
                
                -- 创建索引
                CREATE INDEX IF NOT EXISTS idx_trades_symbol_time 
                ON trades (symbol, time DESC);
            """)
            
            # 指标表
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    time TIMESTAMPTZ NOT NULL,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    metric_type TEXT NOT NULL,
                    value DECIMAL NOT NULL,
                    metadata JSONB
                );
                
                SELECT create_hypertable('metrics', 'time',
                    if_not_exists => TRUE,
                    chunk_time_interval => INTERVAL '1 hour'
                );
                
                CREATE INDEX IF NOT EXISTS idx_metrics_symbol_type_time
                ON metrics (symbol, metric_type, time DESC);
            """)
    
    # ============ Hot Path: Redis ============
    
    async def publish_realtime(self, channel: str, data: Dict) -> None:
        """
        发布实时数据到Redis Pub/Sub
        
        频道设计:
        - unitrade:ticker:{symbol}  - 行情
        - unitrade:obi:{symbol}     - 订单簿不平衡
        - unitrade:cvd:{symbol}     - 累积成交量差
        - unitrade:oi:{symbol}      - 持仓量
        - unitrade:regime:{symbol}  - 市场状态
        """
        await self._redis.publish(channel, json.dumps(data, default=str))
    
    async def cache_latest(self, key: str, data: Dict, expire: int = 60) -> None:
        """
        缓存最新状态到Redis Hash
        
        Key设计:
        - unitrade:latest:{symbol} - 最新综合指标
        """
        await self._redis.hset(key, mapping={
            k: json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v)
            for k, v in data.items()
        })
        await self._redis.expire(key, expire)
    
    async def publish_orderbook_metrics(self, metrics: OrderBookMetrics) -> None:
        """发布订单簿指标"""
        data = {
            "mid_price": str(metrics.mid_price),
            "spread": str(metrics.spread),
            "spread_bps": str(metrics.spread_bps),
            "obi": str(metrics.obi),
            "bid_depth_10": str(metrics.bid_depth_10),
            "ask_depth_10": str(metrics.ask_depth_10),
            "timestamp": metrics.timestamp
        }
        
        # Pub/Sub
        await self.publish_realtime(f"unitrade:obi:{metrics.symbol}", data)
        
        # Cache
        await self.cache_latest(f"unitrade:latest:{metrics.symbol}", data)
    
    async def publish_trade_metrics(self, symbol: str, metrics: Dict) -> None:
        """发布成交指标 (CVD, Volatility)"""
        await self.publish_realtime(f"unitrade:cvd:{symbol}", metrics)
        
        # 更新缓存
        await self._redis.hset(
            f"unitrade:latest:{symbol}",
            mapping={
                "cvd": str(metrics["cvd"]),
                "realized_volatility": str(metrics.get("realized_volatility", 0)),
                "buy_volume": str(metrics["buy_volume"]),
                "sell_volume": str(metrics["sell_volume"])
            }
        )
    
    async def publish_oi_metrics(self, metrics: OIMetrics) -> None:
        """发布OI指标"""
        data = {
            "current_oi": str(metrics.current_oi),
            "oi_delta_5m": str(metrics.oi_delta_5m),
            "oi_delta_pct_5m": metrics.oi_delta_pct_5m,
            "price_change_5m": metrics.price_change_5m,
            "regime": metrics.regime.value,
            "timestamp": metrics.timestamp
        }
        
        await self.publish_realtime(f"unitrade:oi:{metrics.symbol}", data)
        await self.publish_realtime(f"unitrade:regime:{metrics.symbol}", {
            "regime": metrics.regime.value,
            "timestamp": metrics.timestamp
        })
    
    # ============ Cold Path: TimescaleDB ============
    
    async def store_trade(self, trade: TradeTick) -> None:
        """
        存储成交记录
        
        使用缓冲批量写入优化性能
        """
        self._trade_buffer.append(trade)
        
        if len(self._trade_buffer) >= self._buffer_size:
            await self._flush_trades()
    
    async def _flush_trades(self) -> None:
        """批量写入成交记录"""
        if not self._trade_buffer:
            return
        
        trades = self._trade_buffer.copy()
        self._trade_buffer.clear()
        
        async with self._pg_pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO trades (time, exchange, symbol, trade_id, price, quantity, side, notional)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """, [
                (
                    datetime.fromtimestamp(t.timestamp / 1000),
                    t.exchange,
                    t.symbol,
                    t.trade_id,
                    t.price,
                    t.quantity,
                    t.side.value,
                    t.notional
                )
                for t in trades
            ])
    
    async def store_metric(
        self, 
        exchange: str,
        symbol: str, 
        metric_type: str, 
        value: Decimal,
        metadata: Optional[Dict] = None
    ) -> None:
        """存储指标数据"""
        self._metrics_buffer.append({
            "time": datetime.utcnow(),
            "exchange": exchange,
            "symbol": symbol,
            "metric_type": metric_type,
            "value": value,
            "metadata": metadata or {}
        })
        
        if len(self._metrics_buffer) >= self._buffer_size:
            await self._flush_metrics()
    
    async def _flush_metrics(self) -> None:
        """批量写入指标"""
        if not self._metrics_buffer:
            return
        
        metrics = self._metrics_buffer.copy()
        self._metrics_buffer.clear()
        
        async with self._pg_pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO metrics (time, exchange, symbol, metric_type, value, metadata)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, [
                (
                    m["time"],
                    m["exchange"],
                    m["symbol"],
                    m["metric_type"],
                    m["value"],
                    json.dumps(m["metadata"])
                )
                for m in metrics
            ])
    
    async def close(self) -> None:
        """关闭连接前刷新缓冲"""
        await self._flush_trades()
        await self._flush_metrics()
        
        if self._redis:
            await self._redis.close()
        if self._pg_pool:
            await self._pg_pool.close()
```

---

## 3. Order Management System (The "Execution")

### 3.1 Order Router

```python
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    LIMIT = "limit"
    MARKET = "market"
    STOP_MARKET = "stop_market"
    STOP_LIMIT = "stop_limit"


class TimeInForce(Enum):
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    POST_ONLY = "post_only"


@dataclass
class OrderRequest:
    """统一订单请求"""
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    time_in_force: TimeInForce = TimeInForce.GTC
    reduce_only: bool = False
    client_order_id: Optional[str] = None
    algo_id: Optional[str] = None


class OrderRouter:
    """
    订单路由器
    
    职责:
    1. 将统一OrderRequest转换为交易所特定格式
    2. 验证订单参数
    3. 管理ClientOrderId
    """
    
    def __init__(self, symbol_manager):
        self.symbol_manager = symbol_manager
        self.id_manager = OrderIdManager()
    
    def route(self, exchange: str, request: OrderRequest) -> Dict[str, Any]:
        """路由订单到指定交易所"""
        # 生成ClientOrderId
        if not request.client_order_id:
            request.client_order_id = self.id_manager.generate(
                algo_id=request.algo_id or "MANUAL",
                exchange=exchange,
                symbol=request.symbol
            )
        
        if exchange == "binance":
            return self._to_binance(request)
        elif exchange == "bybit":
            return self._to_bybit(request)
        else:
            raise ValueError(f"Unknown exchange: {exchange}")
    
    def _to_binance(self, req: OrderRequest) -> Dict[str, Any]:
        """
        转换为Binance USDT-M Futures格式
        
        Endpoint: POST /fapi/v1/order
        """
        payload = {
            "symbol": req.symbol,
            "side": req.side.name.upper(),
            "type": self._map_type_binance(req.order_type),
            "quantity": str(req.quantity),
            "newClientOrderId": req.client_order_id
        }
        
        if req.price:
            payload["price"] = str(req.price)
        
        if req.stop_price:
            payload["stopPrice"] = str(req.stop_price)
        
        payload["timeInForce"] = self._map_tif_binance(req.time_in_force)
        
        if req.reduce_only:
            payload["reduceOnly"] = "true"
        
        return payload
    
    def _to_bybit(self, req: OrderRequest) -> Dict[str, Any]:
        """
        转换为Bybit V5格式
        
        Endpoint: POST /v5/order/create
        """
        payload = {
            "category": "linear",
            "symbol": req.symbol,
            "side": "Buy" if req.side == OrderSide.BUY else "Sell",
            "orderType": "Limit" if req.order_type == OrderType.LIMIT else "Market",
            "qty": str(req.quantity),
            "orderLinkId": req.client_order_id
        }
        
        if req.price:
            payload["price"] = str(req.price)
        
        if req.stop_price:
            payload["triggerPrice"] = str(req.stop_price)
        
        payload["timeInForce"] = self._map_tif_bybit(req.time_in_force)
        
        if req.reduce_only:
            payload["reduceOnly"] = True
        
        return payload
    
    @staticmethod
    def _map_type_binance(ot: OrderType) -> str:
        return {
            OrderType.LIMIT: "LIMIT",
            OrderType.MARKET: "MARKET",
            OrderType.STOP_MARKET: "STOP_MARKET",
            OrderType.STOP_LIMIT: "STOP",
        }[ot]
    
    @staticmethod
    def _map_tif_binance(tif: TimeInForce) -> str:
        return {
            TimeInForce.GTC: "GTC",
            TimeInForce.IOC: "IOC",
            TimeInForce.FOK: "FOK",
            TimeInForce.POST_ONLY: "GTX",
        }[tif]
    
    @staticmethod
    def _map_tif_bybit(tif: TimeInForce) -> str:
        return {
            TimeInForce.GTC: "GTC",
            TimeInForce.IOC: "IOC",
            TimeInForce.FOK: "FOK",
            TimeInForce.POST_ONLY: "PostOnly",
        }[tif]
```

### 3.2 Client Order ID Management

```python
import uuid
import time
from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class OrderIdEntry:
    """订单ID映射条目"""
    client_order_id: str
    exchange_order_id: Optional[str] = None
    exchange: str = ""
    symbol: str = ""
    algo_id: str = ""
    created_at: int = 0


class OrderIdManager:
    """
    订单ID管理器
    
    ClientOrderId格式: {PREFIX}-{ALGO_ID}-{UUID8}
    
    示例: UT-MM01-a1b2c3d4
    
    组成:
    - PREFIX: "UT" (UniTrade标识)
    - ALGO_ID: 策略ID (最多6字符)
    - UUID8: UUID前8位 (防碰撞)
    
    限制:
    - Binance: 最长36字符
    - Bybit: 最长36字符
    """
    
    PREFIX = "UT"
    MAX_LENGTH = 36
    
    def __init__(self):
        self._by_client_id: Dict[str, OrderIdEntry] = {}
        self._by_exchange_id: Dict[str, str] = {}
    
    def generate(self, algo_id: str, exchange: str, symbol: str) -> str:
        """生成新的ClientOrderId"""
        uuid8 = uuid.uuid4().hex[:8]
        
        # 截断algo_id确保总长度不超限
        max_algo_len = self.MAX_LENGTH - len(self.PREFIX) - 10  # PREFIX + UUID8 + 2个分隔符
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
        """关联交易所订单ID"""
        if client_order_id in self._by_client_id:
            self._by_client_id[client_order_id].exchange_order_id = exchange_order_id
            self._by_exchange_id[exchange_order_id] = client_order_id
    
    def get_by_client_id(self, client_order_id: str) -> Optional[OrderIdEntry]:
        return self._by_client_id.get(client_order_id)
    
    def get_by_exchange_id(self, exchange_order_id: str) -> Optional[OrderIdEntry]:
        client_id = self._by_exchange_id.get(exchange_order_id)
        return self._by_client_id.get(client_id) if client_id else None
```

### 3.3 Dual-Strategy Rate Limiter

```python
import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional, Dict


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
    - 2400权重/分钟 (USDT-M Futures)
    - 不同端点权重不同
    - 超限返回HTTP 429 + Retry-After
    
    端点权重示例:
    - /fapi/v1/order: 1
    - /fapi/v1/batchOrders: 5
    - /fapi/v1/depth (limit=500): 10
    - /fapi/v1/depth (limit=1000): 20
    """
    
    ENDPOINT_WEIGHTS = {
        "/fapi/v1/order": 1,
        "/fapi/v1/batchOrders": 5,
        "/fapi/v1/allOpenOrders": 1,
        "/fapi/v1/depth": 10,  # 默认，实际根据limit变化
        "/fapi/v1/openInterest": 1,
        "/fapi/v1/account": 5,
        "/fapi/v1/positionRisk": 5,
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
            
            # 预留10%缓冲
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
        """处理429响应"""
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
        
        # depth端点根据limit调整
        if endpoint == "/fapi/v1/depth" and params:
            limit = params.get("limit", 500)
            if limit <= 100:
                return 5
            elif limit <= 500:
                return 10
            else:
                return 20
        
        return base_weight


class BybitRateLimiter:
    """
    Bybit 计数制速率限制器
    
    规则:
    - 订单端点: 10次/秒/symbol, 100次/秒/总
    - 公共端点: 120次/秒
    """
    
    def __init__(self):
        self._global_requests: deque = deque()
        self._symbol_requests: Dict[str, deque] = {}
        self._blocked_until: float = 0
        self._lock = asyncio.Lock()
        
        self.global_limit = 100
        self.symbol_limit = 10
        self.window = 1.0
    
    async def acquire(self, endpoint: str, symbol: Optional[str] = None) -> None:
        """获取请求许可"""
        async with self._lock:
            now = time.time()
            
            if now < self._blocked_until:
                await asyncio.sleep(self._blocked_until - now)
            
            # 全局限制
            await self._check_limit(self._global_requests, self.global_limit, now)
            
            # 按symbol限制 (仅订单端点)
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
        if exchange == "binance":
            await self.binance.acquire(endpoint, kwargs.get("params"))
        else:
            await self.bybit.acquire(endpoint, kwargs.get("symbol"))
```

---

## 4. Implementation Specs (Class Skeletons)

### 4.1 MarketDataNormalizer (完整实现)

```python
from typing import Dict, List, Optional, Callable, Awaitable
from decimal import Decimal
import asyncio
import logging

logger = logging.getLogger(__name__)


class MarketDataNormalizer:
    """
    市场数据标准化器
    
    职责:
    1. 接收原始交易所数据
    2. 标准化为统一格式
    3. 计算实时指标 (OBI, CVD, Volatility, OI Delta)
    4. 分发到持久化和策略层
    """
    
    def __init__(self, persister: DataPersister):
        self.persister = persister
        
        # 订单簿管理
        self._orderbooks: Dict[str, LocalOrderBook] = {}
        
        # 成交分析
        self._trade_analytics: Dict[str, TradeAnalytics] = {}
        
        # OI分析
        self._oi_analyzers: Dict[str, OpenInterestAnalyzer] = {}
        
        # 回调
        self._on_metrics: Optional[Callable] = None
    
    def _get_key(self, exchange: str, symbol: str) -> str:
        return f"{exchange}:{symbol}"
    
    # ============ Binance Handlers ============
    
    async def handle_binance_depth(self, event: Dict) -> None:
        """
        处理Binance深度更新
        
        事件格式:
        {
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "U": 157,  # First update ID
            "u": 160,  # Final update ID
            "b": [["10000", "1"]],
            "a": [["10001", "0.5"]]
        }
        """
        symbol = event["s"]
        key = self._get_key("binance", symbol)
        
        if key not in self._orderbooks:
            self._orderbooks[key] = LocalOrderBook(symbol, "binance")
        
        ob = self._orderbooks[key]
        
        if not ob._initialized:
            # 需要先获取快照同步
            return
        
        # 应用增量
        ob.apply_delta(event["b"], event["a"], event["u"])
        
        # 计算指标
        metrics = ob.get_metrics()
        if metrics:
            await self.persister.publish_orderbook_metrics(metrics)
            
            # 存储OBI指标
            await self.persister.store_metric(
                exchange="binance",
                symbol=symbol,
                metric_type="obi",
                value=metrics.obi,
                metadata={"spread_bps": float(metrics.spread_bps)}
            )
    
    async def handle_binance_trade(self, event: Dict) -> None:
        """
        处理Binance aggTrade
        
        事件格式:
        {
            "e": "aggTrade",
            "s": "BTCUSDT",
            "a": 12345,  # Aggregate trade ID
            "p": "10000.00",
            "q": "1.000",
            "T": 1234567890123,
            "m": true  # isBuyerMaker
        }
        """
        # 标准化
        trade = TradeNormalizer.from_binance(event)
        
        symbol = trade.symbol
        key = self._get_key("binance", symbol)
        
        # 获取分析器
        if key not in self._trade_analytics:
            self._trade_analytics[key] = TradeAnalytics(symbol)
        
        analytics = self._trade_analytics[key]
        
        # 处理成交，获取指标
        metrics = analytics.process_trade(trade)
        
        # 发布CVD和波动率
        await self.persister.publish_trade_metrics(symbol, metrics)
        
        # 存储成交
        await self.persister.store_trade(trade)
        
        # 存储CVD指标
        await self.persister.store_metric(
            exchange="binance",
            symbol=symbol,
            metric_type="cvd",
            value=Decimal(str(metrics["cvd"])),
            metadata={"volatility": metrics.get("realized_volatility")}
        )
        
        # 更新OI分析器的价格
        if key in self._oi_analyzers:
            self._oi_analyzers[key].update_price(trade.price, trade.timestamp)
    
    async def handle_binance_oi(self, data: Dict) -> None:
        """
        处理Binance OI数据 (来自REST轮询)
        
        数据格式:
        {
            "symbol": "BTCUSDT",
            "openInterest": "12345.678",
            "time": 1699999999999
        }
        """
        symbol = data["symbol"]
        key = self._get_key("binance", symbol)
        
        if key not in self._oi_analyzers:
            self._oi_analyzers[key] = OpenInterestAnalyzer(symbol)
        
        analyzer = self._oi_analyzers[key]
        oi = Decimal(data["openInterest"])
        
        # 更新OI
        analyzer.update_oi(oi, data["time"])
        
        # 获取指标
        metrics = analyzer.get_metrics("binance")
        if metrics:
            await self.persister.publish_oi_metrics(metrics)
            
            # 存储
            await self.persister.store_metric(
                exchange="binance",
                symbol=symbol,
                metric_type="oi",
                value=oi,
                metadata={
                    "delta_5m": float(metrics.oi_delta_5m),
                    "regime": metrics.regime.value
                }
            )
    
    # ============ Bybit Handlers ============
    
    async def handle_bybit_orderbook(self, msg: Dict) -> None:
        """
        处理Bybit订单簿
        
        消息格式:
        {
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot" | "delta",
            "data": {
                "s": "BTCUSDT",
                "b": [...],
                "a": [...],
                "u": 12345
            }
        }
        """
        data = msg["data"]
        symbol = data["s"]
        key = self._get_key("bybit", symbol)
        
        if key not in self._orderbooks:
            self._orderbooks[key] = LocalOrderBook(symbol, "bybit")
        
        ob = self._orderbooks[key]
        
        if msg["type"] == "snapshot":
            ob.apply_snapshot(data["b"], data["a"], data["u"])
        else:
            ob.apply_delta(data["b"], data["a"], data["u"])
        
        # 计算并发布指标
        metrics = ob.get_metrics()
        if metrics:
            await self.persister.publish_orderbook_metrics(metrics)
    
    async def handle_bybit_trade(self, msg: Dict) -> None:
        """
        处理Bybit成交
        
        消息格式:
        {
            "topic": "publicTrade.BTCUSDT",
            "data": [
                {
                    "s": "BTCUSDT",
                    "S": "Buy",
                    "v": "1.000",
                    "p": "10000.00",
                    "i": "12345",
                    "T": 1234567890123
                }
            ]
        }
        """
        for trade_data in msg["data"]:
            trade = TradeNormalizer.from_bybit(trade_data)
            
            key = self._get_key("bybit", trade.symbol)
            
            if key not in self._trade_analytics:
                self._trade_analytics[key] = TradeAnalytics(trade.symbol)
            
            metrics = self._trade_analytics[key].process_trade(trade)
            
            await self.persister.publish_trade_metrics(trade.symbol, metrics)
            await self.persister.store_trade(trade)
            
            # 更新价格用于OI分析
            if key in self._oi_analyzers:
                self._oi_analyzers[key].update_price(trade.price, trade.timestamp)
    
    async def handle_bybit_ticker(self, msg: Dict) -> None:
        """
        处理Bybit Ticker (包含OI)
        
        消息格式:
        {
            "topic": "tickers.BTCUSDT",
            "data": {
                "symbol": "BTCUSDT",
                "openInterest": "12345.678",
                ...
            }
        }
        """
        data = msg["data"]
        symbol = data["symbol"]
        key = self._get_key("bybit", symbol)
        
        if "openInterest" not in data:
            return
        
        if key not in self._oi_analyzers:
            self._oi_analyzers[key] = OpenInterestAnalyzer(symbol)
        
        analyzer = self._oi_analyzers[key]
        oi = Decimal(data["openInterest"])
        timestamp = msg.get("ts", int(time.time() * 1000))
        
        analyzer.update_oi(oi, timestamp)
        
        metrics = analyzer.get_metrics("bybit")
        if metrics:
            await self.persister.publish_oi_metrics(metrics)
    
    # ============ Aggregated Metrics ============
    
    def get_symbol_snapshot(self, exchange: str, symbol: str) -> Optional[Dict]:
        """
        获取symbol完整指标快照
        
        用于策略实时决策
        """
        key = self._get_key(exchange, symbol)
        
        snapshot = {"exchange": exchange, "symbol": symbol}
        
        # 订单簿指标
        if key in self._orderbooks:
            ob_metrics = self._orderbooks[key].get_metrics()
            if ob_metrics:
                snapshot["mid_price"] = float(ob_metrics.mid_price)
                snapshot["spread_bps"] = float(ob_metrics.spread_bps)
                snapshot["obi"] = float(ob_metrics.obi)
        
        # 成交指标
        if key in self._trade_analytics:
            trade_metrics = self._trade_analytics[key].get_metrics()
            snapshot["cvd"] = trade_metrics["cvd"]
            snapshot["realized_volatility"] = trade_metrics["realized_volatility"]
            snapshot["buy_volume"] = float(trade_metrics["buy_volume"])
            snapshot["sell_volume"] = float(trade_metrics["sell_volume"])
        
        # OI指标
        if key in self._oi_analyzers:
            oi_metrics = self._oi_analyzers[key].get_metrics(exchange)
            if oi_metrics:
                snapshot["open_interest"] = float(oi_metrics.current_oi)
                snapshot["oi_delta_5m"] = float(oi_metrics.oi_delta_5m)
                snapshot["market_regime"] = oi_metrics.regime.value
        
        return snapshot if len(snapshot) > 2 else None
```

---

# PART 2: IMPLEMENTATION ROADMAP

## 6-Week Agile Plan (Gantt Chart)

```mermaid
gantt
    title UniTrade Analytics Gateway - 6 Week Development Plan
    dateFormat  YYYY-MM-DD
    
    section Sprint 0: Infrastructure
    Project Setup & Docker           :s0_1, 2024-01-01, 2d
    Redis & TimescaleDB Containers   :s0_2, 2024-01-01, 2d
    Mock Exchange Server             :s0_3, 2024-01-03, 3d
    CI/CD Pipeline                   :s0_4, 2024-01-05, 1d
    
    section Sprint 1: Data Connection
    Binance Connection Pool          :s1_1, 2024-01-08, 3d
    Bybit V5 Gateway                 :s1_2, 2024-01-08, 2d
    Heartbeat & Reconnection         :s1_3, 2024-01-11, 2d
    OI Poller (Binance)              :s1_4, 2024-01-12, 1d
    
    section Sprint 2: Analytics Engine
    OrderBook Reconstruction         :s2_1, 2024-01-15, 3d
    Trade Normalization & CVD        :s2_2, 2024-01-15, 2d
    OBI Calculator                   :s2_3, 2024-01-18, 1d
    Volatility Calculator            :s2_4, 2024-01-18, 1d
    OI Delta & Regime Detection      :s2_5, 2024-01-19, 2d
    CVD/OI Validation Test           :s2_6, 2024-01-20, 1d
    
    section Sprint 3: Persistence
    Redis Hot Path                   :s3_1, 2024-01-22, 2d
    TimescaleDB Cold Path            :s3_2, 2024-01-22, 3d
    Batch Write Optimization         :s3_3, 2024-01-25, 2d
    
    section Sprint 4: Execution Layer
    Private Channel Auth             :s4_1, 2024-01-29, 2d
    Order Router                     :s4_2, 2024-01-29, 2d
    Rate Limiter (Dual)              :s4_3, 2024-01-31, 2d
    Position Reconciler              :s4_4, 2024-02-02, 2d
    
    section Sprint 5: Monitoring
    Prometheus Metrics               :s5_1, 2024-02-05, 2d
    Grafana Dashboard                :s5_2, 2024-02-05, 3d
    CVD/OI Overlay Charts            :s5_3, 2024-02-08, 2d
    
    section Sprint 6: Production
    Docker Compose Stack             :s6_1, 2024-02-12, 2d
    E2E Integration Tests            :s6_2, 2024-02-12, 3d
    Documentation                    :s6_3, 2024-02-15, 2d
    Final Review                     :s6_4, 2024-02-16, 1d
```

---

## Sprint 0: Infrastructure & Storage (Week 0)

### Goals
- 建立项目基础结构
- 部署Redis和TimescaleDB
- 构建Mock Exchange用于测试

### Project Structure

```
unitrade-analytics/
├── src/
│   └── unitrade/
│       ├── __init__.py
│       ├── core/
│       │   ├── config.py
│       │   └── exceptions.py
│       ├── connection/
│       │   ├── base.py
│       │   ├── binance.py
│       │   ├── bybit.py
│       │   └── pool.py
│       ├── analytics/
│       │   ├── orderbook.py
│       │   ├── trade.py
│       │   ├── volatility.py
│       │   └── open_interest.py
│       ├── persistence/
│       │   ├── redis_client.py
│       │   └── timescale_client.py
│       ├── order/
│       │   ├── router.py
│       │   ├── id_manager.py
│       │   └── rate_limiter.py
│       └── gateway.py
├── tests/
│   ├── mocks/
│   │   └── exchange_server.py
│   ├── unit/
│   └── integration/
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── grafana/
│   └── dashboards/
│       └── trading_analytics.json
├── config/
│   └── default.yaml
└── scripts/
    └── validate_indicators.py
```

### Docker Compose Setup

```yaml
# docker/docker-compose.yml
version: '3.8'

services:
  # 主应用
  unitrade:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    container_name: unitrade-gateway
    environment:
      - REDIS_URL=redis://redis:6379
      - POSTGRES_URL=postgresql://unitrade:password@timescaledb:5432/unitrade
    depends_on:
      - redis
      - timescaledb
    ports:
      - "8000:8000"
    networks:
      - unitrade-net

  # Redis (Hot Path)
  redis:
    image: redis:7-alpine
    container_name: unitrade-redis
    command: redis-server --appendonly yes
    volumes:
      - redis-data:/data
    ports:
      - "6379:6379"
    networks:
      - unitrade-net

  # TimescaleDB (Cold Path)
  timescaledb:
    image: timescale/timescaledb:latest-pg15
    container_name: unitrade-timescaledb
    environment:
      - POSTGRES_USER=unitrade
      - POSTGRES_PASSWORD=password
      - POSTGRES_DB=unitrade
    volumes:
      - timescale-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    networks:
      - unitrade-net

  # Grafana (Monitoring)
  grafana:
    image: grafana/grafana:latest
    container_name: unitrade-grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_INSTALL_PLUGINS=redis-datasource
    volumes:
      - ../grafana/dashboards:/etc/grafana/provisioning/dashboards
      - grafana-data:/var/lib/grafana
    ports:
      - "3000:3000"
    depends_on:
      - redis
      - timescaledb
    networks:
      - unitrade-net

  # Prometheus (Metrics)
  prometheus:
    image: prom/prometheus:latest
    container_name: unitrade-prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"
    networks:
      - unitrade-net

networks:
  unitrade-net:
    driver: bridge

volumes:
  redis-data:
  timescale-data:
  grafana-data:
```

### Definition of Done (Sprint 0)
- [ ] Docker Compose启动Redis, TimescaleDB, Grafana
- [ ] Mock Exchange可生成模拟数据
- [ ] 项目结构创建完成
- [ ] CI管道配置完成

---

## Phase 1: Data Pipeline (Sprints 1-2)

### Sprint 1: Connection Layer

**目标:** 建立稳定的WebSocket连接

**任务:**
1. Binance连接池实现
2. Bybit V5网关实现
3. 心跳和重连逻辑
4. Binance OI轮询器

### Sprint 2: Analytics Engine

**目标:** 实现所有Alpha指标计算

**任务:**
1. 订单簿重建与OBI计算
2. 成交标准化与CVD跟踪
3. 实现波动率计算
4. OI Delta与市场状态检测

### Indicator Validation Script

```python
# scripts/validate_indicators.py
"""
指标验证脚本

对比本地计算的CVD/OI与交易所图表数据
确保计算逻辑正确
"""

import asyncio
import aiohttp
from decimal import Decimal
from datetime import datetime, timedelta


class IndicatorValidator:
    """指标验证器"""
    
    def __init__(self):
        self.local_cvd = Decimal("0")
        self.local_oi_history = []
        self.trade_count = 0
    
    async def validate_cvd(self, duration_minutes: int = 5):
        """
        验证CVD计算
        
        方法:
        1. 收集N分钟成交数据
        2. 计算本地CVD
        3. 与交易所提供的买卖成交额对比
        """
        print(f"开始CVD验证 ({duration_minutes}分钟)...")
        
        # 连接Binance WebSocket
        url = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        buy_volume = Decimal("0")
        sell_volume = Decimal("0")
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url) as ws:
                while datetime.now() < end_time:
                    msg = await asyncio.wait_for(ws.receive(), timeout=5)
                    
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        
                        qty = Decimal(data["q"])
                        is_buyer_maker = data["m"]
                        
                        # Binance m=true表示买方是maker，所以卖方是taker
                        if is_buyer_maker:
                            sell_volume += qty
                        else:
                            buy_volume += qty
                        
                        self.trade_count += 1
                    
                    # 每30秒打印进度
                    if self.trade_count % 1000 == 0:
                        elapsed = (datetime.now() - start_time).seconds
                        print(f"  已处理 {self.trade_count} 笔成交 ({elapsed}秒)")
        
        # 计算CVD
        self.local_cvd = buy_volume - sell_volume
        
        print(f"\n=== CVD验证结果 ===")
        print(f"持续时间: {duration_minutes} 分钟")
        print(f"成交笔数: {self.trade_count}")
        print(f"买入量: {buy_volume:.4f} BTC")
        print(f"卖出量: {sell_volume:.4f} BTC")
        print(f"CVD: {self.local_cvd:.4f} BTC")
        print(f"CVD方向: {'净买入' if self.local_cvd > 0 else '净卖出'}")
        
        return {
            "cvd": float(self.local_cvd),
            "buy_volume": float(buy_volume),
            "sell_volume": float(sell_volume),
            "trade_count": self.trade_count
        }
    
    async def validate_oi(self, duration_minutes: int = 10, poll_interval: int = 5):
        """
        验证OI计算
        
        方法:
        1. 轮询OI数据
        2. 计算5分钟Delta
        3. 结合价格判断市场状态
        """
        print(f"\n开始OI验证 ({duration_minutes}分钟, {poll_interval}秒间隔)...")
        
        url = "https://fapi.binance.com/fapi/v1/openInterest"
        price_url = "https://fapi.binance.com/fapi/v1/ticker/price"
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        oi_data = []
        price_data = []
        
        async with aiohttp.ClientSession() as session:
            while datetime.now() < end_time:
                # 获取OI
                async with session.get(url, params={"symbol": "BTCUSDT"}) as resp:
                    data = await resp.json()
                    oi_data.append({
                        "oi": Decimal(data["openInterest"]),
                        "time": data["time"]
                    })
                
                # 获取价格
                async with session.get(price_url, params={"symbol": "BTCUSDT"}) as resp:
                    data = await resp.json()
                    price_data.append({
                        "price": Decimal(data["price"]),
                        "time": int(datetime.now().timestamp() * 1000)
                    })
                
                print(f"  OI: {oi_data[-1]['oi']:.2f}, Price: {price_data[-1]['price']:.2f}")
                
                await asyncio.sleep(poll_interval)
        
        # 分析
        if len(oi_data) >= 2:
            oi_start = oi_data[0]["oi"]
            oi_end = oi_data[-1]["oi"]
            oi_delta = oi_end - oi_start
            oi_pct = float(oi_delta / oi_start * 100)
            
            price_start = price_data[0]["price"]
            price_end = price_data[-1]["price"]
            price_pct = float((price_end - price_start) / price_start * 100)
            
            # 判断市场状态
            if price_pct > 0.1 and oi_pct > 0.5:
                regime = "LONG_BUILD (多头建仓)"
            elif price_pct < -0.1 and oi_pct > 0.5:
                regime = "SHORT_BUILD (空头建仓)"
            elif price_pct < -0.1 and oi_pct < -0.5:
                regime = "LONG_UNWIND (多头平仓)"
            elif price_pct > 0.1 and oi_pct < -0.5:
                regime = "SHORT_COVER (空头回补)"
            else:
                regime = "NEUTRAL (中性)"
            
            print(f"\n=== OI验证结果 ===")
            print(f"持续时间: {duration_minutes} 分钟")
            print(f"数据点数: {len(oi_data)}")
            print(f"OI起始: {oi_start:.2f}")
            print(f"OI结束: {oi_end:.2f}")
            print(f"OI变化: {oi_delta:.2f} ({oi_pct:.2f}%)")
            print(f"价格变化: {price_pct:.2f}%")
            print(f"市场状态: {regime}")
            
            return {
                "oi_delta": float(oi_delta),
                "oi_pct": oi_pct,
                "price_pct": price_pct,
                "regime": regime
            }


async def main():
    validator = IndicatorValidator()
    
    # 验证CVD (5分钟)
    cvd_result = await validator.validate_cvd(5)
    
    # 验证OI (10分钟)
    oi_result = await validator.validate_oi(10, 5)
    
    print("\n" + "="*50)
    print("验证完成!")
    print("请与交易所图表对比以上数据")


if __name__ == "__main__":
    asyncio.run(main())
```

### Definition of Done (Phase 1)
- [ ] WebSocket连接稳定运行24小时
- [ ] 订单簿重建准确 (校验和验证)
- [ ] CVD计算与交易所图表一致
- [ ] OI Delta正确反映持仓变化
- [ ] 市场状态检测逻辑验证通过

---

## Phase 2: Execution & Safety (Sprints 3-4)

### Sprint 3: Persistence Layer

**任务:**
1. Redis Pub/Sub实时发布
2. TimescaleDB批量存储
3. 写入性能优化

### Sprint 4: Execution Layer

**任务:**
1. 私有频道认证
2. 订单路由器
3. 双策略速率限制器
4. 仓位调节器

### Position Reconciler

```python
class PositionReconciler:
    """
    仓位调节器
    
    解决问题:
    - WebSocket可能丢失消息导致仓位不一致
    - 定期通过REST查询修正
    """
    
    RECONCILE_INTERVAL = 30  # 30秒
    
    def __init__(self, gateway):
        self.gateway = gateway
        self._ws_positions: Dict[str, Decimal] = {}
        self._running = False
    
    async def start(self):
        self._running = True
        asyncio.create_task(self._reconcile_loop())
    
    async def _reconcile_loop(self):
        while self._running:
            await asyncio.sleep(self.RECONCILE_INTERVAL)
            
            for exchange in ["binance", "bybit"]:
                try:
                    rest_positions = await self.gateway.fetch_positions(exchange)
                    
                    for pos in rest_positions:
                        key = f"{exchange}:{pos['symbol']}"
                        ws_size = self._ws_positions.get(key, Decimal("0"))
                        rest_size = Decimal(str(pos["size"]))
                        
                        if abs(ws_size - rest_size) > Decimal("0.0001"):
                            logging.warning(
                                f"Position mismatch {key}: WS={ws_size}, REST={rest_size}"
                            )
                            self._ws_positions[key] = rest_size
                            
                except Exception as e:
                    logging.error(f"Reconciliation error for {exchange}: {e}")
    
    def update_from_ws(self, exchange: str, symbol: str, size: Decimal):
        """WebSocket更新仓位"""
        key = f"{exchange}:{symbol}"
        self._ws_positions[key] = size
```

### Definition of Done (Phase 2)
- [ ] Redis实时发布延迟 < 5ms
- [ ] TimescaleDB批量写入性能 > 10000 rows/s
- [ ] 订单提交延迟 < 50ms
- [ ] 仓位调节器检测到差异时自动修正

---

## Phase 3: Production & Monitoring (Sprints 5-6)

### Sprint 5: Monitoring

**任务:**
1. Prometheus指标导出
2. Grafana仪表板

### Grafana Dashboard Configuration

```json
{
  "title": "UniTrade Analytics",
  "panels": [
    {
      "title": "Price & CVD Overlay",
      "type": "timeseries",
      "gridPos": {"x": 0, "y": 0, "w": 12, "h": 8},
      "targets": [
        {
          "datasource": "TimescaleDB",
          "rawSql": "SELECT time, value as price FROM metrics WHERE metric_type = 'price' AND symbol = '$symbol' ORDER BY time"
        },
        {
          "datasource": "TimescaleDB",
          "rawSql": "SELECT time, value as cvd FROM metrics WHERE metric_type = 'cvd' AND symbol = '$symbol' ORDER BY time"
        }
      ],
      "fieldConfig": {
        "overrides": [
          {"matcher": {"id": "byName", "options": "cvd"}, "properties": [{"id": "custom.axisPlacement", "value": "right"}]}
        ]
      }
    },
    {
      "title": "Open Interest & Price",
      "type": "timeseries",
      "gridPos": {"x": 12, "y": 0, "w": 12, "h": 8},
      "targets": [
        {
          "datasource": "TimescaleDB",
          "rawSql": "SELECT time, value as oi FROM metrics WHERE metric_type = 'oi' AND symbol = '$symbol' ORDER BY time"
        },
        {
          "datasource": "TimescaleDB",
          "rawSql": "SELECT time, value as price FROM metrics WHERE metric_type = 'price' AND symbol = '$symbol' ORDER BY time"
        }
      ]
    },
    {
      "title": "Order Book Imbalance (OBI)",
      "type": "timeseries",
      "gridPos": {"x": 0, "y": 8, "w": 8, "h": 6},
      "targets": [
        {
          "datasource": "Redis",
          "command": "ts.range",
          "keyName": "unitrade:obi:$symbol"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "color": {"mode": "thresholds"},
          "thresholds": {
            "steps": [
              {"value": -0.5, "color": "red"},
              {"value": 0, "color": "yellow"},
              {"value": 0.5, "color": "green"}
            ]
          }
        }
      }
    },
    {
      "title": "Market Regime",
      "type": "stat",
      "gridPos": {"x": 8, "y": 8, "w": 4, "h": 6},
      "targets": [
        {
          "datasource": "Redis",
          "command": "hget",
          "keyName": "unitrade:latest:$symbol",
          "field": "regime"
        }
      ],
      "options": {
        "colorMode": "background",
        "textMode": "value"
      }
    },
    {
      "title": "Realized Volatility (1min)",
      "type": "gauge",
      "gridPos": {"x": 12, "y": 8, "w": 4, "h": 6},
      "targets": [
        {
          "datasource": "Redis",
          "command": "hget",
          "keyName": "unitrade:latest:$symbol",
          "field": "realized_volatility"
        }
      ],
      "options": {
        "min": 0,
        "max": 100,
        "thresholds": {
          "steps": [
            {"value": 0, "color": "green"},
            {"value": 30, "color": "yellow"},
            {"value": 60, "color": "red"}
          ]
        }
      }
    },
    {
      "title": "Volume Delta (Buy vs Sell)",
      "type": "barchart",
      "gridPos": {"x": 16, "y": 8, "w": 8, "h": 6},
      "targets": [
        {
          "datasource": "TimescaleDB",
          "rawSql": "SELECT time_bucket('1 minute', time) as bucket, SUM(CASE WHEN side = 'buy' THEN notional ELSE 0 END) as buy_volume, SUM(CASE WHEN side = 'sell' THEN notional ELSE 0 END) as sell_volume FROM trades WHERE symbol = '$symbol' GROUP BY bucket ORDER BY bucket"
        }
      ]
    }
  ],
  "templating": {
    "list": [
      {
        "name": "symbol",
        "type": "custom",
        "options": [
          {"text": "BTCUSDT", "value": "BTCUSDT"},
          {"text": "ETHUSDT", "value": "ETHUSDT"}
        ]
      }
    ]
  }
}
```

### Sprint 6: Production Deployment

**任务:**
1. Docker Compose完整栈
2. E2E集成测试
3. 文档完善

### Definition of Done (Phase 3)
- [ ] Grafana显示Price/CVD/OI叠加图
- [ ] 市场状态实时更新
- [ ] Docker Compose一键启动
- [ ] E2E测试覆盖核心流程

---

## High-Frequency Trade vs Low-Frequency OI Alignment Strategy

```
时间线示例 (5秒窗口):

成交数据 (毫秒级):
T0+0ms:    Trade $50000 BUY 0.1
T0+50ms:   Trade $50001 BUY 0.2
T0+120ms:  Trade $50000 SELL 0.15
...
T0+4950ms: Trade $50010 BUY 0.3

OI数据 (Binance 5秒轮询):
T0+0ms:    OI = 50000 BTC
T0+5000ms: OI = 50050 BTC  ← 对齐点

对齐策略:
┌─────────────────────────────────────────────────────────────┐
│ 1. OI更新时，记录当前时间戳                                    │
│ 2. 在同一时间窗口内的成交数据标记相同的OI值                      │
│ 3. 计算该窗口内的CVD与OI变化的相关性                           │
│                                                              │
│ 数据结构:                                                     │
│ {                                                            │
│   "window_start": T0,                                        │
│   "window_end": T0+5000ms,                                   │
│   "oi_start": 50000,                                         │
│   "oi_end": 50050,                                           │
│   "oi_delta": +50,                                           │
│   "cvd_in_window": +0.45,  (买0.6 - 卖0.15)                  │
│   "price_change": +10,                                       │
│   "regime": "LONG_BUILD"                                     │
│ }                                                            │
└─────────────────────────────────────────────────────────────┘
```

---

## 交付清单

### 代码交付
- [ ] 完整源代码 (`src/unitrade/`)
- [ ] 单元测试 (`tests/unit/`)
- [ ] 集成测试 (`tests/integration/`)
- [ ] Mock服务器 (`tests/mocks/`)

### 基础设施
- [ ] Docker Compose配置
- [ ] Grafana仪表板JSON
- [ ] Prometheus配置

### 文档
- [ ] API文档
- [ ] 部署指南
- [ ] 指标说明文档

---

## 核心指标说明

### 1. Order Book Imbalance (OBI)
- **公式**: `(Bid_Volume - Ask_Volume) / (Bid_Volume + Ask_Volume)`
- **范围**: [-1, 1]
- **解读**: OBI > 0 表示买盘压力大，OBI < 0 表示卖盘压力大

### 2. Cumulative Volume Delta (CVD)
- **公式**: `Σ(Buy_Volume) - Σ(Sell_Volume)`
- **用途**: 跟踪净买卖力量
- **信号**: CVD上升+价格上升=强势; CVD下降+价格上升=弱势

### 3. Realized Volatility
- **公式**: `std(log_returns) * sqrt(annualization_factor)`
- **窗口**: 1分钟滚动
- **单位**: 年化百分比

### 4. OI Delta & Market Regime
- **计算**: 5分钟OI变化
- **状态判定**:
  - LONG_BUILD: 价格↑ + OI↑
  - SHORT_BUILD: 价格↓ + OI↑
  - LONG_UNWIND: 价格↓ + OI↓
  - SHORT_COVER: 价格↑ + OI↓
