"""
WebSocket Manager - 币安全市场数据接入

使用 unicorn-binance-websocket-api 实现:
- 全币种 aggTrade 订阅
- 全币种 kline_1m 订阅
- 自动重连
- 非阻塞异步处理
"""

import asyncio
import json
import logging
import time
from typing import Callable, Dict, List, Optional, Set
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# 尝试导入 unicorn-binance-websocket-api
try:
    from unicorn_binance_websocket_api import BinanceWebSocketApiManager
    HAS_UNICORN = True
except ImportError:
    HAS_UNICORN = False
    logger.warning("unicorn-binance-websocket-api not installed, using fallback")

import aiohttp

from .calculators import TradeData, KlineData


@dataclass
class WSConfig:
    """WebSocket 配置"""
    # 市场
    market: str = "futures"  # spot 或 futures
    
    # 订阅
    streams: List[str] = None  # ["aggTrade", "kline_1m"]
    symbols: List[str] = None  # 空 = 全币种
    
    # 过滤
    min_quote_volume_24h: float = 10_000_000  # 24h 成交额过滤
    max_symbols: int = 50  # max symbols to subscribe (top by 24h quoteVolume)
    
    # 性能
    process_interval_ms: int = 100  # 处理间隔 (批量处理)
    
    def __post_init__(self):
        if self.streams is None:
            self.streams = ["aggTrade", "kline_1m"]


class BinanceWSManager:
    """
    币安 WebSocket 管理器
    
    支持两种模式:
    1. unicorn-binance-websocket-api (推荐, 高性能)
    2. 原生 aiohttp (fallback)
    """
    
    FUTURES_WS_URL = "wss://fstream.binance.com"
    SPOT_WS_URL = "wss://stream.binance.com:9443"
    
    def __init__(self, config: WSConfig = None):
        self.config = config or WSConfig()
        
        # 回调
        self._on_trade: Optional[Callable[[TradeData], None]] = None
        self._on_kline: Optional[Callable[[KlineData], None]] = None
        
        # 状态
        self._running = False
        self._symbols: Set[str] = set()
        self._ws_sessions: List = []
        
        # 缓冲区 (批量处理)
        self._trade_buffer: List[TradeData] = []
        self._kline_buffer: List[KlineData] = []
        
        # Unicorn manager
        self._ubwa: Optional[BinanceWebSocketApiManager] = None
    
    def on_trade(self, callback: Callable[[TradeData], None]):
        """注册交易回调"""
        self._on_trade = callback
    
    def on_kline(self, callback: Callable[[KlineData], None]):
        """注册 K 线回调"""
        self._on_kline = callback
    
    async def start(self):
        """启动 WebSocket"""
        self._running = True
        
        # 获取交易对列表
        await self._fetch_symbols()
        
        # 强制使用 native aiohttp (更稳定)
        # if HAS_UNICORN:
        #     await self._start_unicorn()
        # else:
        await self._start_native()
        logger.info(f"WebSocket started with {len(self._symbols)} symbols")
    
    async def stop(self):
        """停止"""
        self._running = False
        
        if self._ubwa:
            self._ubwa.stop_manager_with_all_streams()
    
    async def _fetch_symbols(self):
        """获取活跃交易对"""
        if self.config.symbols:
            self._symbols = set(self.config.symbols)
            return
        
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                tickers = await resp.json()
        
        # 过滤 USDT 永续 + 按成交额排序
        usdt_tickers = [
            (t["symbol"], float(t.get("quoteVolume", 0)))
            for t in tickers if t["symbol"].endswith("USDT")
        ]
        usdt_tickers.sort(key=lambda x: x[1], reverse=True)
        
        # 取 top N (避免 WebSocket 过载)
        max_symbols = int(self.config.max_symbols) if self.config.max_symbols else 50
        if max_symbols <= 0:
            max_symbols = 50
        for symbol, vol in usdt_tickers[:max_symbols]:
            if vol >= self.config.min_quote_volume_24h:
                self._symbols.add(symbol)
        
        logger.info(f"Fetched {len(self._symbols)} active symbols")
    
    async def _start_unicorn(self):
        """使用 unicorn-binance-websocket-api"""
        logger.info("Starting WebSocket with unicorn-binance-websocket-api")
        
        self._ubwa = BinanceWebSocketApiManager(
            exchange="binance.com-futures",
            output_default="UnicornFy"
        )
        
        # 转换为小写
        symbols = [s.lower() for s in self._symbols]
        
        # 订阅 aggTrade
        if "aggTrade" in self.config.streams:
            self._ubwa.create_stream(
                channels=["aggTrade"],
                markets=symbols,
                stream_label="aggTrade"
            )
        
        # 订阅 kline
        if "kline_1m" in self.config.streams:
            self._ubwa.create_stream(
                channels=["kline_1m"],
                markets=symbols,
                stream_label="kline"
            )
        
        # 启动处理任务
        asyncio.create_task(self._process_unicorn())
    
    async def _process_unicorn(self):
        """处理 unicorn 数据流"""
        while self._running and self._ubwa:
            try:
                oldest = self._ubwa.pop_stream_data_from_stream_buffer()
                
                if oldest:
                    await self._handle_unicorn_data(oldest)
                else:
                    await asyncio.sleep(0.001)  # 1ms
                    
            except Exception as e:
                logger.error(f"Unicorn process error: {e}")
                await asyncio.sleep(0.1)
    
    async def _handle_unicorn_data(self, data):
        """处理 unicorn 数据"""
        if isinstance(data, str):
            data = json.loads(data)
        
        event_type = data.get("event_type") or data.get("e")
        
        if event_type == "aggTrade":
            trade = self._parse_agg_trade(data)
            if trade and self._on_trade:
                self._on_trade(trade)
        
        elif event_type == "kline":
            kline = self._parse_kline(data)
            if kline and self._on_kline:
                self._on_kline(kline)
    
    async def _start_native(self):
        """使用原生 aiohttp (fallback)"""
        logger.info("Starting WebSocket with native aiohttp")
        
        # 分批订阅 (每个连接最多 200 个流)
        symbols = list(self._symbols)
        batch_size = 100
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            asyncio.create_task(self._native_stream(batch))
    
    async def _native_stream(self, symbols: List[str]):
        """原生 WebSocket 流"""
        streams = []
        for s in symbols:
            s_lower = s.lower()
            if "aggTrade" in self.config.streams:
                streams.append(f"{s_lower}@aggTrade")
            if "kline_1m" in self.config.streams:
                streams.append(f"{s_lower}@kline_1m")
        
        url = f"{self.FUTURES_WS_URL}/stream?streams={'/'.join(streams)}"
        
        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    async with session.ws_connect(url) as ws:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._handle_native_message(msg.data)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                break
                except Exception as e:
                    logger.error(f"WebSocket error: {e}")
                    await asyncio.sleep(5)  # 重连延迟
    
    async def _handle_native_message(self, raw: str):
        """处理原生消息"""
        try:
            msg = json.loads(raw)
            data = msg.get("data", {})
            event_type = data.get("e")
            
            if event_type == "aggTrade":
                trade = self._parse_agg_trade(data)
                if trade and self._on_trade:
                    self._on_trade(trade)
            
            elif event_type == "kline":
                kline = self._parse_kline(data.get("k", {}))
                if kline and self._on_kline:
                    self._on_kline(kline)
                    
        except Exception as e:
            logger.debug(f"Parse error: {e}")
    
    def _parse_agg_trade(self, data: Dict) -> Optional[TradeData]:
        """解析 aggTrade"""
        try:
            symbol = data.get("s") or data.get("symbol", "")
            if not symbol.endswith("USDT"):
                return None
            
            price = float(data.get("p", 0))
            qty = float(data.get("q", 0))
            is_maker = data.get("m", False)
            ts = float(data.get("T", 0)) / 1000 if data.get("T") else time.time()
            
            return TradeData(
                symbol=symbol.upper(),
                price=price,
                quantity=qty,
                quote_volume=price * qty,
                is_buyer_maker=is_maker,
                timestamp=ts
            )
        except Exception:
            return None
    
    def _parse_kline(self, data: Dict) -> Optional[KlineData]:
        """解析 kline"""
        try:
            symbol = data.get("s", "")
            if not symbol.endswith("USDT"):
                return None
            
            return KlineData(
                symbol=symbol.upper(),
                open=float(data.get("o", 0)),
                high=float(data.get("h", 0)),
                low=float(data.get("l", 0)),
                close=float(data.get("c", 0)),
                volume=float(data.get("v", 0)),
                quote_volume=float(data.get("q", 0)),
                taker_buy_volume=float(data.get("V", 0)),
                taker_buy_quote_volume=float(data.get("Q", 0)),
                timestamp=float(data.get("t", 0)) / 1000 if data.get("t") else time.time(),
                is_closed=bool(data.get("x", False)),
            )
        except Exception:
            return None

    @property
    def symbols(self) -> Set[str]:
        """Return the currently tracked symbols."""
        return set(self._symbols)
