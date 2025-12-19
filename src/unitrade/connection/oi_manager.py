"""
Open Interest 管理器

处理不同交易所的 OI 数据获取差异:
- Bybit: 通过 tickers WebSocket 实时推送
- Binance: 需要 REST 轮询
"""

import asyncio
import logging
from collections import deque
from decimal import Decimal
from typing import Callable, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)


class OpenInterestManager:
    """
    Open Interest 数据管理器
    
    关键差异:
    - Bybit: 通过 tickers WebSocket 实时推送 OI
    - Binance: 需要 REST 轮询 (无实时 WS 推送)
    
    策略:
    - Bybit: 直接使用 WS 推送，延迟 ~100ms
    - Binance: REST 轮询，默认 5 秒间隔，高频模式可降至 1 秒
    """
    
    BINANCE_OI_URL = "https://fapi.binance.com/fapi/v1/openInterest"
    BINANCE_TESTNET_OI_URL = "https://testnet.binancefuture.com/fapi/v1/openInterest"
    
    def __init__(self, testnet: bool = False):
        self.testnet = testnet
        self.binance_url = self.BINANCE_TESTNET_OI_URL if testnet else self.BINANCE_OI_URL
        
        # 最新 OI 数据缓存
        self._oi_cache: Dict[str, Dict] = {}  # key: "exchange:symbol"
        
        # OI 历史用于计算 Delta
        self._oi_history: Dict[str, deque] = {}  # 保留 5 分钟数据
        
        # Binance 轮询任务
        self._binance_poll_tasks: Dict[str, asyncio.Task] = {}
        
        # 回调
        self._on_oi_update: Optional[Callable] = None
        
        # HTTP 会话
        self._session: Optional[aiohttp.ClientSession] = None
        
        # 运行状态
        self._running = False
    
    async def start(self) -> None:
        """启动管理器"""
        self._session = aiohttp.ClientSession()
        self._running = True
    
    async def stop(self) -> None:
        """停止管理器"""
        self._running = False
        
        # 取消所有轮询任务
        for task in self._binance_poll_tasks.values():
            task.cancel()
        self._binance_poll_tasks.clear()
        
        if self._session:
            await self._session.close()
    
    def set_callback(self, callback: Callable) -> None:
        """设置 OI 更新回调"""
        self._on_oi_update = callback
    
    async def start_binance_polling(
        self,
        symbols: list,
        interval: float = 5.0
    ) -> None:
        """
        启动 Binance OI 轮询
        
        Endpoint: GET /fapi/v1/openInterest
        参数: symbol
        返回: {"symbol": "BTCUSDT", "openInterest": "12345.678", "time": 1699999999999}
        
        注意:
        - 权重: 1
        - 限制: 2400 权重/分钟，可支持 2400 次/分钟
        - 建议: 5 秒轮询足够捕捉趋势变化
        """
        for symbol in symbols:
            if symbol in self._binance_poll_tasks:
                continue
            
            task = asyncio.create_task(
                self._poll_binance_oi(symbol, interval)
            )
            self._binance_poll_tasks[symbol] = task
            
        logger.info(f"Started Binance OI polling for {len(symbols)} symbols, interval={interval}s")
    
    def stop_binance_polling(self, symbols: Optional[list] = None) -> None:
        """停止 Binance OI 轮询"""
        if symbols is None:
            symbols = list(self._binance_poll_tasks.keys())
        
        for symbol in symbols:
            if task := self._binance_poll_tasks.pop(symbol, None):
                task.cancel()
    
    async def _poll_binance_oi(self, symbol: str, interval: float) -> None:
        """轮询单个 symbol 的 OI"""
        while self._running:
            try:
                async with self._session.get(
                    self.binance_url, 
                    params={"symbol": symbol}
                ) as resp:
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
                        logger.warning(f"Binance OI rate limited, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        logger.error(f"Binance OI poll error: HTTP {resp.status}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Binance OI poll error for {symbol}: {e}")
            
            await asyncio.sleep(interval)
    
    async def on_bybit_oi(self, oi_data: Dict) -> None:
        """处理 Bybit 实时 OI 推送"""
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
        """处理 OI 更新，存入缓存和历史"""
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
            self._oi_history[key] = deque(maxlen=60)  # 5 分钟 @ 5 秒间隔
        
        self._oi_history[key].append({
            "oi": oi,
            "timestamp": timestamp
        })
        
        # 触发回调
        if self._on_oi_update:
            try:
                if asyncio.iscoroutinefunction(self._on_oi_update):
                    await self._on_oi_update(self._oi_cache[key])
                else:
                    self._on_oi_update(self._oi_cache[key])
            except Exception as e:
                logger.error(f"OI callback error: {e}")
    
    def get_current_oi(self, exchange: str, symbol: str) -> Optional[Dict]:
        """获取当前 OI"""
        key = f"{exchange}:{symbol}"
        return self._oi_cache.get(key)
    
    def get_oi_delta(
        self,
        exchange: str,
        symbol: str,
        window_minutes: int = 5
    ) -> Optional[Decimal]:
        """
        计算 OI 变化量
        
        返回: 指定时间窗口内的 OI 变化
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
    
    def get_oi_delta_percent(
        self,
        exchange: str,
        symbol: str,
        window_minutes: int = 5
    ) -> Optional[float]:
        """计算 OI 变化百分比"""
        key = f"{exchange}:{symbol}"
        history = self._oi_history.get(key)
        
        if not history or len(history) < 2:
            return None
        
        current_oi = history[-1]["oi"]
        
        window_ms = window_minutes * 60 * 1000
        current_ts = history[-1]["timestamp"]
        
        baseline_oi = None
        for entry in history:
            if current_ts - entry["timestamp"] >= window_ms:
                baseline_oi = entry["oi"]
                break
        
        if baseline_oi is None:
            baseline_oi = history[0]["oi"]
        
        if baseline_oi == 0:
            return None
        
        return float((current_oi - baseline_oi) / baseline_oi * 100)
