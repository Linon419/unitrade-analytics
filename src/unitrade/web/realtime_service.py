"""
实时数据服务 - 多周期 WebSocket 集成

功能:
1. 连接 Binance WebSocket (aggTrade + depth)
2. 维护 LocalOrderBook 计算 OBI
3. 维护 TradeAnalytics 计算 CVD/波动率
4. 多周期聚合: 1m, 5m, 15m, 1h, 4h, 1d
"""

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp

from unitrade.analytics.orderbook import LocalOrderBook
from unitrade.analytics.trade import TradeAnalytics, TradeNormalizer, TradeTick


from unitrade.data.hygiene import SQLiteManager
from unitrade.core.config import Config

logger = logging.getLogger(__name__)

# 周期定义 (秒)
TIMEFRAMES = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


@dataclass
class TimeframeBucket:
    """时间周期数据桶"""
    obi_sum: float = 0
    obi_count: int = 0
    # 统一口径：USDT（quote）
    cvd_delta: float = 0  # 该周期内的 CVD 变化（USDT）
    buy_volume: float = 0  # buy notional (USDT)
    sell_volume: float = 0  # sell notional (USDT)

    # 辅助口径：数量（base qty），不持久化，仅用于展示/调试
    cvd_delta_qty: float = 0
    buy_volume_qty: float = 0
    sell_volume_qty: float = 0
    trade_count: int = 0
    high_price: float = 0
    low_price: float = float('inf')
    open_price: float = 0
    close_price: float = 0
    start_time: float = 0
    
    @property
    def obi_avg(self) -> float:
        return self.obi_sum / self.obi_count if self.obi_count > 0 else 0
    
    @property
    def net_volume(self) -> float:
        return self.buy_volume - self.sell_volume

    @property
    def net_volume_qty(self) -> float:
        return self.buy_volume_qty - self.sell_volume_qty
    
    def to_dict(self, symbol: str, timeframe: str) -> dict:
        """转换为字典 (用于存储)"""
        return {
            "timestamp": int(self.start_time),
            "symbol": symbol,
            "timeframe": timeframe,
            "obi_avg": self.obi_avg,
            "cvd_delta": self.cvd_delta,
            "buy_volume": self.buy_volume,
            "sell_volume": self.sell_volume,
            "high_price": self.high_price,
            "low_price": self.low_price if self.low_price != float('inf') else 0,
            "open_price": self.open_price,
            "close_price": self.close_price,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'TimeframeBucket':
        """从字典恢复"""
        bucket = cls()
        bucket.start_time = data["timestamp"]
        bucket.obi_sum = data.get("obi_avg", 0) * 1  # 恢复时 count设为1
        bucket.obi_count = 1
        bucket.cvd_delta = data.get("cvd_delta", 0)
        bucket.buy_volume = data.get("buy_volume", 0)
        bucket.sell_volume = data.get("sell_volume", 0)
        bucket.cvd_delta_qty = 0
        bucket.buy_volume_qty = 0
        bucket.sell_volume_qty = 0
        bucket.high_price = data.get("high_price", 0)
        bucket.low_price = data.get("low_price", 0)
        bucket.open_price = data.get("open_price", 0)
        bucket.close_price = data.get("close_price", 0)
        return bucket


@dataclass
class RealtimeConfig:
    """实时数据配置"""
    symbols: List[str] = field(default_factory=list)
    ws_url: str = "wss://fstream.binance.com/ws"
    rest_url: str = "https://fapi.binance.com"
    max_buckets: int = 100
    db_path: str = "data/market_data.db"
    
    @classmethod
    def from_config(cls) -> 'RealtimeConfig':
        """从全局配置加载"""
        from unitrade.core.config import load_config, resolve_config_path

        conf_path = resolve_config_path("config/default.yaml")
        conf = load_config(str(conf_path) if conf_path is not None else None)
        conf_data = conf.realtime or {}

        base_dir = Path(conf_path).resolve().parent.parent if conf_path is not None else Path.cwd()
        
        # 默认 10 个热门币种
        default_symbols = [
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT",
            "XRPUSDT", "PEPEUSDT", "WIFUSDT", "AVAXUSDT", "SUIUSDT"
        ]
        
        db_path_raw = str(conf_data.get("db_path", "data/market_data.db"))
        db_path = str((base_dir / Path(db_path_raw)) if not Path(db_path_raw).is_absolute() else Path(db_path_raw))

        return cls(
            symbols=conf_data.get("symbols", default_symbols),
            ws_url=conf_data.get("ws_url", "wss://fstream.binance.com/ws"),
            rest_url=conf_data.get("rest_url", "https://fapi.binance.com"),
            max_buckets=conf_data.get("max_buckets", 100),
            db_path=db_path,
        )


class MultiTimeframeAggregator:
    """多周期聚合器"""
    
    def __init__(self, symbol: str, db_manager: Optional[SQLiteManager] = None, max_buckets: int = 100):
        self.symbol = symbol
        self.db_manager = db_manager
        self.max_buckets = max_buckets
        
        # 每个周期的历史数据 (最近 N 个完成的周期)
        self.history: Dict[str, deque] = {
            tf: deque(maxlen=max_buckets) for tf in TIMEFRAMES
        }
        
        # 从数据库加载历史
        if self.db_manager:
            self._load_history()
        
        # 当前正在进行的周期
        self.current: Dict[str, TimeframeBucket] = {
            tf: TimeframeBucket(start_time=self._get_period_start(tf))
            for tf in TIMEFRAMES
        }
        
        # 累积 CVD (从启动开始)
        self.cumulative_cvd: float = 0  # USDT
        self.cumulative_cvd_qty: float = 0  # base qty
    
    def _load_history(self) -> None:
        """加载历史数据"""
        try:
            for tf in TIMEFRAMES:
                rows = self.db_manager.get_metric_history(self.symbol, tf, self.max_buckets)
                for row in reversed(rows):  # 数据库是倒序的，这里反转回正序
                    bucket = TimeframeBucket.from_dict(row)
                    self.history[tf].append(bucket)
            logger.info(f"Loaded history for {self.symbol}")
        except Exception as e:
            logger.error(f"Failed to load history for {self.symbol}: {e}")
            
    def _save_bucket(self, timeframe: str, bucket: TimeframeBucket) -> None:
        """保存完成的桶"""
        if not self.db_manager:
            return
            
        try:
            data = bucket.to_dict(self.symbol, timeframe)
            self.db_manager.save_metric_bucket(data)
        except Exception as e:
            logger.error(f"Failed to save bucket {self.symbol} {timeframe}: {e}")
    
    def _get_period_start(self, timeframe: str) -> float:
        """获取当前周期的开始时间"""
        now = time.time()
        period_seconds = TIMEFRAMES[timeframe]
        return (now // period_seconds) * period_seconds
    
    def update_obi(self, obi: float) -> None:
        """更新 OBI 数据"""
        now = time.time()
        
        for tf, bucket in self.current.items():
            period_start = self._get_period_start(tf)
            
            # 检查是否进入新周期
            if period_start > bucket.start_time:
                # 保存旧周期
                if bucket.obi_count > 0:
                    self.history[tf].append(bucket)
                    self._save_bucket(tf, bucket)  # 持久化
                
                # 创建新周期
                self.current[tf] = TimeframeBucket(start_time=period_start)
                bucket = self.current[tf]
            
            # 更新当前周期
            bucket.obi_sum += obi
            bucket.obi_count += 1
    
    def update_trade(self, trade: TradeTick) -> None:
        """更新成交数据"""
        now = time.time()
        price = float(trade.price)
        qty = float(trade.quantity)
        is_buy = trade.side.value == "buy"
        
        # 更新累积 CVD
        delta_qty = qty if is_buy else -qty
        delta_usdt = (qty * price) if is_buy else -(qty * price)
        self.cumulative_cvd_qty += delta_qty
        self.cumulative_cvd += delta_usdt
        
        for tf, bucket in self.current.items():
            period_start = self._get_period_start(tf)
            
            # 检查是否进入新周期
            if period_start > bucket.start_time:
                # 保存旧周期
                if bucket.trade_count > 0:
                    self.history[tf].append(bucket)
                    self._save_bucket(tf, bucket)  # 持久化
                
                # 创建新周期
                self.current[tf] = TimeframeBucket(start_time=period_start)
                bucket = self.current[tf]
                bucket.open_price = price
            
            # 更新当前周期
            bucket.trade_count += 1
            bucket.close_price = price
            
            if bucket.open_price == 0:
                bucket.open_price = price
            if price > bucket.high_price:
                bucket.high_price = price
            if price < bucket.low_price:
                bucket.low_price = price
            
            if is_buy:
                bucket.buy_volume += qty * price
                bucket.buy_volume_qty += qty
            else:
                bucket.sell_volume += qty * price
                bucket.sell_volume_qty += qty
            
            bucket.cvd_delta += delta_usdt
            bucket.cvd_delta_qty += delta_qty
    
    def get_timeframe_metrics(self, timeframe: str) -> dict:
        """获取指定周期的指标"""
        if timeframe not in TIMEFRAMES:
            return {"error": f"Invalid timeframe: {timeframe}"}
        
        current = self.current[timeframe]
        history = list(self.history[timeframe])
        
        # 计算历史周期的汇总
        total_obi = current.obi_sum
        total_obi_count = current.obi_count
        total_cvd = current.cvd_delta
        total_buy = current.buy_volume
        total_sell = current.sell_volume
        total_buy_qty = current.buy_volume_qty
        total_sell_qty = current.sell_volume_qty
        total_cvd_qty = current.cvd_delta_qty
        
        for bucket in history[-5:]:  # 最近 5 个完成的周期
            total_obi += bucket.obi_sum
            total_obi_count += bucket.obi_count
            total_cvd += bucket.cvd_delta
            total_buy += bucket.buy_volume
            total_sell += bucket.sell_volume
            total_buy_qty += bucket.buy_volume_qty
            total_sell_qty += bucket.sell_volume_qty
            total_cvd_qty += bucket.cvd_delta_qty
        
        return {
            "symbol": self.symbol,
            "timeframe": timeframe,
            "units": {
                "obi_avg": "percent",
                "cvd": "usdt",
                "buy_volume": "usdt",
                "sell_volume": "usdt",
                "net_volume": "usdt",
                "cvd_qty": "base_qty",
                "buy_volume_qty": "base_qty",
                "sell_volume_qty": "base_qty",
                "net_volume_qty": "base_qty",
                "price": "quote_price",
            },
            "current": {
                "obi_avg": round(current.obi_avg * 100, 2),  # 百分比
                "cvd": round(current.cvd_delta, 2),
                "buy_volume": round(current.buy_volume, 2),
                "sell_volume": round(current.sell_volume, 2),
                "net_volume": round(current.net_volume, 2),
                "cvd_qty": round(current.cvd_delta_qty, 4),
                "buy_volume_qty": round(current.buy_volume_qty, 4),
                "sell_volume_qty": round(current.sell_volume_qty, 4),
                "net_volume_qty": round(current.net_volume_qty, 4),
                "trade_count": current.trade_count,
                "open": current.open_price,
                "high": current.high_price,
                "low": current.low_price if current.low_price != float('inf') else 0,
                "close": current.close_price,
            },
            # 近 5 个完成周期 + 当前周期的汇总（用于更稳定的展示/策略过滤）
            "rolling_5": {
                "obi_avg": round((total_obi / total_obi_count) * 100, 2) if total_obi_count > 0 else 0,
                "cvd": round(total_cvd, 2),
                "buy_volume": round(total_buy, 2),
                "sell_volume": round(total_sell, 2),
                "net_volume": round(total_buy - total_sell, 2),
                "cvd_qty": round(total_cvd_qty, 4),
                "buy_volume_qty": round(total_buy_qty, 4),
                "sell_volume_qty": round(total_sell_qty, 4),
                "net_volume_qty": round(total_buy_qty - total_sell_qty, 4),
            },
            "cumulative_cvd": round(self.cumulative_cvd, 2),
            "cumulative_cvd_qty": round(self.cumulative_cvd_qty, 4),
            "period_start": datetime.fromtimestamp(current.start_time).isoformat(),
            "history_count": len(history),
            "timestamp": datetime.now().isoformat(),
        }
    
    def get_all_timeframes(self) -> dict:
        """获取所有周期的指标"""
        return {
            tf: self.get_timeframe_metrics(tf)
            for tf in TIMEFRAMES
        }


class RealtimeDataService:
    """
    实时数据服务 - 多周期版本
    
    管理 WebSocket 连接，计算实时指标:
    - OBI (Order Book Imbalance) - 多周期
    - CVD (Cumulative Volume Delta) - 多周期
    - Realized Volatility
    """
    
    def __init__(self, config: Optional[RealtimeConfig] = None):
        if config is None:
            # 尝试从全局配置加载
            try:
                self.config = RealtimeConfig.from_config()
                # 确保配置加载成功，至少有默认币种
                if not self.config.symbols:
                    logger.warning("No symbols found in config, falling back to defaults")
                    self.config = RealtimeConfig()
            except Exception as e:
                logger.warning(f"Failed to load config: {e}, using defaults")
                self.config = RealtimeConfig()
        else:
            self.config = config
        
        # 持久化管理器
        self.db_manager = SQLiteManager(self.config.db_path)
        
        # 订单簿 (每个币种一个)
        self.orderbooks: Dict[str, LocalOrderBook] = {}
        
        # 成交分析 (每个币种一个)
        self.trade_analytics: Dict[str, TradeAnalytics] = {}
        
        # 多周期聚合器 (每个币种一个)
        self.aggregators: Dict[str, MultiTimeframeAggregator] = {}
        
        # WebSocket
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        
        # 初始化分析器
        for symbol in self.config.symbols:
            self.orderbooks[symbol] = LocalOrderBook(symbol, "binance")
            self.trade_analytics[symbol] = TradeAnalytics(symbol)
            self.aggregators[symbol] = MultiTimeframeAggregator(
                symbol, 
                db_manager=self.db_manager,
                max_buckets=self.config.max_buckets
            )
    
    async def start(self) -> None:
        """启动实时数据服务"""
        if self._running:
            return
        
        self._running = True
        self._session = aiohttp.ClientSession()
        
        # 初始化订单簿快照
        for symbol in self.config.symbols:
            await self._fetch_orderbook_snapshot(symbol)
        
        # 启动 WebSocket
        self._task = asyncio.create_task(self._ws_loop())
        
        logger.info(f"RealtimeDataService (multi-timeframe) started for {self.config.symbols}")
    
    async def stop(self) -> None:
        """停止服务"""
        self._running = False
        
        if self._ws:
            await self._ws.close()
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        if self._session:
            await self._session.close()
        
        logger.info("RealtimeDataService stopped")
    
    async def _fetch_orderbook_snapshot(self, symbol: str) -> None:
        """获取订单簿快照"""
        try:
            url = f"{self.config.rest_url}/fapi/v1/depth"
            params = {"symbol": symbol, "limit": 100}
            
            async with self._session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    self.orderbooks[symbol].apply_snapshot(
                        bids=data["bids"],
                        asks=data["asks"],
                        update_id=data["lastUpdateId"]
                    )
                    logger.info(f"Orderbook snapshot loaded: {symbol}")
        except Exception as e:
            logger.error(f"Failed to fetch orderbook snapshot for {symbol}: {e}")
    
    async def _ws_loop(self) -> None:
        """WebSocket 消息循环"""
        # 构建订阅流
        streams = []
        for symbol in self.config.symbols:
            sym = symbol.lower()
            streams.append(f"{sym}@aggTrade")      # 成交数据
            streams.append(f"{sym}@depth@100ms")   # 深度更新
        
        stream_path = "/".join(streams)
        ws_url = f"{self.config.ws_url}/{stream_path}"
        
        while self._running:
            try:
                async with self._session.ws_connect(ws_url) as ws:
                    self._ws = ws
                    logger.info(f"WebSocket connected: {ws_url}")
                    
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_message(json.loads(msg.data))
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"WS error: {ws.exception()}")
                            break
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                if self._running:
                    await asyncio.sleep(5)  # 重连延迟
    
    async def _handle_message(self, data: dict) -> None:
        """处理 WebSocket 消息"""
        try:
            stream = data.get("stream", "")
            payload = data.get("data", data)
            
            if "@aggTrade" in stream or "e" in payload and payload.get("e") == "aggTrade":
                await self._handle_trade(payload)
            elif "@depth" in stream or "e" in payload and payload.get("e") == "depthUpdate":
                await self._handle_depth(payload)
        except Exception as e:
            logger.debug(f"Message handling error: {e}")
    
    async def _handle_trade(self, data: dict) -> None:
        """处理成交数据"""
        try:
            trade = TradeNormalizer.from_binance(data)
            symbol = trade.symbol
            
            if symbol in self.trade_analytics:
                self.trade_analytics[symbol].process_trade(trade)
            
            # 更新多周期聚合器
            if symbol in self.aggregators:
                self.aggregators[symbol].update_trade(trade)
        except Exception as e:
            logger.debug(f"Trade handling error: {e}")
    
    async def _handle_depth(self, data: dict) -> None:
        """处理深度更新"""
        try:
            symbol = data.get("s", "")
            
            if symbol in self.orderbooks:
                ob = self.orderbooks[symbol]

                # Binance depthUpdate 序号字段
                first_update_id = int(data.get("U", 0) or 0)
                final_update_id = int(data.get("u", 0) or 0)
                prev_final_update_id = data.get("pu", None)
                prev_final_update_id = int(prev_final_update_id) if prev_final_update_id is not None else None

                if not ob.is_initialized:
                    await self._fetch_orderbook_snapshot(symbol)
                    if not ob.is_initialized:
                        return

                ok = ob.apply_binance_delta(
                    bids=data.get("b", []),
                    asks=data.get("a", []),
                    first_update_id=first_update_id,
                    final_update_id=final_update_id,
                    prev_final_update_id=prev_final_update_id,
                )

                if not ok:
                    # 断档/不同步：重拉快照，避免 OBI “漂移”误导交易决策
                    logger.warning(f"Orderbook out of sync for {symbol}, resyncing snapshot...")
                    ob.clear()
                    await self._fetch_orderbook_snapshot(symbol)
                    return
                
                # 更新 OBI 到多周期聚合器
                metrics = ob.get_metrics()
                if metrics and symbol in self.aggregators:
                    self.aggregators[symbol].update_obi(float(metrics.obi))
        except Exception as e:
            logger.debug(f"Depth handling error: {e}")
    
    def get_obi(self, symbol: str, timeframe: str = "1m") -> Optional[dict]:
        """获取 OBI 数据 (支持多周期)"""
        if symbol not in self.aggregators:
            return None
        
        return self.aggregators[symbol].get_timeframe_metrics(timeframe)
    
    def get_cvd(self, symbol: str, timeframe: str = "1m") -> Optional[dict]:
        """获取 CVD 数据 (支持多周期)"""
        if symbol not in self.aggregators:
            return None
        
        return self.aggregators[symbol].get_timeframe_metrics(timeframe)
    
    def get_volatility(self, symbol: str) -> Optional[dict]:
        """获取波动率数据"""
        if symbol not in self.trade_analytics:
            return None
        
        ta = self.trade_analytics[symbol]
        vol = ta.calculate_realized_volatility(60)  # 1分钟窗口
        
        return {
            "symbol": symbol,
            "volatility_pct": float(vol) if vol else 0,
            "timestamp": datetime.now().isoformat(),
        }
    
    def get_all_timeframes(self, symbol: str) -> Optional[dict]:
        """获取指定币种所有周期的数据"""
        if symbol not in self.aggregators:
            return None
        
        return self.aggregators[symbol].get_all_timeframes()
    
    def get_all_metrics(self) -> dict:
        """获取所有币种的所有指标"""
        result = {}
        
        for symbol in self.config.symbols:
            result[symbol] = {
                "timeframes": self.get_all_timeframes(symbol),
                "volatility": self.get_volatility(symbol),
            }
        
        return result
    
    @property
    def is_running(self) -> bool:
        return self._running


# 全局实例 (供 Dashboard 使用)
_realtime_service: Optional[RealtimeDataService] = None


def get_realtime_service() -> Optional[RealtimeDataService]:
    """获取全局实时数据服务实例"""
    return _realtime_service


async def start_realtime_service(symbols: List[str] = None) -> Optional[RealtimeDataService]:
    """启动全局实时数据服务 (受 default.yaml 开关控制)"""
    global _realtime_service
    
    if _realtime_service is None:
        if not symbols:
            try:
                from pathlib import Path
                from unitrade.core.config import load_config

                if Path("config/default.yaml").exists():
                    conf = load_config("config/default.yaml")
                    if not bool((conf.realtime or {}).get("enabled", True)):
                        logger.info("Realtime service disabled (realtime.enabled=false)")
                        return None
                    if conf.get_exchange("binance") is None:
                        logger.info("Realtime service disabled (exchanges.binance.enabled=false)")
                        return None
            except Exception as e:
                logger.warning(f"Failed to load config for realtime.enabled check: {e}")

        if symbols:
            config = RealtimeConfig(symbols=symbols)
            _realtime_service = RealtimeDataService(config)
        else:
            # Let RealtimeDataService load from global config
            _realtime_service = RealtimeDataService()
              
        await _realtime_service.start()
    
    return _realtime_service


async def stop_realtime_service() -> None:
    """停止全局实时数据服务"""
    global _realtime_service
    
    if _realtime_service:
        await _realtime_service.stop()
        _realtime_service = None
