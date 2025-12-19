"""
实时异动监测器 (Real-time Anomaly Detector)

触发条件 (三者同时满足):
1. OI 异动: 持仓量增加 ≥ 3%
2. 价格突破 EMA200: 价格从下方突破 EMA200
3. 交易量放大: 当前成交量 > 3 倍平均量

支持时间周期: 15分钟、30分钟

技术栈:
- Binance Futures @kline WebSocket
- Binance Open Interest API
- Redis (数据存储 + 冷却锁)
- Telegram 报警

使用方法:
    python anomaly_detector.py
"""

import asyncio
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
import numpy as np

from .redis_state import RedisStateManager

logger = logging.getLogger(__name__)


# ==================== CONFIG ====================
@dataclass
class AnomalyConfig:
    """异动监测配置"""
    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_topic_id: Optional[int] = None  # message_thread_id for topic
    
    # 时间周期
    timeframes: List[str] = field(default_factory=lambda: ["15m", "30m"])
    
    # OI 异动阈值
    oi_change_threshold: float = 0.03   # OI 增加 3%
    oi_lookback_bars: int = 3           # 对比 N 根 K 线前的 OI
    
    # EMA 突破
    ema_period: int = 200               # EMA200
    
    # 量能异动阈值
    rvol_threshold: float = 3.0         # 放量倍数 3x
    rvol_lookback: int = 20             # 计算平均量的 K 线数
    
    # 冷却时间 (秒)
    cooldown_seconds: int = 1800        # 30分钟
    cooldown_across_timeframes: bool = False  # True: 同币种跨周期共享冷却
    
    # Redis
    redis_url: str = "redis://localhost:6379"
    redis_prefix: str = "anomaly"
    
    # 币种筛选
    scan_all: bool = True               # 扫描全部合约币种
    top_n: int = 50                     # 若 scan_all=False, 仅监控 Top N
    exclude_symbols: List[str] = field(default_factory=list)

    # Universe selection (liquidity + candidate filter)
    min_quote_volume_24h: float = 0.0   # 24h quoteVolume floor (USDT)
    min_trade_count_24h: int = 0        # 24h trade count floor
    universe_max_symbols: int = 300     # hard cap even when scan_all=True
    universe_preselect_multiplier: int = 3  # prefetch N = top_n * multiplier
    universe_refresh_minutes: int = 60      # 0 disables auto refresh
    min_universe_dwell_minutes: int = 180   # min time to keep a symbol before removal
    universe_max_add_per_refresh: int = 20
    universe_max_remove_per_refresh: int = 20

    # Candidate filter (EMA200 proximity on a reference timeframe)
    universe_reference_timeframe: Optional[str] = None  # default: first in timeframes
    candidate_max_ema200_distance_pct: float = 0.05     # e.g. 0.05 = within 5%
    candidate_below_ema200_only: bool = True

    # OI sampling optimization
    oi_symbol_cache_seconds: float = 2.0  # reuse OI across timeframes within this window
    
    # WebSocket
    max_streams_per_conn: int = 200     # 每个 WebSocket 连接最多订阅数
    reconnect_delay: float = 1.0
    max_reconnect_delay: float = 60.0
    
    # API 限流
    oi_concurrency: int = 50            # OI API 最大并发数
    rest_concurrency: int = 10          # REST API 最大并发数 (K线/发现交易对等)

    # OI 拉取重试
    oi_max_retries: int = 3
    oi_retry_base_delay: float = 0.5
    oi_retry_max_delay: float = 5.0
    
    # K 线历史数据量 (用于计算 EMA200, 需要足够数据热启动)
    kline_limit: int = 500
    
    @classmethod
    def from_env(cls) -> "AnomalyConfig":
        """从环境变量加载配置"""
        return cls(
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"),
            oi_change_threshold=float(os.getenv("OI_CHANGE_THRESHOLD", "0.03")),
            rvol_threshold=float(os.getenv("RVOL_THRESHOLD", "3.0")),
            cooldown_seconds=int(os.getenv("COOLDOWN_SECONDS", "1800")),
            top_n=int(os.getenv("TOP_N_SYMBOLS", "50")),
        )
    
    @classmethod
    def from_config(cls, config) -> "AnomalyConfig":
        """从 YAML 配置对象加载 (config/default.yaml)"""
        ad = config.get("anomaly_detector", {})
        tg = config.get("telegram", {})
        db = config.get("database", {})

        tg_enabled = bool(tg.get("enabled", True))
        telegram_bot_token = (tg.get("bot_token") or os.getenv("TELEGRAM_BOT_TOKEN", "")) if tg_enabled else ""
        telegram_chat_id = (str(tg.get("chat_id", "")) or os.getenv("TELEGRAM_CHAT_ID", "")) if tg_enabled else ""
        telegram_topic_id = tg.get("topics", {}).get("signal_anomaly") if tg_enabled else None
        
        return cls(
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
            telegram_topic_id=telegram_topic_id,  # topic ID 62
            timeframes=ad.get("timeframes", ["15m", "30m"]),
            oi_change_threshold=ad.get("oi_change_threshold", 0.03),
            oi_lookback_bars=ad.get("oi_lookback_bars", 3),
            ema_period=ad.get("ema_period", 200),
            rvol_threshold=ad.get("rvol_threshold", 3.0),
            rvol_lookback=ad.get("rvol_lookback", 20),
            cooldown_seconds=ad.get("cooldown_seconds", 1800),
            cooldown_across_timeframes=ad.get("cooldown_across_timeframes", False),
            redis_url=db.get("redis_url") or os.getenv("REDIS_URL", "redis://localhost:6379"),
            scan_all=ad.get("scan_all", True),
            top_n=ad.get("top_n", 50),
            exclude_symbols=ad.get("exclude_symbols", []),
            kline_limit=ad.get("kline_limit", 500),
            min_quote_volume_24h=float(ad.get("min_quote_volume_24h", 0.0) or 0.0),
            min_trade_count_24h=int(ad.get("min_trade_count_24h", 0) or 0),
            universe_max_symbols=int(ad.get("universe_max_symbols", 300) or 300),
            universe_preselect_multiplier=int(ad.get("universe_preselect_multiplier", 3) or 3),
            universe_refresh_minutes=int(ad.get("universe_refresh_minutes", 60) or 60),
            min_universe_dwell_minutes=int(ad.get("min_universe_dwell_minutes", 180) or 180),
            universe_max_add_per_refresh=int(ad.get("universe_max_add_per_refresh", 20) or 20),
            universe_max_remove_per_refresh=int(ad.get("universe_max_remove_per_refresh", 20) or 20),
            universe_reference_timeframe=ad.get("universe_reference_timeframe"),
            candidate_max_ema200_distance_pct=float(ad.get("candidate_max_ema200_distance_pct", 0.05) or 0.05),
            candidate_below_ema200_only=bool(ad.get("candidate_below_ema200_only", True)),
            oi_symbol_cache_seconds=float(ad.get("oi_symbol_cache_seconds", 2.0) or 2.0),
            max_streams_per_conn=ad.get("max_streams_per_conn", 200),
            reconnect_delay=ad.get("reconnect_delay", 1.0),
            max_reconnect_delay=ad.get("max_reconnect_delay", 60.0),
            oi_concurrency=ad.get("oi_concurrency", 50),
            rest_concurrency=ad.get("rest_concurrency", 10),
            oi_max_retries=ad.get("oi_max_retries", 3),
            oi_retry_base_delay=ad.get("oi_retry_base_delay", 0.5),
            oi_retry_max_delay=ad.get("oi_retry_max_delay", 5.0),
        )
# ===============================================


@dataclass
class BreakoutSignal:
    """突破信号"""
    symbol: str
    timeframe: str
    price: float
    ema200: float
    oi_change_pct: float    # OI 变化百分比
    rvol: float             # 放量倍数
    timestamp: str = ""
    
    def format_message(self) -> str:
        """格式化 Telegram 消息 (Markdown)"""
        symbol_clean = self.symbol.replace("USDT", "/USDT")
        
        # 价格格式化
        if self.price >= 1000:
            price_str = f"{self.price:,.2f}"
        elif self.price >= 1:
            price_str = f"{self.price:.4f}"
        else:
            price_str = f"{self.price:.6f}"
        
        msg = f"""🚀 *#{symbol_clean}* 突破信号 ({self.timeframe})

📈 突破 EMA200
💰 现价: `{price_str}`
📊 EMA200: `{self.ema200:.6f}`
🔥 OI 变化: `+{self.oi_change_pct:.2%}`
🌊 量能: `{self.rvol:.1f}x`"""
        
        return msg


class AnomalyDetector:
    """
    实时异动监测器
    
    检测条件 (三者同时满足):
    1. OI 异动: 当前 OI vs N 根 K 线前的 OI, 增加 ≥ 3%
    2. EMA200 突破: 价格从下方向上穿越 EMA200
    3. 量能放大: 当前成交量 > 3 倍平均量
    
    工作流程:
    1. 启动时获取 Top N 交易量币种
    2. 预拉取每个币种的历史 K 线数据 (用于计算 EMA200)
    3. 连接 WebSocket 订阅 @kline_15m 和 @kline_30m
    4. 每根 K 线收盘时:
       - 获取最新 OI
       - 计算 EMA200
       - 判断是否满足三条件
       - 触发报警
    """
    
    def __init__(self, config: Optional[AnomalyConfig] = None):
        self.config = config or AnomalyConfig.from_env()
        self._state_manager: Optional[RedisStateManager] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._symbols: Set[str] = set()
        self._reconnect_delay = self.config.reconnect_delay
        self._oi_semaphore: Optional[asyncio.Semaphore] = None
        self._rest_semaphore: Optional[asyncio.Semaphore] = None  # REST API 并发限制

        # 已收盘 K 线事件队列 (避免 WebSocket 消息处理被 REST 阻塞)
        self._event_queue: Optional[asyncio.Queue] = None
        self._worker_tasks: List[asyncio.Task] = []
        self._pair_locks: Dict[Tuple[str, str], asyncio.Lock] = {}

        # Universe refresh
        self._universe_lock = asyncio.Lock()
        self._universe_refresh_event = asyncio.Event()
        self._universe_task: Optional[asyncio.Task] = None
        self._symbol_added_at: Dict[str, float] = {}

        # OI short TTL cache (symbol-level)
        self._oi_value_cache: Dict[str, Tuple[float, float]] = {}
        
        # 内存缓存: symbol -> {timeframe -> {closes: [], volumes: [], oi_history: []}}
        self._data_cache: Dict[str, Dict[str, Dict]] = {}
    
    async def start(self) -> None:
        """启动监测器"""
        logger.info("Starting Anomaly Detector...")
        
        # 连接 Redis (使用共享的 RedisStateManager)
        self._state_manager = RedisStateManager(redis_url=self.config.redis_url)
        await self._state_manager.connect()
        logger.info(f"Redis connected via RedisStateManager: {self.config.redis_url}")
        
        # 创建 HTTP session
        self._session = aiohttp.ClientSession()
        
        # OI API 并发限制
        self._oi_semaphore = asyncio.Semaphore(self.config.oi_concurrency)

        # REST API 并发限制
        self._rest_semaphore = asyncio.Semaphore(self.config.rest_concurrency)

        # 启动事件工作者
        self._event_queue = asyncio.Queue(maxsize=5000)
        worker_count = max(1, min(self.config.oi_concurrency, 20))
        self._worker_tasks = [asyncio.create_task(self._event_worker(i)) for i in range(worker_count)]
        
        self._running = True

        # Universe init (liquidity + EMA proximity filter)
        await self._refresh_universe(initial=True)
        logger.info(f"Monitoring {len(self._symbols)} symbols")

        # Periodic universe refresh
        if int(self.config.universe_refresh_minutes) > 0:
            self._universe_task = asyncio.create_task(self._universe_refresh_loop())
    
    async def stop(self) -> None:
        """停止监测器"""
        self._running = False

        if self._universe_task:
            self._universe_task.cancel()
            await asyncio.gather(self._universe_task, return_exceptions=True)
            self._universe_task = None

        for task in self._worker_tasks:
            task.cancel()
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks = []

        if self._session:
            await self._session.close()
        if self._state_manager:
            await self._state_manager.close()
        logger.info("Anomaly Detector stopped")
    
    async def run(self) -> None:
        """主运行循环 (含重连机制)"""
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                if self._running:
                    logger.info(f"Reconnecting in {self._reconnect_delay:.1f}s...")
                    await asyncio.sleep(self._reconnect_delay)
                    # Exponential backoff
                    self._reconnect_delay = min(
                        self._reconnect_delay * 2,
                        self.config.max_reconnect_delay
                    )
            else:
                # Normal return means universe refresh requested; reconnect quickly
                self._reconnect_delay = self.config.reconnect_delay

    def _get_reference_timeframe(self) -> str:
        if self.config.universe_reference_timeframe and self.config.universe_reference_timeframe in self.config.timeframes:
            return str(self.config.universe_reference_timeframe)
        return self.config.timeframes[0] if self.config.timeframes else "15m"

    async def _fetch_24h_tickers(self) -> List[dict]:
        if not self._session:
            return []
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        async with self._session.get(url) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            return data if isinstance(data, list) else []

    async def _discover_ranked_symbols(self) -> List[str]:
        """Rank symbols by liquidity and return a candidate list (ordered)."""
        tickers = await self._fetch_24h_tickers()
        usdt_tickers = [t for t in tickers if str(t.get("symbol", "")).endswith("USDT")]

        min_qv = float(self.config.min_quote_volume_24h or 0.0)
        min_cnt = int(self.config.min_trade_count_24h or 0)

        def _qv(t: dict) -> float:
            try:
                return float(t.get("quoteVolume", 0) or 0)
            except Exception:
                return 0.0

        def _cnt(t: dict) -> int:
            try:
                return int(t.get("count", 0) or 0)
            except Exception:
                return 0

        filtered = []
        for t in usdt_tickers:
            sym = str(t.get("symbol", ""))
            if not sym or sym in self.config.exclude_symbols:
                continue
            if min_qv and _qv(t) < min_qv:
                continue
            if min_cnt and _cnt(t) < min_cnt:
                continue
            filtered.append(t)

        # Primary: quoteVolume; secondary: trade count
        filtered.sort(key=lambda x: (_qv(x), _cnt(x)), reverse=True)

        if self.config.scan_all:
            cap = int(self.config.universe_max_symbols or 300)
            cap = max(1, cap)
            filtered = filtered[:cap]
        else:
            pre_mult = max(1, int(self.config.universe_preselect_multiplier or 1))
            pre_n = max(int(self.config.top_n or 50), int(self.config.top_n or 50) * pre_mult)
            cap = max(1, int(self.config.universe_max_symbols or 300))
            filtered = filtered[: min(pre_n, cap)]

        return [str(t.get("symbol")) for t in filtered if t.get("symbol")]

    async def _discover_symbols(self) -> Set[str]:
        """Backwards-compatible: return universe as a set (unordered)."""
        return set(await self._discover_ranked_symbols())

    def _filter_symbols_by_ema_candidate(self, symbols: List[str]) -> List[str]:
        """Keep only symbols close to EMA200 on reference timeframe (uses prefetched cache)."""
        tf_ref = self._get_reference_timeframe()
        max_dist = float(self.config.candidate_max_ema200_distance_pct or 0.0)
        below_only = bool(self.config.candidate_below_ema200_only)

        if max_dist <= 0:
            return symbols

        kept: List[str] = []
        for sym in symbols:
            cache = self._data_cache.get(sym, {}).get(tf_ref)
            if not cache:
                continue

            closes = cache.get("closes") or []
            if not closes:
                continue

            ema = float(cache.get("ema200") or 0.0)
            if ema <= 0:
                continue

            last_close = float(closes[-1])
            if below_only and last_close >= ema:
                continue

            dist = abs(last_close - ema) / ema if ema else 1.0
            if dist <= max_dist:
                kept.append(sym)

        return kept

    async def _universe_refresh_loop(self) -> None:
        interval_s = max(10, int(self.config.universe_refresh_minutes) * 60)
        while self._running:
            await asyncio.sleep(interval_s)
            try:
                changed = await self._refresh_universe(initial=False)
                if changed:
                    self._universe_refresh_event.set()
            except Exception as e:
                logger.debug(f"Universe refresh error: {e}")

    async def _refresh_universe(self, *, initial: bool) -> bool:
        if not self._session:
            return False

        async with self._universe_lock:
            ranked = await self._discover_ranked_symbols()

            # Prefetch reference timeframe first (to apply EMA200 distance filter)
            tf_ref = self._get_reference_timeframe()
            to_prefetch_ref = []
            for s in ranked:
                if tf_ref not in (self._data_cache.get(s) or {}):
                    to_prefetch_ref.append(s)

            if to_prefetch_ref:
                await self._prefetch_timeframes_for_symbols(set(to_prefetch_ref), [tf_ref])

            filtered = self._filter_symbols_by_ema_candidate(ranked)

            target_n = int(self.config.universe_max_symbols or 300) if self.config.scan_all else int(self.config.top_n or 50)
            target_n = max(1, target_n)
            desired = filtered[:target_n]
            desired_set = set(desired)

            now = time.time()
            min_dwell_s = max(0, int(self.config.min_universe_dwell_minutes) * 60)

            if initial:
                self._symbols = desired_set
                for s in desired_set:
                    self._symbol_added_at.setdefault(s, now)
                # Ensure all monitored timeframes are prefetched for selected universe
                other_tfs = [tf for tf in self.config.timeframes if tf != tf_ref]
                if other_tfs:
                    await self._prefetch_timeframes_for_symbols(set(desired_set), other_tfs)
                return True

            current = set(self._symbols)
            if current == desired_set:
                return False

            # Determine removable symbols (respect min dwell)
            removable = []
            for s in sorted(current):
                if s in desired_set:
                    continue
                added_at = float(self._symbol_added_at.get(s, 0.0) or 0.0)
                if min_dwell_s and added_at and (now - added_at) < min_dwell_s:
                    continue
                removable.append(s)

            add_candidates = [s for s in desired if s not in current]

            max_add = max(0, int(self.config.universe_max_add_per_refresh or 0))
            max_remove = max(0, int(self.config.universe_max_remove_per_refresh or 0))
            if max_add:
                add_candidates = add_candidates[:max_add]
            if max_remove:
                removable = removable[:max_remove]

            # Don't remove too aggressively if we'd go below a reasonable universe size
            max_removable = max(0, len(current) + len(add_candidates) - max(1, min(target_n, len(desired_set))))
            if len(removable) > max_removable:
                removable = removable[:max_removable]

            new_set = (current - set(removable)) | set(add_candidates)
            if new_set == current:
                return False

            self._symbols = new_set
            for s in add_candidates:
                self._symbol_added_at[s] = now

            # Prefetch missing timeframes for newly added symbols
            if add_candidates:
                other_tfs = [tf for tf in self.config.timeframes if tf != tf_ref]
                if other_tfs:
                    await self._prefetch_timeframes_for_symbols(set(add_candidates), other_tfs)

            logger.info(
                f"Universe updated: {len(current)} -> {len(new_set)} "
                f"(+{len(add_candidates)}, -{len(removable)})"
            )
            return True
    
    async def _prefetch_all_data(self) -> None:
        """Backwards-compatible shim."""
        await self._prefetch_all_data_for_symbols(set(self._symbols))

    async def _prefetch_all_data_for_symbols(self, symbols: Set[str]) -> None:
        """Prefetch historical klines for a symbol set (rate limited)."""
        if not symbols:
            return

        logger.info(f"Prefetching historical data (rate limited) for {len(symbols)} symbols...")

        if not self._rest_semaphore:
            self._rest_semaphore = asyncio.Semaphore(self.config.rest_concurrency)

        tasks: List[asyncio.Task] = []
        for tf in self.config.timeframes:
            for symbol in symbols:
                tasks.append(asyncio.create_task(self._prefetch_symbol_data(symbol, tf)))

        completed = 0
        for fut in asyncio.as_completed(tasks):
            await fut
            completed += 1
            if completed % 50 == 0:
                logger.info(f"Prefetch progress: {completed}/{len(tasks)}")

        logger.info(f"Prefetch complete ({len(tasks)} requests)")

    async def _prefetch_timeframes_for_symbols(self, symbols: Set[str], timeframes: List[str]) -> None:
        """Prefetch klines for specific timeframes and symbols (rate limited)."""
        if not symbols or not timeframes:
            return

        if not self._rest_semaphore:
            self._rest_semaphore = asyncio.Semaphore(self.config.rest_concurrency)

        tasks: List[asyncio.Task] = []
        for tf in timeframes:
            for symbol in symbols:
                tasks.append(asyncio.create_task(self._prefetch_symbol_data(symbol, tf)))

        for fut in asyncio.as_completed(tasks):
            await fut
     
    async def _prefetch_symbol_data(self, symbol: str, timeframe: str) -> None:
        """预拉取单个币种数据 (带速率限制)"""
        # 使用信号量限制并发
        async with self._rest_semaphore:
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {
                "symbol": symbol,
                "interval": timeframe,
                "limit": self.config.kline_limit
            }
            
            try:
                async with self._session.get(url, params=params) as resp:
                    if resp.status != 200:
                        return
                    klines = await resp.json()
                
                closes = [float(k[4]) for k in klines]
                volumes = [float(k[5]) for k in klines]
                
                # OI history is warmed up from live sampling to avoid cold-start false positives
                
                # 初始化缓存
                if symbol not in self._data_cache:
                    self._data_cache[symbol] = {}
                
                ema_val = self._calc_ema(closes, self.config.ema_period)
                self._data_cache[symbol][timeframe] = {
                    "closes": closes,
                    "volumes": volumes,
                    "oi_history": [],
                    "oi_warmup_remaining": int(self.config.oi_lookback_bars),
                    "last_oi": None,
                    "prev_close": closes[-2] if len(closes) >= 2 else closes[-1],
                    "ema200": ema_val,
                    "ema200_prev": ema_val,
                }
                
            except Exception as e:
                logger.debug(f"Error prefetching {symbol} {timeframe}: {e}")
            
            # 请求延迟 (100ms) 避免触发 rate limit
            await asyncio.sleep(0.1)
    
    async def _fetch_oi(self, symbol: str) -> Optional[float]:
        """获取当前 OI (带并发限制 + 重试). 失败返回 None"""
        url = "https://fapi.binance.com/fapi/v1/openInterest"
        params = {"symbol": symbol}
        
        if not self._session or not self._oi_semaphore:
            return None

        now = time.time()
        ttl = float(self.config.oi_symbol_cache_seconds or 0.0)
        if ttl > 0:
            cached = self._oi_value_cache.get(symbol)
            if cached and (now - float(cached[0])) <= ttl:
                return float(cached[1])

        delay = float(self.config.oi_retry_base_delay)
        for attempt in range(1, int(self.config.oi_max_retries) + 1):
            try:
                async with self._oi_semaphore:
                    async with self._session.get(url, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            oi = data.get("openInterest")
                            value = float(oi) if oi is not None else None
                            if value is not None and ttl > 0:
                                self._oi_value_cache[symbol] = (now, float(value))
                            return value

                        if resp.status in (418, 429):
                            retry_after = resp.headers.get("Retry-After")
                            logger.warning(
                                f"OI API rate limited for {symbol} (status={resp.status}, retry_after={retry_after})"
                            )
                        else:
                            logger.debug(f"OI API non-200 for {symbol}: {resp.status} {await resp.text()}")
            except Exception as e:
                logger.debug(f"OI fetch error for {symbol} (attempt={attempt}): {e}")

            jitter = random.random() * 0.2 * delay
            await asyncio.sleep(min(delay + jitter, self.config.oi_retry_max_delay))
            delay = min(delay * 2, self.config.oi_retry_max_delay)

        return None
    
    def _calc_ema(self, closes: List[float], period: int) -> float:
        if len(closes) < period:
            return closes[-1] if closes else 0.0

        multiplier = 2 / (period + 1)
        ema = sum(closes[:period]) / period
        for price in closes[period:]:
            ema = (price - ema) * multiplier + ema
        return float(ema)

    def _ema_next(self, ema_prev: float, close_price: float, period: int) -> float:
        """EMA 递推更新"""
        multiplier = 2 / (period + 1)
        return float((close_price - ema_prev) * multiplier + ema_prev)
    
    async def _connect_and_listen(self) -> None:
        """
        连接 WebSocket 并监听
        
        多连接支持:
        - Binance 限制每个 WebSocket 最多 200 个流
        - 超过则拆分为多个连接并行运行
        """
        # 构建组合流
        streams = []
        for symbol in sorted(self._symbols):
            for tf in self.config.timeframes:
                streams.append(f"{symbol.lower()}@kline_{tf}")
        
        # 拆分为多个连接
        max_per_conn = self.config.max_streams_per_conn
        stream_chunks = [
            streams[i:i + max_per_conn]
            for i in range(0, len(streams), max_per_conn)
        ]
        
        logger.info(
            f"Connecting {len(stream_chunks)} WebSocket(s) "
            f"for {len(streams)} streams..."
        )
        
        if not stream_chunks:
            await asyncio.sleep(5)
            return

        # 并行运行所有 WebSocket 连接，并在 universe 更新时触发重建订阅
        ws_tasks = [
            asyncio.create_task(self._run_single_websocket(chunk, idx))
            for idx, chunk in enumerate(stream_chunks)
        ]
        refresh_task = asyncio.create_task(self._universe_refresh_event.wait())

        done, _pending = await asyncio.wait(
            [*ws_tasks, refresh_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if refresh_task in done:
            for t in ws_tasks:
                t.cancel()
            await asyncio.gather(*ws_tasks, return_exceptions=True)
            self._universe_refresh_event.clear()
            await asyncio.gather(refresh_task, return_exceptions=True)
            return

        # Unexpected: websocket tasks ended
        refresh_task.cancel()
        await asyncio.gather(*ws_tasks, return_exceptions=True)
        await asyncio.gather(refresh_task, return_exceptions=True)
    
    async def _run_single_websocket(self, streams: List[str], conn_id: int) -> None:
        """运行单个 WebSocket 连接"""
        url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
        
        while self._running:
            try:
                async with self._session.ws_connect(url) as ws:
                    logger.info(f"WebSocket #{conn_id} connected ({len(streams)} streams)")
                    self._reconnect_delay = self.config.reconnect_delay
                    
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = msg.json()
                            except Exception as e:
                                logger.debug(f"WebSocket message json error: {e}")
                                continue
                            await self._process_message(payload)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise Exception(f"WebSocket #{conn_id} error: {ws.exception()}")
            except Exception as e:
                logger.error(f"WebSocket #{conn_id} error: {e}")
                if self._running:
                    await asyncio.sleep(self._reconnect_delay)
                    self._reconnect_delay = min(
                        self._reconnect_delay * 2,
                        self.config.max_reconnect_delay
                    )
    
    async def _process_message(self, data: dict) -> None:
        """处理 WebSocket 消息 (只提取已收盘 K 线事件)"""
        try:
            kline = data.get("data", {}).get("k", {})
            if not kline or not kline.get("x", False):
                return

            if not self._event_queue:
                return

            try:
                self._event_queue.put_nowait(kline)
            except asyncio.QueueFull:
                logger.warning("Event queue full; dropping closed kline event")
        except Exception as e:
            logger.debug(f"Error processing message: {e}")

    async def _event_worker(self, worker_id: int) -> None:
        """后台处理已收盘 K 线事件"""
        assert self._event_queue is not None
        while True:
            kline = await self._event_queue.get()
            try:
                await self._handle_closed_kline(kline)
            except Exception as e:
                logger.debug(f"Event worker#{worker_id} error: {e}")
            finally:
                self._event_queue.task_done()

    async def _handle_closed_kline(self, kline: dict) -> None:
        symbol = kline.get("s")
        timeframe = kline.get("i")
        if not symbol or not timeframe:
            return

        cache = self._data_cache.get(symbol, {}).get(timeframe)
        if not cache:
            return

        lock_key = (symbol, timeframe)
        lock = self._pair_locks.get(lock_key)
        if lock is None:
            lock = asyncio.Lock()
            self._pair_locks[lock_key] = lock

        async with lock:
            close_price = float(kline.get("c", 0))
            volume = float(kline.get("v", 0))
            close_ts_ms = kline.get("T")
            cache["last_close_ts_ms"] = int(close_ts_ms) if isinstance(close_ts_ms, int) else None

            prev_close = cache["closes"][-1] if cache["closes"] else close_price

            ema_prev = float(cache.get("ema200") or self._calc_ema(cache["closes"], self.config.ema_period))
            cache["ema200_prev"] = ema_prev
            ema_curr = self._ema_next(ema_prev, close_price, self.config.ema_period)
            cache["ema200"] = ema_curr

            cache["closes"].append(close_price)
            cache["volumes"].append(volume)

            if len(cache["closes"]) > self.config.kline_limit:
                cache["closes"] = cache["closes"][-self.config.kline_limit:]
            if len(cache["volumes"]) > self.config.kline_limit:
                cache["volumes"] = cache["volumes"][-self.config.kline_limit:]

            current_oi = await self._fetch_oi(symbol)
            if current_oi is None:
                return

            cache["last_oi"] = current_oi

            oi_hist = cache.get("oi_history")
            if not isinstance(oi_hist, list):
                oi_hist = []

            oi_hist.append(float(current_oi))
            max_len = int(self.config.oi_lookback_bars) + 1
            if max_len < 2:
                max_len = 2

            if len(oi_hist) > max_len:
                oi_hist = oi_hist[-max_len:]
            cache["oi_history"] = oi_hist

            # Cold-start protection: wait until we have enough history
            warmup_remaining = int(cache.get("oi_warmup_remaining", self.config.oi_lookback_bars) or 0)
            cache["oi_warmup_remaining"] = max(0, warmup_remaining - 1)
            if len(oi_hist) < max_len:
                return

            old_oi = float(oi_hist[0])

            await self._detect_breakout(
                symbol,
                timeframe,
                close_price,
                prev_close,
                volume,
                float(current_oi),
                float(old_oi),
                cache,
            )
    
    async def _detect_breakout(
        self, symbol: str, timeframe: str,
        close_price: float, prev_close: float,
        volume: float, current_oi: float, old_oi: float,
        cache: Dict
    ) -> None:
        """检测突破信号 (三条件 AND)"""
        
        # 1. 检查: 价格向上突破 EMA200 (更严格：用上一根 EMA 与当前 EMA)
        ema_prev = cache.get("ema200_prev")
        ema_curr = cache.get("ema200") or self._calc_ema(cache["closes"], self.config.ema_period)
        if ema_prev is None:
            ema_prev = ema_curr

        broke_ema200_up = (prev_close < float(ema_prev)) and (close_price > float(ema_curr))
        
        if not broke_ema200_up:
            return  # 未突破, 不继续检测
        
        # 3. 检查: OI 增加 ≥ 阈值
        oi_change_pct = (current_oi - old_oi) / old_oi if old_oi > 0 else 0
        oi_spike = oi_change_pct >= self.config.oi_change_threshold
        
        if not oi_spike:
            return
        
        # 4. 检查: 量能放大
        if len(cache["volumes"]) < self.config.rvol_lookback + 1:
            return

        recent_volumes = cache["volumes"][-self.config.rvol_lookback:-1]  # 不含当前
        avg_volume = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
        rvol = volume / avg_volume if avg_volume > 0 else 0
        volume_spike = rvol >= self.config.rvol_threshold
        
        if not volume_spike:
            return
        
        # 所有条件满足! 触发信号
        signal = BreakoutSignal(
            symbol=symbol,
            timeframe=timeframe,
            price=close_price,
            ema200=float(ema_curr),
            oi_change_pct=oi_change_pct,
            rvol=rvol,
            timestamp=(
                datetime.fromtimestamp(cache.get("last_close_ts_ms", 0) / 1000, tz=timezone.utc).isoformat()
                if cache.get("last_close_ts_ms")
                else datetime.now(tz=timezone.utc).isoformat()
            ),
        )
        
        # 检查冷却并发送
        if await self._check_cooldown(symbol, timeframe):
            await self._send_alert(signal)
            await self._store_signal(signal)  # 存储信号用于排行
            logger.info(f"🚀 BREAKOUT: {symbol} ({timeframe}) OI+{oi_change_pct:.2%} RVol={rvol:.1f}x")
    
    async def _store_signal(self, signal: BreakoutSignal) -> None:
        """存储信号到 Redis (使用 RedisStateManager)"""
        await self._state_manager.store_breakout_signal(
            symbol=signal.symbol,
            oi_change_pct=signal.oi_change_pct,
            rvol=signal.rvol,
            price=signal.price,
            ema200=signal.ema200,
            timeframe=signal.timeframe,
            prefix=self.config.redis_prefix,
            ttl_days=5
        )
    
    async def _check_cooldown(self, symbol: str, timeframe: str) -> bool:
        """检查并设置冷却 (使用 RedisStateManager)"""
        tf_key = "ALL" if self.config.cooldown_across_timeframes else timeframe
        return await self._state_manager.check_anomaly_cooldown(
            symbol=symbol,
            timeframe=tf_key,
            cooldown_seconds=self.config.cooldown_seconds,
            prefix=self.config.redis_prefix
        )
    
    async def _send_alert(self, signal: BreakoutSignal) -> None:
        """发送 Telegram 报警"""
        if not self.config.telegram_bot_token or not self.config.telegram_chat_id:
            logger.info(f"Alert (no TG): {signal.symbol} {signal.timeframe}")
            return
        
        url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
        payload = {
            "chat_id": self.config.telegram_chat_id,
            "text": signal.format_message(),
            "parse_mode": "Markdown",
        }
        
        # 添加话题 ID (如果配置了)
        if self.config.telegram_topic_id:
            payload["message_thread_id"] = self.config.telegram_topic_id
        
        try:
            async with self._session.post(url, json=payload) as resp:
                if resp.status != 200:
                    logger.warning(f"Telegram send failed: {await resp.text()}")
                else:
                    logger.info(f"Alert sent: {signal.symbol} {signal.timeframe}")
        except Exception as e:
            logger.error(f"Telegram error: {e}")


async def run_anomaly_detector(config: Optional[AnomalyConfig] = None) -> None:
    """运行异动监测器"""
    detector = AnomalyDetector(config)
    await detector.start()
    try:
        await detector.run()
    finally:
        await detector.stop()


# ==================== 上涨指数排行 ====================

@dataclass
class RisingIndexConfig:
    """上涨指数配置"""
    redis_url: str = "redis://localhost:6379"
    redis_prefix: str = "anomaly"
    
    # 权重
    price_structure_weight: float = 0.35
    oi_flow_weight: float = 0.30
    recency_weight: float = 0.20
    volume_weight: float = 0.15
    
    # 时间衰减
    recency_decay_hours: float = 24.0  # 24小时后信号权重衰减一半
    
    # 信号保留时间
    signal_ttl_days: int = 5
    
    # EMA 周期
    ema_periods: List[int] = field(default_factory=lambda: [21, 55, 100, 200])


@dataclass
class RisingScore:
    """上涨指数得分"""
    symbol: str
    total_score: float
    price_structure_score: float
    oi_flow_score: float
    recency_score: float
    volume_score: float
    signal_count: int
    cumulative_oi_change: float
    price_change_5d: float
    ema_alignment: str  # 'bullish', 'bearish', 'neutral'
    
    def format_text(self, rank: int) -> str:
        """格式化为文本"""
        trend_emoji = "↗" if self.ema_alignment == "bullish" else "↘" if self.ema_alignment == "bearish" else "→"
        return (
            f"{rank}. {self.symbol.replace('USDT', '')} "
            f"⚡{self.total_score:.1f} "
            f"资金{'+' if self.cumulative_oi_change > 0 else ''}{self.cumulative_oi_change:.1%} | "
            f"趋势{trend_emoji}"
        )


class RisingIndex:
    """
    上涨指数排行系统
    
    评分算法:
    - 价格结构 (35%): 更高低点、EMA 排列、EMA200 距离、突破持续性
    - 资金流入 (30%): 累计 OI 变化、OI 增加一致性
    - 动量新鲜度 (20%): 近期信号权重更高 (指数衰减)
    - 成交量持续性 (15%): 平均放量倍数
    """
    
    def __init__(self, config: Optional[RisingIndexConfig] = None):
        self.config = config or RisingIndexConfig()
        self._state_manager: Optional[RedisStateManager] = None
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def connect(self) -> None:
        """连接 Redis (使用 RedisStateManager)"""
        self._state_manager = RedisStateManager(redis_url=self.config.redis_url)
        await self._state_manager.connect()
        self._session = aiohttp.ClientSession()
    
    async def close(self) -> None:
        """关闭连接"""
        if self._session:
            await self._session.close()
        if self._state_manager:
            await self._state_manager.close()
    
    async def store_signal(self, signal: BreakoutSignal) -> None:
        """存储信号到 Redis (使用 RedisStateManager)"""
        await self._state_manager.store_breakout_signal(
            symbol=signal.symbol,
            oi_change_pct=signal.oi_change_pct,
            rvol=signal.rvol,
            price=signal.price,
            ema200=signal.ema200,
            timeframe=signal.timeframe,
            prefix=self.config.redis_prefix,
            ttl_days=self.config.signal_ttl_days
        )
    
    async def get_ranking(self, top_n: int = 20) -> List[RisingScore]:
        """获取上涨指数排行"""
        # 获取所有有信号的币种
        keys = await self._state_manager.scan_signal_keys(prefix=self.config.redis_prefix)
        
        scores = []
        for key in keys:
            symbol = key.split(":")[-1]
            score = await self._calc_symbol_score(symbol)
            if score:
                scores.append(score)
        
        # 按总分排序
        scores.sort(key=lambda x: x.total_score, reverse=True)
        return scores[:top_n]
    
    async def _calc_symbol_score(self, symbol: str) -> Optional[RisingScore]:
        """计算单个币种的上涨指数"""
        # 获取所有信号 (使用 RedisStateManager)
        now = time.time()
        signals = await self._state_manager.get_breakout_signals(
            symbol=symbol,
            prefix=self.config.redis_prefix
        )
        
        if not signals:
            return None
        
        # 解析信号
        oi_changes = []
        rvols = []
        recency_weights = []
        prices = []
        ema200s = []
        
        for signal_data, timestamp in signals:
            parts = signal_data.split("|")
            if len(parts) >= 4:
                oi_change = float(parts[0])
                rvol = float(parts[1])
                price = float(parts[2])
                ema200 = float(parts[3])
                
                oi_changes.append(oi_change)
                rvols.append(rvol)
                prices.append(price)
                ema200s.append(ema200)
                
                # 时间衰减权重
                hours_ago = (now - timestamp) / 3600
                decay = np.exp(-hours_ago / self.config.recency_decay_hours)
                recency_weights.append(decay)
        
        if not oi_changes:
            return None
        
        # 1. 资金流入得分 (30%)
        cumulative_oi = sum(oi_changes)
        oi_consistency = sum(1 for oi in oi_changes if oi > 0) / len(oi_changes)
        oi_score = self._normalize(cumulative_oi, 0, 0.5) * 0.6 + oi_consistency * 0.4
        
        # 2. 动量新鲜度得分 (20%)
        recency_score = sum(recency_weights) / len(recency_weights)
        
        # 3. 成交量得分 (15%)
        avg_rvol = sum(rvols) / len(rvols)
        volume_score = self._normalize(avg_rvol, 1, 10)
        
        # 4. 价格结构得分 (35%) - 需要获取当前市场数据
        price_score, ema_alignment, price_change = await self._calc_price_structure(
            symbol, prices[-1] if prices else 0, ema200s[-1] if ema200s else 0
        )
        
        # 加权总分
        total = (
            self.config.price_structure_weight * price_score +
            self.config.oi_flow_weight * oi_score +
            self.config.recency_weight * recency_score +
            self.config.volume_weight * volume_score
        ) * 100
        
        return RisingScore(
            symbol=symbol,
            total_score=total,
            price_structure_score=price_score * 100,
            oi_flow_score=oi_score * 100,
            recency_score=recency_score * 100,
            volume_score=volume_score * 100,
            signal_count=len(signals),
            cumulative_oi_change=cumulative_oi,
            price_change_5d=price_change,
            ema_alignment=ema_alignment,
        )
    
    async def _calc_price_structure(
        self, symbol: str, last_signal_price: float, last_ema200: float
    ) -> tuple:
        """计算价格结构得分"""
        try:
            # 获取最新价格和 K 线
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {"symbol": symbol, "interval": "4h", "limit": 50}
            
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    return 0.5, "neutral", 0
                klines = await resp.json()
            
            closes = [float(k[4]) for k in klines]
            current_price = closes[-1]
            
            # 计算 EMAs
            ema21 = self._calc_ema(closes, 21)
            ema55 = self._calc_ema(closes, 55)
            ema100 = self._calc_ema(closes, 100)
            ema200 = self._calc_ema(closes, 200) if len(closes) >= 200 else last_ema200
            
            # EMA 排列检查
            if ema21 > ema55 > ema100:
                ema_alignment = "bullish"
                alignment_score = 1.0
            elif ema21 < ema55 < ema100:
                ema_alignment = "bearish"
                alignment_score = 0.0
            else:
                ema_alignment = "neutral"
                alignment_score = 0.5
            
            # EMA200 距离
            ema200_distance = (current_price - ema200) / ema200 if ema200 > 0 else 0
            distance_score = self._normalize(ema200_distance, -0.1, 0.3)
            
            # 更高低点检测 (简化: 检查最近 5 个低点)
            lows = [float(k[3]) for k in klines]
            recent_lows = lows[-10:]
            higher_lows = sum(1 for i in range(1, len(recent_lows)) if recent_lows[i] > recent_lows[i-1])
            hl_score = higher_lows / max(len(recent_lows) - 1, 1)
            
            # 5日价格变化
            if len(closes) >= 30:
                price_5d_ago = closes[-30]  # 约5天 (4h * 30 = 120h)
                price_change = (current_price - price_5d_ago) / price_5d_ago
            else:
                price_change = 0
            
            # 综合价格结构得分
            price_score = (
                0.30 * hl_score +
                0.30 * alignment_score +
                0.25 * distance_score +
                0.15 * self._normalize(price_change, -0.1, 0.3)
            )
            
            return price_score, ema_alignment, price_change
            
        except Exception as e:
            logger.debug(f"Error calculating price structure for {symbol}: {e}")
            return 0.5, "neutral", 0
    
    def _calc_ema(self, closes: List[float], period: int) -> float:
        """计算 EMA"""
        if len(closes) < period:
            return closes[-1] if closes else 0
        
        multiplier = 2 / (period + 1)
        ema = sum(closes[:period]) / period
        
        for price in closes[period:]:
            ema = (price - ema) * multiplier + ema
        
        return ema
    
    def _normalize(self, value: float, min_val: float, max_val: float) -> float:
        """归一化到 0-1"""
        if max_val == min_val:
            return 0.5
        return max(0, min(1, (value - min_val) / (max_val - min_val)))
    
    def format_ranking(self, scores: List[RisingScore]) -> str:
        """格式化排行榜"""
        lines = ["🏆 *上涨潜力排行 (5日评估)*", "━" * 25]
        for i, score in enumerate(scores, 1):
            lines.append(score.format_text(i))
        return "\n".join(lines)


async def get_rising_ranking(
    redis_url: str = "redis://localhost:6379",
    top_n: int = 20
) -> List[RisingScore]:
    """便捷方法: 获取上涨指数排行"""
    config = RisingIndexConfig(redis_url=redis_url)
    index = RisingIndex(config)
    await index.connect()
    try:
        return await index.get_ranking(top_n)
    finally:
        await index.close()


async def scheduled_ranking_push(
    bot_token: str,
    chat_id: str,
    redis_url: str = "redis://localhost:6379",
    interval_hours: int = 4,
    top_n: int = 10
) -> None:
    """
    定时推送上涨指数排行到 Telegram
    
    每 N 小时推送 Top M 币种排行榜
    """
    import aiohttp
    
    async def push_ranking():
        try:
            config = RisingIndexConfig(redis_url=redis_url)
            index = RisingIndex(config)
            await index.connect()
            
            try:
                scores = await index.get_ranking(top_n)
                if not scores:
                    logger.info("No ranking data to push")
                    return
                
                message = index.format_ranking(scores)
                
                async with aiohttp.ClientSession() as session:
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    payload = {
                        "chat_id": chat_id,
                        "text": message,
                        "parse_mode": "Markdown",
                    }
                    async with session.post(url, json=payload) as resp:
                        if resp.status == 200:
                            logger.info(f"Ranking pushed to Telegram (Top {top_n})")
                        else:
                            logger.warning(f"Push failed: {await resp.text()}")
            finally:
                await index.close()
        except Exception as e:
            logger.error(f"Ranking push error: {e}")
    
    while True:
        await push_ranking()
        await asyncio.sleep(interval_hours * 3600)


# 兼容旧的 import 路径: anomaly_detector.RisingIndex*
from .rising_index import (  # noqa: E402
    RisingIndex,
    RisingIndexConfig,
    RisingScore,
    get_rising_ranking,
    scheduled_ranking_push,
)


async def main():
    """主函数"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    config = AnomalyConfig.from_env()
    
    print("=" * 60)
    print("🔍 Real-time Breakout Detector")
    print("=" * 60)
    print(f"⏰ Timeframes: {', '.join(config.timeframes)}")
    print(f"📈 EMA Period: {config.ema_period}")
    print(f"🔥 OI Threshold: +{config.oi_change_threshold:.0%}")
    print(f"🌊 RVol Threshold: {config.rvol_threshold}x")
    print(f"⏱️ Cooldown: {config.cooldown_seconds // 60} minutes")
    print(f"📱 Telegram: {'✅' if config.telegram_bot_token else '❌'}")
    print(f"🔴 Redis: {config.redis_url}")
    print("=" * 60)
    print()
    print("Conditions for signal:")
    print("  1. Price breaks above EMA200")
    print("  2. OI increases ≥ 3%")
    print("  3. Volume ≥ 3x average")
    print()
    
    await run_anomaly_detector(config)


if __name__ == "__main__":
    asyncio.run(main())

