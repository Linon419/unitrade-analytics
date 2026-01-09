"""
Squeeze Momentum Scanner (波动预警扫描器)

检测 BB/KC 挤压信号 (Bollinger Bands 收缩进入 Keltner Channels):
- L1: 基础挤压警报 (BB 收缩进入 KC，持续 N 根 K 线)
- L2: 强烈挤压警报 (额外收缩，tightness ratio 低于阈值)

挤压释放通常预示着大幅波动即将到来。
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np

try:
    import pandas as pd
    import pandas_ta as ta
except ModuleNotFoundError as e:  # pragma: no cover
    raise ModuleNotFoundError(
        "SqueezeMomentumScanner requires optional dependencies `pandas` and `pandas_ta`. "
        "Install with `pip install -e \".[ta]\"` (or install them manually), "
        "or keep `squeeze_scanner.enabled=false` / related features disabled."
    ) from e

from unitrade.order.rate_limiter import BinanceRateLimiter

logger = logging.getLogger(__name__)


@dataclass
class SqueezeConfig:
    """Squeeze Scanner 配置"""
    # 时间周期
    timeframes: List[str] = field(default_factory=lambda: ["1h", "4h", "1d"])
    
    # BB/KC 参数
    squeeze_len: int = 20        # BB/KC 计算周期
    bb_mult: float = 2.0         # BB 标准差乘数
    kc_mult: float = 1.5         # KC ATR 乘数
    atr_len: int = 20            # ATR 周期
    
    # 触发条件
    min_squeeze_duration: int = 5      # 最少持续 K 线数
    tightness_l2_ratio: float = 0.80   # L2 级别的 BB/KC 宽度比
    
    # 动量参数 (可选, 用于判断挤压释放方向)
    momentum_len: int = 12
    
    # 币种筛选
    auto_top_n: int = 50
    extra_whitelist: List[str] = field(default_factory=list)
    kline_limit: int = 200

    # Liquidity filters (optional; 0 disables)
    min_quote_volume_24h: float = 0.0
    min_trade_count_24h: int = 0

    # Symbol universe caching (default 30m; matches recommended scan interval)
    symbol_discovery_cache_seconds: int = 1800
    
    # 并发控制
    max_concurrent: int = 20

    # REST safety (anti 418/429)
    request_timeout_seconds: float = 15.0
    min_request_interval_ms: int = 80
    request_jitter_ms: int = 40
    max_retries: int = 2
    retry_base_delay: float = 0.5
    retry_max_delay: float = 5.0

    # Release -> "volatility start" confirmation (reduce false releases)
    release_min_confirmations: int = 2
    release_require_atr_rise: bool = True
    release_atr_rise_pct: float = 0.05            # ATR up >= 5%
    release_require_range_atr: bool = True
    release_range_atr_mult: float = 1.2           # candle range >= 1.2 * ATR
    release_require_bb_expand: bool = True
    release_bb_expand_pct: float = 0.10           # BB width up >= 10%

    # De-dup (per closed candle) and cooldown
    signal_dedupe: bool = True
    signal_cooldown_seconds: int = 0              # 0 disables extra cooldown (per-candle dedupe still applies)
    
    @classmethod
    def from_config(cls, config) -> "SqueezeConfig":
        """从全局 Config 对象创建"""
        if isinstance(config, dict):
            sq = config.get("squeeze_scanner", {})
        else:
            sq = config.squeeze_scanner if hasattr(config, "squeeze_scanner") else {}
        if isinstance(sq, dict):
            return cls(
                timeframes=sq.get("timeframes", ["1h", "4h", "1d"]),
                squeeze_len=sq.get("squeeze_len", 20),
                bb_mult=sq.get("bb_mult", 2.0),
                kc_mult=sq.get("kc_mult", 1.5),
                atr_len=sq.get("atr_len", 20),
                min_squeeze_duration=sq.get("min_squeeze_duration", 5),
                tightness_l2_ratio=sq.get("tightness_l2_ratio", 0.80),
                momentum_len=sq.get("momentum_len", 12),
                auto_top_n=sq.get("top_n", 50),
                extra_whitelist=sq.get("extra_whitelist", []),
                kline_limit=sq.get("kline_limit", 200),
                max_concurrent=sq.get("max_concurrent", 20),
                min_quote_volume_24h=float(sq.get("min_quote_volume_24h", 0.0) or 0.0),
                min_trade_count_24h=int(sq.get("min_trade_count_24h", 0) or 0),
                symbol_discovery_cache_seconds=int(sq.get("symbol_discovery_cache_seconds", 1800) or 1800),
                request_timeout_seconds=float(sq.get("request_timeout_seconds", 15.0) or 15.0),
                min_request_interval_ms=int(sq.get("min_request_interval_ms", 80) or 0),
                request_jitter_ms=int(sq.get("request_jitter_ms", 40) or 0),
                max_retries=int(sq.get("max_retries", 2) or 0),
                retry_base_delay=float(sq.get("retry_base_delay", 0.5) or 0.5),
                retry_max_delay=float(sq.get("retry_max_delay", 5.0) or 5.0),
                release_min_confirmations=int(sq.get("release_min_confirmations", 2) or 2),
                release_require_atr_rise=bool(sq.get("release_require_atr_rise", True)),
                release_atr_rise_pct=float(sq.get("release_atr_rise_pct", 0.05) or 0.0),
                release_require_range_atr=bool(sq.get("release_require_range_atr", True)),
                release_range_atr_mult=float(sq.get("release_range_atr_mult", 1.2) or 0.0),
                release_require_bb_expand=bool(sq.get("release_require_bb_expand", True)),
                release_bb_expand_pct=float(sq.get("release_bb_expand_pct", 0.10) or 0.0),
                signal_dedupe=bool(sq.get("signal_dedupe", True)),
                signal_cooldown_seconds=int(sq.get("signal_cooldown_seconds", 0) or 0),
            )
        return cls()


@dataclass
class SqueezeSignal:
    """Squeeze 信号结果"""
    symbol: str
    timeframe: str
    signal_type: str  # squeeze_l1, squeeze_l2, squeeze_release
    price: float
    timestamp: str
    squeeze_duration: int = 0        # 挤压持续周期
    tightness_ratio: float = 0.0     # BB/KC 宽度比
    momentum: float = 0.0            # 动量值 (正=向上, 负=向下)
    momentum_direction: str = ""     # up, down, neutral
    
    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "signal_type": self.signal_type,
            "price": float(self.price),
            "timestamp": self.timestamp,
            "squeeze_bars": int(self.squeeze_duration),
            "tightness": round(float(self.tightness_ratio), 3),
            "momentum": round(float(self.momentum), 2),
            "direction": self.momentum_direction,
        }
    
    def format_text(self) -> str:
        """格式化为文本"""
        emoji_map = {
            "squeeze_l1": "🟡",
            "squeeze_l2": "🔴",
            "squeeze_release": "🚀",
        }
        emoji = emoji_map.get(self.signal_type, "📊")
        
        if self.signal_type == "squeeze_l1":
            text = f"{emoji} {self.symbol} ({self.timeframe}) 波动预警L1 ({self.squeeze_duration}bars)"
        elif self.signal_type == "squeeze_l2":
            text = f"{emoji} {self.symbol} ({self.timeframe}) 波动预警L2 ({self.squeeze_duration}bars, ratio={self.tightness_ratio:.2f})"
        elif self.signal_type == "squeeze_release":
            dir_emoji = "📈" if self.momentum > 0 else "📉" if self.momentum < 0 else ""
            text = f"{emoji} {self.symbol} ({self.timeframe}) 挤压释放 {dir_emoji}"
        else:
            text = f"{emoji} {self.symbol} ({self.timeframe}) {self.signal_type}"
        
        return text


class SqueezeMomentumScanner:
    """BB/KC 波动预警扫描器"""
    
    def __init__(self, config: Optional[SqueezeConfig] = None):
        self.config = config or SqueezeConfig()
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore: Optional[asyncio.Semaphore] = None

        self._rate_limiter = BinanceRateLimiter()
        self._blocked_until: float = 0.0  # circuit breaker on 418/429

        self._request_pacer_lock = asyncio.Lock()
        self._next_request_time: float = 0.0

        self._symbols_cache: List[str] = []
        self._symbols_cache_at: float = 0.0

        # Signal de-dup / cooldown (in-process)
        self._last_emitted_bar_open_ms: Dict[Tuple[str, str, str], int] = {}
        self._last_emitted_ts: Dict[Tuple[str, str, str], float] = {}

    def _should_emit(self, symbol: str, timeframe: str, signal_type: str, bar_open_ms: int) -> bool:
        key = (symbol, timeframe, signal_type)

        if self.config.signal_dedupe:
            last_bar = self._last_emitted_bar_open_ms.get(key)
            if last_bar is not None and int(last_bar) == int(bar_open_ms):
                return False

        cooldown = int(self.config.signal_cooldown_seconds or 0)
        if cooldown > 0:
            now = time.time()
            last_ts = self._last_emitted_ts.get(key)
            if last_ts is not None and (now - float(last_ts)) < cooldown:
                return False

        self._last_emitted_bar_open_ms[key] = int(bar_open_ms)
        self._last_emitted_ts[key] = time.time()
        return True
    
    async def start(self) -> None:
        """启动扫描器"""
        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout_seconds)
        self._session = aiohttp.ClientSession(timeout=timeout)
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        logger.info("Squeeze Momentum Scanner started")
    
    async def stop(self) -> None:
        """停止扫描器"""
        if self._session:
            await self._session.close()
        logger.info("Squeeze Momentum Scanner stopped")
    
    async def scan(self, timeframe: str = "1h", symbols: Optional[List[str]] = None) -> Dict[str, List[SqueezeSignal]]:
        """
        扫描指定时间周期的所有挤压信号
        
        Returns:
            {
                "squeeze_l1": [...],
                "squeeze_l2": [...],
                "squeeze_release": [...],
            }
        """
        logger.info(f"Starting Squeeze scan for {timeframe}...")

        if time.time() < self._blocked_until:
            retry_after = int(max(0.0, self._blocked_until - time.time()))
            logger.warning(f"Binance blocked (418/429). Skipping scan for {retry_after}s.")
            return {"squeeze_l1": [], "squeeze_l2": [], "squeeze_release": []}

        symbols = symbols or await self._discover_symbols()
        logger.info(f"Scanning {len(symbols)} symbols...")
        
        all_signals = await self._analyze_batch(symbols, timeframe)
        
        result = {
            "squeeze_l1": [s for s in all_signals if s.signal_type == "squeeze_l1"],
            "squeeze_l2": [s for s in all_signals if s.signal_type == "squeeze_l2"],
            "squeeze_release": [s for s in all_signals if s.signal_type == "squeeze_release"],
        }
        
        logger.info(
            f"Scan complete: {len(result['squeeze_l1'])} L1, "
            f"{len(result['squeeze_l2'])} L2, "
            f"{len(result['squeeze_release'])} releases"
        )
        
        return result
    
    async def scan_all_timeframes(self) -> Dict[str, Dict[str, List[SqueezeSignal]]]:
        """扫描所有配置的时间周期"""
        results = {}
        symbols = await self._discover_symbols()
        for tf in self.config.timeframes:
            results[tf] = await self.scan(tf, symbols=symbols)
        return results

    async def _pace_request(self) -> None:
        min_interval = max(0.0, float(self.config.min_request_interval_ms) / 1000.0)
        jitter = max(0.0, float(self.config.request_jitter_ms) / 1000.0)
        if min_interval <= 0:
            return

        async with self._request_pacer_lock:
            now = time.time()
            if now < self._next_request_time:
                await asyncio.sleep(self._next_request_time - now)
            self._next_request_time = time.time() + min_interval + random.random() * jitter

    async def _get_json(self, endpoint: str, params: Optional[dict] = None):
        if not self._session:
            raise RuntimeError("Scanner not started")

        if time.time() < self._blocked_until:
            return None

        base_url = "https://fapi.binance.com"
        url = f"{base_url}{endpoint}"

        max_attempts = max(1, int(self.config.max_retries) + 1)
        for attempt in range(1, max_attempts + 1):
            if time.time() < self._blocked_until:
                return None

            try:
                await self._pace_request()
                await self._rate_limiter.acquire(endpoint, params=params)

                async with self._session.get(url, params=params) as resp:
                    if resp.status == 200:
                        return await resp.json()

                    if resp.status in (418, 429):
                        raw_retry_after = (resp.headers.get("Retry-After") or "60").strip()
                        try:
                            retry_after = int(float(raw_retry_after))
                        except Exception:
                            retry_after = 60

                        self._rate_limiter.handle_429(retry_after)
                        self._blocked_until = max(self._blocked_until, time.time() + retry_after)
                        logger.warning(
                            f"Binance rate limited/banned (HTTP {resp.status}), retry_after={retry_after}s, endpoint={endpoint}"
                        )
                        return None

                    if 500 <= resp.status < 600:
                        raise aiohttp.ClientResponseError(
                            request_info=resp.request_info,
                            history=resp.history,
                            status=resp.status,
                            message=await resp.text(),
                            headers=resp.headers,
                        )

                    logger.debug(f"Binance request failed (status={resp.status}, endpoint={endpoint})")
                    return None

            except asyncio.TimeoutError:
                err = "timeout"
            except aiohttp.ClientError as e:
                err = str(e)
            except Exception as e:
                err = str(e)

            if attempt >= max_attempts:
                logger.debug(f"Binance request failed after retries: {endpoint} ({err})")
                return None

            delay = min(
                float(self.config.retry_max_delay),
                float(self.config.retry_base_delay) * (2 ** (attempt - 1)),
            )
            delay *= 1.0 + random.random() * 0.2
            await asyncio.sleep(delay)
    
    async def _discover_symbols(self) -> List[str]:
        """获取 Top N 交易量币种 + 白名单"""
        now = time.time()
        if (
            self._symbols_cache
            and self.config.symbol_discovery_cache_seconds > 0
            and (now - self._symbols_cache_at) < float(self.config.symbol_discovery_cache_seconds)
        ):
            return list(self._symbols_cache)

        try:
            tickers = await self._get_json("/fapi/v1/ticker/24hr")
            if not isinstance(tickers, list):
                return []
            
            usdt_tickers = [t for t in tickers if t["symbol"].endswith("USDT")]
            if self.config.min_quote_volume_24h > 0:
                usdt_tickers = [
                    t for t in usdt_tickers
                    if float(t.get("quoteVolume", 0) or 0) >= float(self.config.min_quote_volume_24h)
                ]
            if self.config.min_trade_count_24h > 0:
                usdt_tickers = [
                    t for t in usdt_tickers
                    if int(float(t.get("count", 0) or 0)) >= int(self.config.min_trade_count_24h)
                ]
            usdt_tickers.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
            
            top_symbols = [t["symbol"] for t in usdt_tickers[:self.config.auto_top_n]]
            
            all_symbols = set(top_symbols)
            for s in self.config.extra_whitelist:
                if s.upper() not in all_symbols:
                    all_symbols.add(s.upper())

            symbols = sorted(all_symbols)
            self._symbols_cache = symbols
            self._symbols_cache_at = now
            return list(symbols)
        except Exception as e:
            logger.error(f"Symbol discovery error: {e}")
            return []
    
    async def _analyze_batch(self, symbols: List[str], timeframe: str) -> List[SqueezeSignal]:
        """批量分析"""
        tasks = [self._analyze_symbol(symbol, timeframe) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        signals = []
        for result in results:
            if isinstance(result, list):
                signals.extend(result)
        
        return signals
    
    async def _analyze_symbol(self, symbol: str, timeframe: str) -> List[SqueezeSignal]:
        """分析单个币种"""
        async with self._semaphore:
            try:
                klines = await self._fetch_klines(symbol, timeframe, self.config.kline_limit)
                if len(klines) < 50:
                    return []
                
                df = pd.DataFrame(klines, columns=[
                    'time', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                    'taker_buy_quote', 'ignore'
                ])
                df['time'] = pd.to_datetime(df['time'], unit='ms')
                df['open'] = df['open'].astype(float)
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                df['close'] = df['close'].astype(float)
                df['volume'] = df['volume'].astype(float)
                
                # 丢弃未完成的 K 线
                interval_ms = self._interval_to_ms(timeframe)
                if interval_ms > 0 and len(df) > 0:
                    now_ms = int(datetime.utcnow().timestamp() * 1000)
                    last_open_ms = int(df['time'].iloc[-1].timestamp() * 1000)
                    if last_open_ms + interval_ms > now_ms:
                        df = df.iloc[:-1]
                
                if len(df) < 50:
                    return []
                
                signals = self._detect_squeeze(df, symbol, timeframe)
                return signals
                
            except Exception as e:
                logger.debug(f"Error analyzing {symbol}: {e}")
                return []
    
    def _detect_squeeze(self, df: pd.DataFrame, symbol: str, timeframe: str) -> List[SqueezeSignal]:
        """检测 BB/KC 挤压信号"""
        signals = []
        n = len(df)
        cfg = self.config
        
        if n < max(cfg.squeeze_len, cfg.atr_len) + 2:
            return signals
        
        close = df['close']
        current_price = close.iloc[-1]

        last_bar_open_ms = int(df["time"].iloc[-1].timestamp() * 1000)
        signal_ts = df["time"].iloc[-1].to_pydatetime().isoformat()
        
        # 计算 Keltner Channels (EMA + ATR)
        atr_val = ta.atr(df['high'], df['low'], df['close'], length=cfg.atr_len)
        kc_mid = ta.ema(close, length=cfg.squeeze_len)
        kc_top = kc_mid + cfg.kc_mult * atr_val
        kc_bot = kc_mid - cfg.kc_mult * atr_val
        
        # 计算 Bollinger Bands
        bb_mid = ta.sma(close, length=cfg.squeeze_len)
        bb_std = ta.stdev(close, length=cfg.squeeze_len)
        bb_top = bb_mid + cfg.bb_mult * bb_std
        bb_bot = bb_mid - cfg.bb_mult * bb_std
        
        # 挤压条件: BB 在 KC 内部
        is_squeeze = (bb_top < kc_top) & (bb_bot > kc_bot)
        
        # 计算动量 (线性回归斜率)
        momentum = self._calc_momentum(df, cfg.momentum_len)
        
        def _streak(series: pd.Series, idx: int) -> int:
            """计算连续 True 的数量"""
            count = 0
            for i in range(idx, -1, -1):
                if bool(series.iloc[i]):
                    count += 1
                else:
                    break
            return count
        
        curr_count = _streak(is_squeeze, n - 1)
        prev_count = _streak(is_squeeze, n - 2) if n >= 2 else 0
        
        # 计算 tightness ratio (BB 宽度 / KC 宽度)
        kc_width = kc_top - kc_bot
        bb_width = bb_top - bb_bot
        tightness_ratio = bb_width / kc_width.replace(0, np.nan)
        curr_tightness = float(tightness_ratio.iloc[-1]) if not np.isnan(tightness_ratio.iloc[-1]) else 1.0
        prev_tightness = float(tightness_ratio.iloc[-2]) if n >= 2 and not np.isnan(tightness_ratio.iloc[-2]) else 1.0
        
        curr_momentum = momentum.iloc[-1] if len(momentum) > 0 else 0
        momentum_dir = "up" if curr_momentum > 0 else "down" if curr_momentum < 0 else "neutral"
        
        # 判断信号
        curr_squeeze = bool(is_squeeze.iloc[-1])
        prev_squeeze = bool(is_squeeze.iloc[-2]) if n >= 2 else False
        
        vol_warning_l1 = curr_squeeze and curr_count >= cfg.min_squeeze_duration
        vol_warning_l2 = vol_warning_l1 and curr_tightness <= cfg.tightness_l2_ratio
        
        prev_vol_warning_l1 = prev_squeeze and prev_count >= cfg.min_squeeze_duration
        prev_vol_warning_l2 = prev_vol_warning_l1 and prev_tightness <= cfg.tightness_l2_ratio
        
        # L2 优先级高于 L1
        if vol_warning_l2 and not prev_vol_warning_l2:
            if self._should_emit(symbol, timeframe, "squeeze_l2", last_bar_open_ms):
                signals.append(SqueezeSignal(
                    symbol=symbol,
                    timeframe=timeframe,
                    signal_type="squeeze_l2",
                    price=current_price,
                    timestamp=signal_ts,
                    squeeze_duration=curr_count,
                    tightness_ratio=curr_tightness,
                    momentum=float(curr_momentum),
                    momentum_direction=momentum_dir,
                ))
        elif vol_warning_l1 and not prev_vol_warning_l1:
            if self._should_emit(symbol, timeframe, "squeeze_l1", last_bar_open_ms):
                signals.append(SqueezeSignal(
                    symbol=symbol,
                    timeframe=timeframe,
                    signal_type="squeeze_l1",
                    price=current_price,
                    timestamp=signal_ts,
                    squeeze_duration=curr_count,
                    tightness_ratio=curr_tightness,
                    momentum=float(curr_momentum),
                    momentum_direction=momentum_dir,
                ))
        
        # 挤压释放检测 (从挤压状态退出) -> 作为“波动开始”信号：需要满足波动扩张确认
        if not curr_squeeze and prev_squeeze and prev_count >= cfg.min_squeeze_duration:
            confirmations = 0

            try:
                atr_now = float(atr_val.iloc[-1]) if len(atr_val) else 0.0
                atr_prev = float(atr_val.iloc[-2]) if len(atr_val) >= 2 else 0.0
            except Exception:
                atr_now, atr_prev = 0.0, 0.0

            if cfg.release_require_atr_rise and atr_prev > 0:
                if (atr_now / atr_prev - 1.0) >= float(cfg.release_atr_rise_pct):
                    confirmations += 1

            if cfg.release_require_range_atr and atr_now > 0:
                candle_range = float(df["high"].iloc[-1] - df["low"].iloc[-1])
                if (candle_range / atr_now) >= float(cfg.release_range_atr_mult):
                    confirmations += 1

            if cfg.release_require_bb_expand:
                try:
                    bb_now = float(bb_width.iloc[-1])
                    bb_prev = float(bb_width.iloc[-2]) if len(bb_width) >= 2 else 0.0
                except Exception:
                    bb_now, bb_prev = 0.0, 0.0
                if bb_prev > 0 and (bb_now / bb_prev - 1.0) >= float(cfg.release_bb_expand_pct):
                    confirmations += 1

            if confirmations >= int(cfg.release_min_confirmations):
                if self._should_emit(symbol, timeframe, "squeeze_release", last_bar_open_ms):
                    signals.append(SqueezeSignal(
                        symbol=symbol,
                        timeframe=timeframe,
                        signal_type="squeeze_release",
                        price=current_price,
                        timestamp=signal_ts,
                        squeeze_duration=prev_count,
                        tightness_ratio=prev_tightness,
                        momentum=float(curr_momentum),
                        momentum_direction=momentum_dir,
                    ))
        
        return signals
    
    def _calc_momentum(self, df: pd.DataFrame, length: int) -> pd.Series:
        """计算动量 (线性回归斜率, 类似 TTM Squeeze)"""
        close = df['close']
        high = df['high']
        low = df['low']
        
        # 使用 (最高价 + 最低价) / 2 的偏离
        mid = (ta.sma(high, length=length) + ta.sma(low, length=length)) / 2
        src = close - mid
        
        # 线性回归
        momentum = ta.linreg(src, length=length)
        return momentum.fillna(0)
    
    def _interval_to_ms(self, interval: str) -> int:
        mapping = {
            "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
            "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000, "8h": 28_800_000,
            "12h": 43_200_000, "1d": 86_400_000, "3d": 259_200_000, "1w": 604_800_000,
        }
        return mapping.get(interval, 0)
    
    async def _fetch_klines(self, symbol: str, interval: str, limit: int) -> list:
        """获取 K 线数据"""
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        try:
            data = await self._get_json("/fapi/v1/klines", params=params)
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.debug(f"Error fetching klines {symbol}: {e}")
        return []


# 便捷运行函数
async def run_squeeze_scanner(
    config: Optional[SqueezeConfig] = None,
    timeframe: str = "1h",
) -> Dict[str, List[SqueezeSignal]]:
    """运行一次挤压扫描"""
    scanner = SqueezeMomentumScanner(config)
    await scanner.start()
    try:
        return await scanner.scan(timeframe)
    finally:
        await scanner.stop()
