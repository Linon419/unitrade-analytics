"""
WaveTrend Scanner (波段过滤器) - 使用 pandas_ta 像素级复刻 TradingView

基于用户提供的精确实现，使用 pandas_ta 库确保与 TradingView 指标一致。
支持批量扫描全市场币种的以下信号：
1. 超买/超卖进入
2. 波动预警 (BB/KC 挤压 L1/L2)
3. 背离检测 (L1-L4 + M+ 放量确认)
"""

import asyncio
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np

try:
    import pandas as pd
    import pandas_ta as ta
except ModuleNotFoundError as e:  # pragma: no cover
    raise ModuleNotFoundError(
        "WaveTrendScanner requires optional dependencies `pandas` and `pandas_ta`. "
        "Install with `pip install -e \".[ta]\"` (or install them manually), "
        "or keep `wavetrend_scanner.enabled=false` in `config/default.yaml`."
    ) from e

logger = logging.getLogger(__name__)


@dataclass
class WaveTrendConfig:
    """WaveTrend Scanner 配置"""
    # 时间周期
    timeframes: List[str] = field(default_factory=lambda: ["1h", "4h", "1d"])
    algo_type: str = "WaveTrend + MFI Hybrid"
    use_sigmoid: bool = True
    use_ha: bool = True
    
    # 核心算法参数 (与 TradingView 一致)
    channel_len: int = 9
    avg_len: int = 7
    smooth_len: int = 4
    mfi_len: int = 7
    hybrid_mfi_len: int = 7
    wt_weight: float = 0.4
    
    # Sigmoid 参数
    mult: float = 1.2
    sigmoid_gain: float = 1.8
    step_size: float = 6.6
    osc_min: float = -60.0
    osc_max: float = 60.0
    
    # 平滑算法 (可选: SMA, EMA, HMA, ALMA, Laguerre)
    ma_type: str = "ALMA"
    laguerre_gamma: float = 0.66
    
    # 阈值
    lvl_1: float = 45.0
    lvl_2_floor: float = 50.0
    lvl_2_base: float = 60.0
    lvl_3_base: float = 68.0
    lvl_3_floor: float = 48.0
    
    # 成交量
    vol_len: int = 60
    vol_trigger: float = 1.6
    vol_weight: float = 3.0
    
    # 波动预警
    squeeze_len: int = 20
    bb_mult: float = 2.0
    kc_mult: float = 1.5
    atr_len: int = 20
    min_squeeze_duration: int = 5
    tightness_l2_ratio: float = 0.80
    
    # 背离 (Pine Script 默认: 左侧结构=5, 右侧确认=3)
    lb_left: int = 5
    lb_right: int = 3
    search_limit: int = 65
    use_adx_filter_div: bool = False
    use_adx_gray_color: bool = False
    require_candle_conf: bool = True
    adx_len: int = 14
    adx_limit: int = 30
    kline_limit: int = 500
    ha_warmup: int = 0
    use_kline_cache: bool = False
    kline_db_path: str = "data/klines.db"
    
    # 币种筛选
    auto_top_n: int = 50
    extra_whitelist: List[str] = field(default_factory=list)
    
    # 并发控制
    max_concurrent: int = 20
    
    @classmethod
    def from_config(cls, config) -> "WaveTrendConfig":
        """从全局 Config 对象创建"""
        wt = config.wavetrend_scanner if hasattr(config, 'wavetrend_scanner') else {}
        if isinstance(wt, dict):
            algo = wt.get("algo_type", wt.get("algo", "WaveTrend + MFI Hybrid"))
            return cls(
                timeframes=wt.get("timeframes", ["1h", "4h", "1d"]),
                algo_type=algo,
                use_sigmoid=wt.get("use_sigmoid", True),
                use_ha=wt.get("use_ha", True),
                channel_len=wt.get("channel_len", 9),
                avg_len=wt.get("avg_len", 7),
                smooth_len=wt.get("smooth_len", 4),
                mult=wt.get("mult", 1.2),
                step_size=wt.get("step_size", 6.6),
                sigmoid_gain=wt.get("sigmoid_gain", 1.8),
                osc_min=wt.get("osc_min", -60.0),
                osc_max=wt.get("osc_max", 60.0),
                mfi_len=wt.get("mfi_len", 7),
                hybrid_mfi_len=wt.get("hybrid_mfi_len", wt.get("mfi_len", 7)),
                wt_weight=wt.get("wt_weight", 0.4),
                ma_type=wt.get("ma_type", "ALMA"),
                laguerre_gamma=wt.get("laguerre_gamma", 0.66),
                lvl_1=wt.get("lvl_1", 45.0),
                lvl_2_floor=wt.get("lvl_2_floor", 50.0),
                lvl_2_base=wt.get("lvl_2_base", 60.0),
                lvl_3_base=wt.get("lvl_3_base", 68.0),
                lvl_3_floor=wt.get("lvl_3_floor", 48.0),
                vol_len=wt.get("vol_len", 60),
                vol_trigger=wt.get("vol_trigger", 1.6),
                vol_weight=wt.get("vol_weight", 3.0),
                squeeze_len=wt.get("squeeze_len", 20),
                bb_mult=wt.get("bb_mult", 2.0),
                kc_mult=wt.get("kc_mult", 1.5),
                atr_len=wt.get("atr_len", 20),
                min_squeeze_duration=wt.get("min_squeeze_duration", 5),
                tightness_l2_ratio=wt.get("tightness_l2_ratio", 0.80),
                auto_top_n=wt.get("top_n", 50),
                extra_whitelist=wt.get("extra_whitelist", []),
                ha_warmup=wt.get("ha_warmup", 0),
                max_concurrent=wt.get("max_concurrent", 20),
                use_adx_filter_div=wt.get("use_adx_filter_div", False),
                require_candle_conf=wt.get("require_candle_conf", True),
                adx_len=wt.get("adx_len", 14),
                adx_limit=wt.get("adx_limit", 30),
                kline_limit=wt.get("kline_limit", 500),
                use_kline_cache=wt.get("use_kline_cache", False),
                kline_db_path=wt.get("kline_db_path", "data/klines.db"),
            )
        return cls()


@dataclass
class WaveTrendSignal:
    """WaveTrend 信号结果"""
    symbol: str
    timeframe: str
    signal_type: str  # overbought, oversold, exit_overbought, exit_oversold, squeeze_l1, squeeze_l2, bull_div, bear_div
    osc_value: float
    price: float
    timestamp: str
    level: Optional[int] = None  # 背离级别 1-4
    is_m_plus: bool = False  # 是否有放量确认
    squeeze_duration: int = 0  # 挤压持续周期
    severity: Optional[str] = None  # L1/L2/L3 based on dynamic thresholds
    lvl2_dyn: Optional[float] = None
    lvl3_dyn: Optional[float] = None
    
    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "signal_type": self.signal_type,
            "osc": round(float(self.osc_value), 2),
            "price": float(self.price),
            "timestamp": self.timestamp,
            "level": int(self.level) if self.level is not None else None,
            "m_plus": bool(self.is_m_plus),
            "squeeze_bars": int(self.squeeze_duration),
            "severity": self.severity,
            "lvl2_dyn": round(float(self.lvl2_dyn), 2) if self.lvl2_dyn is not None else None,
            "lvl3_dyn": round(float(self.lvl3_dyn), 2) if self.lvl3_dyn is not None else None,
        }
    
    def format_text(self) -> str:
        """????????"""
        emoji_map = {
            "overbought": "??",
            "oversold": "??",
            "exit_overbought": "??",
            "exit_oversold": "??",
            "squeeze_l1": "??",
            "squeeze_l2": "??",
            "bull_div": "??",
            "bear_div": "??",
        }
        emoji = emoji_map.get(self.signal_type, "??")
        
        text = f"{emoji} {self.symbol} ({self.timeframe})"
        
        if self.signal_type == "overbought":
            text += f" ????? OSC={self.osc_value:.1f}"
        elif self.signal_type == "oversold":
            text += f" ????? OSC={self.osc_value:.1f}"
        elif self.signal_type == "exit_overbought":
            text += f" ????? OSC={self.osc_value:.1f}"
        elif self.signal_type == "exit_oversold":
            text += f" ????? OSC={self.osc_value:.1f}"
        elif self.signal_type == "squeeze_l1":
            text += f" ????L1 ({self.squeeze_duration}bars)"
        elif self.signal_type == "squeeze_l2":
            text += f" ????L2 ({self.squeeze_duration}bars)"
        elif self.signal_type == "bull_div":
            m_plus = " M+" if self.is_m_plus else ""
            text += f" ???? L{self.level}{m_plus}"
        elif self.signal_type == "bear_div":
            m_plus = " M+" if self.is_m_plus else ""
            text += f" ???? L{self.level}{m_plus}"
        
        return text

def _pine_alma(src: pd.Series, length: int, offset: float = 0.85, sigma: float = 6.0) -> pd.Series:
    """
    Pine Script ALMA 精确实现

    Pine 公式:
    windowSize = floor(offset * (length - 1))
    s = length / sigma
    for i = 0 to length - 1:
        w = exp(-(i - windowSize)^2 / (2 * s * s))
        sum += w * src[length - 1 - i]
        norm += w
    alma = sum / norm
    """
    import math
    n = len(src)
    result = np.full(n, np.nan)

    window_size = math.floor(offset * (length - 1))
    s = length / sigma

    src_values = src.values if hasattr(src, 'values') else src

    for idx in range(length - 1, n):
        sum_val = 0.0
        norm = 0.0
        for i in range(length):
            w = math.exp(-((i - window_size) ** 2) / (2 * s * s))
            sum_val += w * src_values[idx - (length - 1 - i)]
            norm += w
        result[idx] = sum_val / norm if norm != 0 else 0

    return pd.Series(result, index=src.index if hasattr(src, 'index') else None)


def _get_laguerre(src: pd.Series, gamma: float = 0.66) -> pd.Series:
    """
    Laguerre Filter 实现

    Pine Script:
    L0 := (1.0 - gamma) * src + gamma * nz(L0[1])
    L1 := -gamma * L0 + nz(L0[1]) + gamma * nz(L1[1])
    L2 := -gamma * L1 + nz(L1[1]) + gamma * nz(L2[1])
    L3 := -gamma * L2 + nz(L2[1]) + gamma * nz(L3[1])
    result := (L0 + 2*L1 + 2*L2 + L3) / 6
    """
    n = len(src)
    L0 = np.zeros(n)
    L1 = np.zeros(n)
    L2 = np.zeros(n)
    L3 = np.zeros(n)
    result = np.zeros(n)

    for i in range(n):
        L0[i] = (1.0 - gamma) * src.iloc[i] + gamma * (L0[i-1] if i > 0 else 0)
        L1[i] = -gamma * L0[i] + (L0[i-1] if i > 0 else 0) + gamma * (L1[i-1] if i > 0 else 0)
        L2[i] = -gamma * L1[i] + (L1[i-1] if i > 0 else 0) + gamma * (L2[i-1] if i > 0 else 0)
        L3[i] = -gamma * L2[i] + (L2[i-1] if i > 0 else 0) + gamma * (L3[i-1] if i > 0 else 0)
        result[i] = (L0[i] + 2.0 * L1[i] + 2.0 * L2[i] + L3[i]) / 6.0

    return pd.Series(result, index=src.index)


def _get_ma(source: pd.Series, length: int, ma_type: str = "ALMA", laguerre_gamma: float = 0.66) -> pd.Series:
    """
    通用 MA 函数，匹配 Pine Script get_ma

    Pine Script:
    switch type
        "SMA"  => ta.sma(source, length)
        "EMA"  => ta.ema(source, length)
        "HMA"  => ta.hma(source, length)
        "ALMA" => ta.alma(source, length, 0.85, 6)
        "Laguerre" => get_laguerre(source, laguerre_gamma)
    """
    if ma_type == "SMA":
        return ta.sma(source, length=length)
    elif ma_type == "EMA":
        return ta.ema(source, length=length)
    elif ma_type == "HMA":
        return ta.hma(source, length=length)
    elif ma_type == "ALMA":
        return _pine_alma(source, length=length, offset=0.85, sigma=6)
    elif ma_type == "Laguerre":
        return _get_laguerre(source, laguerre_gamma)
    else:
        return ta.sma(source, length=length)


class WaveTrendScanner:
    """WaveTrend 波段过滤器扫描器 (使用 pandas_ta)"""

    def __init__(self, config: Optional[WaveTrendConfig] = None):
        self.config = config or WaveTrendConfig()
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._last_dynamic_levels: Tuple[float, float] = (
            self.config.lvl_2_base,
            self.config.lvl_3_base,
        )
        self._cache_conn: Optional[sqlite3.Connection] = None
    
    async def start(self) -> None:
        """启动扫描器"""
        self._session = aiohttp.ClientSession()
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        logger.info("WaveTrend Scanner started (pandas_ta mode)")
    
    async def stop(self) -> None:
        """停止扫描器"""
        if self._session:
            await self._session.close()
        logger.info("WaveTrend Scanner stopped")
        if self._cache_conn:
            try:
                self._cache_conn.close()
            except Exception:
                pass
    
    async def scan(self, timeframe: str = "1h") -> Dict[str, List[WaveTrendSignal]]:
        """
        扫描指定时间周期的所有信号
        
        Returns:
            {
                "overbought": [...],
                "oversold": [...],
                "squeeze": [...],
                "divergence": [...]
            }
        """
        logger.info(f"Starting WaveTrend scan for {timeframe}...")
        
        # 获取币种列表
        symbols = await self._discover_symbols()
        logger.info(f"Scanning {len(symbols)} symbols...")
        
        # 批量分析
        all_signals = await self._analyze_batch(symbols, timeframe)
        
        # 分类
        result = {
            "overbought": [s for s in all_signals if s.signal_type == "overbought"],
            "oversold": [s for s in all_signals if s.signal_type == "oversold"],
            "exit_overbought": [s for s in all_signals if s.signal_type == "exit_overbought"],
            "exit_oversold": [s for s in all_signals if s.signal_type == "exit_oversold"],
            "squeeze": [s for s in all_signals if s.signal_type.startswith("squeeze")],
            "divergence": [s for s in all_signals if s.signal_type.endswith("_div")],
        }
        
        logger.info(
            f"Scan complete: {len(result['overbought'])} overbought, "
            f"{len(result['oversold'])} oversold, "
            f"{len(result['exit_overbought'])} exit_overbought, "
            f"{len(result['exit_oversold'])} exit_oversold, "
            f"{len(result['squeeze'])} squeeze, "
            f"{len(result['divergence'])} divergence"
        )
        
        return result
    
    async def _discover_symbols(self) -> List[str]:
        """获取 Top N 交易量币种 + 白名单"""
        try:
            url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    return []
                tickers = await resp.json()
            
            usdt_tickers = [t for t in tickers if t["symbol"].endswith("USDT")]
            usdt_tickers.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
            
            # Top N 币种
            top_symbols = [t["symbol"] for t in usdt_tickers[:self.config.auto_top_n]]
            
            # 合并白名单 (去重)
            all_symbols = set(top_symbols)
            for s in self.config.extra_whitelist:
                if s.upper() not in all_symbols:
                    all_symbols.add(s.upper())
            
            return list(all_symbols)
        except Exception as e:
            logger.error(f"Symbol discovery error: {e}")
            return []
    
    async def _analyze_batch(self, symbols: List[str], timeframe: str) -> List[WaveTrendSignal]:
        """批量分析"""
        tasks = [self._analyze_symbol(symbol, timeframe) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        signals = []
        for result in results:
            if isinstance(result, list):
                signals.extend(result)
        
        return signals
    
    async def _analyze_symbol(self, symbol: str, timeframe: str) -> List[WaveTrendSignal]:
        """分析单个币种 (使用 pandas_ta)"""
        async with self._semaphore:
            try:
                # 获取 K 线数据 (多抓一些用于 HA 收敛)
                klines = await self._fetch_klines(symbol, timeframe, self.config.kline_limit)
                if len(klines) < 100:
                    return []
                
                # 转换为 DataFrame
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

                # 仅用已收盘 K 线（Binance 返回的最后一根可能仍在形成）
                interval_ms = self._interval_to_ms(timeframe)
                if interval_ms > 0 and len(df) > 0:
                    now_ms = int(datetime.utcnow().timestamp() * 1000)
                    last_open_ms = int(df['time'].iloc[-1].timestamp() * 1000)
                    if last_open_ms + interval_ms > now_ms:
                        df = df.iloc[:-1]
                        if len(df) < 2:
                            return []
                
                if len(df) < 100:
                    return []
                
                # 计算 Heikin Ashi (手动实现确保收敛)
                df_ha = self._calc_heikin_ashi_df(df)
                
                # 计算 WaveTrend OSC (使用 pandas_ta)
                osc = self._calc_wavetrend_pandas(df, df_ha)
                
                # 丢弃 HA 收敛期
                warmup = self.config.ha_warmup
                df = df.iloc[warmup:].reset_index(drop=True)
                df_ha = df_ha.iloc[warmup:].reset_index(drop=True)
                osc = osc.iloc[warmup:].reset_index(drop=True)
                
                signals = []
                current_price = df['close'].iloc[-1]
                timestamp = datetime.now().isoformat()
                lvl2_dyn, lvl3_dyn = getattr(self, "_last_dynamic_levels", (self.config.lvl_2_base, self.config.lvl_3_base))
                lvl2_dyn = float(lvl2_dyn)
                lvl3_dyn = float(lvl3_dyn)

                def _severity(val: float) -> Optional[str]:
                    abs_v = abs(val)
                    if abs_v >= lvl3_dyn:
                        return "L3"
                    if abs_v >= lvl2_dyn:
                        return "L2"
                    if abs_v >= self.config.lvl_1:
                        return "L1"
                    return None
                
                # 1. 超买/超卖检测（进入/退出）
                if len(osc) >= 2:
                    prev_osc = osc.iloc[-2]
                    curr_osc = osc.iloc[-1]
                    lvl_1 = self.config.lvl_1

                    if curr_osc >= lvl_1 and prev_osc < lvl_1:
                        signals.append(WaveTrendSignal(
                            symbol=symbol, timeframe=timeframe,
                            signal_type="overbought", osc_value=float(curr_osc),
                            price=current_price, timestamp=timestamp,
                            severity=_severity(curr_osc), lvl2_dyn=lvl2_dyn, lvl3_dyn=lvl3_dyn,
                        ))
                    if curr_osc <= -lvl_1 and prev_osc > -lvl_1:
                        signals.append(WaveTrendSignal(
                            symbol=symbol, timeframe=timeframe,
                            signal_type="oversold", osc_value=float(curr_osc),
                            price=current_price, timestamp=timestamp,
                            severity=_severity(curr_osc), lvl2_dyn=lvl2_dyn, lvl3_dyn=lvl3_dyn,
                        ))
                    if curr_osc < lvl_1 and prev_osc >= lvl_1:
                        signals.append(WaveTrendSignal(
                            symbol=symbol, timeframe=timeframe,
                            signal_type="exit_overbought", osc_value=float(curr_osc),
                            price=current_price, timestamp=timestamp,
                            severity=_severity(curr_osc), lvl2_dyn=lvl2_dyn, lvl3_dyn=lvl3_dyn,
                        ))
                    if curr_osc > -lvl_1 and prev_osc <= -lvl_1:
                        signals.append(WaveTrendSignal(
                            symbol=symbol, timeframe=timeframe,
                            signal_type="exit_oversold", osc_value=float(curr_osc),
                            price=current_price, timestamp=timestamp,
                            severity=_severity(curr_osc), lvl2_dyn=lvl2_dyn, lvl3_dyn=lvl3_dyn,
                        ))
                
                # 2. 波动预警检测
                squeeze_signal = self._detect_squeeze_pandas(df)
                if squeeze_signal:
                    signal_type, duration = squeeze_signal
                    signals.append(WaveTrendSignal(
                        symbol=symbol, timeframe=timeframe,
                        signal_type=signal_type, osc_value=float(osc.iloc[-1]),
                        price=current_price, timestamp=timestamp,
                        squeeze_duration=duration
                    ))
                
                # 3. 背离检测 (使用 HA 数据进行蜡烛确认)
                div_signals = self._detect_divergence_pandas(
                    osc, df, df_ha, symbol, timeframe, current_price
                )
                signals.extend(div_signals)
                
                return signals
                
            except Exception as e:
                logger.debug(f"Error analyzing {symbol}: {e}")
                return []
    
    def _ensure_cache(self) -> Optional[sqlite3.Connection]:
        """初始化本地 K 线缓存"""
        if not self.config.use_kline_cache:
            return None
        if self._cache_conn is None:
            db_path = Path(self.config.kline_db_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(db_path)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS klines (
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open_time INTEGER NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    close_time INTEGER,
                    quote_volume REAL,
                    trades INTEGER,
                    taker_buy_base REAL,
                    taker_buy_quote REAL,
                    PRIMARY KEY (symbol, interval, open_time)
                )
            """)
            conn.commit()
            self._cache_conn = conn
        return self._cache_conn

    def _interval_to_ms(self, interval: str) -> int:
        mapping = {
            "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
            "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000, "8h": 28_800_000,
            "12h": 43_200_000, "1d": 86_400_000, "3d": 259_200_000, "1w": 604_800_000,
        }
        return mapping.get(interval, 0)

    async def _fetch_klines_remote(self, symbol: str, interval: str, limit: int, start_time: Optional[int] = None) -> list:
        """直接从交易所获取 K 线数据"""
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.debug(f"Error fetching klines {symbol}: {e}")
        return []

    async def _fetch_klines(self, symbol: str, interval: str, limit: int) -> list:
        """
        获取 K 线数据，优先使用本地缓存，缺口再去交易所补齐
        """
        conn = self._ensure_cache()
        if conn is None:
            return await self._fetch_klines_remote(symbol, interval, limit)

        cur = conn.cursor()
        cur.execute(
            "SELECT open_time, open, high, low, close, volume, close_time, quote_volume, trades, taker_buy_base, taker_buy_quote "
            "FROM klines WHERE symbol=? AND interval=? ORDER BY open_time DESC LIMIT ?",
            (symbol, interval, limit)
        )
        rows = cur.fetchall()
        rows.reverse()  # oldest first

        interval_ms = self._interval_to_ms(interval)
        cached_count = len(rows)
        missing = max(0, limit - cached_count)
        last_open_time = rows[-1][0] if rows else None

        remote_data = []
        if missing > 0:
            start_time = last_open_time + interval_ms if last_open_time is not None and interval_ms > 0 else None
            remote_data = await self._fetch_klines_remote(symbol, interval, missing, start_time)
            if remote_data:
                try:
                    conn.executemany(
                        "INSERT OR IGNORE INTO klines VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        [
                            (
                                symbol, interval, int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]),
                                int(k[6]), float(k[7]), int(k[8]), float(k[9]), float(k[10])
                            )
                            for k in remote_data
                        ]
                    )
                    conn.commit()
                except Exception as e:
                    logger.debug(f"Cache insert error {symbol}: {e}")

        # 重新读取合并后的最新 limit 根
        cur.execute(
            "SELECT open_time, open, high, low, close, volume, close_time, quote_volume, trades, taker_buy_base, taker_buy_quote "
            "FROM klines WHERE symbol=? AND interval=? ORDER BY open_time DESC LIMIT ?",
            (symbol, interval, limit)
        )
        final_rows = cur.fetchall()
        final_rows.reverse()
        # 转回和交易所一致的列表格式
        result = [
            [
                int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]),
                int(r[6]), float(r[7]), int(r[8]), float(r[9]), float(r[10]), 0
            ]
            for r in final_rows
        ]
        return result
    
    def _calc_heikin_ashi_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算 Heikin Ashi (手动实现确保收敛)
        
        HA Close = (O+H+L+C)/4
        HA Open = (HA_Open[i-1] + HA_Close[i-1]) / 2
        HA High = max(H, HA_Open, HA_Close)
        HA Low = min(L, HA_Open, HA_Close)
        """
        df_ha = df.copy()
        
        # HA Close
        df_ha['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        
        # HA Open (递归计算)
        ha_open = np.zeros(len(df))
        ha_open[0] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2.0
        
        for i in range(1, len(df)):
            ha_open[i] = (ha_open[i-1] + df_ha['close'].iloc[i-1]) / 2
        
        df_ha['open'] = ha_open
        df_ha['high'] = df[['high']].join(df_ha[['open', 'close']]).max(axis=1)
        df_ha['low'] = df[['low']].join(df_ha[['open', 'close']]).min(axis=1)
        
        return df_ha
    

    def _calc_wavetrend_pandas(self, df: pd.DataFrame, df_ha: pd.DataFrame) -> pd.Series:
        """
        ?? pandas_ta ?? WaveTrend OSC (?????TradingView)

        ?? Pine ?????? algo_type ??:
        - WaveTrend Oscillator
        - WaveTrend + MFI Hybrid
        - True Strength Index (TSI)
        - Chaikin Money Flow (CMF)
        - Commodity Channel Index (CCI)
        - Money Flow Index (MFI)
        - Standard RSI
        - Volume Weighted RSI
        - Fisher Transform
        - Connors RSI
        """
        cfg = self.config

        real_hlc3 = (df['high'] + df['low'] + df['close']) / 3
        ha_hlc3 = (df_ha['high'] + df_ha['low'] + df_ha['close']) / 3
        src = ha_hlc3 if cfg.use_ha else real_hlc3

        def _wt(src_series: pd.Series) -> pd.Series:
            esa = ta.ema(src_series, length=cfg.channel_len)
            d = ta.ema((src_series - esa).abs(), length=cfg.channel_len)
            ci = (src_series - esa) / (0.015 * d)
            ci = ci.fillna(0)
            wt1 = ta.ema(ci, length=cfg.avg_len)
            wt2 = _pine_alma(wt1, length=cfg.smooth_len, offset=0.85, sigma=6)
            return wt2

        def _vw_rsi(src_series: pd.Series) -> pd.Series:
            change = src_series.diff()
            up = ta.rma(change.clip(lower=0) * df['volume'], length=cfg.channel_len)
            down = ta.rma((-change.clip(upper=0)) * df['volume'], length=cfg.channel_len)
            ratio = up / down
            rsi = pd.Series(
                np.where(down == 0, 100, np.where(up == 0, 0, 100 - (100 / (1 + ratio)))),
                index=src_series.index
            )
            return rsi - 50

        def _fisher(src_series: pd.Series) -> pd.Series:
            vals = np.zeros(len(src_series))
            fisher_vals = np.zeros(len(src_series))
            src_vals = src_series.values
            for i in range(len(src_series)):
                start_idx = max(0, i - cfg.channel_len + 1)
                window = src_vals[start_idx:i + 1]
                high_val = np.max(window)
                low_val = np.min(window)
                div = high_val - low_val
                prev_val = vals[i - 1] if i > 0 else 0.0
                v = 0.0 if div == 0 else 0.66 * ((src_vals[i] - low_val) / div - 0.5) + 0.67 * prev_val
                v = min(max(v, -0.999), 0.999)
                prev_fisher = fisher_vals[i - 1] if i > 0 else 0.0
                vals[i] = v
                fisher_vals[i] = 0.5 * np.log((1 + v) / (1 - v)) + 0.5 * prev_fisher
            return pd.Series(fisher_vals, index=src_series.index) * 20

        algo_raw = cfg.algo_type.strip().lower() if cfg.algo_type else "wavetrend + mfi hybrid"
        algo_normalized = algo_raw.replace("_", " ").strip()
        alias_map = {
            "wt mfi hybrid": "wavetrend + mfi hybrid",
            "wavetrend": "wavetrend oscillator",
            "wave trend": "wavetrend oscillator",
            "tsi": "true strength index (tsi)",
            "cmf": "chaikin money flow (cmf)",
            "cci": "commodity channel index (cci)",
            "mfi": "money flow index (mfi)",
            "rsi": "standard rsi",
            "vw rsi": "volume weighted rsi",
            "fisher": "fisher transform",
            "connors": "connors rsi",
        }
        algo = alias_map.get(algo_normalized, algo_normalized)

        if algo == "wavetrend oscillator":
            raw_val = _wt(src)
        elif algo == "wavetrend + mfi hybrid":
            wt_val = _wt(src)
            mfi_val = ta.mfi(df['high'], df['low'], df['close'], df['volume'], length=cfg.hybrid_mfi_len)
            mfi_normalized = (mfi_val - 50) * 1.5
            raw_val = cfg.wt_weight * wt_val + (1 - cfg.wt_weight) * mfi_normalized.fillna(0)
        elif algo == "true strength index (tsi)":
            raw_val = ta.tsi(src, long=cfg.channel_len, short=cfg.avg_len) * 0.6
        elif algo == "chaikin money flow (cmf)":
            high = df['high']
            low = df['low']
            close = df['close']
            cond = ((close == high) & (close == low)) | (high == low)
            mf = pd.Series(
                np.where(cond, 0, ((2 * close - low - high) / (high - low)) * df['volume']),
                index=df.index
            )
            mf = mf.replace([np.inf, -np.inf], 0).fillna(0)
            mf_mean = mf.rolling(cfg.channel_len).mean()
            vol_mean = df['volume'].rolling(cfg.channel_len).mean()
            raw_val = (mf_mean / vol_mean).replace([np.inf, -np.inf], 0).fillna(0) * 150.0
        elif algo == "commodity channel index (cci)":
            raw_val = ta.cci(df['high'], df['low'], df['close'], length=cfg.channel_len) / 3.0
        elif algo == "money flow index (mfi)":
            raw_val = ta.mfi(df['high'], df['low'], df['close'], df['volume'], length=cfg.channel_len) - 50.0
        elif algo == "standard rsi":
            raw_val = ta.rsi(src, length=cfg.channel_len) - 50.0
        elif algo == "volume weighted rsi":
            raw_val = _vw_rsi(src)
        elif algo == "fisher transform":
            raw_val = _fisher(src)
        elif algo == "connors rsi":
            raw_val = ta.rsi(src, length=cfg.channel_len) - 50.0
        else:
            wt_val = _wt(src)
            mfi_val = ta.mfi(df['high'], df['low'], df['close'], df['volume'], length=cfg.hybrid_mfi_len)
            mfi_normalized = (mfi_val - 50) * 1.5
            raw_val = cfg.wt_weight * wt_val + (1 - cfg.wt_weight) * mfi_normalized.fillna(0)

        raw_val = pd.Series(raw_val).fillna(0)
        scaled_val = raw_val * cfg.mult

        if cfg.use_sigmoid:
            val_in = scaled_val / 100.0
            processed_val = (2.0 / (1.0 + np.exp(-cfg.sigmoid_gain * val_in)) - 1.0) * 100.0
        else:
            processed_val = scaled_val

        final_val = _get_ma(pd.Series(processed_val), cfg.smooth_len, cfg.ma_type, cfg.laguerre_gamma)
        
        # 动态阈值 (仅用于与 Pine 视觉一致，信号仍基于 lvl_1)
        vol_ma = ta.sma(df['volume'], length=cfg.vol_len)
        vol_std = ta.stdev(df['volume'], length=cfg.vol_len)
        vol_zscore = (df['volume'] - vol_ma) / vol_std.replace(0, np.nan)
        vol_zscore = vol_zscore.fillna(0)
        effective_vol = (vol_zscore - cfg.vol_trigger).clip(lower=0)
        lvl_2_dynamic = (cfg.lvl_2_base - effective_vol * cfg.vol_weight).clip(lower=cfg.lvl_2_floor)
        lvl_3_dynamic = (cfg.lvl_3_base - effective_vol * cfg.vol_weight).clip(lower=cfg.lvl_3_floor)
        self._last_dynamic_levels = (
            float(lvl_2_dynamic.iloc[-1]) if len(lvl_2_dynamic) else cfg.lvl_2_base,
            float(lvl_3_dynamic.iloc[-1]) if len(lvl_3_dynamic) else cfg.lvl_3_base,
        )
        
        osc_limited = final_val.clip(lower=cfg.osc_min, upper=cfg.osc_max)
        scaled_val = osc_limited / cfg.step_size
        osc_raw = np.sign(scaled_val) * np.floor(np.abs(scaled_val)) * cfg.step_size
        osc = osc_raw.clip(lower=cfg.osc_min, upper=cfg.osc_max)
        
        return osc

    def _detect_squeeze_pandas(self, df: pd.DataFrame) -> Optional[Tuple[str, int]]:
        """?? BB/KC ?? (????)??? Pine ? EMA+ATR KC ??"""
        n = len(df)
        cfg = self.config
        if n < max(cfg.squeeze_len, cfg.atr_len) + 2:
            return None

        close = df['close']
        atr_val = ta.atr(df['high'], df['low'], df['close'], length=cfg.atr_len)
        kc_mid = ta.ema(close, length=cfg.squeeze_len)
        kc_top = kc_mid + cfg.kc_mult * atr_val
        kc_bot = kc_mid - cfg.kc_mult * atr_val

        bb_mid = ta.sma(close, length=cfg.squeeze_len)
        bb_std = ta.stdev(close, length=cfg.squeeze_len)
        bb_top = bb_mid + cfg.bb_mult * bb_std
        bb_bot = bb_mid - cfg.bb_mult * bb_std

        is_squeeze = (bb_top < kc_top) & (bb_bot > kc_bot)

        def _streak(series: pd.Series, idx: int) -> int:
            count = 0
            for i in range(idx, -1, -1):
                if bool(series.iloc[i]):
                    count += 1
                else:
                    break
            return count

        curr_count = _streak(is_squeeze, n - 1)
        prev_count = _streak(is_squeeze, n - 2) if n >= 2 else 0

        kc_width = kc_top - kc_bot
        bb_width = bb_top - bb_bot
        tightness_ratio = bb_width / kc_width.replace(0, np.nan)

        vol_warning_l1 = bool(is_squeeze.iloc[-1]) and curr_count >= cfg.min_squeeze_duration
        vol_warning_l2 = vol_warning_l1 and (tightness_ratio.iloc[-1] <= cfg.tightness_l2_ratio if not np.isnan(tightness_ratio.iloc[-1]) else False)

        prev_vol_warning_l1 = bool(is_squeeze.iloc[-2]) and prev_count >= cfg.min_squeeze_duration if n >= 2 else False
        prev_vol_warning_l2 = prev_vol_warning_l1 and (tightness_ratio.iloc[-2] <= cfg.tightness_l2_ratio if n >= 2 and not np.isnan(tightness_ratio.iloc[-2]) else False)

        if vol_warning_l2 and not prev_vol_warning_l2:
            return ("squeeze_l2", curr_count)
        if vol_warning_l1 and not prev_vol_warning_l1:
            return ("squeeze_l1", curr_count)

        return None

    def _detect_divergence_pandas(
        self, osc: pd.Series, df: pd.DataFrame, df_ha: pd.DataFrame,
        symbol: str, timeframe: str, current_price: float
    ) -> List[WaveTrendSignal]:
        """???????? Pine ???? ADX ?????????"""
        signals = []
        cfg = self.config
        n = len(osc)
        lb_right = cfg.lb_right
        lb_left = cfg.lb_left
        lvl_1 = cfg.lvl_1
        if n < max(lb_left + lb_right + 2, 30):
            return signals

        def _is_pivot(idx: int, mode: str) -> bool:
            if idx - lb_left < 0 or idx + lb_right >= n:
                return False
            window = osc.iloc[idx - lb_left: idx + lb_right + 1]
            val = osc.iloc[idx]
            if mode == "high":
                return val == window.max()
            return val == window.min()

        def _level_from_distance(dist: int) -> int:
            return 1 if dist <= 15 else 2 if dist <= 35 else 3 if dist <= 55 else 4

        vol_ma = ta.sma(df['volume'], length=cfg.vol_len)
        vol_std = ta.stdev(df['volume'], length=cfg.vol_len)
        vol_zscore = (df['volume'] - vol_ma) / vol_std.replace(0, np.nan)
        vol_zscore = vol_zscore.fillna(0)
        is_m_plus = vol_zscore.iloc[-1] > cfg.vol_trigger

        allow_div_by_adx = True
        if cfg.use_adx_filter_div:
            dmi = ta.dmi(df['high'], df['low'], df['close'], length=cfg.adx_len)
            adx_col = f"ADX_{cfg.adx_len}"
            if dmi is not None and adx_col in dmi:
                adx_val = dmi[adx_col].iloc[-1]
                allow_div_by_adx = adx_val <= cfg.adx_limit

        timestamp = datetime.now().isoformat()
        highs = df['high']
        lows = df['low']
        opens = df_ha['open'] if cfg.use_ha else df['open']
        closes = df_ha['close'] if cfg.use_ha else df['close']
        open_curr = opens.iloc[-1]
        close_curr = closes.iloc[-1]
        is_red_candle = close_curr < open_curr
        is_green_candle = close_curr >= open_curr

        curr_idx = n - lb_right - 1
        if curr_idx < lb_left or curr_idx - 1 < 0:
            return signals

        # 看跌背离
        if allow_div_by_adx and _is_pivot(curr_idx, "high"):
            curr_osc = osc.iloc[curr_idx]
            curr_price_high = highs.iloc[curr_idx]
            if not cfg.require_candle_conf or is_red_candle:
                for dist in range(1, cfg.search_limit + 1):
                    check_idx = curr_idx - dist
                    if check_idx - 1 < 0:
                        break
                    check_osc = osc.iloc[check_idx]
                    if np.isnan(check_osc) or np.isnan(osc.iloc[check_idx + 1]) or np.isnan(osc.iloc[check_idx - 1]):
                        continue
                    is_peak = (check_osc >= osc.iloc[check_idx + 1]) and (check_osc >= osc.iloc[check_idx - 1])
                    if is_peak and check_osc >= lvl_1 and curr_price_high > highs.iloc[check_idx] and curr_osc <= check_osc:
                        level = _level_from_distance(dist)
                        signals.append(WaveTrendSignal(
                            symbol=symbol, timeframe=timeframe,
                            signal_type="bear_div", osc_value=float(curr_osc),
                            price=float(curr_price_high), timestamp=timestamp,
                            level=level, is_m_plus=is_m_plus
                        ))
                        break

        # 看涨背离
        if allow_div_by_adx and _is_pivot(curr_idx, "low"):
            curr_osc = osc.iloc[curr_idx]
            curr_price_low = lows.iloc[curr_idx]
            if not cfg.require_candle_conf or is_green_candle:
                for dist in range(1, cfg.search_limit + 1):
                    check_idx = curr_idx - dist
                    if check_idx - 1 < 0:
                        break
                    check_osc = osc.iloc[check_idx]
                    if np.isnan(check_osc) or np.isnan(osc.iloc[check_idx + 1]) or np.isnan(osc.iloc[check_idx - 1]):
                        continue
                    is_bottom = (check_osc <= osc.iloc[check_idx + 1]) and (check_osc <= osc.iloc[check_idx - 1])
                    if is_bottom and check_osc <= -lvl_1 and curr_price_low < lows.iloc[check_idx] and curr_osc >= check_osc:
                        level = _level_from_distance(dist)
                        signals.append(WaveTrendSignal(
                            symbol=symbol, timeframe=timeframe,
                            signal_type="bull_div", osc_value=float(curr_osc),
                            price=float(curr_price_low), timestamp=timestamp,
                            level=level, is_m_plus=is_m_plus
                        ))
                        break

        return signals
    # ========== 兼容旧接口的方法 ==========
    
    def _calc_heikin_ashi(self, opens: np.ndarray, highs: np.ndarray, 
                          lows: np.ndarray, closes: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """计算 Heikin Ashi (numpy 版本，兼容旧接口)"""
        n = len(opens)
        ha_open = np.zeros(n)
        ha_close = np.zeros(n)
        ha_high = np.zeros(n)
        ha_low = np.zeros(n)
        
        ha_close[0] = (opens[0] + highs[0] + lows[0] + closes[0]) / 4
        ha_open[0] = (opens[0] + closes[0]) / 2
        ha_high[0] = highs[0]
        ha_low[0] = lows[0]
        
        for i in range(1, n):
            ha_close[i] = (opens[i] + highs[i] + lows[i] + closes[i]) / 4
            ha_open[i] = (ha_open[i-1] + ha_close[i-1]) / 2
            ha_high[i] = max(highs[i], ha_open[i], ha_close[i])
            ha_low[i] = min(lows[i], ha_open[i], ha_close[i])
        
        return ha_open, ha_high, ha_low, ha_close
    
    def _calc_wavetrend(self, hlc3: np.ndarray, closes: np.ndarray, volumes: np.ndarray) -> np.ndarray:
        """计算 WaveTrend (numpy 版本，兼容旧接口)"""
        # 转换为 DataFrame 并使用 pandas_ta
        df = pd.DataFrame({
            'high': hlc3,  # 近似
            'low': hlc3,
            'close': closes,
            'volume': volumes
        })
        df_ha = df.copy()  # 假设已经是 HA 数据
        
        osc = self._calc_wavetrend_pandas(df, df_ha)
        return osc.fillna(0).values
    
    def _ema(self, data: np.ndarray, period: int) -> np.ndarray:
        """EMA (兼容旧接口)"""
        return ta.ema(pd.Series(data), length=period).fillna(data[0]).values
    
    def _calc_atr(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int) -> np.ndarray:
        """ATR (兼容旧接口)"""
        df = pd.DataFrame({'high': highs, 'low': lows, 'close': closes})
        atr = ta.atr(df['high'], df['low'], df['close'], length=period)
        return atr.fillna(0).values
    
    def _calc_ob_os_streak(self, osc: np.ndarray, lvl_1: float = 45.0) -> Tuple[str, int]:
        """计算当前连续超买/超卖 K 线数"""
        n = len(osc)
        if n < 2:
            return ("neutral", 0)
        
        current_osc = osc[-1] if isinstance(osc, np.ndarray) else osc.iloc[-1]
        
        if current_osc >= lvl_1:
            zone_type = "ob"
            streak = 0
            for i in range(n - 1, -1, -1):
                val = osc[i] if isinstance(osc, np.ndarray) else osc.iloc[i]
                if val >= lvl_1:
                    streak += 1
                else:
                    break
            return (zone_type, streak)
        
        elif current_osc <= -lvl_1:
            zone_type = "os"
            streak = 0
            for i in range(n - 1, -1, -1):
                val = osc[i] if isinstance(osc, np.ndarray) else osc.iloc[i]
                if val <= -lvl_1:
                    streak += 1
                else:
                    break
            return (zone_type, streak)
        
        return ("neutral", 0)
    
    def _calc_signal_accuracy(self, osc: np.ndarray, closes: np.ndarray, 
                               lvl_1: float = 45.0, confirm_bars: int = 5, 
                               confirm_move_pct: float = 0.01) -> int:
        """计算连对次数"""
        n = len(osc)
        if n < 20:
            return 0
        
        signals = []
        i = 0
        while i < n - confirm_bars - 1:
            osc_val = osc[i] if isinstance(osc, np.ndarray) else osc.iloc[i]
            prev_osc = osc[i-1] if i > 0 else 0
            if isinstance(osc, pd.Series) and i > 0:
                prev_osc = osc.iloc[i-1]
            
            if osc_val >= lvl_1 and prev_osc < lvl_1:
                entry_price = closes[i] if isinstance(closes, np.ndarray) else closes.iloc[i]
                future_closes = closes[i+1:i+1+confirm_bars]
                if isinstance(future_closes, pd.Series):
                    min_price = future_closes.min()
                else:
                    min_price = min(future_closes)
                success = (entry_price - min_price) / entry_price >= confirm_move_pct
                signals.append(("ob", success))
                i += confirm_bars
                continue
            
            if osc_val <= -lvl_1 and prev_osc > -lvl_1:
                entry_price = closes[i] if isinstance(closes, np.ndarray) else closes.iloc[i]
                future_closes = closes[i+1:i+1+confirm_bars]
                if isinstance(future_closes, pd.Series):
                    max_price = future_closes.max()
                else:
                    max_price = max(future_closes)
                success = (max_price - entry_price) / entry_price >= confirm_move_pct
                signals.append(("os", success))
                i += confirm_bars
                continue
            
            i += 1
        
        consecutive = 0
        for signal_type, success in reversed(signals):
            if success:
                consecutive += 1
            else:
                break
        
        return consecutive
