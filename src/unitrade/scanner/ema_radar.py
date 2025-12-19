"""
EMA Trend Radar - EMA 趋势雷达

功能:
1. 扫描所有币种的 EMA 排列
2. 检测 "开花" (完美 EMA 排列)
3. 统计连续趋势 K 线数
4. 检测价格接近 EMA 的回调机会
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class EMARadarConfig:
    """EMA 雷达配置"""
    # API 配置
    base_url: str = "https://fapi.binance.com"
    
    # EMA 周期
    ema_periods: List[int] = field(default_factory=lambda: [21, 55, 100, 200])
    
    # K线周期 (可配置)
    timeframes: List[str] = field(default_factory=lambda: ["1h", "4h", "1d"])
    
    # 需要获取的 K线数量 (足够计算 EMA200)
    kline_limit: int = 250
    
    # 接近 EMA 阈值
    near_ema_threshold: float = 0.01  # 1%
    
    # 币种筛选 (复用 scanner 逻辑)
    auto_top_n: int = 100
    extra_whitelist: List[str] = field(default_factory=list)
    ignore_list: List[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT"])
    
    # 并发控制
    max_concurrent_requests: int = 10
    request_delay: float = 0.1
    
    # 输出控制
    top_n_results: int = 10  # 每个方向显示前 N 个
    
    @classmethod
    def from_config(cls, config) -> "EMARadarConfig":
        """从全局 Config 对象创建配置，复用 scanner 的币种筛选规则"""
        scanner = config.scanner or {}
        ema_radar = config.ema_radar or {}
        
        return cls(
            # 从 ema_radar 节读取
            ema_periods=ema_radar.get("ema_periods", [21, 55, 100, 200]),
            timeframes=ema_radar.get("timeframes", ["1h", "4h", "1d"]),
            near_ema_threshold=ema_radar.get("near_ema_threshold", 0.01),
            top_n_results=ema_radar.get("top_n_results", 10),
            
            # 从 scanner 节读取 (复用币种筛选规则)
            auto_top_n=scanner.get("auto_top_n", 100),
            extra_whitelist=scanner.get("extra_whitelist", []),
            ignore_list=scanner.get("ignore_list", []),
            
            # 复用 scanner 的并发控制
            max_concurrent_requests=scanner.get("max_concurrent_requests", 10),
            request_delay=scanner.get("request_delay", 0.1),
        )


@dataclass
class EMATrendSignal:
    """EMA 趋势信号"""
    symbol: str
    timeframe: str  # "1h", "4h", "1d"
    
    # EMA 值
    ema21: float
    ema55: float
    ema100: float
    ema200: float
    current_price: float
    
    # 趋势判断
    trend: str  # "up", "down", "neutral"
    streak_bars: int  # 连续趋势 K线数
    
    # 开花状态
    is_flowering: bool = False
    flower_type: Optional[str] = None  # "bullish" | "bearish"
    flower_streak: int = 0  # 连续开花 K线数 (用于检测"刚进入排列")
    
    # 接近 EMA
    near_ema: Optional[str] = None  # "EMA21", "EMA55", etc.
    
    timestamp: datetime = field(default_factory=datetime.now)
    
    def format_telegram(self) -> str:
        """格式化 Telegram 输出"""
        parts = []
        
        # 优先级1: 开花标签
        if self.is_flowering:
            emoji = "🌸" if self.flower_type == "bullish" else "🥀"
            parts.append(f"【{emoji}】")
        
        # 币种 + 连续bars
        parts.append(f"{self.symbol.replace('USDT', '')}: ({self.streak_bars} Bars)")
        
        # 优先级3: 接近EMA
        if self.near_ema:
            parts.append(f"【Near {self.near_ema}】")
        
        return " ".join(parts)
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "ema21": self.ema21,
            "ema55": self.ema55,
            "ema100": self.ema100,
            "ema200": self.ema200,
            "price": self.current_price,
            "trend": self.trend,
            "streak": self.streak_bars,
            "flowering": self.is_flowering,
            "flower_type": self.flower_type,
            "flower_streak": self.flower_streak,
            "near_ema": self.near_ema,
        }


class EMARadar:
    """
    EMA 趋势雷达
    
    扫描所有币种的 EMA 排列状态，检测:
    1. 🌸 多头开花 (EMA21 > EMA55 > EMA100 > EMA200)
    2. 🥀 空头开花 (EMA21 < EMA55 < EMA100 < EMA200)
    3. 连续趋势 K线数 (Streak)
    4. 价格接近 EMA 的回调机会
    """
    
    def __init__(self, config: Optional[EMARadarConfig] = None):
        self.config = config or EMARadarConfig()
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
    
    async def start(self) -> None:
        """启动雷达"""
        self._session = aiohttp.ClientSession()
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        logger.info(f"EMA Radar started (timeframes: {self.config.timeframes})")
    
    async def stop(self) -> None:
        """停止雷达"""
        if self._session:
            await self._session.close()
        logger.info("EMA Radar stopped")
    
    async def scan(self, timeframe: str = "1h") -> Dict[str, List[EMATrendSignal]]:
        """
        扫描指定时间周期的 EMA 趋势
        
        Returns:
            {
                "uptrend": [按 streak 排序的上涨信号],
                "downtrend": [按 streak 排序的下跌信号]
            }
        """
        logger.info(f"Starting EMA scan for {timeframe}...")
        
        # Step 1: 获取币种列表
        symbols = await self._discover_symbols()
        logger.info(f"Scanning {len(symbols)} symbols...")
        
        # Step 2: 批量分析
        signals = await self._analyze_batch(symbols, timeframe)
        
        # Step 3: 分类排序
        uptrend = [s for s in signals if s.trend == "up"]
        downtrend = [s for s in signals if s.trend == "down"]
        
        # 按 streak 排序 (开花优先, 持续时间长优先)
        uptrend.sort(key=lambda x: (x.is_flowering, x.streak_bars), reverse=True)
        downtrend.sort(key=lambda x: (x.is_flowering, x.streak_bars), reverse=True)
        
        # 刚进入趋势的币种 (streak <= 3, 代表刚开始1-3根K线)
        new_uptrend = [s for s in uptrend if 1 <= s.streak_bars <= 3]
        new_downtrend = [s for s in downtrend if 1 <= s.streak_bars <= 3]
        
        # 新进入的按 streak 升序 (最新的优先)
        new_uptrend.sort(key=lambda x: x.streak_bars)
        new_downtrend.sort(key=lambda x: x.streak_bars)
        
        # 🆕 刚进入多头排列/空头排列的币种 (flower_streak <= 5, 代表刚开始1-5根K线)
        new_bullish_flowering = [s for s in signals if s.is_flowering and s.flower_type == "bullish" and 1 <= s.flower_streak <= 5]
        new_bearish_flowering = [s for s in signals if s.is_flowering and s.flower_type == "bearish" and 1 <= s.flower_streak <= 5]
        
        # 按 flower_streak 升序 (最新的优先)
        new_bullish_flowering.sort(key=lambda x: x.flower_streak)
        new_bearish_flowering.sort(key=lambda x: x.flower_streak)
        
        result = {
            "uptrend": uptrend[:self.config.top_n_results],
            "downtrend": downtrend[:self.config.top_n_results],
            "new_uptrend": new_uptrend[:self.config.top_n_results],
            "new_downtrend": new_downtrend[:self.config.top_n_results],
            "new_bullish_flowering": new_bullish_flowering[:self.config.top_n_results],
            "new_bearish_flowering": new_bearish_flowering[:self.config.top_n_results],
        }
        
        logger.info(
            f"Scan complete: {len(uptrend)} uptrend, {len(downtrend)} downtrend, "
            f"{len(new_uptrend)} new_up, {len(new_downtrend)} new_down, "
            f"{sum(1 for s in signals if s.is_flowering)} flowering, "
            f"{len(new_bullish_flowering)} new_bullish, {len(new_bearish_flowering)} new_bearish"
        )
        
        return result
    
    async def _discover_symbols(self) -> List[str]:
        """获取要扫描的币种"""
        final_symbols = set()
        
        try:
            url = f"{self.config.base_url}/fapi/v1/ticker/24hr"
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    return []
                tickers = await resp.json()
            
            # 过滤 USDT 并排序
            usdt_tickers = [t for t in tickers if t["symbol"].endswith("USDT")]
            usdt_tickers.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
            
            # Top N
            if self.config.auto_top_n > 0:
                top_symbols = [t["symbol"] for t in usdt_tickers[:self.config.auto_top_n]]
                final_symbols.update(top_symbols)
            
            # 白名单
            for symbol in self.config.extra_whitelist:
                if any(t["symbol"] == symbol for t in usdt_tickers):
                    final_symbols.add(symbol)
            
            # 忽略列表
            for symbol in self.config.ignore_list:
                final_symbols.discard(symbol)
                
        except Exception as e:
            logger.error(f"Symbol discovery error: {e}")
        
        return list(final_symbols)
    
    async def _analyze_batch(self, symbols: List[str], timeframe: str) -> List[EMATrendSignal]:
        """批量分析所有币种"""
        tasks = []
        for symbol in symbols:
            task = self._analyze_symbol(symbol, timeframe)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        signals = []
        for result in results:
            if isinstance(result, EMATrendSignal):
                signals.append(result)
            elif isinstance(result, Exception):
                logger.debug(f"Analysis error: {result}")
        
        return signals
    
    async def _analyze_symbol(self, symbol: str, timeframe: str) -> Optional[EMATrendSignal]:
        """分析单个币种"""
        async with self._semaphore:
            await asyncio.sleep(self.config.request_delay)
            
            try:
                # 获取 K线数据
                klines = await self._fetch_klines(symbol, timeframe)
                if len(klines) < self.config.kline_limit - 50:
                    return None
                
                # 计算 EMA
                closes = [float(k[4]) for k in klines]  # 收盘价
                ema21 = self._calculate_ema(closes, 21)
                ema55 = self._calculate_ema(closes, 55)
                ema100 = self._calculate_ema(closes, 100)
                ema200 = self._calculate_ema(closes, 200)
                
                current_price = closes[-1]
                
                # 检测开花
                is_flowering, flower_type = self._detect_flowering(ema21, ema55, ema100, ema200)
                
                # 计算开花持续 K 线数 (只有开花时才计算，避免性能开销)
                flower_streak = 0
                if is_flowering:
                    flower_streak = self._calculate_flower_streak(closes)
                
                # 判断趋势和连续bars
                trend, streak = self._calculate_streak(closes, ema21)
                
                # 检测接近 EMA
                near_ema = self._detect_near_ema(current_price, ema21, ema55, ema100, ema200)
                
                return EMATrendSignal(
                    symbol=symbol,
                    timeframe=timeframe,
                    ema21=ema21,
                    ema55=ema55,
                    ema100=ema100,
                    ema200=ema200,
                    current_price=current_price,
                    trend=trend,
                    streak_bars=streak,
                    is_flowering=is_flowering,
                    flower_type=flower_type,
                    flower_streak=flower_streak,
                    near_ema=near_ema,
                )
                
            except Exception as e:
                logger.debug(f"Error analyzing {symbol}: {e}")
                return None
    
    async def _fetch_klines(self, symbol: str, timeframe: str) -> List:
        """获取 K线数据"""
        url = f"{self.config.base_url}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": timeframe,
            "limit": self.config.kline_limit,
        }
        
        async with self._session.get(url, params=params) as resp:
            if resp.status != 200:
                raise Exception(f"Klines API error: {resp.status}")
            return await resp.json()
    
    def _calculate_ema(self, prices: List[float], period: int) -> float:
        """计算 EMA"""
        if len(prices) < period:
            return prices[-1]
        
        multiplier = 2 / (period + 1)
        ema = sum(prices[:period]) / period  # 初始 SMA
        
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        
        return ema
    
    def _detect_flowering(
        self, ema21: float, ema55: float, ema100: float, ema200: float
    ) -> Tuple[bool, Optional[str]]:
        """检测 EMA 完美排列 (开花)"""
        
        # 🌸 多头开花
        if ema21 > ema55 > ema100 > ema200:
            return True, "bullish"
        
        # 🥀 空头开花
        if ema21 < ema55 < ema100 < ema200:
            return True, "bearish"
        
        return False, None
    
    def _calculate_flower_streak(self, closes: List[float], periods: List[int] = [21, 55, 100, 200]) -> int:
        """
        计算连续开花 K线数
        
        从最新往前数，连续满足 EMA 完美排列的 K 线数量
        """
        if len(closes) < max(periods) + 10:
            return 0
        
        # 计算每根K线的所有 EMA 值
        streak = 0
        for i in range(len(closes) - 1, max(periods) - 1, -1):
            # 获取截止到该 K 线的 closes
            sub_closes = closes[:i + 1]
            
            # 计算当时的 EMA 值
            ema21 = self._calculate_ema(sub_closes, 21)
            ema55 = self._calculate_ema(sub_closes, 55)
            ema100 = self._calculate_ema(sub_closes, 100)
            ema200 = self._calculate_ema(sub_closes, 200)
            
            is_flowering, _ = self._detect_flowering(ema21, ema55, ema100, ema200)
            
            if is_flowering:
                streak += 1
            else:
                break
        
        return streak
    
    def _calculate_streak(self, closes: List[float], ema21: float) -> Tuple[str, int]:
        """
        计算连续趋势 K线数
        
        规则: 价格在 EMA21 上方 = uptrend, 下方 = downtrend
        """
        if not closes:
            return "neutral", 0
        
        current_price = closes[-1]
        trend = "up" if current_price > ema21 else "down"
        
        # 从最新往前数连续在同一侧的 K线
        streak = 0
        for i in range(len(closes) - 1, -1, -1):
            if trend == "up" and closes[i] > ema21:
                streak += 1
            elif trend == "down" and closes[i] < ema21:
                streak += 1
            else:
                break
        
        return trend, streak
    
    def _detect_near_ema(
        self, price: float, ema21: float, ema55: float, ema100: float, ema200: float
    ) -> Optional[str]:
        """检测价格是否接近某条 EMA"""
        threshold = self.config.near_ema_threshold
        
        emas = [
            ("EMA21", ema21),
            ("EMA55", ema55),
            ("EMA100", ema100),
            ("EMA200", ema200),
        ]
        
        for name, ema in emas:
            if abs(price - ema) / ema < threshold:
                return name
        
        return None
    
    def format_telegram_report(self, results: Dict[str, List[EMATrendSignal]], timeframe: str) -> str:
        """生成 Telegram 报告"""
        lines = [f"[{timeframe.upper()}] EMA Trend Radar 📡", ""]
        
        # 上涨趋势
        lines.append("🚀 Consecutive Uptrend Top")
        for i, signal in enumerate(results["uptrend"], 1):
            lines.append(f"{i}. {signal.format_telegram()}")
        
        lines.append("")
        
        # 下跌趋势
        lines.append("📉 Consecutive Downtrend Top")
        for i, signal in enumerate(results["downtrend"], 1):
            lines.append(f"{i}. {signal.format_telegram()}")
        
        return "\n".join(lines)


async def main():
    """测试运行"""
    radar = EMARadar(EMARadarConfig(
        auto_top_n=30,  # 测试用较少币种
        timeframes=["1h"],
    ))
    
    await radar.start()
    
    print("=" * 60)
    print("📡 EMA Trend Radar")
    print("=" * 60)
    
    results = await radar.scan("1h")
    report = radar.format_telegram_report(results, "1h")
    print(report)
    
    await radar.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
