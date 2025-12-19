"""
上涨指数排行系统

从 Redis 中读取 AnomalyDetector 产生的突破信号，按资金流/价格结构/新鲜度/量能综合评分。
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional

import aiohttp
import numpy as np

from .redis_state import RedisStateManager

if TYPE_CHECKING:
    from .anomaly_detector import BreakoutSignal

logger = logging.getLogger(__name__)


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
            f"?{self.total_score:.1f} "
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
            ttl_days=self.config.signal_ttl_days,
        )

    async def get_ranking(self, top_n: int = 20) -> List[RisingScore]:
        """获取上涨指数排行"""
        keys = await self._state_manager.scan_signal_keys(prefix=self.config.redis_prefix)

        scores: List[RisingScore] = []
        for key in keys:
            symbol = key.split(":")[-1]
            score = await self._calc_symbol_score(symbol)
            if score:
                scores.append(score)

        scores.sort(key=lambda x: x.total_score, reverse=True)
        return scores[:top_n]

    async def _calc_symbol_score(self, symbol: str) -> Optional[RisingScore]:
        """计算单个币种的上涨指数"""
        now = time.time()
        signals = await self._state_manager.get_breakout_signals(symbol=symbol, prefix=self.config.redis_prefix)

        if not signals:
            return None

        oi_changes: List[float] = []
        rvols: List[float] = []
        recency_weights: List[float] = []
        prices: List[float] = []
        ema200s: List[float] = []

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

                hours_ago = (now - timestamp) / 3600
                decay = float(np.exp(-hours_ago / self.config.recency_decay_hours))
                recency_weights.append(decay)

        if not oi_changes:
            return None

        cumulative_oi = sum(oi_changes)
        oi_consistency = sum(1 for oi in oi_changes if oi > 0) / len(oi_changes)
        oi_score = self._normalize(cumulative_oi, 0, 0.5) * 0.6 + oi_consistency * 0.4

        recency_score = sum(recency_weights) / len(recency_weights) if recency_weights else 0.0

        avg_rvol = sum(rvols) / len(rvols) if rvols else 0.0
        volume_score = self._normalize(avg_rvol, 1, 10)

        price_score, ema_alignment, price_change = await self._calc_price_structure(
            symbol, prices[-1] if prices else 0.0, ema200s[-1] if ema200s else 0.0
        )

        total = (
            self.config.price_structure_weight * price_score
            + self.config.oi_flow_weight * oi_score
            + self.config.recency_weight * recency_score
            + self.config.volume_weight * volume_score
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
    ) -> tuple[float, str, float]:
        """计算价格结构得分"""
        try:
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {"symbol": symbol, "interval": "4h", "limit": 50}

            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    return 0.5, "neutral", 0.0
                klines = await resp.json()

            closes = [float(k[4]) for k in klines]
            current_price = closes[-1] if closes else last_signal_price

            ema21 = self._calc_ema(closes, 21)
            ema55 = self._calc_ema(closes, 55)
            ema100 = self._calc_ema(closes, 100)
            ema200 = self._calc_ema(closes, 200) if len(closes) >= 200 else last_ema200

            if ema21 > ema55 > ema100:
                ema_alignment = "bullish"
                alignment_score = 1.0
            elif ema21 < ema55 < ema100:
                ema_alignment = "bearish"
                alignment_score = 0.0
            else:
                ema_alignment = "neutral"
                alignment_score = 0.5

            ema200_distance = (current_price - ema200) / ema200 if ema200 > 0 else 0.0
            distance_score = self._normalize(ema200_distance, -0.1, 0.3)

            lows = [float(k[3]) for k in klines]
            recent_lows = lows[-10:]
            higher_lows = sum(1 for i in range(1, len(recent_lows)) if recent_lows[i] > recent_lows[i - 1])
            hl_score = higher_lows / max(len(recent_lows) - 1, 1)

            if len(closes) >= 30:
                price_5d_ago = closes[-30]  # 约5天 (4h * 30 = 120h)
                price_change = (current_price - price_5d_ago) / price_5d_ago if price_5d_ago > 0 else 0.0
            else:
                price_change = 0.0

            price_score = (
                0.30 * hl_score
                + 0.30 * alignment_score
                + 0.25 * distance_score
                + 0.15 * self._normalize(price_change, -0.1, 0.3)
            )

            return float(price_score), ema_alignment, float(price_change)
        except Exception as e:
            logger.debug(f"Error calculating price structure for {symbol}: {e}")
            return 0.5, "neutral", 0.0

    def _calc_ema(self, closes: List[float], period: int) -> float:
        """计算 EMA"""
        if len(closes) < period:
            return closes[-1] if closes else 0.0

        multiplier = 2 / (period + 1)
        ema = sum(closes[:period]) / period

        for price in closes[period:]:
            ema = (price - ema) * multiplier + ema

        return float(ema)

    def _normalize(self, value: float, min_val: float, max_val: float) -> float:
        """归一化到 0-1"""
        if max_val == min_val:
            return 0.5
        return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))

    def format_ranking(self, scores: List[RisingScore]) -> str:
        """格式化排行榜"""
        lines = ["?? *上涨潜力排行 (5日评估)*", "━" * 25]
        for i, score in enumerate(scores, 1):
            lines.append(score.format_text(i))
        return "\n".join(lines)


async def get_rising_ranking(redis_url: str = "redis://localhost:6379", top_n: int = 20) -> List[RisingScore]:
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
    top_n: int = 10,
) -> None:
    """
    定时推送上涨指数排行到 Telegram

    每 N 小时推送 Top M 币种排行榜
    """

    async def push_ranking() -> None:
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
                    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
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

