"""
上涨指数排行系统

从 Redis 中读取 AnomalyDetector 产生的突破信号，按资金流/价格结构/量能综合评分。
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import aiohttp

from .redis_state import RedisStateManager
from unitrade.core.time import format_ts, resolve_tz

if TYPE_CHECKING:
    from .anomaly_detector import BreakoutSignal

logger = logging.getLogger(__name__)


@dataclass
class RisingIndexConfig:
    """上涨指数配置"""

    redis_url: str = "redis://localhost:6379"
    redis_prefix: str = "anomaly"

    # 权重
    price_structure_weight: float = 0.60
    oi_flow_weight: float = 0.25
    volume_weight: float = 0.15

    # 时间衰减
    # 信号保留时间
    signal_ttl_days: int = 5

    # EMA 周期
    ema_periods: List[int] = field(default_factory=lambda: [21, 55, 100, 200])

    # Trend component (ported from nofx/analysis/trend/trend.go)
    # Blended into price_structure_score:
    # price_score = (1 - trend_mix_weight) * old_price_score + trend_mix_weight * trend_score
    trend_mix_weight: float = 0.2
    trend_angle_cap_deg: float = 0.6
    trend_dev_cap_pct: float = 3.0


@dataclass
class RisingScore:
    """上涨指数得分"""

    symbol: str
    total_score: float
    price_structure_score: float
    oi_flow_score: float
    volume_score: float
    signal_count: int
    cumulative_oi_change: float
    price_change_5d: float
    ema_alignment: str  # 'bullish', 'bearish', 'neutral'
    current_price: float = 0.0
    first_ranked_ts: Optional[float] = None
    first_ranked_price: Optional[float] = None
    price_change_since_rank: Optional[float] = None

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
    - 价格结构 (60%): 更高低点、EMA 排列、EMA200 距离、突破持续性
    - 资金流入 (25%): 累计 OI 变化、OI 增加一致性
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
        top = scores[:top_n]
        await self._enrich_first_ranked(top)
        return top

    async def _enrich_first_ranked(self, scores: List[RisingScore]) -> None:
        if not scores or not self._state_manager:
            return

        now = time.time()
        for s in scores:
            ts, price = await self._state_manager.get_or_set_first_ranked(
                symbol=s.symbol,
                ts=now,
                price=float(s.current_price or 0.0),
                prefix=self.config.redis_prefix,
            )
            s.first_ranked_ts = float(ts)
            s.first_ranked_price = float(price)
            if price and s.current_price:
                s.price_change_since_rank = (float(s.current_price) - float(price)) / float(price)

    async def _calc_symbol_score(self, symbol: str) -> Optional[RisingScore]:
        """计算单个币种的上涨指数"""
        signals = await self._state_manager.get_breakout_signals(symbol=symbol, prefix=self.config.redis_prefix)

        if not signals:
            return None

        oi_changes: List[float] = []
        rvols: List[float] = []
        prices: List[float] = []
        ema200s: List[float] = []

        for signal_data, _timestamp in signals:
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

        if not oi_changes:
            return None

        cumulative_oi = sum(oi_changes)
        oi_consistency = sum(1 for oi in oi_changes if oi > 0) / len(oi_changes)
        oi_score = self._normalize(cumulative_oi, 0, 0.5) * 0.6 + oi_consistency * 0.4

        avg_rvol = sum(rvols) / len(rvols) if rvols else 0.0
        volume_score = self._normalize(avg_rvol, 1, 10)

        price_score, ema_alignment, price_change, current_price = await self._calc_price_structure(
            symbol, prices[-1] if prices else 0.0, ema200s[-1] if ema200s else 0.0
        )

        weight_sum = (
            float(self.config.price_structure_weight)
            + float(self.config.oi_flow_weight)
            + float(self.config.volume_weight)
        )
        weight_sum = weight_sum if weight_sum > 0 else 1.0
        total = (
            (
                self.config.price_structure_weight * price_score
                + self.config.oi_flow_weight * oi_score
                + self.config.volume_weight * volume_score
            )
            / weight_sum
        ) * 100

        return RisingScore(
            symbol=symbol,
            total_score=total,
            price_structure_score=price_score * 100,
            oi_flow_score=oi_score * 100,
            volume_score=volume_score * 100,
            signal_count=len(signals),
            cumulative_oi_change=cumulative_oi,
            price_change_5d=price_change,
            ema_alignment=ema_alignment,
            current_price=current_price,
        )

    async def _calc_price_structure(
        self, symbol: str, last_signal_price: float, last_ema200: float
    ) -> tuple[float, str, float, float]:
        """计算价格结构得分"""
        try:
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {"symbol": symbol, "interval": "30m", "limit": 50}

            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    return 0.5, "neutral", 0.0, float(last_signal_price)
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

            price_score_old = (
                0.30 * hl_score
                + 0.30 * alignment_score
                + 0.25 * distance_score
                + 0.15 * self._normalize(price_change, -0.1, 0.3)
            )

            trend_score = self._calc_trend_score(closes)
            if trend_score is None:
                price_score = float(price_score_old)
            else:
                w = max(0.0, min(1.0, float(self.config.trend_mix_weight or 0.0)))
                price_score = float((1.0 - w) * price_score_old + w * trend_score)

            return float(price_score), ema_alignment, float(price_change), float(current_price)
        except Exception as e:
            logger.debug(f"Error calculating price structure for {symbol}: {e}")
            return 0.5, "neutral", 0.0, float(last_signal_price)

    def _fit_line(self, series: List[float]) -> Tuple[float, float]:
        """Simple linear regression on y=series, x=[0..n-1]. Returns (slope, intercept)."""
        if not series:
            return 0.0, 0.0

        n = float(len(series))
        sum_x = sum_y = sum_xy = sum_xx = 0.0
        for i, y in enumerate(series):
            x = float(i)
            y = float(y)
            sum_x += x
            sum_y += y
            sum_xy += x * y
            sum_xx += x * x

        denom = n * sum_xx - sum_x * sum_x
        if denom == 0.0:
            return 0.0, float(series[-1])

        slope = (n * sum_xy - sum_x * sum_y) / denom
        intercept = (sum_y - slope * sum_x) / n
        return float(slope), float(intercept)

    def _calc_trend_score(self, closes: List[float]) -> Optional[float]:
        """
        Trend score ported from nofx/analysis/trend/trend.go.

        Uses normalized close series (close/close[0]) for cross-symbol comparability.
        Returns [0,1], where higher means stronger upward trend with limited baseline deviation.
        """
        if len(closes) < 5:
            return None

        base = float(closes[0])
        if base <= 0.0:
            return None

        normalized = [float(c) / base for c in closes]
        slope, intercept = self._fit_line(normalized)

        angle_deg = math.degrees(math.atan(slope))
        if angle_deg <= 0.0:
            trend_strength = 0.0
        else:
            cap = float(self.config.trend_angle_cap_deg or 0.6)
            cap = max(1e-9, cap)
            trend_strength = self._normalize(angle_deg, 0.0, cap)

        baseline = intercept + slope * float(len(normalized) - 1)
        if baseline != 0.0:
            deviation_pct = (normalized[-1] - baseline) / baseline * 100.0
        else:
            deviation_pct = 0.0

        dev_cap = float(self.config.trend_dev_cap_pct or 3.0)
        dev_cap = max(1e-9, dev_cap)
        deviation_penalty = 1.0 - self._normalize(abs(deviation_pct), 0.0, dev_cap)

        return max(0.0, min(1.0, trend_strength * deviation_penalty))

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
        tz = resolve_tz()
        now_str = datetime.now(tz=tz).strftime("%m-%d %H:%M %Z")

        lines = ["🏆 *上涨潜力排行 (5日评估)*", f"⏰ {now_str}", "━" * 25]
        for i, s in enumerate(scores, 1):
            base = s.symbol.replace("USDT", "")
            since = f"{s.price_change_since_rank:+.1%}" if s.price_change_since_rank is not None else "n/a"
            first = format_ts(s.first_ranked_ts, "%m-%d %H:%M") if s.first_ranked_ts else "-"

            lines.append(f"{i}. *{base}* ⚡{s.total_score:.1f}  ({since})")
            lines.append(
                f"   价{s.price_structure_score:.1f} 资{s.oi_flow_score:.1f} 量{s.volume_score:.1f} | 首上榜{first}"
            )
        return "\n".join(lines)


def _load_rising_index_overrides() -> Dict:
    """
    Load `anomaly_detector.rising_index` from global config if available.

    This keeps the ranking weights and trend mix tunable via `config/default.yaml`
    (or `UNITRADE_CONFIG`) while remaining backward-compatible.
    """
    try:
        from unitrade.core.config import load_config

        conf = load_config()
        ad = getattr(conf, "anomaly_detector", None) or {}
        if not isinstance(ad, dict):
            return {}
        ri = ad.get("rising_index") or {}
        return ri if isinstance(ri, dict) else {}
    except Exception:
        return {}


def _build_rising_index_config(redis_url: str) -> RisingIndexConfig:
    overrides = _load_rising_index_overrides()
    allowed = set(RisingIndexConfig.__dataclass_fields__.keys())
    kwargs = {k: v for k, v in overrides.items() if k in allowed and k != "redis_url"}
    return RisingIndexConfig(redis_url=redis_url, **kwargs)


async def get_rising_ranking(redis_url: str = "redis://localhost:6379", top_n: int = 20) -> List[RisingScore]:
    """便捷方法: 获取上涨指数排行"""
    config = _build_rising_index_config(redis_url=redis_url)
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
            config = _build_rising_index_config(redis_url=redis_url)
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
