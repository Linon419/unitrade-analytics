"""
CoinArch-style anomaly reporter bot.

Builds a periodic Telegram message that mimics the "CoinArch" format:
1) Long/Short index table
2) "Startup" (启动) list
3) Funding anomaly list
4) Fast move list (快涨/慢涨/暴拉/暴跌)
5) Volatility count ranking (23h)

Run:
    $env:PYTHONPATH="src"
    python -m unitrade.scanner.signal_detector.coinarch_bot
"""

from __future__ import annotations

import asyncio
import contextlib
import html
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Tuple

import aiohttp

from .calculators import CompositeCalculator, SignalResult
from .websocket import BinanceWSManager, WSConfig

logger = logging.getLogger(__name__)


@dataclass
class CoinArchBotConfig:
    # Reporting cadence
    report_interval_minutes: int = 5

    # Section 1: index
    index_top_n: int = 8
    index_weight_price_change_5m: float = 2.0
    index_weight_price_change_15m: float = 3.0
    index_weight_price_change_60m: float = 2.0
    index_weight_price_change_24h: float = 0.5
    index_weight_flow_ratio: float = 20.0
    index_flow_ratio_clip: float = 0.35
    index_weight_net_flow_5m_per_10k: float = 0.0
    index_weight_rvol_5m_over_1: float = 10.0
    index_rvol_clip: float = 6.0
    index_weight_oi_change_pct: float = 2.0
    index_weight_funding_penalty: float = 30.0
    index_funding_penalty_threshold_pct: float = 0.05
    index_funding_penalty_cap_pct: float = 0.50
    index_rvol_window_minutes: int = 5
    index_oi_enrich_top_m: int = 25
    index_oi_period: str = "15m"
    event_oi_period: str = "5m"
    oi_cache_ttl_seconds: int = 120

    # Section 2: startup
    startup_top_n: int = 3
    startup_min_price_change_1m_pct: float = 2.0
    startup_min_net_flow_5m: float = 10_000.0

    # Section 3: funding anomaly
    funding_top_n: int = 3
    funding_anomaly_threshold_pct: float = 0.50  # absolute funding %, e.g. 0.50 = 0.50%

    # Section 4: fast moves
    move_top_n: int = 5
    move_pump_threshold_pct_5m: float = 4.0
    move_dump_threshold_pct_5m: float = 4.0
    move_fast_threshold_pct_15m: float = 3.0
    move_slow_threshold_pct_60m: float = 2.0

    # Rebound lows cache
    rebound_periods_days: Sequence[int] = (1, 3, 7, 14, 30)
    rebound_cache_max_age_seconds: int = 3600

    # Section 5: volatility ranking
    volatility_window_hours: int = 23
    volatility_event_threshold_pct_1m: float = 1.0
    volatility_top_n: int = 20

    # Binance REST endpoints
    binance_base_url: str = "https://fapi.binance.com"


def _format_flow_cn(value: float) -> str:
    abs_val = abs(value)
    if abs_val >= 1e8:
        return f"{value/1e8:.2f}亿"
    if abs_val >= 1e4:
        return f"{value/1e4:.2f}万"
    return f"{value:.0f}"


def _safe_pct_str(value: float, decimals: int = 2, signed: bool = True) -> str:
    if signed:
        return f"{value:+.{decimals}f}%"
    return f"{value:.{decimals}f}%"


def _compute_return_pct(
    history: Deque[Tuple[int, float]],
    window_minutes: int,
) -> Optional[float]:
    if window_minutes <= 0 or len(history) < 2:
        return None

    latest_ts, latest_price = history[-1]
    cutoff = latest_ts - window_minutes * 60

    past_price: Optional[float] = None
    for ts, price in reversed(history):
        if ts <= cutoff:
            past_price = price
            break

    if past_price is None or past_price <= 0:
        return None
    return (latest_price - past_price) / past_price * 100.0


class CoinArchBot:
    def __init__(
        self,
        *,
        telegram_bot_token: str,
        telegram_chat_id: str,
        telegram_topic_id: Optional[int] = None,
        redis_url: str = "redis://localhost:6379",  # kept for future compatibility
        ws_config: Optional[WSConfig] = None,
        config: Optional[CoinArchBotConfig] = None,
    ):
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.telegram_topic_id = telegram_topic_id

        self.config = config or CoinArchBotConfig()

        self.calculator = CompositeCalculator(
            {
                "flow_window": 5,
                "rvol_window": 60,
                "rebound_periods": list(self.config.rebound_periods_days),
            }
        )
        self.ws_manager = BinanceWSManager(ws_config or WSConfig())

        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._report_task: Optional[asyncio.Task] = None

        # Price history (closed 1m candles): {symbol: deque([(minute_open_ts, close_price), ...])}
        self._minute_closes: Dict[str, Deque[Tuple[int, float]]] = defaultdict(lambda: deque(maxlen=60 * 30))

        # Volatility events timestamps (seconds): {symbol: deque([ts, ...])}
        self._vol_events: Dict[str, Deque[int]] = defaultdict(lambda: deque(maxlen=10_000))

        # Previous long-index top list, for 🆕 marker
        self._prev_long_top: set[str] = set()

        # OI change cache: {(symbol, period): (fetched_at_ts, oi_change_pct)}
        self._oi_change_cache: Dict[Tuple[str, str], Tuple[float, Optional[float]]] = {}
        self._oi_fetch_semaphore = asyncio.Semaphore(10)

        # Simple kline dedupe: only count closed candles once
        self._last_closed_minute_ts: Dict[str, int] = {}

    async def start(self) -> None:
        if not self.telegram_bot_token or not self.telegram_chat_id:
            raise RuntimeError("Telegram config missing (bot_token/chat_id)")

        self._session = aiohttp.ClientSession()

        self.ws_manager.on_trade(self._on_trade)
        self.ws_manager.on_kline(self._on_kline)

        self._running = True
        await self.ws_manager.start()

        self._report_task = asyncio.create_task(self._report_loop())
        logger.info("CoinArch bot started")

    async def stop(self) -> None:
        self._running = False

        if self._report_task:
            self._report_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._report_task

        await self.ws_manager.stop()

        if self._session:
            await self._session.close()

        logger.info("CoinArch bot stopped")

    async def run_forever(self) -> None:
        await self.start()
        try:
            while self._running:
                await asyncio.sleep(1)
        finally:
            await self.stop()

    def _on_trade(self, trade) -> None:
        self.calculator.process_trade(trade)

    def _on_kline(self, kline) -> None:
        self.calculator.process_kline(kline)

        if not getattr(kline, "is_closed", False):
            return

        minute_ts = int(kline.timestamp)
        symbol = kline.symbol

        if self._last_closed_minute_ts.get(symbol) == minute_ts:
            return
        self._last_closed_minute_ts[symbol] = minute_ts

        history = self._minute_closes[symbol]
        prev_close = history[-1][1] if history else None
        history.append((minute_ts, float(kline.close)))

        # Volatility event: 1m close-to-close absolute move >= threshold
        if prev_close and prev_close > 0:
            move_pct = (float(kline.close) - prev_close) / prev_close * 100.0
            if abs(move_pct) >= self.config.volatility_event_threshold_pct_1m:
                self._vol_events[symbol].append(int(time.time()))

        # Prune volatility deque (time-based)
        self._prune_vol_events(symbol)

    def _prune_vol_events(self, symbol: str) -> None:
        window_seconds = self.config.volatility_window_hours * 3600
        cutoff = int(time.time()) - window_seconds
        dq = self._vol_events.get(symbol)
        if not dq:
            return
        while dq and dq[0] < cutoff:
            dq.popleft()

    async def _fetch_json(self, url: str, params: Optional[dict] = None) -> object:
        assert self._session is not None
        async with self._session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _fetch_ticker_24hr(self) -> Dict[str, dict]:
        data = await self._fetch_json(f"{self.config.binance_base_url}/fapi/v1/ticker/24hr")
        out: Dict[str, dict] = {}
        for item in data:
            symbol = item.get("symbol", "")
            if not symbol.endswith("USDT"):
                continue
            out[symbol] = item
        return out

    async def _fetch_premium_index(self) -> Dict[str, float]:
        data = await self._fetch_json(f"{self.config.binance_base_url}/fapi/v1/premiumIndex")
        out: Dict[str, float] = {}
        for item in data:
            symbol = item.get("symbol", "")
            if not symbol.endswith("USDT"):
                continue
            try:
                out[symbol] = float(item.get("lastFundingRate", 0.0)) * 100.0
            except Exception:
                continue
        return out

    async def _fetch_oi_change_pct(self, symbol: str, period: str = "5m") -> Optional[float]:
        try:
            data = await self._fetch_json(
                f"{self.config.binance_base_url}/futures/data/openInterestHist",
                params={"symbol": symbol, "period": period, "limit": 2},
            )
            if not isinstance(data, list) or len(data) < 2:
                return None

            def _to_oi(item: dict) -> float:
                for key in ("sumOpenInterest", "openInterest", "openInterestValue", "sumOpenInterestValue"):
                    if key in item and item[key] not in (None, ""):
                        try:
                            return float(item[key])
                        except Exception:
                            pass
                return 0.0

            prev = _to_oi(data[-2])
            last = _to_oi(data[-1])
            if prev <= 0:
                return None
            return (last - prev) / prev * 100.0
        except Exception:
            return None

    async def _get_oi_change_pct(self, symbol: str, period: str = "5m") -> Optional[float]:
        key = (symbol, period)
        now = time.time()

        cached = self._oi_change_cache.get(key)
        if cached:
            fetched_at, value = cached
            if now - fetched_at <= self.config.oi_cache_ttl_seconds:
                return value

        async with self._oi_fetch_semaphore:
            value = await self._fetch_oi_change_pct(symbol, period)
            self._oi_change_cache[key] = (now, value)
            return value

    async def _ensure_rebound_lows(self, symbol: str) -> None:
        # Only refresh if stale
        if not self.calculator.rebound.needs_update(symbol, self.config.rebound_cache_max_age_seconds):
            return

        max_days = max(self.config.rebound_periods_days) if self.config.rebound_periods_days else 30
        try:
            data = await self._fetch_json(
                f"{self.config.binance_base_url}/fapi/v1/klines",
                params={"symbol": symbol, "interval": "1d", "limit": max_days},
            )
            if not isinstance(data, list) or not data:
                return

            lows: Dict[int, float] = {}
            for days in self.config.rebound_periods_days:
                window = data[-days:] if len(data) >= days else data
                try:
                    lows[days] = min(float(k[3]) for k in window)  # index 3 = low
                except Exception:
                    continue

            if lows:
                self.calculator.rebound.set_lows(symbol, lows)
        except Exception:
            return

    async def _send_telegram(self, text: str) -> None:
        assert self._session is not None
        url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"

        payload = {
            "chat_id": self.telegram_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if self.telegram_topic_id is not None:
            payload["message_thread_id"] = self.telegram_topic_id

        async with self._session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                logger.error("Telegram send failed: %s", await resp.text())

    def _score_long(
        self,
        *,
        price_change_5m: float,
        price_change_15m: float,
        price_change_60m: float,
        price_change_24h: float,
        net_flow_5m: float,
        flow_ratio_5m: float,
        rvol_5m: float,
        funding_pct: float,
        oi_change_pct: Optional[float],
    ) -> float:
        cfg = self.config
        score = 0.0
        score += max(price_change_5m, 0.0) * cfg.index_weight_price_change_5m
        score += max(price_change_15m, 0.0) * cfg.index_weight_price_change_15m
        score += max(price_change_60m, 0.0) * cfg.index_weight_price_change_60m
        score += max(price_change_24h, 0.0) * cfg.index_weight_price_change_24h

        flow_clip = max(float(cfg.index_flow_ratio_clip), 0.0)
        flow_ratio_eff = max(min(flow_ratio_5m, flow_clip), -flow_clip) if flow_clip else flow_ratio_5m
        score += max(flow_ratio_eff, 0.0) * cfg.index_weight_flow_ratio
        if cfg.index_weight_net_flow_5m_per_10k:
            score += max(net_flow_5m, 0.0) / 10_000.0 * cfg.index_weight_net_flow_5m_per_10k

        rvol_eff = min(rvol_5m, float(cfg.index_rvol_clip)) if cfg.index_rvol_clip else rvol_5m
        score += max(rvol_eff - 1.0, 0.0) * cfg.index_weight_rvol_5m_over_1

        # Crowded longs: funding > 0, apply penalty
        funding_over = max(funding_pct - cfg.index_funding_penalty_threshold_pct, 0.0)
        if cfg.index_funding_penalty_cap_pct:
            funding_over = min(funding_over, cfg.index_funding_penalty_cap_pct)
        score -= max(funding_over, 0.0) * cfg.index_weight_funding_penalty

        # Trend-follow confirmation: OI increase while price is moving up (use 15m as proxy)
        if oi_change_pct is not None and price_change_15m > 0:
            score += max(oi_change_pct, 0.0) * cfg.index_weight_oi_change_pct
        return score

    def _score_short(
        self,
        *,
        price_change_5m: float,
        price_change_15m: float,
        price_change_60m: float,
        price_change_24h: float,
        net_flow_5m: float,
        flow_ratio_5m: float,
        rvol_5m: float,
        funding_pct: float,
        oi_change_pct: Optional[float],
    ) -> float:
        cfg = self.config
        score = 0.0
        score += max(-price_change_5m, 0.0) * cfg.index_weight_price_change_5m
        score += max(-price_change_15m, 0.0) * cfg.index_weight_price_change_15m
        score += max(-price_change_60m, 0.0) * cfg.index_weight_price_change_60m
        score += max(-price_change_24h, 0.0) * cfg.index_weight_price_change_24h

        flow_clip = max(float(cfg.index_flow_ratio_clip), 0.0)
        flow_ratio_eff = max(min(flow_ratio_5m, flow_clip), -flow_clip) if flow_clip else flow_ratio_5m
        score += max(-flow_ratio_eff, 0.0) * cfg.index_weight_flow_ratio
        if cfg.index_weight_net_flow_5m_per_10k:
            score += max(-net_flow_5m, 0.0) / 10_000.0 * cfg.index_weight_net_flow_5m_per_10k

        rvol_eff = min(rvol_5m, float(cfg.index_rvol_clip)) if cfg.index_rvol_clip else rvol_5m
        score += max(rvol_eff - 1.0, 0.0) * cfg.index_weight_rvol_5m_over_1

        # Crowded shorts: funding < 0, apply penalty
        funding_over = max((-funding_pct) - cfg.index_funding_penalty_threshold_pct, 0.0)
        if cfg.index_funding_penalty_cap_pct:
            funding_over = min(funding_over, cfg.index_funding_penalty_cap_pct)
        score -= max(funding_over, 0.0) * cfg.index_weight_funding_penalty

        # Trend-follow confirmation: OI increase while price is moving down (use 15m as proxy)
        if oi_change_pct is not None and price_change_15m < 0:
            score += max(oi_change_pct, 0.0) * cfg.index_weight_oi_change_pct
        return score

    def _format_index_table(
        self,
        rows: Sequence[Tuple[str, float, float, bool]],
        *,
        title: str,
    ) -> str:
        # rows: (base, pct24h, score, is_new)
        lines = [html.escape(title), ""]
        table = []
        for base, pct, score, is_new in rows:
            new_tag = " 🆕" if is_new else ""
            table.append(f"{base:<12} ({pct:>7.2f}%) {score:>6.1f}分{new_tag}")
        lines.append("<pre>" + html.escape("\n".join(table)) + "</pre>")
        return "\n".join(lines)

    def _format_vol_table(self, rows: Sequence[Tuple[str, float, int]]) -> str:
        # rows: (base, pct24h, count)
        table = []
        for i, (base, pct, count) in enumerate(rows, start=1):
            table.append(f"{i:>2}.{base:<10} ({pct:>7.2f}%) {count:>6}次")
        return "<pre>" + html.escape("\n".join(table)) + "</pre>"

    async def _build_report(self) -> str:
        cfg = self.config

        symbols = list(self.ws_manager.symbols)
        tickers = await self._fetch_ticker_24hr()
        premium = await self._fetch_premium_index()

        # ========== Section 1: Long/Short index ==========
        symbol_state: Dict[str, Dict] = {}

        for symbol in symbols:
            base = symbol.replace("USDT", "")
            t = tickers.get(symbol, {})
            try:
                pct24h = float(t.get("priceChangePercent", 0.0))
            except Exception:
                pct24h = 0.0

            funding_pct = float(premium.get(symbol, 0.0))

            net_flow, buy_vol, sell_vol = self.calculator.flow.calculate(symbol)
            denom = buy_vol + sell_vol
            flow_ratio = (net_flow / denom) if denom > 0 else 0.0

            rvol_5m, _, _ = self.calculator.rvol.calculate_window(symbol, cfg.index_rvol_window_minutes)

            history = self._minute_closes.get(symbol)
            ret5 = _compute_return_pct(history, 5) if history else None
            ret15 = _compute_return_pct(history, 15) if history else None
            ret60 = _compute_return_pct(history, 60) if history else None

            price_change_5m = float(ret5 or 0.0)
            price_change_15m = float(ret15 or 0.0)
            price_change_60m = float(ret60 or 0.0)

            symbol_state[symbol] = {
                "base": base,
                "pct24h": pct24h,
                "price_change_5m": price_change_5m,
                "price_change_15m": price_change_15m,
                "price_change_60m": price_change_60m,
                "net_flow": net_flow,
                "flow_ratio": flow_ratio,
                "rvol_5m": rvol_5m,
                "funding_pct": funding_pct,
                "oi_change_pct": None,
            }

        # OI enrichment for top candidates only (reduce REST calls)
        enrich_n = max(int(cfg.index_oi_enrich_top_m), cfg.index_top_n)
        long_candidates = sorted(
            symbol_state.items(),
            key=lambda kv: self._score_long(
                price_change_5m=kv[1]["price_change_5m"],
                price_change_15m=kv[1]["price_change_15m"],
                price_change_60m=kv[1]["price_change_60m"],
                price_change_24h=kv[1]["pct24h"],
                net_flow_5m=kv[1]["net_flow"],
                flow_ratio_5m=kv[1]["flow_ratio"],
                rvol_5m=kv[1]["rvol_5m"],
                funding_pct=kv[1]["funding_pct"],
                oi_change_pct=None,
            ),
            reverse=True,
        )[:enrich_n]
        short_candidates = sorted(
            symbol_state.items(),
            key=lambda kv: self._score_short(
                price_change_5m=kv[1]["price_change_5m"],
                price_change_15m=kv[1]["price_change_15m"],
                price_change_60m=kv[1]["price_change_60m"],
                price_change_24h=kv[1]["pct24h"],
                net_flow_5m=kv[1]["net_flow"],
                flow_ratio_5m=kv[1]["flow_ratio"],
                rvol_5m=kv[1]["rvol_5m"],
                funding_pct=kv[1]["funding_pct"],
                oi_change_pct=None,
            ),
            reverse=True,
        )[:enrich_n]

        enrich_symbols = {sym for sym, _ in long_candidates} | {sym for sym, _ in short_candidates}
        if enrich_symbols:
            tasks = {sym: asyncio.create_task(self._get_oi_change_pct(sym, cfg.index_oi_period)) for sym in enrich_symbols}
            for sym, task in tasks.items():
                symbol_state[sym]["oi_change_pct"] = await task

        long_items: List[Tuple[str, float, float]] = []
        short_items: List[Tuple[str, float, float]] = []

        for symbol, st in symbol_state.items():
            long_score = self._score_long(
                price_change_5m=st["price_change_5m"],
                price_change_15m=st["price_change_15m"],
                price_change_60m=st["price_change_60m"],
                price_change_24h=st["pct24h"],
                net_flow_5m=st["net_flow"],
                flow_ratio_5m=st["flow_ratio"],
                rvol_5m=st["rvol_5m"],
                funding_pct=st["funding_pct"],
                oi_change_pct=st["oi_change_pct"],
            )
            short_score = self._score_short(
                price_change_5m=st["price_change_5m"],
                price_change_15m=st["price_change_15m"],
                price_change_60m=st["price_change_60m"],
                price_change_24h=st["pct24h"],
                net_flow_5m=st["net_flow"],
                flow_ratio_5m=st["flow_ratio"],
                rvol_5m=st["rvol_5m"],
                funding_pct=st["funding_pct"],
                oi_change_pct=st["oi_change_pct"],
            )

            if long_score > 0:
                long_items.append((st["base"], st["pct24h"], long_score))
            if short_score > 0:
                short_items.append((st["base"], st["pct24h"], short_score))

        long_items.sort(key=lambda x: x[2], reverse=True)
        short_items.sort(key=lambda x: x[2], reverse=True)

        long_top = long_items[: cfg.index_top_n]
        short_top = short_items[: cfg.index_top_n]

        long_rows = [(b, p, s, b not in self._prev_long_top) for (b, p, s) in long_top]
        self._prev_long_top = {b for (b, _, _, _) in long_rows}

        parts: List[str] = []
        parts.append("<b>1. 多头/空头指数：CoinArch</b>")
        parts.append("")
        parts.append(self._format_index_table(long_rows, title="🈯 多头指数更新"))

        if short_top:
            short_rows = [(b, p, s, False) for (b, p, s) in short_top]
            parts.append("")
            parts.append(self._format_index_table(short_rows, title="🈯 空头指数更新"))

        # ========== Section 2: Startup ==========
        startup_candidates: List[Tuple[SignalResult, float]] = []
        for symbol in symbols:
            result = self.calculator.calculate_all(symbol)
            if not result:
                continue
            if result.price_change_pct < cfg.startup_min_price_change_1m_pct:
                continue
            if result.net_flow < cfg.startup_min_net_flow_5m:
                continue
            startup_candidates.append((result, result.net_flow))

        startup_candidates.sort(key=lambda x: x[1], reverse=True)
        startup_candidates = startup_candidates[: cfg.startup_top_n]

        parts.append("")
        parts.append("<b>2. 启动：</b>")
        if not startup_candidates:
            parts.append("（无）")
        else:
            for result, _ in startup_candidates:
                symbol = result.symbol
                base = symbol.replace("USDT", "")
                rvol_5m, _, _ = self.calculator.rvol.calculate_window(symbol, 5)
                oi_change = await self._get_oi_change_pct(symbol, cfg.event_oi_period)  # optional

                chunks = [
                    f"#{base} - {result.price:.5f} ({result.price_change_pct:.2f}%) 启动",
                    f"量能{rvol_5m:.1f}x",
                ]
                if oi_change is not None:
                    chunks.append(f"持仓{oi_change:+.2f}%")
                chunks.append(f"净流入{_format_flow_cn(result.net_flow)}")
                parts.append(" , ".join(chunks))

        # ========== Section 3: Funding anomaly ==========
        parts.append("")
        parts.append("<b>3. 资金费率异常：</b>")

        premium_items = [(s, pct) for s, pct in premium.items() if s in tickers]
        premium_items.sort(key=lambda x: abs(x[1]), reverse=True)

        rank_by_symbol = {s: i for i, (s, _) in enumerate(premium_items, start=1)}
        anomalies = [
            (s, pct, rank_by_symbol.get(s, 0))
            for s, pct in premium_items
            if abs(pct) >= cfg.funding_anomaly_threshold_pct
        ][: cfg.funding_top_n]

        if not anomalies:
            parts.append("（无）")
        else:
            for symbol, funding_pct, rank in anomalies:
                base = symbol.replace("USDT", "")
                t = tickers.get(symbol, {})
                try:
                    price = float(t.get("lastPrice", 0.0))
                except Exception:
                    price = 0.0
                try:
                    pct24h = float(t.get("priceChangePercent", 0.0))
                except Exception:
                    pct24h = 0.0
                parts.append(
                    f"#{base} - {price:.4f} ({pct24h:.2f}%) , 资金费异常: {funding_pct:+.4f}% [{rank}]"
                )

        # ========== Section 4: Fast moves ==========
        parts.append("")
        parts.append("<b>4. 快涨/慢涨 爆拉/暴跌：</b>")

        move_rows: List[Tuple[str, str, float]] = []  # (symbol, label, move_pct)
        for symbol, history in self._minute_closes.items():
            ret5 = _compute_return_pct(history, 5)
            ret15 = _compute_return_pct(history, 15)
            ret60 = _compute_return_pct(history, 60)

            label: Optional[str] = None
            move_pct: Optional[float] = None

            if ret5 is not None and ret5 >= cfg.move_pump_threshold_pct_5m:
                label, move_pct = "暴拉 🚀", ret5
            elif ret5 is not None and ret5 <= -cfg.move_dump_threshold_pct_5m:
                label, move_pct = "暴跌", ret5
            elif ret15 is not None and ret15 >= cfg.move_fast_threshold_pct_15m:
                label, move_pct = "快涨", ret15
            elif ret15 is not None and ret15 <= -cfg.move_fast_threshold_pct_15m:
                label, move_pct = "快跌", ret15
            elif ret60 is not None and ret60 >= cfg.move_slow_threshold_pct_60m:
                label, move_pct = "慢涨", ret60
            elif ret60 is not None and ret60 <= -cfg.move_slow_threshold_pct_60m:
                label, move_pct = "慢跌", ret60

            if label is None or move_pct is None:
                continue
            move_rows.append((symbol, label, move_pct))

        move_rows.sort(key=lambda x: abs(x[2]), reverse=True)
        move_rows = move_rows[: cfg.move_top_n]

        if not move_rows:
            parts.append("（无）")
        else:
            for symbol, label, move_pct in move_rows:
                result = self.calculator.calculate_all(symbol)
                if not result:
                    continue
                await self._ensure_rebound_lows(symbol)
                rebound_pct, rebound_days, _ = self.calculator.rebound.calculate(symbol, result.price)

                rvol_5m, _, _ = self.calculator.rvol.calculate_window(symbol, 5)
                oi_change = await self._get_oi_change_pct(symbol, cfg.event_oi_period)

                chunks = [
                    f"#{symbol.replace('USDT', '')} - {result.price:.5f} ({move_pct:.2f}%) {label}",
                    f"量能{rvol_5m:.1f}x",
                ]
                if oi_change is not None:
                    chunks.append(f"持仓{oi_change:+.2f}%")
                if abs(result.net_flow) >= 10_000:
                    chunks.append(f"净流入{_format_flow_cn(result.net_flow)}")
                if rebound_pct >= 3.0 and rebound_days > 0:
                    chunks.append(f"自{rebound_days}天前的低点反弹{rebound_pct:.2f}%")
                parts.append(" , ".join(chunks))

        # ========== Section 5: Volatility ranking ==========
        parts.append("")
        parts.append("<b>5. 波动次数排行：</b>今日23小时波动次数排行")

        vol_counts: List[Tuple[str, float, int]] = []
        for symbol in symbols:
            self._prune_vol_events(symbol)
            count = len(self._vol_events.get(symbol, ()))
            if count <= 0:
                continue
            base = symbol.replace("USDT", "")
            t = tickers.get(symbol, {})
            try:
                pct24h = float(t.get("priceChangePercent", 0.0))
            except Exception:
                pct24h = 0.0
            vol_counts.append((base, pct24h, count))

        vol_counts.sort(key=lambda x: x[2], reverse=True)
        vol_counts = vol_counts[: cfg.volatility_top_n]

        if not vol_counts:
            parts.append("（无）")
        else:
            parts.append(self._format_vol_table(vol_counts))

        # Telegram max length ~4096
        message = "\n".join(parts)
        if len(message) > 4000:
            message = message[:3990] + "\n…"
        return message

    async def _report_loop(self) -> None:
        interval = max(self.config.report_interval_minutes, 1) * 60
        # Slight delay so caches warm up
        await asyncio.sleep(5)

        while self._running:
            start_ts = time.time()
            try:
                report = await self._build_report()
                await self._send_telegram(report)
            except Exception as e:
                logger.error("Report loop error: %s", e)

            elapsed = time.time() - start_ts
            await asyncio.sleep(max(1.0, interval - elapsed))


async def main() -> None:
    import yaml

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # Load config/default.yaml if present (same pattern as analyzer.py)
    config_path = Path(__file__).parents[4] / "config" / "default.yaml"
    if not config_path.exists():
        config_path = Path("config/default.yaml")

    cfg: dict = {}
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

    tg_cfg = cfg.get("telegram", {}) if isinstance(cfg, dict) else {}
    sd_cfg = cfg.get("signal_detector", {}) if isinstance(cfg, dict) else {}

    bot_token = tg_cfg.get("bot_token", "")
    chat_id = tg_cfg.get("chat_id", "")
    topic_id = sd_cfg.get("topic_id", tg_cfg.get("topics", {}).get("signal_anomaly"))

    bot_cfg = CoinArchBotConfig(
        report_interval_minutes=int(sd_cfg.get("report_interval_minutes", 5)),
        index_top_n=int(sd_cfg.get("index_top_n", 8)),
        index_weight_price_change_5m=float(sd_cfg.get("index_weight_price_change_5m", 2.0)),
        index_weight_price_change_15m=float(sd_cfg.get("index_weight_price_change_15m", 3.0)),
        index_weight_price_change_60m=float(sd_cfg.get("index_weight_price_change_60m", 2.0)),
        index_weight_price_change_24h=float(sd_cfg.get("index_weight_price_change_24h", 0.5)),
        index_weight_flow_ratio=float(sd_cfg.get("index_weight_flow_ratio", 20.0)),
        index_flow_ratio_clip=float(sd_cfg.get("index_flow_ratio_clip", 0.35)),
        index_weight_net_flow_5m_per_10k=float(sd_cfg.get("index_weight_net_flow_5m_per_10k", 0.0)),
        index_weight_rvol_5m_over_1=float(sd_cfg.get("index_weight_rvol_5m_over_1", 10.0)),
        index_rvol_clip=float(sd_cfg.get("index_rvol_clip", 6.0)),
        index_weight_oi_change_pct=float(sd_cfg.get("index_weight_oi_change_pct", 2.0)),
        index_weight_funding_penalty=float(sd_cfg.get("index_weight_funding_penalty", 30.0)),
        index_funding_penalty_threshold_pct=float(sd_cfg.get("index_funding_penalty_threshold_pct", 0.05)),
        index_funding_penalty_cap_pct=float(sd_cfg.get("index_funding_penalty_cap_pct", 0.50)),
        index_rvol_window_minutes=int(sd_cfg.get("index_rvol_window_minutes", 5)),
        index_oi_enrich_top_m=int(sd_cfg.get("index_oi_enrich_top_m", 25)),
        index_oi_period=str(sd_cfg.get("index_oi_period", "15m")),
        event_oi_period=str(sd_cfg.get("event_oi_period", "5m")),
        oi_cache_ttl_seconds=int(sd_cfg.get("oi_cache_ttl_seconds", 120)),
        startup_top_n=int(sd_cfg.get("startup_top_n", 3)),
        startup_min_price_change_1m_pct=float(sd_cfg.get("startup_min_price_change_1m_pct", 2.0)),
        startup_min_net_flow_5m=float(sd_cfg.get("startup_min_net_flow_5m", 10_000.0)),
        funding_top_n=int(sd_cfg.get("funding_top_n", 3)),
        funding_anomaly_threshold_pct=float(sd_cfg.get("funding_anomaly_threshold_pct", 0.50)),
        move_top_n=int(sd_cfg.get("move_top_n", 5)),
        move_pump_threshold_pct_5m=float(sd_cfg.get("move_pump_threshold_pct_5m", 4.0)),
        move_dump_threshold_pct_5m=float(sd_cfg.get("move_dump_threshold_pct_5m", 4.0)),
        move_fast_threshold_pct_15m=float(sd_cfg.get("move_fast_threshold_pct_15m", 3.0)),
        move_slow_threshold_pct_60m=float(sd_cfg.get("move_slow_threshold_pct_60m", 2.0)),
        volatility_top_n=int(sd_cfg.get("volatility_top_n", 20)),
        volatility_window_hours=int(sd_cfg.get("volatility_window_hours", 23)),
        volatility_event_threshold_pct_1m=float(sd_cfg.get("volatility_event_threshold_pct_1m", 1.0)),
    )

    ws_cfg = WSConfig(
        min_quote_volume_24h=float(sd_cfg.get("min_quote_volume_24h", 1_000_000)),
        max_symbols=int(sd_cfg.get("max_symbols", 50)),
    )

    bot = CoinArchBot(
        telegram_bot_token=bot_token,
        telegram_chat_id=chat_id,
        telegram_topic_id=topic_id,
        ws_config=ws_cfg,
        config=bot_cfg,
    )

    print("=" * 60)
    print("CoinArch-style bot running")
    print(f"Telegram chat_id={chat_id[:10]}... topic={topic_id}")
    print("=" * 60)
    print("Press Ctrl+C to stop")

    try:
        await bot.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    asyncio.run(main())
