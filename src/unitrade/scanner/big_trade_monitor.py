"""
BigTradeMonitor - 逐笔成交大单监控（Binance USDT 永续）

基于 24h ticker 的成交额/成交笔数估算“平均单笔成交额”，按倍数动态生成每个币种的大单阈值：
  threshold = max(min_notional, (quoteVolume / count) * avg_multiplier)

当 aggTrade 单笔名义成交额 (price*qty) >= threshold 时，推送 Telegram（HTML）。
"""

from __future__ import annotations

import asyncio
import html
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Tuple

import aiohttp

from .signal_detector.calculators import TradeData
from .signal_detector.websocket import BinanceWSManager, WSConfig

logger = logging.getLogger(__name__)


def _format_compact_usdt(value: float) -> str:
    abs_val = abs(value)
    if abs_val >= 1e12:
        return f"${value / 1e12:.2f}T"
    if abs_val >= 1e9:
        return f"${value / 1e9:.2f}B"
    if abs_val >= 1e6:
        return f"${value / 1e6:.2f}M"
    if abs_val >= 1e3:
        return f"${value / 1e3:.2f}K"
    return f"${value:,.0f}"


def _format_usdt(value: float) -> str:
    return f"${value:,.0f}"


def _format_qty(value: float) -> str:
    abs_val = abs(value)
    if abs_val >= 1000:
        return f"{value:,.0f}"
    if abs_val >= 10:
        return f"{value:,.2f}"
    return f"{value:,.4f}"


def compute_dynamic_threshold(
    quote_volume_24h: float,
    trade_count_24h: int,
    *,
    min_notional: float,
    avg_multiplier: float,
) -> Tuple[float, float]:
    """Return (threshold, avg_trade_notional)."""
    if trade_count_24h <= 0 or quote_volume_24h <= 0:
        avg = 0.0
        return (float(min_notional), avg)
    avg = float(quote_volume_24h) / int(trade_count_24h)
    threshold = max(float(min_notional), avg * float(avg_multiplier))
    return (threshold, avg)


@dataclass
class BigTradeMonitorConfig:
    enabled: bool = True

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_topic_id: Optional[int] = None

    # Threshold model (dynamic per symbol)
    min_notional: float = 50_000.0
    avg_multiplier: float = 200.0

    # Controls
    cooldown_seconds: int = 30
    refresh_seconds: int = 600
    include_open_interest: bool = True

    # Universe (if symbols is None -> auto top by 24h quoteVolume)
    min_quote_volume_24h: float = 1_000_000.0
    max_symbols: int = 50
    symbols: Optional[list[str]] = None


class BigTradeMonitor:
    BINANCE_REST_BASE = "https://fapi.binance.com"

    def __init__(self, config: BigTradeMonitorConfig):
        self.config = config

        ws_cfg = WSConfig(
            market="futures",
            streams=["aggTrade"],
            symbols=config.symbols,
            min_quote_volume_24h=config.min_quote_volume_24h,
            max_symbols=config.max_symbols,
        )
        self.ws_manager = BinanceWSManager(ws_cfg)
        self.ws_manager.on_trade(self._on_trade)

        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._tasks: list[asyncio.Task] = []

        self._thresholds: Dict[str, float] = {}
        self._avg_trade_notional: Dict[str, float] = {}
        self._quote_volume_24h: Dict[str, float] = {}
        self._trade_count_24h: Dict[str, int] = {}
        self._last_alert_ts: Dict[str, float] = {}

    async def start(self) -> None:
        if not self.config.enabled:
            return
        if not self.config.telegram_bot_token or not self.config.telegram_chat_id:
            raise RuntimeError("Missing telegram credentials")

        self._session = aiohttp.ClientSession()
        self._running = True

        await self.ws_manager.start()

        # Prime stats/thresholds then keep refreshing.
        await self._refresh_thresholds()
        self._tasks.append(asyncio.create_task(self._threshold_refresh_loop()))

        logger.info("BigTradeMonitor started (symbols=%d)", len(self.ws_manager.symbols))

    async def stop(self) -> None:
        self._running = False
        for t in list(self._tasks):
            t.cancel()
        self._tasks.clear()

        await self.ws_manager.stop()
        if self._session:
            await self._session.close()
            self._session = None

        logger.info("BigTradeMonitor stopped")

    async def _threshold_refresh_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(max(30, int(self.config.refresh_seconds)))
                await self._refresh_thresholds()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("BigTradeMonitor refresh error: %s", e)

    async def _refresh_thresholds(self) -> None:
        if not self._session:
            return

        url = f"{self.BINANCE_REST_BASE}/fapi/v1/ticker/24hr"
        async with self._session.get(url) as resp:
            if resp.status != 200:
                raise RuntimeError(f"ticker/24hr failed: {await resp.text()}")
            tickers = await resp.json()

        symbol_set = self.ws_manager.symbols
        for t in tickers:
            symbol = str(t.get("symbol", "")).upper()
            if symbol not in symbol_set:
                continue
            try:
                quote_vol = float(t.get("quoteVolume", 0) or 0)
                count = int(t.get("count", 0) or 0)
            except Exception:
                continue

            threshold, avg = compute_dynamic_threshold(
                quote_vol,
                count,
                min_notional=self.config.min_notional,
                avg_multiplier=self.config.avg_multiplier,
            )
            self._thresholds[symbol] = threshold
            self._avg_trade_notional[symbol] = avg
            self._quote_volume_24h[symbol] = quote_vol
            self._trade_count_24h[symbol] = count

    def _on_trade(self, trade: TradeData) -> None:
        symbol = trade.symbol
        threshold = self._thresholds.get(symbol)
        if threshold is None:
            threshold = float(self.config.min_notional)
        if trade.quote_volume < threshold:
            return

        now = time.time()
        last = self._last_alert_ts.get(symbol, 0.0)
        if now - last < max(1, int(self.config.cooldown_seconds)):
            return
        self._last_alert_ts[symbol] = now

        if not self._session:
            return
        asyncio.create_task(self._send_big_trade_alert(trade, threshold))

    async def _fetch_open_interest(self, symbol: str) -> Optional[float]:
        if not self._session:
            return None
        url = f"{self.BINANCE_REST_BASE}/fapi/v1/openInterest"
        params = {"symbol": symbol}
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                oi = data.get("openInterest")
                return float(oi) if oi is not None else None
        except Exception:
            return None

    def _build_message(self, trade: TradeData, threshold: float, oi: Optional[float]) -> str:
        side = "Ask Side" if trade.is_buy else "Bid Side"
        side_emoji = "🟢" if trade.is_buy else "🔴"

        avg = self._avg_trade_notional.get(trade.symbol, 0.0)
        quote_vol = self._quote_volume_24h.get(trade.symbol, 0.0)
        count = self._trade_count_24h.get(trade.symbol, 0)

        dt = datetime.fromtimestamp(trade.timestamp)
        ts_str = dt.strftime("%Y-%m-%d %H:%M:%S")

        header = f"🔥 <b>大单来了 - {html.escape(side)}</b> {side_emoji}"

        lines = [
            f"Symbol: {trade.symbol}",
            f"Notional: {_format_usdt(trade.quote_volume)}",
            f"Price: {trade.price:,.6g}",
            f"Qty: {_format_qty(trade.quantity)}",
        ]
        if oi is not None:
            lines.append(f"Open Interest: {oi:,.0f}")
        if quote_vol > 0:
            lines.append(f"24h Volume: {_format_compact_usdt(quote_vol)}")
        if count > 0:
            lines.append(f"24h Trades: {count:,d}")
        if avg > 0:
            lines.append(f"Avg Trade: {_format_usdt(avg)}")
        lines.append(
            f"Threshold: {_format_usdt(threshold)} (avg×{self.config.avg_multiplier:g}, min {_format_usdt(self.config.min_notional)})"
        )
        lines.append(f"Time: {ts_str}")

        body = html.escape("\n".join(lines))
        return f"{header}\n<pre>{body}</pre>"

    async def _send_big_trade_alert(self, trade: TradeData, threshold: float) -> None:
        cfg = self.config
        if not cfg.telegram_bot_token or not cfg.telegram_chat_id:
            return
        if not self._session:
            return

        oi: Optional[float] = None
        if cfg.include_open_interest:
            oi = await self._fetch_open_interest(trade.symbol)

        url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendMessage"
        payload = {
            "chat_id": cfg.telegram_chat_id,
            "text": self._build_message(trade, threshold, oi),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if cfg.telegram_topic_id:
            payload["message_thread_id"] = cfg.telegram_topic_id

        try:
            async with self._session.post(url, json=payload) as resp:
                if resp.status != 200:
                    logger.error("Big trade telegram send failed: %s", await resp.text())
        except Exception as e:
            logger.error("Big trade telegram error: %s", e)

