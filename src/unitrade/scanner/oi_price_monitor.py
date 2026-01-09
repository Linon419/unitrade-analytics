"""
OIPRICE Monitor - OI 异动 + 价格异动（5m 开收）报警（Binance USDT 永续）

信号定义（默认）：
  - OI(5m) 变化率 >= oi_change_threshold
  - 价格(5m 开收) 变化率 >= price_change_threshold
  - OI 价值 >= min_oi_value_usdt

注：
  - OI 来自 /futures/data/openInterestHist(period=5m)（离散 5m 点），因此最早在 K 线收盘时才能确认信号。
"""

from __future__ import annotations

import asyncio
import html
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import aiohttp

logger = logging.getLogger(__name__)


def classify_oi_price(oi_change_pct: float, price_change_pct: float) -> str:
    """Return a short label describing OI/price regime."""
    if oi_change_pct >= 0 and price_change_pct >= 0:
        return "📈 Long Build"
    if oi_change_pct >= 0 and price_change_pct < 0:
        return "📉 Short Build"
    if oi_change_pct < 0 and price_change_pct >= 0:
        return "🟢 Short Cover"
    return "🔴 Long Unwind"


def pct_change(new: float, old: float) -> Optional[float]:
    if old <= 0:
        return None
    return (new - old) / old


def _format_pct(value: float) -> str:
    return f"{value * 100:+.2f}%"


def _format_money(value: float) -> str:
    return f"${value:,.0f}"


def _floor_to_5m_end(ts_s: float) -> int:
    """Return 5m boundary timestamp (ms) for the last *closed* candle end."""
    ts_ms = int(ts_s * 1000)
    bucket = (ts_ms // 300_000) * 300_000
    # If exactly on boundary, treat previous bucket as last closed.
    if ts_ms % 300_000 == 0:
        bucket -= 300_000
    return bucket


@dataclass
class OIPriceMonitorConfig:
    enabled: bool = True

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_topic_id: Optional[int] = None

    # thresholds
    oi_change_threshold: float = 0.03  # 3%
    price_change_threshold: float = 0.03  # 3% (5m open->close)
    min_oi_value_usdt: float = 2_000_000.0

    # universe
    min_quote_volume_24h: float = 1_000_000.0
    max_symbols: int = 50
    symbols: Optional[list[str]] = None

    # controls
    poll_interval_seconds: int = 10
    cooldown_seconds: int = 600  # per symbol
    max_concurrent_requests: int = 10


class OIPriceMonitor:
    BINANCE_REST_BASE = "https://fapi.binance.com"

    def __init__(self, config: OIPriceMonitorConfig):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

        self._symbols: list[str] = []
        self._last_bucket_end_ms: Optional[int] = None
        self._last_alert_ts: Dict[str, float] = {}

    async def start(self) -> None:
        if not self.config.enabled:
            return
        if not self.config.telegram_bot_token or not self.config.telegram_chat_id:
            raise RuntimeError("Missing telegram credentials")

        self._session = aiohttp.ClientSession()
        self._running = True

        await self._refresh_universe()
        self._task = asyncio.create_task(self._loop())
        logger.info("OIPriceMonitor started (symbols=%d)", len(self._symbols))

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None
        if self._session:
            await self._session.close()
            self._session = None
        logger.info("OIPriceMonitor stopped")

    async def _refresh_universe(self) -> None:
        if self.config.symbols:
            self._symbols = [s.strip().upper() for s in self.config.symbols if str(s).strip()]
            return

        if not self._session:
            return

        url = f"{self.BINANCE_REST_BASE}/fapi/v1/ticker/24hr"
        async with self._session.get(url) as resp:
            if resp.status != 200:
                raise RuntimeError(f"ticker/24hr failed: {await resp.text()}")
            tickers = await resp.json()

        usdt = []
        for t in tickers or []:
            symbol = str(t.get("symbol", "")).upper()
            if not symbol.endswith("USDT"):
                continue
            try:
                qv = float(t.get("quoteVolume", 0) or 0)
            except Exception:
                continue
            if qv >= float(self.config.min_quote_volume_24h):
                usdt.append((symbol, qv))

        usdt.sort(key=lambda x: x[1], reverse=True)
        max_symbols = int(self.config.max_symbols) if self.config.max_symbols else 50
        if max_symbols <= 0:
            max_symbols = 50
        self._symbols = [s for s, _ in usdt[:max_symbols]]

    async def _loop(self) -> None:
        assert self._session is not None
        cfg = self.config
        sem = asyncio.Semaphore(max(1, int(cfg.max_concurrent_requests)))

        while self._running:
            try:
                bucket_end_ms = _floor_to_5m_end(time.time())
                if self._last_bucket_end_ms is None or bucket_end_ms > self._last_bucket_end_ms:
                    self._last_bucket_end_ms = bucket_end_ms
                    await self._scan_bucket(bucket_end_ms, sem)

                await asyncio.sleep(max(1, int(cfg.poll_interval_seconds)))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("OIPriceMonitor loop error: %s", e)
                await asyncio.sleep(5)

    async def _scan_bucket(self, bucket_end_ms: int, sem: asyncio.Semaphore) -> None:
        if not self._session:
            return
        if not self._symbols:
            await self._refresh_universe()
            if not self._symbols:
                return

        tasks = [self._analyze_symbol(symbol, bucket_end_ms, sem) for symbol in list(self._symbols)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, dict) and r.get("triggered"):
                await self._send_alert(r)

    async def _analyze_symbol(self, symbol: str, bucket_end_ms: int, sem: asyncio.Semaphore) -> Optional[dict]:
        now = time.time()
        last = self._last_alert_ts.get(symbol, 0.0)
        if now - last < max(1, int(self.config.cooldown_seconds)):
            return None

        async with sem:
            oi = await self._fetch_oi_5m(symbol, bucket_end_ms)
        if not oi:
            return None

        oi_old, oi_new, oi_value, oi_ts_ms = oi
        if oi_old <= 0:
            return None

        oi_change = pct_change(oi_new, oi_old)
        if oi_change is None:
            return None

        if float(oi_value) < float(self.config.min_oi_value_usdt):
            return None

        # Fetch kline for the 5m candle ending at bucket_end_ms
        async with sem:
            candle = await self._fetch_kline_5m(symbol, bucket_end_ms)
        if not candle:
            return None
        open_price, close_price, open_ms = candle
        if open_price <= 0:
            return None
        price_change = (close_price - open_price) / open_price

        if abs(oi_change) < float(self.config.oi_change_threshold):
            return None
        if abs(price_change) < float(self.config.price_change_threshold):
            return None

        label = classify_oi_price(oi_change, price_change)
        return {
            "triggered": True,
            "symbol": symbol,
            "bucket_end_ms": bucket_end_ms,
            "oi_ts_ms": oi_ts_ms,
            "oi_old": oi_old,
            "oi_new": oi_new,
            "oi_value": oi_value,
            "oi_change": oi_change,
            "open": open_price,
            "close": close_price,
            "open_ms": open_ms,
            "price_change": price_change,
            "label": label,
        }

    async def _fetch_oi_5m(self, symbol: str, bucket_end_ms: int) -> Optional[Tuple[float, float, float, int]]:
        if not self._session:
            return None
        url = f"{self.BINANCE_REST_BASE}/futures/data/openInterestHist"
        params = {
            "symbol": symbol,
            "period": "5m",
            "endTime": bucket_end_ms,
            "limit": 2,
        }
        async with self._session.get(url, params=params) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()

        if not data or len(data) < 2:
            return None
        try:
            oi_old = float(data[0]["sumOpenInterest"])
            oi_new = float(data[1]["sumOpenInterest"])
            oi_value = float(data[1]["sumOpenInterestValue"])
            ts_ms = int(data[1]["timestamp"])
        except Exception:
            return None
        return (oi_old, oi_new, oi_value, ts_ms)

    async def _fetch_kline_5m(self, symbol: str, bucket_end_ms: int) -> Optional[Tuple[float, float, int]]:
        if not self._session:
            return None
        open_ms = bucket_end_ms - 300_000
        url = f"{self.BINANCE_REST_BASE}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": "5m",
            "startTime": open_ms,
            "endTime": bucket_end_ms - 1,
            "limit": 1,
        }
        async with self._session.get(url, params=params) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
        if not data:
            return None
        try:
            k = data[0]
            o = float(k[1])
            c = float(k[4])
            open_time_ms = int(k[0])
            return (o, c, open_time_ms)
        except Exception:
            return None

    def _build_message(self, payload: dict) -> str:
        symbol = str(payload["symbol"])
        oi_change = float(payload["oi_change"])
        price_change = float(payload["price_change"])
        label = str(payload.get("label", ""))

        end_dt = datetime.fromtimestamp(int(payload["bucket_end_ms"]) / 1000, tz=timezone.utc)
        end_str = end_dt.strftime("%Y-%m-%d %H:%M UTC")

        header = f"⚡ <b>OI + Price Spike</b> {html.escape(label)}"
        lines = [
            f"Symbol: {symbol}",
            f"Time (bar close): {end_str}",
            f"Price (5m O→C): {payload['open']:.6g} → {payload['close']:.6g} ({_format_pct(price_change)})",
            f"OI (5m): {payload['oi_old']:,.0f} → {payload['oi_new']:,.0f} ({_format_pct(oi_change)})",
            f"OI Value: {_format_money(float(payload['oi_value']))}",
            f"Thresholds: OI≥{self.config.oi_change_threshold*100:.1f}% & Price≥{self.config.price_change_threshold*100:.1f}% & OIValue≥{_format_money(self.config.min_oi_value_usdt)}",
        ]

        body = html.escape("\n".join(lines))
        return f"{header}\n<pre>{body}</pre>"

    async def _send_alert(self, payload: dict) -> None:
        if not self._session:
            return
        cfg = self.config
        symbol = str(payload.get("symbol", ""))
        self._last_alert_ts[symbol] = time.time()

        url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendMessage"
        data = {
            "chat_id": cfg.telegram_chat_id,
            "text": self._build_message(payload),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if cfg.telegram_topic_id:
            data["message_thread_id"] = cfg.telegram_topic_id

        async with self._session.post(url, json=data) as resp:
            if resp.status != 200:
                logger.error("OIPrice telegram send failed: %s", await resp.text())

