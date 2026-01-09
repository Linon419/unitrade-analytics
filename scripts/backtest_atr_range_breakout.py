"""
Backtest ATR-range breakout strategy (1h) on Binance USDT-M futures.

Strategy (as agreed):
1) Compute structure range over the last N candles (excluding current):
   - HH = highest high of previous N bars
   - LL = lowest low of previous N bars
2) Compute ATR(length) (RMA) on the same timeframe.
3) Breakout thresholds:
   - Upper = HH + k * ATR
   - Lower = LL - k * ATR
4) Signal (at bar close):
   - Long if close > Upper
   - Short if close < Lower
5) Entry: next bar open
6) Stop-loss: the threshold line from the signal bar (Upper for long, Lower for short)
7) Exit: reverse signal (stop-and-reverse at next open) OR time stop (max 24h = 24 bars)

Optional:
- Squeeze context (BB inside KC) is computed and printed; can be required via flags.

Example:
  python scripts/backtest_atr_range_breakout.py --symbol BSVUSDT --tz Australia/Sydney --date 2026-01-05 --start 05:00 --end 10:00
  python scripts/backtest_atr_range_breakout.py --symbol BSVUSDT --tz Australia/Sydney --date 2026-01-05 --start 05:00 --duration-hours 24
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import sys
import time
import urllib.parse
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo


_INTERVAL_MS = {
    "1m": 60 * 1000,
    "3m": 3 * 60 * 1000,
    "1h": 60 * 60 * 1000,
}


@dataclass(frozen=True)
class Bar:
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class Window:
    start_utc: datetime
    end_utc: datetime  # end exclusive


@dataclass
class Trade:
    side: str  # "long" | "short"
    entry_time_utc: datetime
    entry_price: float
    exit_time_utc: datetime
    exit_price: float
    exit_reason: str  # stop | reverse | time | end
    stop_price: float

    def pnl_pct(self) -> float:
        if self.entry_price <= 0:
            return 0.0
        if self.side == "long":
            return (self.exit_price / self.entry_price - 1.0) * 100.0
        return (self.entry_price / self.exit_price - 1.0) * 100.0 if self.exit_price > 0 else 0.0


def _http_get_json(url: str) -> object:
    req = urllib.request.Request(url, headers={"User-Agent": "unitrade-analytics"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_get_bytes(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "unitrade-analytics"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def _download_to_cache(url: str, cache_path: Path) -> Path:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path
    data = _http_get_bytes(url)
    cache_path.write_bytes(data)
    return cache_path


def _binance_vision_bookdepth_zip_url(symbol: str, utc_date: str) -> str:
    return (
        "https://data.binance.vision/data/futures/um/daily/bookDepth/"
        f"{symbol}/{symbol}-bookDepth-{utc_date}.zip"
    )


def _read_bookdepth_csv_from_zip(zip_bytes: bytes):
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = zf.namelist()
    if not names:
        return []
    name = names[0]
    with zf.open(name) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
        for row in reader:
            yield row


def _obi(bid: float, ask: float) -> float:
    denom = bid + ask
    if denom <= 0:
        return 0.0
    return (bid - ask) / denom


def _fetch_fapi_klines(symbol: str, interval: str, start_ms: int, end_ms_incl: int) -> List[list]:
    base = "https://fapi.binance.com"
    ep = "/fapi/v1/klines"

    if interval not in _INTERVAL_MS:
        raise ValueError(f"Unsupported interval: {interval}")

    out: List[list] = []
    cursor = start_ms
    step = _INTERVAL_MS[interval]

    while cursor <= end_ms_incl:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cursor,
            "endTime": end_ms_incl,
            "limit": 1500,
        }
        url = base + ep + "?" + urllib.parse.urlencode(params)
        batch = _http_get_json(url)
        if not isinstance(batch, list) or not batch:
            break

        out.extend(batch)
        last_open = int(batch[-1][0])
        next_cursor = last_open + step
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(batch) < 1500:
            break
        time.sleep(0.05)

    return out


def _parse_hhmm(value: str) -> Tuple[int, int]:
    parts = value.split(":")
    if len(parts) != 2:
        raise ValueError("Expected HH:MM")
    return int(parts[0]), int(parts[1])


def _make_window(
    date_yyyy_mm_dd: str,
    tz_name: str,
    start_hhmm: str,
    *,
    end_hhmm: Optional[str] = None,
    duration_hours: Optional[float] = None,
) -> Window:
    tz = ZoneInfo(tz_name)
    y, m, d = map(int, date_yyyy_mm_dd.split("-"))
    sh, sm = _parse_hhmm(start_hhmm)

    start_local = datetime(y, m, d, sh, sm, tzinfo=tz)

    if duration_hours is not None and end_hhmm is not None:
        raise ValueError("Provide either end_hhmm or duration_hours, not both")

    if duration_hours is not None:
        if duration_hours <= 0:
            raise ValueError("duration_hours must be > 0")
        end_local = start_local + timedelta(hours=float(duration_hours))
    else:
        if end_hhmm is None:
            raise ValueError("Either end_hhmm or duration_hours is required")
        eh, em = _parse_hhmm(end_hhmm)
        end_local = datetime(y, m, d, eh, em, tzinfo=tz)
        if end_local <= start_local:
            raise ValueError("end must be after start (same day)")

    return Window(
        start_utc=start_local.astimezone(ZoneInfo("UTC")),
        end_utc=end_local.astimezone(ZoneInfo("UTC")),
    )


def _to_bars(klines: List[list]) -> List[Bar]:
    bars: List[Bar] = []
    for k in klines:
        bars.append(
            Bar(
                open_time_ms=int(k[0]),
                open=float(k[1]),
                high=float(k[2]),
                low=float(k[3]),
                close=float(k[4]),
            )
        )
    bars.sort(key=lambda b: b.open_time_ms)
    return bars


def _atr_rma(bars: List[Bar], length: int) -> List[Optional[float]]:
    n = len(bars)
    if n == 0:
        return []
    atr: List[Optional[float]] = [None] * n

    trs: List[float] = [0.0] * n
    for i in range(1, n):
        high = bars[i].high
        low = bars[i].low
        prev_close = bars[i - 1].close
        trs[i] = max(high - low, abs(high - prev_close), abs(low - prev_close))

    if n <= length:
        return atr

    first = sum(trs[1 : length + 1]) / length
    atr[length] = first
    for i in range(length + 1, n):
        prev = atr[i - 1]
        if prev is None:
            atr[i] = None
        else:
            atr[i] = (prev * (length - 1) + trs[i]) / length
    return atr


def _ema(values: List[float], length: int) -> List[Optional[float]]:
    n = len(values)
    out: List[Optional[float]] = [None] * n
    if n < length:
        return out
    alpha = 2.0 / (length + 1.0)
    sma = sum(values[:length]) / length
    out[length - 1] = sma
    for i in range(length, n):
        prev = out[i - 1]
        if prev is None:
            out[i] = None
        else:
            out[i] = prev + alpha * (values[i] - prev)
    return out


def _sma_std(values: List[float], length: int) -> Tuple[List[Optional[float]], List[Optional[float]]]:
    n = len(values)
    sma: List[Optional[float]] = [None] * n
    std: List[Optional[float]] = [None] * n
    if n < length:
        return sma, std
    for i in range(length - 1, n):
        window = values[i - length + 1 : i + 1]
        mean = sum(window) / length
        var = sum((x - mean) ** 2 for x in window) / length
        sma[i] = mean
        std[i] = math.sqrt(var)
    return sma, std


def _compute_squeeze(
    bars: List[Bar],
    *,
    squeeze_len: int = 20,
    bb_mult: float = 2.0,
    kc_mult: float = 1.5,
    atr_len: int = 20,
) -> Tuple[List[bool], List[int], List[Optional[float]]]:
    closes = [b.close for b in bars]
    atr = _atr_rma(bars, atr_len)
    kc_mid = _ema(closes, squeeze_len)
    bb_mid, bb_std = _sma_std(closes, squeeze_len)

    n = len(bars)
    is_sq: List[bool] = [False] * n
    streak: List[int] = [0] * n
    ratio: List[Optional[float]] = [None] * n

    run = 0
    for i in range(n):
        if atr[i] is None or kc_mid[i] is None or bb_mid[i] is None or bb_std[i] is None:
            is_sq[i] = False
            run = 0
            streak[i] = 0
            ratio[i] = None
            continue

        kc_top = kc_mid[i] + kc_mult * atr[i]
        kc_bot = kc_mid[i] - kc_mult * atr[i]
        bb_top = bb_mid[i] + bb_mult * bb_std[i]
        bb_bot = bb_mid[i] - bb_mult * bb_std[i]

        sq = (bb_top < kc_top) and (bb_bot > kc_bot)
        is_sq[i] = sq

        if sq:
            run += 1
        else:
            run = 0
        streak[i] = run

        kc_w = kc_top - kc_bot
        bb_w = bb_top - bb_bot
        ratio[i] = (bb_w / kc_w) if kc_w != 0 else None

    return is_sq, streak, ratio


def _compute_atr_range_levels(
    bars: List[Bar],
    *,
    lookback_n: int,
    atr_len: int,
    k: float,
) -> Tuple[List[Optional[float]], List[Optional[float]]]:
    bars = sorted(bars, key=lambda b: b.open_time_ms)
    n = len(bars)
    atr = _atr_rma(bars, atr_len)

    upper: List[Optional[float]] = [None] * n
    lower: List[Optional[float]] = [None] * n

    for i in range(n):
        if i < lookback_n or atr[i] is None:
            continue
        prev = bars[i - lookback_n : i]
        hh = max(b.high for b in prev)
        ll = min(b.low for b in prev)
        upper[i] = hh + k * float(atr[i])
        lower[i] = ll - k * float(atr[i])

    return upper, lower


def _calc_cvd_from_futures_1m_klines(
    klines_1m: List[list],
    *,
    window: Window,
    tz: ZoneInfo,
) -> Dict[str, object]:
    """
    Uses futures 1m klines:
      - total_quote = k[7]
      - taker_buy_quote = k[10]
      - taker_sell_quote = total_quote - taker_buy_quote
      - cvd_net = buy - sell
    """
    start_ms = int(window.start_utc.timestamp() * 1000)
    end_ms = int(window.end_utc.timestamp() * 1000)

    buy = sell = 0.0
    hourly: Dict[str, Dict[str, float]] = {}

    for k in klines_1m:
        try:
            open_ms = int(k[0])
        except Exception:
            continue
        if open_ms < start_ms or open_ms >= end_ms:
            continue
        try:
            quote_vol = float(k[7])
            taker_buy_quote = float(k[10])
        except Exception:
            continue

        taker_sell_quote = quote_vol - taker_buy_quote
        buy += taker_buy_quote
        sell += taker_sell_quote

        hour_local = (
            datetime.fromtimestamp(open_ms / 1000, tz=ZoneInfo("UTC"))
            .astimezone(tz)
            .replace(minute=0, second=0, microsecond=0)
        )
        key = hour_local.strftime("%Y-%m-%d %H:%M")
        bucket = hourly.setdefault(key, {"buy": 0.0, "sell": 0.0, "net": 0.0})
        bucket["buy"] += taker_buy_quote
        bucket["sell"] += taker_sell_quote
        bucket["net"] += taker_buy_quote - taker_sell_quote

    net = buy - sell

    # Build ordered hourly list with cumulative
    hours_sorted = sorted(hourly.keys())
    cumulative = 0.0
    hourly_rows: List[Dict[str, object]] = []
    for h in hours_sorted:
        cumulative += float(hourly[h]["net"])
        hourly_rows.append(
            {
                "hour_local": h,
                "buy_usdt": float(hourly[h]["buy"]),
                "sell_usdt": float(hourly[h]["sell"]),
                "net_usdt": float(hourly[h]["net"]),
                "cumulative_net_usdt": cumulative,
            }
        )

    return {
        "buy_usdt": buy,
        "sell_usdt": sell,
        "net_usdt": net,
        "hourly": hourly_rows,
        "source": "binance_futures_klines_1m (taker buy quote vs total quote)",
    }


def _calc_obi_from_binance_vision_bookdepth(
    *,
    symbol: str,
    window: Window,
    tz: ZoneInfo,
    cache_dir: Path,
    bands: Optional[List[int]] = None,
) -> Dict[str, object]:
    """
    OBI proxy from Binance Vision `bookDepth` daily dataset (UTC-partitioned).
    Computes imbalance on the aggregated +/- bands (default 1..5) using notional (USDT) depth.
    """
    bands = bands or [1, 2, 3, 4, 5]
    bands_set = set(bands)

    start_utc = window.start_utc
    end_utc = window.end_utc

    # Determine which UTC daily files are needed for the window.
    d0 = start_utc.date()
    d1 = (end_utc - timedelta(microseconds=1)).date()
    dates: List[str] = []
    cur = d0
    while cur <= d1:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur = cur + timedelta(days=1)

    hourly_sum: Dict[str, float] = {}
    hourly_count: Dict[str, int] = {}

    samples: List[float] = []

    for utc_date in dates:
        url = _binance_vision_bookdepth_zip_url(symbol, utc_date)
        cache_path = (
            cache_dir
            / "binance_vision"
            / "futures"
            / "um"
            / "daily"
            / "bookDepth"
            / symbol
            / f"{symbol}-bookDepth-{utc_date}.zip"
        )

        try:
            zip_bytes = _download_to_cache(url, cache_path).read_bytes()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return {
                    "error": f"Binance Vision dataset not available yet: {url}",
                    "hint": "Daily bookDepth files may lag; try again later or use a different window.",
                }
            raise

        rows = _read_bookdepth_csv_from_zip(zip_bytes)

        # Group per timestamp: expect 10 rows per timestamp (±1..±5).
        current_ts: Optional[datetime] = None
        bid_notional = ask_notional = 0.0

        def flush():
            nonlocal bid_notional, ask_notional, current_ts
            if current_ts is None:
                return
            if not (start_utc <= current_ts < end_utc):
                bid_notional = ask_notional = 0.0
                return
            v = _obi(bid_notional, ask_notional)
            samples.append(v)

            hour_local = current_ts.astimezone(tz).replace(minute=0, second=0, microsecond=0)
            key = hour_local.strftime("%Y-%m-%d %H:%M")
            hourly_sum[key] = hourly_sum.get(key, 0.0) + v
            hourly_count[key] = hourly_count.get(key, 0) + 1

            bid_notional = ask_notional = 0.0

        had_any = False
        for row in rows:
            had_any = True
            ts_str = row.get("timestamp") or ""
            try:
                perc = int(float(row.get("percentage") or "0"))
            except Exception:
                continue
            if abs(perc) not in bands_set:
                continue

            try:
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("UTC"))
            except Exception:
                continue

            if ts < start_utc:
                current_ts = ts
                continue
            if ts >= end_utc and current_ts is not None and ts > current_ts:
                flush()
                break

            if current_ts is None:
                current_ts = ts
            elif ts != current_ts:
                flush()
                current_ts = ts

            try:
                notional = float(row.get("notional") or 0.0)
            except Exception:
                notional = 0.0

            if perc < 0:
                bid_notional += notional
            elif perc > 0:
                ask_notional += notional

        if not had_any:
            continue
        flush()

    if not samples:
        return {
            "symbol": symbol,
            "bands": bands,
            "sample_count": 0,
            "obi_notional": {"avg": 0.0, "min": 0.0, "max": 0.0},
            "hourly": [],
            "source": "binance_vision: futures/um/daily/bookDepth",
            "note": "Orderbook depth-band imbalance proxy (+/- bands around mid), not top-10-level OBI.",
        }

    avg = sum(samples) / len(samples)
    hourly_rows: List[Dict[str, object]] = []
    for h in sorted(hourly_sum.keys()):
        c = hourly_count[h]
        hourly_rows.append({"hour_local": h, "obi_notional_avg": (hourly_sum[h] / c) if c else 0.0, "samples": c})

    return {
        "symbol": symbol,
        "bands": bands,
        "sample_count": len(samples),
        "obi_notional": {"avg": avg, "min": min(samples), "max": max(samples)},
        "hourly": hourly_rows,
        "source": "binance_vision: futures/um/daily/bookDepth",
        "note": "Orderbook depth-band imbalance proxy (+/- bands around mid), not top-10-level OBI.",
    }


def backtest_3m_breakout_ema_exit(
    *,
    bars_1h: List[Bar],
    bars_3m: List[Bar],
    window: Window,
    lookback_n: int = 20,
    atr_len: int = 20,
    k: float = 1.0,
    ema_len: int = 200,
) -> Dict[str, object]:
    """
    Rule (user-selected B):
      - Range levels computed on 1H using HH/LL + k*ATR (same as before).
      - Entry trigger on 3m: if 3m CLOSE breaks above/below the latest completed 1H level.
      - Entry execution: next 3m OPEN.
      - Exit: for long, 3m CLOSE < EMA(ema_len) -> exit next 3m OPEN (short symmetric: CLOSE > EMA).
      - Forced exit at window end boundary.
    """
    bars_1h = sorted(bars_1h, key=lambda b: b.open_time_ms)
    bars_3m = sorted(bars_3m, key=lambda b: b.open_time_ms)
    if not bars_1h or not bars_3m:
        return {"trades": [], "error": "Not enough bars"}

    upper_1h, lower_1h = _compute_atr_range_levels(bars_1h, lookback_n=lookback_n, atr_len=atr_len, k=k)

    closes_3m = [b.close for b in bars_3m]
    ema_3m = _ema(closes_3m, ema_len)

    start_ms = int(window.start_utc.timestamp() * 1000)
    end_ms = int(window.end_utc.timestamp() * 1000)
    tf_3m = _INTERVAL_MS["3m"]
    tf_1h = _INTERVAL_MS["1h"]

    trades: List[Trade] = []
    position: Optional[Trade] = None
    pending_entry: Optional[str] = None  # "long" | "short"
    pending_exit_reason: Optional[str] = None

    # Pointer to latest completed 1H bar (by 1H close time).
    h = -1

    for i, b3 in enumerate(bars_3m):
        if b3.open_time_ms >= end_ms and position is None and pending_entry is None:
            break

        # Forced exit at first 3m bar that opens at/after window end.
        if b3.open_time_ms >= end_ms and position is not None:
            trades.append(
                Trade(
                    side=position.side,
                    entry_time_utc=position.entry_time_utc,
                    entry_price=position.entry_price,
                    exit_time_utc=datetime.fromtimestamp(b3.open_time_ms / 1000, tz=ZoneInfo("UTC")),
                    exit_price=b3.open,
                    exit_reason="end",
                    stop_price=position.stop_price,
                )
            )
            position = None
            pending_entry = None
            pending_exit_reason = None
            break

        # Apply pending exit at open.
        if position is not None and pending_exit_reason is not None:
            trades.append(
                Trade(
                    side=position.side,
                    entry_time_utc=position.entry_time_utc,
                    entry_price=position.entry_price,
                    exit_time_utc=datetime.fromtimestamp(b3.open_time_ms / 1000, tz=ZoneInfo("UTC")),
                    exit_price=b3.open,
                    exit_reason=pending_exit_reason,
                    stop_price=position.stop_price,
                )
            )
            position = None
            pending_exit_reason = None

        # Apply pending entry at open.
        if position is None and pending_entry is not None:
            entry_time = datetime.fromtimestamp(b3.open_time_ms / 1000, tz=ZoneInfo("UTC"))
            position = Trade(
                side=pending_entry,
                entry_time_utc=entry_time,
                entry_price=b3.open,
                exit_time_utc=entry_time,
                exit_price=b3.open,
                exit_reason="",
                stop_price=float("nan"),
            )
            pending_entry = None

        # Signals/exit decisions evaluated on close of current 3m bar.
        close_ms = b3.open_time_ms + tf_3m
        if close_ms <= start_ms or close_ms > end_ms:
            continue
        if i + 1 >= len(bars_3m):
            continue

        # Advance 1H pointer so that bars_1h[h] is the latest completed 1H bar by this 3m close.
        while h + 1 < len(bars_1h) and (bars_1h[h + 1].open_time_ms + tf_1h) <= close_ms:
            h += 1

        if h < 0:
            continue

        up = upper_1h[h]
        lo = lower_1h[h]

        # Exit check first (close-based) to avoid same-bar enter/exit artifacts.
        if position is not None:
            e = ema_3m[i]
            if e is not None:
                if position.side == "long" and b3.close < float(e):
                    pending_exit_reason = "ema200"
                elif position.side == "short" and b3.close > float(e):
                    pending_exit_reason = "ema200"
            continue

        # Entry (close-confirm breakout) when flat; execute next bar open.
        if up is not None and b3.close > float(up):
            pending_entry = "long"
        elif lo is not None and b3.close < float(lo):
            pending_entry = "short"

    pnl_pcts = [t.pnl_pct() for t in trades]
    win_rate = (sum(1 for x in pnl_pcts if x > 0) / len(pnl_pcts) * 100.0) if pnl_pcts else 0.0
    return {
        "trades": trades,
        "stats": {
            "count": len(trades),
            "win_rate_pct": win_rate,
            "avg_pnl_pct": (sum(pnl_pcts) / len(pnl_pcts)) if pnl_pcts else 0.0,
            "sum_pnl_pct": sum(pnl_pcts),
        },
    }


def backtest_atr_range_breakout(
    bars: List[Bar],
    *,
    window: Window,
    lookback_n: int = 20,
    atr_len: int = 20,
    k: float = 1.0,
    max_hold_bars: int = 24,
    require_squeeze: bool = False,
    require_squeeze_min_bars: int = 5,
    require_squeeze_l2: bool = False,
    squeeze_l2_ratio: float = 0.80,
) -> Dict[str, object]:
    bars = sorted(bars, key=lambda b: b.open_time_ms)
    n = len(bars)
    if n < max(lookback_n, atr_len) + 5:
        return {"trades": [], "error": "Not enough bars"}

    atr = _atr_rma(bars, atr_len)
    is_sq, sq_streak, sq_ratio = _compute_squeeze(bars, atr_len=atr_len)

    upper: List[Optional[float]] = [None] * n
    lower: List[Optional[float]] = [None] * n
    long_sig: List[bool] = [False] * n
    short_sig: List[bool] = [False] * n

    for i in range(n):
        if atr[i] is None:
            continue
        if i < lookback_n:
            continue

        prev = bars[i - lookback_n : i]
        hh = max(b.high for b in prev)
        ll = min(b.low for b in prev)
        upper[i] = hh + k * atr[i]
        lower[i] = ll - k * atr[i]

        c = bars[i].close
        long_sig[i] = upper[i] is not None and c > float(upper[i])
        short_sig[i] = lower[i] is not None and c < float(lower[i])

        if require_squeeze:
            ok = is_sq[i] and sq_streak[i] >= require_squeeze_min_bars
            if require_squeeze_l2:
                r = sq_ratio[i]
                ok = ok and (r is not None and r <= squeeze_l2_ratio)
            if not ok:
                long_sig[i] = False
                short_sig[i] = False

    start_ms = int(window.start_utc.timestamp() * 1000)
    end_ms = int(window.end_utc.timestamp() * 1000)

    def in_window(i: int) -> bool:
        t = bars[i].open_time_ms
        return start_ms <= t < end_ms

    trades: List[Trade] = []
    position: Optional[Trade] = None
    pending_entry: Optional[Tuple[str, float]] = None  # (side, stop_price)
    pending_flip: Optional[Tuple[str, float]] = None
    pending_exit_reason: Optional[str] = None
    time_stop_index: Optional[int] = None

    # Iterate bars by open, apply pending entry/exit at open, then stop checks, then signal at close.
    for i in range(n):
        # Stop backtest once we've passed end and have no open/pending actions.
        if bars[i].open_time_ms >= end_ms and position is None and pending_entry is None:
            break

        # Force close at the first bar that opens at/after end boundary.
        if bars[i].open_time_ms >= end_ms and position is not None:
            trades.append(
                Trade(
                    side=position.side,
                    entry_time_utc=position.entry_time_utc,
                    entry_price=position.entry_price,
                    exit_time_utc=datetime.fromtimestamp(bars[i].open_time_ms / 1000, tz=ZoneInfo("UTC")),
                    exit_price=bars[i].open,
                    exit_reason="end",
                    stop_price=position.stop_price,
                )
            )
            position = None
            pending_entry = None
            pending_flip = None
            pending_exit_reason = None
            time_stop_index = None
            break

        # Execute pending exit at open (reverse/time)
        if position is not None and pending_exit_reason is not None:
            trades.append(
                Trade(
                    side=position.side,
                    entry_time_utc=position.entry_time_utc,
                    entry_price=position.entry_price,
                    exit_time_utc=datetime.fromtimestamp(bars[i].open_time_ms / 1000, tz=ZoneInfo("UTC")),
                    exit_price=bars[i].open,
                    exit_reason=pending_exit_reason,
                    stop_price=position.stop_price,
                )
            )
            position = None
            pending_exit_reason = None

        # Execute flip entry at same open after exit
        if position is None and pending_flip is not None:
            side, stop_price = pending_flip
            entry_time = datetime.fromtimestamp(bars[i].open_time_ms / 1000, tz=ZoneInfo("UTC"))
            position = Trade(
                side=side,
                entry_time_utc=entry_time,
                entry_price=bars[i].open,
                exit_time_utc=entry_time,
                exit_price=bars[i].open,
                exit_reason="",
                stop_price=stop_price,
            )
            time_stop_index = i + max_hold_bars
            pending_flip = None

        # Execute pending entry at open
        if position is None and pending_entry is not None:
            side, stop_price = pending_entry
            entry_time = datetime.fromtimestamp(bars[i].open_time_ms / 1000, tz=ZoneInfo("UTC"))
            position = Trade(
                side=side,
                entry_time_utc=entry_time,
                entry_price=bars[i].open,
                exit_time_utc=entry_time,
                exit_price=bars[i].open,
                exit_reason="",
                stop_price=stop_price,
            )
            time_stop_index = i + max_hold_bars
            pending_entry = None

        # Time stop executes at open of the time_stop_index bar.
        if position is not None and time_stop_index is not None and i == time_stop_index:
            pending_exit_reason = "time"
            # exit at next iteration open if possible; but we're already at open of i.
            trades.append(
                Trade(
                    side=position.side,
                    entry_time_utc=position.entry_time_utc,
                    entry_price=position.entry_price,
                    exit_time_utc=datetime.fromtimestamp(bars[i].open_time_ms / 1000, tz=ZoneInfo("UTC")),
                    exit_price=bars[i].open,
                    exit_reason="time",
                    stop_price=position.stop_price,
                )
            )
            position = None
            pending_entry = None
            pending_flip = None
            pending_exit_reason = None
            time_stop_index = None
            continue

        # Stop-loss checks on the current bar (after entry, within the bar)
        if position is not None:
            stop = position.stop_price
            if position.side == "long":
                # gap below stop
                if bars[i].open <= stop:
                    trades.append(
                        Trade(
                            side=position.side,
                            entry_time_utc=position.entry_time_utc,
                            entry_price=position.entry_price,
                            exit_time_utc=datetime.fromtimestamp(bars[i].open_time_ms / 1000, tz=ZoneInfo("UTC")),
                            exit_price=bars[i].open,
                            exit_reason="stop",
                            stop_price=stop,
                        )
                    )
                    position = None
                    pending_entry = None
                    pending_flip = None
                    pending_exit_reason = None
                    time_stop_index = None
                    continue
                # intrabar
                if bars[i].low <= stop:
                    trades.append(
                        Trade(
                            side=position.side,
                            entry_time_utc=position.entry_time_utc,
                            entry_price=position.entry_price,
                            exit_time_utc=datetime.fromtimestamp(bars[i].open_time_ms / 1000, tz=ZoneInfo("UTC")),
                            exit_price=stop,
                            exit_reason="stop",
                            stop_price=stop,
                        )
                    )
                    position = None
                    pending_entry = None
                    pending_flip = None
                    pending_exit_reason = None
                    time_stop_index = None
                    continue
            else:
                if bars[i].open >= stop:
                    trades.append(
                        Trade(
                            side=position.side,
                            entry_time_utc=position.entry_time_utc,
                            entry_price=position.entry_price,
                            exit_time_utc=datetime.fromtimestamp(bars[i].open_time_ms / 1000, tz=ZoneInfo("UTC")),
                            exit_price=bars[i].open,
                            exit_reason="stop",
                            stop_price=stop,
                        )
                    )
                    position = None
                    pending_entry = None
                    pending_flip = None
                    pending_exit_reason = None
                    time_stop_index = None
                    continue
                if bars[i].high >= stop:
                    trades.append(
                        Trade(
                            side=position.side,
                            entry_time_utc=position.entry_time_utc,
                            entry_price=position.entry_price,
                            exit_time_utc=datetime.fromtimestamp(bars[i].open_time_ms / 1000, tz=ZoneInfo("UTC")),
                            exit_price=stop,
                            exit_reason="stop",
                            stop_price=stop,
                        )
                    )
                    position = None
                    pending_entry = None
                    pending_flip = None
                    pending_exit_reason = None
                    time_stop_index = None
                    continue

        # Signals are evaluated at close of the current bar; entries/exits happen on next bar open.
        if i + 1 >= n:
            continue

        if not in_window(i):
            continue

        if position is None:
            if long_sig[i] and upper[i] is not None:
                pending_entry = ("long", float(upper[i]))
            elif short_sig[i] and lower[i] is not None:
                pending_entry = ("short", float(lower[i]))
        else:
            # reverse signals: schedule exit and flip on next open
            if position.side == "long" and short_sig[i] and lower[i] is not None:
                pending_exit_reason = "reverse"
                pending_flip = ("short", float(lower[i]))
            elif position.side == "short" and long_sig[i] and upper[i] is not None:
                pending_exit_reason = "reverse"
                pending_flip = ("long", float(upper[i]))

    # Stats
    pnl_pcts = [t.pnl_pct() for t in trades]
    win_rate = (sum(1 for x in pnl_pcts if x > 0) / len(pnl_pcts) * 100.0) if pnl_pcts else 0.0

    # If we ran out of bars (e.g., caller didn't fetch a post-window candle), force-close at the
    # last available close inside the window so the position is not silently dropped.
    if position is not None:
        last_in_window = None
        for j in range(n - 1, -1, -1):
            if bars[j].open_time_ms < end_ms:
                last_in_window = j
                break
        if last_in_window is not None:
            trades.append(
                Trade(
                    side=position.side,
                    entry_time_utc=position.entry_time_utc,
                    entry_price=position.entry_price,
                    exit_time_utc=datetime.fromtimestamp(bars[last_in_window].open_time_ms / 1000, tz=ZoneInfo("UTC")),
                    exit_price=bars[last_in_window].close,
                    exit_reason="end",
                    stop_price=position.stop_price,
                )
            )

    return {
        "trades": trades,
        "stats": {
            "count": len(trades),
            "win_rate_pct": win_rate,
            "avg_pnl_pct": (sum(pnl_pcts) / len(pnl_pcts)) if pnl_pcts else 0.0,
            "sum_pnl_pct": sum(pnl_pcts),
        },
    }


def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True, help="Binance UM futures symbol, e.g. BSVUSDT")
    p.add_argument("--tz", default="Australia/Sydney", help="IANA timezone, default Australia/Sydney")
    p.add_argument("--date", required=True, help="Local date in --tz (YYYY-MM-DD)")
    p.add_argument("--start", required=True, help="Start time HH:MM in --tz")
    p.add_argument("--end", help="End time HH:MM in --tz (end exclusive, same day)")
    p.add_argument("--duration-hours", type=float, help="Alternative to --end; supports cross-day windows")

    p.add_argument(
        "--mode",
        default="1h_atr",
        choices=["1h_atr", "3m_breakout_ema200_exit"],
        help="Backtest mode: 1h_atr (default) or 3m_breakout_ema200_exit",
    )

    p.add_argument("--lookback", type=int, default=20, help="HH/LL lookback N (default 20)")
    p.add_argument("--atr-len", type=int, default=20, help="ATR length (default 20)")
    p.add_argument("--k", type=float, default=1.0, help="ATR buffer multiplier (default 1.0)")
    p.add_argument("--max-hold-bars", type=int, default=24, help="Max hold bars (1h bars), default 24")
    p.add_argument("--ema-len", type=int, default=200, help="EMA length for 3m exit mode (default 200)")

    p.add_argument("--require-squeeze", action="store_true", help="Only take signals when squeeze is on")
    p.add_argument("--require-squeeze-min-bars", type=int, default=5, help="Min consecutive squeeze bars (default 5)")
    p.add_argument("--require-squeeze-l2", action="store_true", help="Require L2 (tightness ratio <= threshold)")
    p.add_argument("--squeeze-l2-ratio", type=float, default=0.80, help="L2 threshold (default 0.80)")

    p.add_argument("--no-cvd", action="store_true", help="Skip CVD computation (futures 1m klines)")
    p.add_argument("--hourly", action="store_true", help="Print hourly CVD breakdown (and cumulative)")

    p.add_argument("--obi", action="store_true", help="Compute OBI proxy from Binance Vision bookDepth dataset")
    p.add_argument("--obi-cache-dir", default="data", help="Cache dir for Binance Vision downloads (default data)")

    args = p.parse_args(argv)

    tz = ZoneInfo(args.tz)
    symbol = args.symbol.upper()

    try:
        window = _make_window(args.date, args.tz, args.start, end_hhmm=args.end, duration_hours=args.duration_hours)
    except ValueError as e:
        print(f"argument_error={e}")
        return 2

    # Fetch enough pre-history for indicators
    warmup_hours = max(args.lookback, args.atr_len) * 3
    fetch_start = window.start_utc - timedelta(hours=warmup_hours)
    start_ms = int(fetch_start.timestamp() * 1000)
    # Fetch one extra 1h candle after the window to allow end-boundary exits at next open.
    end_ms_incl = int((window.end_utc + timedelta(hours=1)).timestamp() * 1000) - 1

    kl = _fetch_fapi_klines(symbol, "1h", start_ms, end_ms_incl)
    if not isinstance(kl, list) or not kl:
        print("No klines returned")
        return 2

    bars_1h = _to_bars(kl)

    # Squeeze context/events (1h, based on the same BB/KC params as the filter).
    is_sq, sq_streak, sq_ratio = _compute_squeeze(bars_1h, atr_len=args.atr_len)
    start_ms_window = int(window.start_utc.timestamp() * 1000)
    end_ms_window = int(window.end_utc.timestamp() * 1000)

    def _bar_open_local(i: int) -> str:
        return datetime.fromtimestamp(bars_1h[i].open_time_ms / 1000, tz=ZoneInfo("UTC")).astimezone(tz).strftime(
            "%Y-%m-%d %H:%M"
        )

    def _sq_on(i: int) -> bool:
        return bool(is_sq[i]) and int(sq_streak[i]) >= int(args.require_squeeze_min_bars)

    def _sq_l2(i: int) -> bool:
        if not _sq_on(i):
            return False
        r = sq_ratio[i]
        return r is not None and float(r) <= float(args.squeeze_l2_ratio)

    squeeze_events: List[str] = []
    prev_on = False
    prev_l2 = False
    emitted_initial = False

    # Include the last bar before window start for correct transition detection.
    for i, b in enumerate(bars_1h):
        if b.open_time_ms < start_ms_window - _INTERVAL_MS["1h"]:
            continue
        if b.open_time_ms >= end_ms_window:
            break

        in_window = start_ms_window <= b.open_time_ms < end_ms_window
        on = _sq_on(i)
        l2 = _sq_l2(i)

        if in_window and not emitted_initial:
            emitted_initial = True
            if on:
                tag = "L2" if l2 else "L1"
                r = sq_ratio[i]
                r_s = f"{float(r):.3f}" if r is not None else "n/a"
                squeeze_events.append(f"{_bar_open_local(i)} SQUEEZE_ON({tag}) streak={sq_streak[i]} ratio={r_s}")

        if prev_on is False and on is True and in_window:
            tag = "L2" if l2 else "L1"
            r = sq_ratio[i]
            r_s = f"{float(r):.3f}" if r is not None else "n/a"
            squeeze_events.append(f"{_bar_open_local(i)} SQUEEZE_START({tag}) streak={sq_streak[i]} ratio={r_s}")
        if prev_on is True and on is False and in_window:
            squeeze_events.append(f"{_bar_open_local(i)} SQUEEZE_RELEASE")

        if prev_l2 is False and l2 is True and in_window:
            r = sq_ratio[i]
            r_s = f"{float(r):.3f}" if r is not None else "n/a"
            squeeze_events.append(f"{_bar_open_local(i)} SQUEEZE_L2_ON ratio={r_s}")
        if prev_l2 is True and l2 is False and on is True and in_window:
            r = sq_ratio[i]
            r_s = f"{float(r):.3f}" if r is not None else "n/a"
            squeeze_events.append(f"{_bar_open_local(i)} SQUEEZE_L2_OFF ratio={r_s}")

        prev_on = on
        prev_l2 = l2

    if args.mode == "1h_atr":
        result = backtest_atr_range_breakout(
            bars_1h,
            window=window,
            lookback_n=args.lookback,
            atr_len=args.atr_len,
            k=args.k,
            max_hold_bars=args.max_hold_bars,
            require_squeeze=args.require_squeeze,
            require_squeeze_min_bars=args.require_squeeze_min_bars,
            require_squeeze_l2=args.require_squeeze_l2,
            squeeze_l2_ratio=args.squeeze_l2_ratio,
        )
    else:
        # Warmup for EMA on 3m: ensure we fetch enough history (ema_len * 3m).
        warmup_minutes_3m = max(args.ema_len * 3, 12 * 60)
        fetch_start_3m = window.start_utc - timedelta(minutes=warmup_minutes_3m)
        start_ms_3m = int(fetch_start_3m.timestamp() * 1000)
        end_ms_incl_3m = int((window.end_utc + timedelta(minutes=3)).timestamp() * 1000) - 1
        kl3 = _fetch_fapi_klines(symbol, "3m", start_ms_3m, end_ms_incl_3m)
        if not isinstance(kl3, list) or not kl3:
            print("No 3m klines returned")
            return 2
        bars_3m = _to_bars(kl3)
        result = backtest_3m_breakout_ema_exit(
            bars_1h=bars_1h,
            bars_3m=bars_3m,
            window=window,
            lookback_n=args.lookback,
            atr_len=args.atr_len,
            k=args.k,
            ema_len=args.ema_len,
        )

    print(f"symbol={symbol}")
    print(
        f"window_sydney={window.start_utc.astimezone(tz).isoformat()} -> {window.end_utc.astimezone(tz).isoformat()} (end exclusive)"
    )
    print(f"window_utc={window.start_utc.isoformat()} -> {window.end_utc.isoformat()} (end exclusive)")
    print(f"params=lookback={args.lookback} atr_len={args.atr_len} k={args.k} max_hold_bars={args.max_hold_bars}")
    print(f"mode={args.mode}")
    if args.require_squeeze:
        mode = "L2" if args.require_squeeze_l2 else "L1"
        print(f"filter=squeeze:{mode} min_bars={args.require_squeeze_min_bars} l2_ratio={args.squeeze_l2_ratio}")

    if squeeze_events:
        print("\nsqueeze_events (1h, local):")
        for e in squeeze_events:
            print(e)

    if "error" in result:
        print("error=" + str(result["error"]))
        return 3

    if not args.no_cvd:
        cvd_end_ms_incl = int(window.end_utc.timestamp() * 1000) - 1
        cvd_kl = _fetch_fapi_klines(symbol, "1m", int(window.start_utc.timestamp() * 1000), cvd_end_ms_incl)
        cvd = _calc_cvd_from_futures_1m_klines(cvd_kl, window=window, tz=tz)
        print(
            f"cvd_usdt=net {cvd['net_usdt']:+,.2f} (buy {cvd['buy_usdt']:,.2f} / sell {cvd['sell_usdt']:,.2f})"
        )
        if args.hourly:
            print("\ncvd_hourly (local):")
            print("hour,buy_usdt,sell_usdt,net_usdt,cumulative_net_usdt")
            for row in cvd["hourly"]:
                print(
                    ",".join(
                        [
                            str(row["hour_local"]),
                            f"{float(row['buy_usdt']):.2f}",
                            f"{float(row['sell_usdt']):.2f}",
                            f"{float(row['net_usdt']):+.2f}",
                            f"{float(row['cumulative_net_usdt']):+.2f}",
                        ]
                    )
                )

    if args.obi:
        obi = _calc_obi_from_binance_vision_bookdepth(
            symbol=symbol,
            window=window,
            tz=tz,
            cache_dir=Path(args.obi_cache_dir),
        )
        if "error" in obi:
            print(f"obi_error={obi['error']}")
            if "hint" in obi:
                print("obi_hint=" + str(obi["hint"]))
        else:
            o = obi["obi_notional"]
            print(
                f"obi_proxy_notional=avg {float(o['avg']):+.6f} min {float(o['min']):+.6f} max {float(o['max']):+.6f} samples={obi['sample_count']}"
            )
            print("obi_source=" + str(obi["source"]))
            print("obi_note=" + str(obi["note"]))
            if args.hourly and obi["hourly"]:
                print("\nobi_hourly (local):")
                print("hour,obi_notional_avg,samples")
                for row in obi["hourly"]:
                    print(f"{row['hour_local']},{float(row['obi_notional_avg']):+.6f},{int(row['samples'])}")

    stats = result["stats"]
    print(f"trades={stats['count']} win_rate={stats['win_rate_pct']:.1f}% avg_pnl={stats['avg_pnl_pct']:+.3f}% sum_pnl={stats['sum_pnl_pct']:+.3f}%")

    trades: List[Trade] = result["trades"]
    if not trades:
        return 0

    print("\ntrades:")
    print("side,entry_time_sydney,entry,exit_time_sydney,exit,exit_reason,stop,pnl_pct")
    for t in trades:
        entry_local = t.entry_time_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M")
        exit_local = t.exit_time_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M")
        stop_s = ""
        try:
            if t.stop_price == t.stop_price:  # not NaN
                stop_s = f"{t.stop_price:.6f}"
            else:
                stop_s = "na"
        except Exception:
            stop_s = "na"
        print(
            ",".join(
                [
                    t.side,
                    entry_local,
                    f"{t.entry_price:.6f}",
                    exit_local,
                    f"{t.exit_price:.6f}",
                    t.exit_reason,
                    stop_s,
                    f"{t.pnl_pct():+.3f}",
                ]
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
