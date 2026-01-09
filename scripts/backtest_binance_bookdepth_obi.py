"""
Backtest orderbook depth imbalance (OBI-like) from Binance Vision `bookDepth` dataset.

Binance Vision `bookDepth` (UM futures) provides aggregated depth snapshots at fixed percentage bands
around mid price (e.g., -1..-5 for bids, +1..+5 for asks). This is NOT the same as top-10-level OBI
computed from full orderbook L2 updates, but it is still an orderbook-based imbalance proxy.

Example:
  python scripts/backtest_binance_bookdepth_obi.py --symbol BSVUSDT --date 2026-01-06 --tz Australia/Sydney --start 13:00 --end 17:00
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import urllib.request
import urllib.error


def _http_get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "unitrade-analytics"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def _download_to_cache(url: str, cache_path: Path) -> Path:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path
    data = _http_get(url)
    cache_path.write_bytes(data)
    return cache_path


def _parse_hhmm(value: str) -> Tuple[int, int]:
    parts = value.split(":")
    if len(parts) != 2:
        raise ValueError("Expected HH:MM")
    return int(parts[0]), int(parts[1])


@dataclass(frozen=True)
class Window:
    start_utc: datetime
    end_utc: datetime  # end exclusive


def _make_window(date_yyyy_mm_dd: str, tz_name: str, start_hhmm: str, end_hhmm: str) -> Window:
    tz = ZoneInfo(tz_name)
    y, m, d = map(int, date_yyyy_mm_dd.split("-"))
    sh, sm = _parse_hhmm(start_hhmm)
    eh, em = _parse_hhmm(end_hhmm)

    start_local = datetime(y, m, d, sh, sm, tzinfo=tz)
    end_local = datetime(y, m, d, eh, em, tzinfo=tz)
    if end_local <= start_local:
        raise ValueError("end must be after start (same day)")

    return Window(start_utc=start_local.astimezone(ZoneInfo("UTC")), end_utc=end_local.astimezone(ZoneInfo("UTC")))


def _binance_vision_bookdepth_zip_url(symbol: str, utc_date: str) -> str:
    # Binance Vision uses UTC dates in path/filename for daily datasets.
    return (
        "https://data.binance.vision/data/futures/um/daily/bookDepth/"
        f"{symbol}/{symbol}-bookDepth-{utc_date}.zip"
    )


def _read_bookdepth_csv_from_zip(zip_bytes: bytes) -> Iterable[Dict[str, str]]:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = zf.namelist()
    if not names:
        return []

    # Expected exactly one CSV.
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


def backtest_bookdepth_obi(
    *,
    symbol: str,
    utc_date: str,
    window: Window,
    cache_dir: Path,
    bands: Optional[List[int]] = None,
) -> Dict[str, object]:
    """
    Returns summary for the given window:
      - obi_depth_avg/min/max (base qty proxy)
      - obi_notional_avg/min/max (USDT proxy)
      - sample_count
    """
    bands = bands or [1, 2, 3, 4, 5]
    bands_set = set(bands)

    url = _binance_vision_bookdepth_zip_url(symbol, utc_date)
    cache_path = cache_dir / "binance_vision" / "futures" / "um" / "daily" / "bookDepth" / symbol / f"{symbol}-bookDepth-{utc_date}.zip"

    try:
        zip_bytes = _download_to_cache(url, cache_path).read_bytes()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise FileNotFoundError(f"Binance Vision dataset not available yet: {url}") from e
        raise

    # Group per timestamp: expect 10 rows per timestamp (±1..±5).
    current_ts: Optional[datetime] = None
    bid_depth = bid_notional = 0.0
    ask_depth = ask_notional = 0.0

    samples_depth: List[float] = []
    samples_notional: List[float] = []

    def flush():
        nonlocal bid_depth, ask_depth, bid_notional, ask_notional
        if current_ts is None:
            return
        if not (window.start_utc <= current_ts < window.end_utc):
            bid_depth = ask_depth = bid_notional = ask_notional = 0.0
            return
        samples_depth.append(_obi(bid_depth, ask_depth))
        samples_notional.append(_obi(bid_notional, ask_notional))
        bid_depth = ask_depth = bid_notional = ask_notional = 0.0

    for row in _read_bookdepth_csv_from_zip(zip_bytes):
        ts_str = row.get("timestamp") or ""
        perc = int(float(row.get("percentage") or "0"))
        if abs(perc) not in bands_set:
            continue

        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("UTC"))

        if current_ts is None:
            current_ts = ts
        elif ts != current_ts:
            flush()
            current_ts = ts

        depth = float(row.get("depth") or 0.0)
        notional = float(row.get("notional") or 0.0)

        if perc < 0:
            bid_depth += depth
            bid_notional += notional
        elif perc > 0:
            ask_depth += depth
            ask_notional += notional

    flush()

    def stats(xs: List[float]) -> Dict[str, float]:
        if not xs:
            return {"avg": 0.0, "min": 0.0, "max": 0.0}
        return {"avg": sum(xs) / len(xs), "min": min(xs), "max": max(xs)}

    return {
        "symbol": symbol,
        "utc_date": utc_date,
        "window_start_utc": window.start_utc.isoformat(),
        "window_end_utc": window.end_utc.isoformat(),
        "bands": bands,
        "sample_count": len(samples_depth),
        "obi_depth": stats(samples_depth),
        "obi_notional": stats(samples_notional),
        "source": "binance_vision: futures/um/daily/bookDepth",
        "note": "This is an orderbook depth-band imbalance proxy (+/- bands around mid), not top-10-level OBI.",
        "cache_file": str(cache_path),
    }


def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True, help="e.g. BSVUSDT (Binance UM futures symbol)")
    p.add_argument("--date", required=True, help="Local date (YYYY-MM-DD) in the provided --tz")
    p.add_argument("--tz", default="Australia/Sydney", help="IANA timezone, default Australia/Sydney")
    p.add_argument("--start", required=True, help="Start time HH:MM in --tz")
    p.add_argument("--end", required=True, help="End time HH:MM in --tz (end exclusive)")
    p.add_argument("--cache-dir", default="data", help="Cache directory for downloaded datasets")
    args = p.parse_args(argv)

    # Binance Vision daily datasets are partitioned by UTC date, not local date.
    window = _make_window(args.date, args.tz, args.start, args.end)
    utc_date = window.start_utc.strftime("%Y-%m-%d")

    try:
        result = backtest_bookdepth_obi(
            symbol=args.symbol.upper(),
            utc_date=utc_date,
            window=window,
            cache_dir=Path(args.cache_dir),
        )
    except FileNotFoundError as e:
        print(str(e))
        print("Hint: Binance Vision daily bookDepth files may lag; try again later, or backtest a previous UTC date.")
        return 2

    print(f"symbol={result['symbol']} source={result['source']}")
    print(f"window_utc={result['window_start_utc']} -> {result['window_end_utc']} (end exclusive)")
    print(f"bands=+/-{result['bands']} sample_count={result['sample_count']}")
    obi_d = result["obi_depth"]
    obi_n = result["obi_notional"]
    print(f"OBI(depth qty proxy):    avg={obi_d['avg']:+.6f} min={obi_d['min']:+.6f} max={obi_d['max']:+.6f}")
    print(f"OBI(notional USDT):      avg={obi_n['avg']:+.6f} min={obi_n['min']:+.6f} max={obi_n['max']:+.6f}")
    print(f"cache_file={result['cache_file']}")
    print("note=" + result["note"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
