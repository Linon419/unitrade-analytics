"""
Analyze public Telegram channel alerts and infer likely trigger filters.

This script scrapes the public web view: https://t.me/s/<channel>
and parses message texts to extract patterns like:
  "<SYMBOL> 币安期货价格在过去180秒内飙升9.8%"

Then it summarizes observed windows and percent-change ranges, which can be used
to infer approximate thresholds (the smallest observed value per window is an
upper bound of the real threshold).
"""

from __future__ import annotations

import argparse
import html as htmllib
import re
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class Alert:
    msg_id: int
    symbol: str
    exchange: str
    window_seconds: int
    direction: str
    pct: float  # signed (+ up, - down)
    raw: str


def normalize_exchange(exchange: str) -> str:
    ex = exchange or ""
    if "币安" in ex and ("期货" in ex or "合约" in ex):
        return "binance_futures"
    if "币安" in ex:
        return "binance"
    if "OKX" in ex or "欧易" in ex:
        return "okx"
    if "Bybit" in ex or "bybit" in ex or "币安" in ex:
        return "bybit"
    return "unknown"


def _http_get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "unitrade-analytics"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _strip_html(raw_html: str) -> str:
    raw_html = raw_html.replace("<br/>", "\n").replace("<br>", "\n")
    raw_html = re.sub(r"<.*?>", "", raw_html, flags=re.S)
    raw_html = htmllib.unescape(raw_html)
    raw_html = re.sub(r"\s+", " ", raw_html).strip()
    return raw_html


def _extract_messages(channel: str, page_html: str) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    # Match message id via data-post and capture the nearest text block.
    # This is intentionally tolerant to small Telegram HTML changes.
    pattern = re.compile(
        r'data-post="%s/(\d+)".*?tgme_widget_message_text[^>]*>(.*?)</div>'
        % re.escape(channel),
        flags=re.S,
    )
    for m in pattern.finditer(page_html):
        msg_id = int(m.group(1))
        text = _strip_html(m.group(2))
        if text:
            out.append((msg_id, text))
    return out


def crawl_channel(channel: str, *, max_pages: int = 50, sleep_seconds: float = 0.3) -> Dict[int, str]:
    all_msgs: Dict[int, str] = {}

    url = f"https://t.me/s/{channel}"
    html = _http_get(url)
    for msg_id, text in _extract_messages(channel, html):
        all_msgs[msg_id] = text

    if not all_msgs:
        return all_msgs

    min_id = min(all_msgs)
    pages = 1
    while min_id > 1 and pages < max_pages:
        pages += 1
        url = f"https://t.me/s/{channel}?before={min_id}"
        html = _http_get(url)
        msgs = _extract_messages(channel, html)
        if not msgs:
            break
        for msg_id, text in msgs:
            all_msgs[msg_id] = text
        min_id = min(all_msgs)
        time.sleep(max(0.0, sleep_seconds))

    return all_msgs


def _parse_window_seconds(text: str) -> Optional[int]:
    # Supports "过去180秒", "过去5分钟", "过去2小时"
    m = re.search(r"过去(\d+)(秒|分钟|小时)", text)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    if unit == "秒":
        return n
    if unit == "分钟":
        return n * 60
    if unit == "小时":
        return n * 3600
    return None


def parse_alerts(messages: Dict[int, str]) -> List[Alert]:
    # Try to parse "SYMBOL <exchange> 价格在过去...内 <dir> <pct>%"
    # Exchange is captured loosely to support variants like "币安期货/币安现货/OKX永续".
    alerts: List[Alert] = []

    # Common direction keywords
    dir_up = ("飙升", "上涨", "拉升", "暴涨")
    dir_down = ("下跌", "跳水", "暴跌", "下挫")
    dir_all = dir_up + dir_down
    dir_re = "|".join(map(re.escape, dir_all))

    # Symbol: allow uppercase tickers ending with USDT (or other common quote)
    sym_re = r"([A-Z0-9_]{2,}(?:USDT|USD|PERP)?)"

    # Percent: allow 1-2 decimal places
    pct_re = r"([0-9]+(?:\.[0-9]+)?)%"

    # Capture: symbol, exchange, (window handled separately), direction, percent
    pat = re.compile(
        rf"{sym_re}\s+(.{{0,16}}?)价格在过去\d+(?:秒|分钟|小时)内.*?({dir_re})\s*{pct_re}"
    )

    for msg_id, raw in messages.items():
        window = _parse_window_seconds(raw)
        if window is None:
            continue
        m = pat.search(raw)
        if not m:
            continue
        else:
            symbol = m.group(1)
            exchange = (m.group(2) or "").strip()
            direction = m.group(3)
            pct_val = float(m.group(4))

        signed = pct_val if direction in dir_up else -pct_val
        alerts.append(
            Alert(
                msg_id=msg_id,
                symbol=symbol,
                exchange=exchange or "UNKNOWN",
                window_seconds=window,
                direction=direction,
                pct=signed,
                raw=raw,
            )
        )

    return sorted(alerts, key=lambda a: a.msg_id)


def summarize(alerts: List[Alert]) -> str:
    if not alerts:
        return "No recognizable alerts found in fetched messages."

    # group by (exchange, window)
    groups: Dict[Tuple[str, int], List[Alert]] = defaultdict(list)
    for a in alerts:
        groups[(a.exchange, a.window_seconds)].append(a)

    lines: List[str] = []
    lines.append(f"parsed_alerts={len(alerts)}")
    lines.append("")
    lines.append("By exchange/window (observed abs%):")
    for (exchange, window), xs in sorted(groups.items(), key=lambda kv: (normalize_exchange(kv[0][0]), kv[0][1], kv[0][0])):
        absvals = sorted(abs(a.pct) for a in xs)
        min_v = absvals[0]
        max_v = absvals[-1]
        # A conservative guess: round down the minimum to nearest 0.1
        guess = int(min_v * 10) / 10.0
        ex_norm = normalize_exchange(exchange)
        lines.append(
            f"- {ex_norm} {window:>4}s: n={len(xs):<3} min={min_v:.2f}% max={max_v:.2f}% -> threshold<=~{guess:.1f}%"
        )

    # show a few examples
    lines.append("")
    lines.append("Examples (msg_id: symbol window dir pct):")
    for a in alerts[:10]:
        dir_norm = "up" if a.pct >= 0 else "down"
        lines.append(f"- {a.msg_id}: {a.symbol} {a.window_seconds}s {dir_norm} {a.pct:+.2f}%")
    return "\n".join(lines)


def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("channel", help="Telegram channel username, e.g. BWE_Reserved6")
    p.add_argument("--max-pages", type=int, default=50)
    p.add_argument("--sleep", type=float, default=0.3, help="sleep between page fetches (seconds)")
    args = p.parse_args(argv)

    messages = crawl_channel(args.channel, max_pages=args.max_pages, sleep_seconds=args.sleep)
    print(f"messages_fetched={len(messages)} ids={min(messages) if messages else None}..{max(messages) if messages else None}")
    alerts = parse_alerts(messages)
    print(summarize(alerts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
