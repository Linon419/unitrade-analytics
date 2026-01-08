import os
from datetime import datetime, timezone
from typing import Optional

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[misc,assignment]


def resolve_tz(name: Optional[str] = None):
    """
    Resolve a timezone for display.

    Order:
    1) explicit `name`
    2) env `UNITRADE_TZ`
    3) default: Australia/Sydney (AEDT/AEST)
    """
    tz_name = (name or os.getenv("UNITRADE_TZ") or "Australia/Sydney").strip()
    if not tz_name:
        return timezone.utc
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return timezone.utc


def format_ts(ts: float, fmt: str = "%m-%d %H:%M", tz_name: Optional[str] = None) -> str:
    tz = resolve_tz(tz_name)
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone(tz).strftime(fmt)

