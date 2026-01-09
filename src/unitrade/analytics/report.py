import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional
import aiohttp

logger = logging.getLogger(__name__)


def _calc_ema(prices: list, period: int) -> float:
    """Calculate EMA for a list of prices."""
    if len(prices) < period:
        return prices[-1] if prices else 0
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema
    return ema


def _analyze_ema_trend(prices: list) -> str:
    """Analyze EMA trend from price data."""
    if not prices or len(prices) < 200:
        return "Insufficient Data"
        
    ema21 = _calc_ema(prices, 21)
    ema55 = _calc_ema(prices, 55)
    ema200 = _calc_ema(prices, 200)
    
    if ema21 > ema55 > ema200:
        return f"🟢 Bullish (21>55>200)"
    elif ema21 < ema55 < ema200:
        return f"🔴 Bearish (21<55<200)"
    else:
        rel = []
        rel.append("21>55" if ema21 > ema55 else "21<55")
        rel.append("55>200" if ema55 > ema200 else "55<200")
        return f"⚠️ Entangled ({', '.join(rel)})"


class MarketReportGenerator:
    """Market report generator with EMA analysis."""
    
    def __init__(self, session: aiohttp.ClientSession, realtime_service, cvd_service):
        self.session = session
        self.realtime = realtime_service
        self.cvd = cvd_service
    
    async def get_ticker_24h(self, symbol: str, is_futures: bool) -> Dict:
        """Fetch 24h ticker stats from Binance."""
        base_url = "https://fapi.binance.com/fapi/v1/ticker/24hr" if is_futures else "https://api.binance.com/api/v3/ticker/24hr"
        try:
            async with self.session.get(base_url, params={"symbol": symbol}) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.error(f"Error fetching ticker {symbol}: {e}")
        return {}
    
    async def get_klines(self, symbol: str, interval: str = "1h", limit: int = 250) -> list:
        """Fetch K-lines for EMA calculation."""
        base_url = "https://fapi.binance.com/fapi/v1/klines"
        try:
            async with self.session.get(base_url, params={"symbol": symbol, "interval": interval, "limit": limit}) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.error(f"Error fetching klines for {symbol}: {e}")
        return []

    async def generate_report(self, symbol: str) -> Dict[str, Any]:
        """Generate comprehensive market report with EMA analysis."""
        symbol = symbol.upper()
        
        # Parallel fetch all data
        tasks = [
            self.get_ticker_24h(symbol, is_futures=False),
            self.get_ticker_24h(symbol, is_futures=True),
            self.cvd.get_cvd_analysis(symbol),
            self.get_klines(symbol, "1h", 250)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        spot_ticker = results[0] if not isinstance(results[0], Exception) else {}
        fut_ticker = results[1] if not isinstance(results[1], Exception) else {}
        cvd_data = results[2] if not isinstance(results[2], Exception) else {}
        klines = results[3] if not isinstance(results[3], Exception) else []
        
        # EMA Analysis
        closes = [float(k[4]) for k in klines] if klines else []
        ema_status = _analyze_ema_trend(closes)
        
        # Realtime metrics
        obi_data = self.realtime.get_obi(symbol, "1m") if self.realtime else None
        vol_data = self.realtime.get_volatility(symbol) if self.realtime else None
        obi_1m = 0.0
        if isinstance(obi_data, dict):
            obi_1m = float((obi_data.get("current") or {}).get("obi_avg") or 0)
        
        # Construct Report
        report = {
            "symbol": symbol,
            "timestamp": datetime.now().isoformat(),
            "spot": self._process_ticker(spot_ticker),
            "futures": self._process_ticker(fut_ticker),
            "cvd": {
                "1h": cvd_data.get("1h", {"spot": 0, "futures": 0}),
                "4h": cvd_data.get("4h", {"spot": 0, "futures": 0}),
                "24h": cvd_data.get("24h", {"spot": 0, "futures": 0}),
            },
            "realtime": {
                "obi_1m": obi_1m,
                "volatility": vol_data.get("volatility_pct", 0) if vol_data else 0
            },
            "ema_status": ema_status
        }
        
        report["text"] = self._format_telegram_msg(report)
        return report

    def _process_ticker(self, ticker: Dict) -> Dict:
        if not ticker:
            return {"price": 0, "change_pct": 0, "volume": 0}
        return {
            "price": float(ticker.get("lastPrice", 0)),
            "change_pct": float(ticker.get("priceChangePercent", 0)),
            "volume": float(ticker.get("quoteVolume", 0))
        }

    def _format_telegram_msg(self, data: Dict) -> str:
        s = data["spot"]
        f = data["futures"]
        cvd = data["cvd"]
        rt = data["realtime"]
        ema = data["ema_status"]
        sym = data["symbol"].replace("USDT", "")
        
        trend = "🟢" if f["change_pct"] > 0 else "🔴"
        
        lines = [
            f"📊 **{sym} Market Analysis** {trend}",
            f"Time: {datetime.now().strftime('%H:%M:%S')}",
            "",
            f"**EMA Trend (1H)**: {ema}",
            "",
            "**FUTURES**",
            f"Price: ${f['price']:,.2f} ({f['change_pct']:+.2f}%)",
            f"Volume (24h): ${self._fmt_num(f['volume'])}",
            f"OBI (1m): {rt['obi_1m']:.2f}",
            "",
            "**SPOT**",
            f"Price: ${s['price']:,.2f} ({s['change_pct']:+.2f}%)",
            f"Volume (24h): ${self._fmt_num(s['volume'])}",
            "",
            "**CVD FLOW (Net)**",
            f"1H:  Spot {self._fmt_num(cvd['1h']['spot'])} | Fut {self._fmt_num(cvd['1h']['futures'])}",
            f"4H:  Spot {self._fmt_num(cvd['4h']['spot'])} | Fut {self._fmt_num(cvd['4h']['futures'])}",
            f"24H: Spot {self._fmt_num(cvd['24h']['spot'])} | Fut {self._fmt_num(cvd['24h']['futures'])}",
            "",
            f"Basis: ${(f['price'] - s['price']):.2f} ({(f['price']/s['price']-1)*100:+.2f}%)" if s['price'] else "",
        ]
        return "\n".join([l for l in lines if l])

    def _fmt_num(self, n: float) -> str:
        if n is None: 
            return "0"
        abs_n = abs(n)
        if abs_n >= 1e9: 
            return f"{n/1e9:.2f}B"
        if abs_n >= 1e6: 
            return f"{n/1e6:.2f}M"
        if abs_n >= 1e3: 
            return f"{n/1e3:.2f}K"
        return f"{n:.2f}"
