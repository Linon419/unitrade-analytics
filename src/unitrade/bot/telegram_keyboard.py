"""
Telegram 交互菜单 - 正确格式

功能:
- 合约分析: 机构合约 vs 散户合约 (使用 CVDAnalysisService futures)
- 现货分析: 机构现货 vs 散户现货 (使用 CVDAnalysisService spot)
- 数据对比: 合约资金 vs 现货资金
- 多空分析: 多空比历史 (使用 MarketReporter)
- 持仓分析: OI历史
- 热币列表: 涨跌幅榜
"""

import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class InlineButton:
    """内联按钮"""
    text: str
    callback_data: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {"text": self.text, "callback_data": self.callback_data or self.text}


@dataclass
class InlineKeyboard:
    """内联键盘"""
    rows: List[List[InlineButton]] = field(default_factory=list)
    
    def add_row(self, *buttons: InlineButton) -> "InlineKeyboard":
        self.rows.append(list(buttons))
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        return {"inline_keyboard": [[btn.to_dict() for btn in row] for row in self.rows]}


class UniTradeMenus:
    """菜单模板"""
    
    @staticmethod
    def main_menu(symbol: str = "BTCUSDT", active: str = "futures") -> InlineKeyboard:
        """
        主菜单
        
        active: 当前选中的功能 (spot, futures, compare, position, longshort)
        """
        # 根据 active 添加选中标记
        spot_text = "✅ 现货分析" if active == "spot" else "现货分析"
        futures_text = "✅ 合约分析" if active == "futures" else "合约分析"
        compare_text = "✅ 数据对比" if active == "compare" else "数据对比"
        position_text = "✅ 持仓分析" if active == "position" else "持仓分析"
        longshort_text = "✅ 多空分析" if active == "longshort" else "多空分析"
        
        return (InlineKeyboard()
            .add_row(
                InlineButton(spot_text, f"spot:{symbol}"),
                InlineButton(futures_text, f"futures:{symbol}"),
                InlineButton(compare_text, f"compare:{symbol}"),
            )
            .add_row(
                InlineButton("现货热力图", "heatmap:spot"),
                InlineButton("合约热力图", "heatmap:futures"),
            )
            .add_row(
                InlineButton(position_text, f"position:{symbol}"),
                InlineButton(longshort_text, f"longshort:{symbol}"),
            )
            .add_row(
                InlineButton("🏆 上涨排行", f"rising_index:{symbol}"),
                InlineButton("🚀 异动监测", f"anomaly_status:{symbol}"),
            )
            .add_row(
                InlineButton("🔥 热币列表", "hot_coins"),
                InlineButton("🔄 刷新", f"refresh:{symbol}"),
            )
            .add_row(
                InlineButton("↩️ 返回主菜单", "menu:main"),
                InlineButton("❌ 关闭", "close"),
            )
        )


def format_flow(value: float) -> str:
    """格式化资金流 (k/m/b)"""
    abs_val = abs(value)
    if abs_val >= 1e9:
        return f"{value/1e9:.2f}b"
    elif abs_val >= 1e6:
        return f"{value/1e6:.2f}m"
    elif abs_val >= 1e3:
        return f"{value/1e3:.2f}k"
    else:
        return f"{value:.0f}"


class UniTradeBotHandler:
    """UniTrade Bot 处理器 - 使用正确的数据格式"""
    
    API_BASE = "https://api.telegram.org/bot"
    
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._session: Optional[aiohttp.ClientSession] = None
        self._cvd_service = None
    
    @property
    def api_url(self) -> str:
        return f"{self.API_BASE}{self.bot_token}"
    
    async def start(self):
        self._session = aiohttp.ClientSession()
        from unitrade.web.cvd_service import CVDAnalysisService
        self._cvd_service = CVDAnalysisService()
    
    async def stop(self):
        if self._cvd_service:
            await self._cvd_service.close()
        if self._session:
            await self._session.close()
    
    async def send_menu(
        self, 
        text: str, 
        keyboard: InlineKeyboard,
        topic_id: Optional[int] = None
    ) -> Optional[Dict]:
        """发送带菜单的消息"""
        try:
            data = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": keyboard.to_dict(),
            }
            if topic_id:
                data["message_thread_id"] = topic_id
            
            async with self._session.post(f"{self.api_url}/sendMessage", json=data) as resp:
                result = await resp.json()
                if result.get("ok"):
                    return result.get("result")
                logger.error(f"Send error: {result}")
                return None
        except Exception as e:
            logger.error(f"Send menu error: {e}")
            return None
    
    # ========== 合约分析 (机构合约 vs 散户合约) ==========
    
    async def send_futures_analysis(self, symbol: str = "BTCUSDT", topic_id: Optional[int] = None) -> Optional[Dict]:
        """发送合约分析 - 买入量 vs 卖出量 vs 净流入"""
        try:
            # 获取价格
            price_url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
            async with self._session.get(price_url) as resp:
                ticker = await resp.json()
                price = float(ticker.get('lastPrice', 0))
            
            # 获取 CVD 数据
            cvd_data = await self._cvd_service.get_cvd_analysis(symbol)
            
            base = symbol.replace("USDT", "")
            now = datetime.now().strftime("%m-%d %H:%M")
            
            lines = [
                f"<b>📊 {base} 合约分析</b>",
                f"💰 ${price:,.2f}  ⏰ {now}",
                "",
            ]
            
            periods = ["1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "24h", 
                      "2d", "3d", "5d", "7d"]
            
            for period in periods:
                if period in cvd_data:
                    buy_vol = cvd_data[period].get("futures_buy", 0)
                    sell_vol = cvd_data[period].get("futures_sell", 0)
                    net_vol = buy_vol - sell_vol
                    
                    # 紧凑格式: 周期 买|卖|净
                    buy_s = format_flow(buy_vol)
                    sell_s = format_flow(sell_vol)
                    net_s = format_flow(net_vol)
                    net_emoji = "🟢" if net_vol > 0 else "🔴" if net_vol < 0 else "⚪"
                    
                    lines.append(f"{period:>3} {net_emoji}{net_s:>7} 买{buy_s:>6} 卖{sell_s:>6}")
            
            text = "\n".join(lines)
            return await self.send_menu(text, UniTradeMenus.main_menu(symbol, "futures"), topic_id)
            
        except Exception as e:
            logger.error(f"Futures analysis error: {e}")
            return await self.send_menu(f"❌ 合约分析错误: {e}", UniTradeMenus.main_menu(symbol, "futures"), topic_id)
    
    # ========== 现货分析 (机构现货 vs 散户现货) ==========
    
    async def send_spot_analysis(self, symbol: str = "BTCUSDT", topic_id: Optional[int] = None) -> Optional[Dict]:
        """发送现货分析 - 买入量 vs 卖出量 vs 净流入"""
        try:
            # 获取价格
            price_url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
            async with self._session.get(price_url) as resp:
                ticker = await resp.json()
                price = float(ticker.get('lastPrice', 0))
            
            # 获取 CVD 数据
            cvd_data = await self._cvd_service.get_cvd_analysis(symbol)
            
            base = symbol.replace("USDT", "")
            now = datetime.now().strftime("%m-%d %H:%M")
            
            lines = [
                f"<b>📊 {base} 现货分析</b>",
                f"💰 ${price:,.2f}  ⏰ {now}",
                "",
            ]
            
            periods = ["1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "24h", 
                      "2d", "3d", "5d", "7d"]
            
            for period in periods:
                if period in cvd_data:
                    buy_vol = cvd_data[period].get("spot_buy", 0)
                    sell_vol = cvd_data[period].get("spot_sell", 0)
                    net_vol = buy_vol - sell_vol
                    
                    buy_s = format_flow(buy_vol)
                    sell_s = format_flow(sell_vol)
                    net_s = format_flow(net_vol)
                    net_emoji = "🟢" if net_vol > 0 else "🔴" if net_vol < 0 else "⚪"
                    
                    lines.append(f"{period:>3} {net_emoji}{net_s:>7} 买{buy_s:>6} 卖{sell_s:>6}")
            
            text = "\n".join(lines)
            return await self.send_menu(text, UniTradeMenus.main_menu(symbol, "spot"), topic_id)
            
        except Exception as e:
            logger.error(f"Spot analysis error: {e}")
            return await self.send_menu(f"❌ 现货分析错误: {e}", UniTradeMenus.main_menu(symbol, "spot"), topic_id)
    
    # ========== 数据对比 (合约资金 vs 现货资金) ==========
    
    async def send_compare_analysis(self, symbol: str = "BTCUSDT", topic_id: Optional[int] = None) -> Optional[Dict]:
        """发送数据对比 - 合约资金 vs 现货资金"""
        try:
            # 获取价格
            price_url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
            async with self._session.get(price_url) as resp:
                ticker = await resp.json()
                price = float(ticker.get('lastPrice', 0))
            
            # 获取 CVD 数据
            cvd_data = await self._cvd_service.get_cvd_analysis(symbol)
            
            base = symbol.replace("USDT", "")
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            lines = [
                f"<code>[{symbol}] 合约现货数据对比</code>🐵🐵🐵",
                f"⏰ 查询时间: {now}",
                "————————————————————",
                "",
                f"最近交易价格 ({price:,.2f}U)",
                "",
                f"<code>period   合约资金      现货资金</code>",
            ]
            
            periods = ["1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "24h", 
                      "2d", "3d", "5d", "7d", "10d", "15d", "30d", "60d"]
            
            for period in periods:
                if period in cvd_data:
                    futures_net = cvd_data[period].get("futures", 0)
                    spot_net = cvd_data[period].get("spot", 0)
                    
                    fut_str = format_flow(futures_net)
                    spot_str = format_flow(spot_net)
                    
                    lines.append(f"<code>{period:>5}:  {fut_str:>10}   {spot_str:>10}</code>")
            
            text = "\n".join(lines)
            return await self.send_menu(text, UniTradeMenus.main_menu(symbol, "compare"), topic_id)
            
        except Exception as e:
            logger.error(f"Compare analysis error: {e}")
            return await self.send_menu(f"❌ 数据对比错误: {e}", UniTradeMenus.main_menu(symbol, "compare"), topic_id)
    
    # ========== 多空分析 (多空比历史) ==========
    
    async def send_longshort_analysis(self, symbol: str = "BTCUSDT", topic_id: Optional[int] = None) -> Optional[Dict]:
        """发送多空分析 - 使用 MarketReporter 的多空比历史"""
        try:
            from unitrade.tracker import MarketReporter
            
            reporter = MarketReporter()
            await reporter.start()
            
            report = await reporter.generate_report(symbol)
            history = await reporter.get_ratio_history(symbol, 5)
            
            await reporter.stop()
            
            if not report:
                return await self.send_menu("❌ 获取数据失败", UniTradeMenus.main_menu(symbol, "longshort"), topic_id)
            
            # 格式化为用户截图中的格式
            text = reporter.format_telegram_report(report, history)
            
            return await self.send_menu(text, UniTradeMenus.main_menu(symbol, "longshort"), topic_id)
            
        except Exception as e:
            logger.error(f"Long/short analysis error: {e}")
            return await self.send_menu(f"❌ 多空分析错误: {e}", UniTradeMenus.main_menu(symbol, "longshort"), topic_id)
    
    # ========== 持仓分析 (OI 历史) ==========
    
    async def send_position_analysis(self, symbol: str = "BTCUSDT", topic_id: Optional[int] = None) -> Optional[Dict]:
        """发送持仓分析"""
        try:
            url = f"https://fapi.binance.com/futures/data/openInterestHist"
            params = {"symbol": symbol, "period": "1h", "limit": 12}
            
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    return await self.send_menu("❌ 获取持仓数据失败", UniTradeMenus.main_menu(symbol, "position"), topic_id)
                data = await resp.json()
            
            if not data:
                return await self.send_menu("❌ 无持仓数据", UniTradeMenus.main_menu(symbol, "position"), topic_id)
            
            base = symbol.replace("USDT", "")
            latest = data[-1]
            first = data[0]
            
            oi_now = float(latest["sumOpenInterest"])
            oi_value = float(latest["sumOpenInterestValue"])
            oi_old = float(first["sumOpenInterest"])
            oi_change = ((oi_now - oi_old) / oi_old * 100) if oi_old > 0 else 0
            
            lines = [
                f"<b>💰 {base} 持仓分析</b>",
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                "━" * 20,
                f"持仓量: {oi_now/1e4:.2f} 万 ({oi_change:+.2f}%)",
                f"持仓价值: ${oi_value/1e8:.2f} 亿",
                "",
                "<b>12小时持仓变化:</b>",
            ]
            
            for item in data[-6:]:
                ts = datetime.fromtimestamp(item["timestamp"]/1000).strftime("%H:%M")
                oi = float(item["sumOpenInterest"])
                lines.append(f"  {ts}: {oi/1e4:.1f}万")
            
            text = "\n".join(lines)
            return await self.send_menu(text, UniTradeMenus.main_menu(symbol, "position"), topic_id)
            
        except Exception as e:
            logger.error(f"Position analysis error: {e}")
            return await self.send_menu(f"❌ 持仓分析错误: {e}", UniTradeMenus.main_menu(symbol, "position"), topic_id)
    
    # ========== 热币列表 ==========
    
    async def send_hot_coins(self, topic_id: Optional[int] = None) -> Optional[Dict]:
        """Send hot coins list (volume spikes)."""
        try:
            url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    return await self.send_menu("Failed to fetch data", UniTradeMenus.main_menu(), topic_id)
                tickers = await resp.json()

            min_quote_volume = 1e8
            max_symbols = 40
            lookback = 20
            intervals = ["15m", "1h"]

            usdt = [t for t in tickers if t.get("symbol", "").endswith("USDT")]
            usdt = [t for t in usdt if float(t.get("quoteVolume", 0)) > min_quote_volume]
            usdt.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
            symbols = [t["symbol"] for t in usdt[:max_symbols]]

            semaphore = asyncio.Semaphore(8)

            async def fetch_rvol(symbol: str, interval: str):
                params = {"symbol": symbol, "interval": interval, "limit": lookback + 1}
                url = "https://fapi.binance.com/fapi/v1/klines"
                async with semaphore:
                    async with self._session.get(url, params=params) as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json()
                if len(data) < lookback + 1:
                    return None
                vols = [float(k[7]) for k in data]
                last = vols[-1]
                avg = sum(vols[:-1]) / len(vols[:-1]) if vols[:-1] else 0.0
                rvol = last / avg if avg > 0 else 0.0
                return {"symbol": symbol, "interval": interval, "rvol": rvol, "quote_volume": last}

            tasks = [fetch_rvol(sym, interval) for sym in symbols for interval in intervals]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            top_15m = []
            top_1h = []
            for item in results:
                if not isinstance(item, dict):
                    continue
                if item.get("interval") == "15m":
                    top_15m.append(item)
                else:
                    top_1h.append(item)

            top_15m.sort(key=lambda x: x["rvol"], reverse=True)
            top_1h.sort(key=lambda x: x["rvol"], reverse=True)

            lines = [
                "<b>Hot Coins - Volume Spikes</b>",
                f"{datetime.now().strftime('%Y-%m-%d %H:%M')}",
                "-" * 20,
                "",
                "<b>15m RVOL Top</b>",
            ]

            if not top_15m:
                lines.append("  (no data)")
            else:
                for item in top_15m[:5]:
                    symbol = item["symbol"].replace("USDT", "")
                    rvol = item.get("rvol", 0.0)
                    qv = format_flow(item.get("quote_volume", 0.0))
                    lines.append(f"  {symbol}: {rvol:.2f}x qv={qv}")

            lines.append("")
            lines.append("<b>1h RVOL Top</b>")

            if not top_1h:
                lines.append("  (no data)")
            else:
                for item in top_1h[:5]:
                    symbol = item["symbol"].replace("USDT", "")
                    rvol = item.get("rvol", 0.0)
                    qv = format_flow(item.get("quote_volume", 0.0))
                    lines.append(f"  {symbol}: {rvol:.2f}x qv={qv}")

            text = "\n".join(lines)
            return await self.send_menu(text, UniTradeMenus.main_menu(), topic_id)

        except Exception as e:
            logger.error(f"Hot coins error: {e}")
            return await self.send_menu(f"Hot coins error: {e}", UniTradeMenus.main_menu(), topic_id)
    async def send_institution_analysis(self, symbol: str = "BTCUSDT", topic_id: Optional[int] = None) -> Optional[Dict]:
        """发送机构 vs 散户分析 - 使用 Binance 大户 API + 订单大小分析"""
        try:
            from unitrade.analytics.institution_retail import InstitutionRetailAnalyzer
            
            analyzer = InstitutionRetailAnalyzer()
            await analyzer.start()
            
            analysis = await analyzer.get_full_analysis(symbol)
            
            await analyzer.stop()
            
            base = symbol.replace("USDT", "")
            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            
            lines = [
                f"<b>🏛️ {base} 机构 vs 散户分析</b>",
                f"⏰ {now}",
                "━" * 20,
                "",
                "<b>📊 大户持仓多空比:</b>",
            ]
            
            big_trader = analysis.get("big_trader", {})
            
            # 显示各周期的大户持仓比
            for period in ["5m", "15m", "30m", "1h", "4h"]:
                top_pos = big_trader.get(f"top_position_{period}", {})
                if top_pos:
                    ratio = top_pos.get("ratio", 0)
                    long_pct = top_pos.get("long_pct", 0)
                    signal = "🟢" if ratio > 1 else "🔴" if ratio < 1 else "⚪"
                    lines.append(f"  {period}: {signal} {ratio:.2f} (多{long_pct:.1f}%)")
            
            lines.append("")
            lines.append("<b>📈 主动买卖比:</b>")
            
            taker_vol = analysis.get("taker_volume", [])
            for item in taker_vol[:6]:
                ratio = item.get("buy_sell_ratio", 0)
                buy_vol = item.get("buy_vol", 0)
                sell_vol = item.get("sell_vol", 0)
                signal = "🟢" if ratio > 1 else "🔴" if ratio < 1 else "⚪"
                lines.append(f"  {item['time']}: {signal} 买卖比 {ratio:.2f}")
            
            # 判断整体趋势
            lines.append("")
            latest_ratio = big_trader.get("top_position_5m", {}).get("ratio", 1)
            if latest_ratio > 1.2:
                trend = "🟢 机构偏多"
            elif latest_ratio < 0.8:
                trend = "🔴 机构偏空"
            else:
                trend = "⚪ 多空均衡"
            
            lines.append(f"<b>当前趋势: {trend}</b>")
            
            text = "\n".join(lines)
            return await self.send_menu(text, UniTradeMenus.main_menu(symbol), topic_id)
            
        except Exception as e:
            logger.error(f"Institution analysis error: {e}")
            return await self.send_menu(f"❌ 机构散户分析错误: {e}", UniTradeMenus.main_menu(symbol), topic_id)
    
    # ========== 主菜单 ==========
    
    def _get_redis_url(self) -> str:
        env_url = (os.getenv("REDIS_URL") or "").strip()
        if env_url:
            return env_url
        try:
            from unitrade.core.config import load_config

            return load_config().database.redis_url
        except Exception:
            return "redis://localhost:6379"

    @staticmethod
    def _clamp01(x: float) -> float:
        return max(0.0, min(1.0, x))

    @classmethod
    def _normalize(cls, value: float, low: float, high: float) -> float:
        if high <= low:
            return 0.0
        return cls._clamp01((value - low) / (high - low))

    async def send_rising_index(
        self,
        symbol: str = "BTCUSDT",
        top_n: int = 10,
        topic_id: Optional[int] = None,
    ) -> Optional[Dict]:
        """
        轻量版「上涨排行」：仅基于 AnomalyDetector 写入 Redis 的突破信号计算，避免额外 Binance REST 请求。
        """
        try:
            from unitrade.scanner.signal_detector import RedisStateManager

            redis_url = self._get_redis_url()
            state_manager = RedisStateManager(redis_url=redis_url)
            await state_manager.connect()

            keys = await state_manager.scan_signal_keys(prefix="anomaly")
            now = time.time()
            decay_hours = 24.0

            rows: List[tuple] = []
            for key in keys:
                sym = key.split(":")[-1]
                signals = await state_manager.get_breakout_signals(sym, prefix="anomaly")
                if not signals:
                    continue

                oi_changes: List[float] = []
                rvols: List[float] = []
                last_ts = 0.0
                last_price = 0.0
                last_ema200 = 0.0

                for signal_data, ts in signals:
                    last_ts = max(last_ts, float(ts))
                    parts = str(signal_data).split("|")
                    if len(parts) >= 4:
                        try:
                            oi_changes.append(float(parts[0]))
                            rvols.append(float(parts[1]))
                            last_price = float(parts[2])
                            last_ema200 = float(parts[3])
                        except Exception:
                            continue

                if not oi_changes:
                    continue

                cumulative_oi = float(sum(oi_changes))
                avg_rvol = float(sum(rvols) / len(rvols)) if rvols else 0.0
                recency_score = math.exp(-((now - last_ts) / 3600.0) / decay_hours) if last_ts > 0 else 0.0

                oi_score = self._normalize(cumulative_oi, 0.0, 0.5)
                volume_score = self._normalize(avg_rvol, 1.0, 10.0)

                total_score = 100.0 * (0.45 * oi_score + 0.30 * recency_score + 0.25 * volume_score)

                if last_price and last_ema200:
                    ema_alignment = "bullish" if last_price >= last_ema200 else "bearish"
                else:
                    ema_alignment = "neutral"

                rows.append((sym, total_score, cumulative_oi, avg_rvol, ema_alignment, len(signals)))

            rows.sort(key=lambda x: x[1], reverse=True)
            top = rows[: max(1, int(top_n))]

            lines = [
                "<b>🏆 上涨排行（轻量版）</b>",
                f"⏱ {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                "—" * 20,
            ]

            if not top:
                lines += [
                    "",
                    "暂无数据：需要 <b>Anomaly Detector</b> 运行并写入 Redis。",
                    f"当前 Redis: <code>{redis_url}</code>",
                ]
            else:
                lines.append("")
                for i, (sym, score, cum_oi, avg_rvol, ema_align, count) in enumerate(top, 1):
                    base = sym.replace("USDT", "")
                    trend = "↑" if ema_align == "bullish" else "↓" if ema_align == "bearish" else "→"
                    oi_sign = "+" if cum_oi >= 0 else ""
                    lines.append(
                        f"{i}. <b>{base}</b> ⚡{score:.1f}  OI {oi_sign}{cum_oi:.1%}  RVOL {avg_rvol:.1f}x  {trend}  ({count}次)"
                    )

            await state_manager.close()
            return await self.send_menu("\n".join(lines), UniTradeMenus.main_menu(symbol, "futures"), topic_id)

        except Exception as e:
            logger.error(f"Rising index error: {e}")
            return await self.send_menu(
                f"❌ 上涨排行错误: {e}",
                UniTradeMenus.main_menu(symbol, "futures"),
                topic_id,
            )

    async def send_anomaly_status(
        self,
        symbol: str = "BTCUSDT",
        topic_id: Optional[int] = None,
    ) -> Optional[Dict]:
        """显示异动监测状态（从 Redis 读取最近突破信号汇总）。"""
        try:
            from unitrade.scanner.signal_detector import RedisStateManager

            redis_url = self._get_redis_url()
            state_manager = RedisStateManager(redis_url=redis_url)
            await state_manager.connect()

            keys = await state_manager.scan_signal_keys(prefix="anomaly")

            lines = [
                "<b>🚀 异动监测状态</b>",
                f"⏱ {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                "—" * 20,
                "",
                f"📡 触发过信号的币种: <b>{len(keys)}</b>",
                "",
                "<b>触发条件</b>",
                "• EMA200 突破（从下向上）",
                "• OI 增加 ≥ 3%（相对 N 根 K 线前）",
                "• 成交量放大 ≥ 3x 均量",
                "",
            ]

            if keys:
                lines.append("<b>最近信号（最多 10 个）</b>")
                for key in keys[:10]:
                    sym = key.split(":")[-1]
                    base = sym.replace("USDT", "")
                    signals = await state_manager.get_breakout_signals(sym, prefix="anomaly")
                    lines.append(f"• {base}: {len(signals) if signals else 0} 次")
            else:
                lines += [
                    "暂无信号数据：",
                    "• 监测器未运行，或",
                    "• Redis 不可用 / 未连接（in-memory fallback 无法跨进程共享）",
                    f"当前 Redis: <code>{redis_url}</code>",
                ]

            await state_manager.close()
            return await self.send_menu("\n".join(lines), UniTradeMenus.main_menu(symbol, "futures"), topic_id)

        except Exception as e:
            logger.error(f"Anomaly status error: {e}")
            return await self.send_menu(
                f"❌ 异动监测错误: {e}",
                UniTradeMenus.main_menu(symbol, "futures"),
                topic_id,
            )

    async def send_main_menu(self, symbol: str = "BTCUSDT", topic_id: Optional[int] = None) -> Optional[Dict]:
        """发送主菜单"""
        text = f"""
<b>🤖 UniTrade Analytics</b>

⏰ {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

查询币种: {symbol}
请选择功能:
"""
        return await self.send_menu(text, UniTradeMenus.main_menu(symbol), topic_id)


async def test_bot():
    """测试 Bot"""
    import os

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID")

    bot = UniTradeBotHandler(
        bot_token=bot_token,
        chat_id=chat_id,
    )
    
    await bot.start()
    
    print("1. 发送主菜单 (含机构散户按钮)...")
    await bot.send_main_menu("BTCUSDT")
    
    await bot.stop()
    print("完成!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_bot())
