"""
UniTrade Analytics Telegram Bot - 使用 aiogram 3.x

功能:
- /btc /eth 等命令查询币种
- 合约分析、现货分析、数据对比
- 持仓分析、多空分析、热币列表
"""

import asyncio
import logging
import os
from datetime import datetime
from typing import Optional

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode

logger = logging.getLogger(__name__)

# ===== 配置 =====

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

# 支持的币种
SUPPORTED_SYMBOLS = [
    "btc", "eth", "sol", "bnb", "xrp", "doge", "ada", "avax",
    "dot", "link", "matic", "atom", "uni", "ltc", "etc",
    "arb", "op", "apt", "sui", "sei", "inj", "jup",
]

# ===== 工具函数 =====

def format_flow(value: float) -> str:
    """格式化资金流 (k/m/b)"""
    abs_val = abs(value)
    if abs_val >= 1e9:
        return f"{value/1e9:.2f}b"
    elif abs_val >= 1e6:
        return f"{value/1e6:.2f}m"
    elif abs_val >= 1e3:
        return f"{value/1e3:.1f}k"
    else:
        return f"{value:.0f}"


# ===== 键盘构建器 =====

def build_main_keyboard(symbol: str = "BTCUSDT", active: str = "futures") -> InlineKeyboardMarkup:
    """构建主菜单键盘"""
    builder = InlineKeyboardBuilder()
    
    # 根据 active 添加选中标记
    spot_text = "✅ 现货分析" if active == "spot" else "现货分析"
    futures_text = "✅ 合约分析" if active == "futures" else "合约分析"
    compare_text = "✅ 数据对比" if active == "compare" else "数据对比"
    position_text = "✅ 持仓分析" if active == "position" else "持仓分析"
    longshort_text = "✅ 多空分析" if active == "longshort" else "多空分析"
    
    # 第一行
    builder.row(
        InlineKeyboardButton(text=spot_text, callback_data=f"spot:{symbol}"),
        InlineKeyboardButton(text=futures_text, callback_data=f"futures:{symbol}"),
        InlineKeyboardButton(text=compare_text, callback_data=f"compare:{symbol}"),
    )
    
    # 第二行
    builder.row(
        InlineKeyboardButton(text="现货热力图", callback_data="heatmap:spot"),
        InlineKeyboardButton(text="合约热力图", callback_data="heatmap:futures"),
    )
    
    # 第三行
    builder.row(
        InlineKeyboardButton(text=position_text, callback_data=f"position:{symbol}"),
        InlineKeyboardButton(text=longshort_text, callback_data=f"longshort:{symbol}"),
    )
    
    # 第四行 - 异动监测
    builder.row(
        InlineKeyboardButton(text="🏆 上涨排行", callback_data="rising_index"),
        InlineKeyboardButton(text="🚀 异动监测", callback_data="anomaly_status"),
    )
    
    # 第五行
    builder.row(
        InlineKeyboardButton(text="🔥 热币列表", callback_data="hot_coins"),
        InlineKeyboardButton(text="🔄 刷新", callback_data=f"refresh:{symbol}"),
    )
    
    # 第五行
    builder.row(
        InlineKeyboardButton(text="↩️ 返回主菜单", callback_data="menu:main"),
        InlineKeyboardButton(text="❌ 关闭", callback_data="close"),
    )
    
    return builder.as_markup()


# ===== 数据获取服务 =====

class DataService:
    """数据获取服务"""
    
    def __init__(self):
        self._cvd_service = None
        self._session = None
    
    async def start(self):
        import aiohttp
        self._session = aiohttp.ClientSession()
        from unitrade.web.cvd_service import CVDAnalysisService
        self._cvd_service = CVDAnalysisService()
    
    async def stop(self):
        if self._cvd_service:
            await self._cvd_service.close()
        if self._session:
            await self._session.close()
    
    async def get_futures_price(self, symbol: str) -> float:
        url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
        async with self._session.get(url) as resp:
            data = await resp.json()
            return float(data.get('lastPrice', 0))
    
    async def get_spot_price(self, symbol: str) -> float:
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
        async with self._session.get(url) as resp:
            data = await resp.json()
            return float(data.get('lastPrice', 0))
    
    async def get_cvd_analysis(self, symbol: str):
        return await self._cvd_service.get_cvd_analysis(symbol)
    
    async def get_hot_coins(self) -> tuple:
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        async with self._session.get(url) as resp:
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
        return top_15m[:5], top_1h[:5]
    async def get_oi_history(self, symbol: str, limit: int = 12):
        url = "https://fapi.binance.com/futures/data/openInterestHist"
        params = {"symbol": symbol, "period": "1h", "limit": limit}
        async with self._session.get(url, params=params) as resp:
            if resp.status == 200:
                return await resp.json()
            return []


# 全局数据服务
data_service = DataService()

# ===== 路由器 =====

router = Router()


# ----- 命令处理 -----

@router.message(Command(*SUPPORTED_SYMBOLS))
async def cmd_symbol(message: Message):
    """处理币种命令 /btc /eth 等"""
    command = message.text.strip().lower()[1:].split("@")[0]
    symbol = f"{command.upper()}USDT"
    
    text = await generate_futures_analysis(symbol)
    await message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard(symbol, "futures")
    )


@router.message(Command("start"))
async def cmd_start(message: Message):
    """处理 /start 命令"""
    text = """
<b>🤖 UniTrade Analytics Bot</b>

欢迎使用！发送币种命令查看分析：

<b>常用命令:</b>
/btc - 比特币分析
/eth - 以太坊分析
/sol - Solana分析

发送 /help 获取更多帮助
"""
    await message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard("BTCUSDT", "futures")
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    """处理 /help 命令"""
    text = """
<b>📚 帮助</b>

<b>命令格式:</b>
/币种名 - 例如 /btc /eth /sol

<b>功能说明:</b>
• 现货分析 - 现货买卖资金流
• 合约分析 - 合约买卖资金流  
• 数据对比 - 合约 vs 现货净流入
• 持仓分析 - OI 持仓量变化
• 多空分析 - 多空比历史
• 热币列表 - 涨跌幅榜
"""
    await message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard("BTCUSDT", "futures")
    )


# ----- 回调处理 -----

@router.callback_query(F.data.startswith("futures:"))
async def callback_futures(callback: CallbackQuery):
    """合约分析回调"""
    symbol = callback.data.split(":")[1]
    await callback.answer("加载合约分析...")
    
    text = await generate_futures_analysis(symbol)
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard(symbol, "futures")
    )


@router.callback_query(F.data.startswith("spot:"))
async def callback_spot(callback: CallbackQuery):
    """现货分析回调"""
    symbol = callback.data.split(":")[1]
    await callback.answer("加载现货分析...")
    
    text = await generate_spot_analysis(symbol)
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard(symbol, "spot")
    )


@router.callback_query(F.data.startswith("compare:"))
async def callback_compare(callback: CallbackQuery):
    """数据对比回调"""
    symbol = callback.data.split(":")[1]
    await callback.answer("加载数据对比...")
    
    text = await generate_compare_analysis(symbol)
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard(symbol, "compare")
    )


@router.callback_query(F.data.startswith("position:"))
async def callback_position(callback: CallbackQuery):
    """持仓分析回调"""
    symbol = callback.data.split(":")[1]
    await callback.answer("加载持仓分析...")
    
    text = await generate_position_analysis(symbol)
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard(symbol, "position")
    )


@router.callback_query(F.data.startswith("longshort:"))
async def callback_longshort(callback: CallbackQuery):
    """多空分析回调"""
    symbol = callback.data.split(":")[1]
    await callback.answer("加载多空分析...")
    
    text = await generate_longshort_analysis(symbol)
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard(symbol, "longshort")
    )


@router.callback_query(F.data == "hot_coins")
async def callback_hot_coins(callback: CallbackQuery):
    """热币列表回调"""
    await callback.answer("加载热币列表...")
    
    text = await generate_hot_coins()
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard("BTCUSDT", "futures")
    )


@router.callback_query(F.data.startswith("refresh:"))
async def callback_refresh(callback: CallbackQuery):
    """刷新回调"""
    symbol = callback.data.split(":")[1]
    await callback.answer("刷新中...")
    
    text = await generate_futures_analysis(symbol)
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard(symbol, "futures")
    )


@router.callback_query(F.data == "close")
async def callback_close(callback: CallbackQuery):
    """关闭回调"""
    await callback.answer("已关闭")
    await callback.message.delete()


@router.callback_query(F.data.startswith("heatmap:"))
async def callback_heatmap(callback: CallbackQuery):
    """热力图回调"""
    await callback.answer("热力图功能开发中...", show_alert=True)


@router.callback_query(F.data == "rising_index")
async def callback_rising_index(callback: CallbackQuery):
    """上涨排行回调"""
    await callback.answer("加载上涨排行...")
    
    text = await generate_rising_index()
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard("BTCUSDT", "futures")
    )


@router.callback_query(F.data == "anomaly_status")
async def callback_anomaly_status(callback: CallbackQuery):
    """异动监测状态回调"""
    await callback.answer("加载异动监测...")
    
    text = await generate_anomaly_status()
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard("BTCUSDT", "futures")
    )


@router.callback_query(F.data.startswith("menu:"))
async def callback_menu(callback: CallbackQuery):
    """菜单回调"""
    await callback.answer()
    text = await generate_futures_analysis("BTCUSDT")
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_main_keyboard("BTCUSDT", "futures")
    )


# ===== 数据生成函数 =====

async def generate_futures_analysis(symbol: str) -> str:
    """生成合约分析文本"""
    try:
        price = await data_service.get_futures_price(symbol)
        cvd_data = await data_service.get_cvd_analysis(symbol)
        
        base = symbol.replace("USDT", "")
        now = datetime.now().strftime("%m-%d %H:%M")
        
        lines = [
            f"<b>📊 {base} 合约分析</b>",
            f"💰 ${price:,.2f}  ⏰ {now}",
            "",
        ]
        
        periods = ["1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "24h", "2d", "3d", "5d", "7d"]
        
        for period in periods:
            if period in cvd_data:
                buy = cvd_data[period].get("futures_buy", 0)
                sell = cvd_data[period].get("futures_sell", 0)
                net = buy - sell
                
                emoji = "🟢" if net > 0 else "🔴" if net < 0 else "⚪"
                lines.append(f"{period:>3} {emoji}{format_flow(net):>7} 买{format_flow(buy):>6} 卖{format_flow(sell):>6}")
        
        return "\n".join(lines)
        
    except Exception as e:
        logger.error(f"Futures analysis error: {e}")
        return f"❌ 合约分析错误: {e}"


async def generate_spot_analysis(symbol: str) -> str:
    """生成现货分析文本"""
    try:
        price = await data_service.get_spot_price(symbol)
        cvd_data = await data_service.get_cvd_analysis(symbol)
        
        base = symbol.replace("USDT", "")
        now = datetime.now().strftime("%m-%d %H:%M")
        
        lines = [
            f"<b>📊 {base} 现货分析</b>",
            f"💰 ${price:,.2f}  ⏰ {now}",
            "",
        ]
        
        periods = ["1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "24h", "2d", "3d", "5d", "7d"]
        
        for period in periods:
            if period in cvd_data:
                buy = cvd_data[period].get("spot_buy", 0)
                sell = cvd_data[period].get("spot_sell", 0)
                net = buy - sell
                
                emoji = "🟢" if net > 0 else "🔴" if net < 0 else "⚪"
                lines.append(f"{period:>3} {emoji}{format_flow(net):>7} 买{format_flow(buy):>6} 卖{format_flow(sell):>6}")
        
        return "\n".join(lines)
        
    except Exception as e:
        logger.error(f"Spot analysis error: {e}")
        return f"❌ 现货分析错误: {e}"


async def generate_compare_analysis(symbol: str) -> str:
    """生成数据对比文本"""
    try:
        price = await data_service.get_futures_price(symbol)
        cvd_data = await data_service.get_cvd_analysis(symbol)
        
        base = symbol.replace("USDT", "")
        now = datetime.now().strftime("%m-%d %H:%M")
        
        lines = [
            f"<b>📊 {base} 合约 vs 现货</b>",
            f"💰 ${price:,.2f}  ⏰ {now}",
            "",
            "周期  合约净流入  现货净流入",
        ]
        
        periods = ["1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "24h", "2d", "3d", "5d", "7d"]
        
        for period in periods:
            if period in cvd_data:
                fut = cvd_data[period].get("futures", 0)
                spot = cvd_data[period].get("spot", 0)
                
                lines.append(f"{period:>3}   {format_flow(fut):>9}  {format_flow(spot):>9}")
        
        return "\n".join(lines)
        
    except Exception as e:
        logger.error(f"Compare analysis error: {e}")
        return f"❌ 数据对比错误: {e}"


async def generate_position_analysis(symbol: str) -> str:
    """生成持仓分析文本"""
    try:
        data = await data_service.get_oi_history(symbol)
        
        if not data:
            return "❌ 无持仓数据"
        
        base = symbol.replace("USDT", "")
        latest = data[-1]
        first = data[0]
        
        oi_now = float(latest["sumOpenInterest"])
        oi_value = float(latest["sumOpenInterestValue"])
        oi_old = float(first["sumOpenInterest"])
        oi_change = ((oi_now - oi_old) / oi_old * 100) if oi_old > 0 else 0
        
        lines = [
            f"<b>💰 {base} 持仓分析</b>",
            f"⏰ {datetime.now().strftime('%m-%d %H:%M')}",
            "",
            f"持仓量: {oi_now/1e4:.2f}万 ({oi_change:+.2f}%)",
            f"持仓价值: ${oi_value/1e8:.2f}亿",
            "",
            "<b>12小时持仓变化:</b>",
        ]
        
        for item in data[-6:]:
            ts = datetime.fromtimestamp(item["timestamp"]/1000).strftime("%H:%M")
            oi = float(item["sumOpenInterest"])
            lines.append(f"  {ts}: {oi/1e4:.1f}万")
        
        return "\n".join(lines)
        
    except Exception as e:
        logger.error(f"Position analysis error: {e}")
        return f"❌ 持仓分析错误: {e}"


async def generate_longshort_analysis(symbol: str) -> str:
    """生成多空分析文本"""
    try:
        from unitrade.tracker import MarketReporter
        
        reporter = MarketReporter()
        await reporter.start()
        
        report = await reporter.generate_report(symbol)
        history = await reporter.get_ratio_history(symbol, 5)
        
        await reporter.stop()
        
        if not report:
            return "❌ 获取多空数据失败"
        
        return reporter.format_telegram_report(report, history)
        
    except Exception as e:
        logger.error(f"Long/short analysis error: {e}")
        return f"❌ 多空分析错误: {e}"


async def generate_hot_coins() -> str:
    """Generate hot coins text (volume spikes)."""
    try:
        top_15m, top_1h = await data_service.get_hot_coins()

        lines = [
            "<b>Hot Coins - Volume Spikes</b>",
            f"{datetime.now().strftime('%m-%d %H:%M')}",
            "",
            "<b>15m RVOL Top</b>",
        ]

        if not top_15m:
            lines.append("  (no data)")
        else:
            for item in top_15m:
                symbol = item["symbol"].replace("USDT", "")
                rvol = item.get("rvol", 0.0)
                qv = format_flow(item.get("quote_volume", 0.0))
                lines.append(f"  {symbol}: {rvol:.2f}x qv={qv}")

        lines.append("")
        lines.append("<b>1h RVOL Top</b>")

        if not top_1h:
            lines.append("  (no data)")
        else:
            for item in top_1h:
                symbol = item["symbol"].replace("USDT", "")
                rvol = item.get("rvol", 0.0)
                qv = format_flow(item.get("quote_volume", 0.0))
                lines.append(f"  {symbol}: {rvol:.2f}x qv={qv}")

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"Hot coins error: {e}")
        return f"Hot coins error: {e}"

async def generate_rising_index() -> str:
    """生成上涨潜力排行文本"""
    try:
        from unitrade.scanner.signal_detector import get_rising_ranking
        from unitrade.core.time import format_ts, resolve_tz
        
        scores = await get_rising_ranking(top_n=10)
        
        if not scores:
            return "<b>🏆 上涨潜力排行</b>\n\n暂无数据 (需要先运行异动监测器收集信号)"
        
        tz = resolve_tz()
        lines = [
            "<b>🏆 上涨潜力排行 (5日评估)</b>",
            f"⏰ {datetime.now(tz=tz).strftime('%m-%d %H:%M %Z')}",
            "━" * 20,
            "",
        ]
        
        for i, score in enumerate(scores, 1):
            base = score.symbol.replace("USDT", "")
            trend = "↗" if score.ema_alignment == "bullish" else "↘" if score.ema_alignment == "bearish" else "→"
            oi_sign = "+" if score.cumulative_oi_change > 0 else ""
            since = f"{score.price_change_since_rank:+.1%}" if getattr(score, "price_change_since_rank", None) is not None else "n/a"
            first = format_ts(score.first_ranked_ts, "%m-%d %H:%M") if getattr(score, "first_ranked_ts", None) else "-"
            
            lines.append(
                f"{i}. <b>{base}</b> ⚡{score.total_score:.1f}分  ({since})\n"
                f"   价{score.price_structure_score:.1f} 资{score.oi_flow_score:.1f} 新{score.recency_score:.1f} 量{score.volume_score:.1f}"
                f" | 首上榜{first} | 趋势{trend} | 信号{score.signal_count}次"
            )
        
        return "\n".join(lines)
        
    except Exception as e:
        logger.error(f"Rising index error: {e}")
        return f"❌ 上涨排行错误: {e}"


async def generate_anomaly_status() -> str:
    """生成异动监测状态文本"""
    try:
        from unitrade.scanner.signal_detector import RedisStateManager
        
        state_manager = RedisStateManager()
        await state_manager.connect()
        
        # 获取最近的信号 keys
        keys = await state_manager.scan_signal_keys(prefix="anomaly")
        
        lines = [
            "<b>🚀 异动监测状态</b>",
            f"⏰ {datetime.now().strftime('%m-%d %H:%M')}",
            "━" * 20,
            "",
            f"📊 监测币种: {len(keys)} 个有信号",
            "",
            "<b>触发条件:</b>",
            "• EMA200 突破",
            "• OI 增加 ≥ 3%",
            "• 成交量 ≥ 3x 平均",
            "",
        ]
        
        if keys:
            lines.append("<b>最近信号币种:</b>")
            for key in keys[:10]:
                symbol = key.split(":")[-1]
                base = symbol.replace("USDT", "")
                signals = await state_manager.get_breakout_signals(symbol, prefix="anomaly")
                count = len(signals) if signals else 0
                lines.append(f"  • {base}: {count} 次信号")
        else:
            lines.append("暂无信号 (监测器可能未运行或无突破发生)")
        
        await state_manager.close()
        return "\n".join(lines)
        
    except Exception as e:
        logger.error(f"Anomaly status error: {e}")
        return f"❌ 异动监测错误: {e}"


# ===== 主程序 =====

async def main():
    """启动 Bot"""
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if not BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    
    # 创建 Bot 和 Dispatcher
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    
    # 注册路由器
    dp.include_router(router)
    
    # 启动数据服务
    await data_service.start()
    
    print("=" * 50)
    print("🤖 UniTrade Bot 已启动 (aiogram)")
    print("=" * 50)
    print(f"支持命令: /{', /'.join(SUPPORTED_SYMBOLS[:8])}...")
    print("按 Ctrl+C 停止")
    print("=" * 50)
    
    try:
        # 启动轮询
        await dp.start_polling(bot)
    finally:
        await data_service.stop()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
