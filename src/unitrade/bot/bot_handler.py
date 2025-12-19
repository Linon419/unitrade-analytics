"""
Telegram Bot 命令处理器

流程:
1. 用户发送 /btc /eth 等命令
2. Bot 根据币种显示菜单
3. 用户点击按钮
4. Bot 响应回调，执行相应功能

使用方法:
    python -m unitrade.bot.bot_handler
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from typing import Dict, Optional

import aiohttp

from unitrade.bot.telegram_keyboard import UniTradeBotHandler, UniTradeMenus

logger = logging.getLogger(__name__)


class TelegramBotServer:
    """
    Telegram Bot 服务器
    
    使用 Long Polling 模式接收用户消息和回调
    """
    
    API_BASE = "https://api.telegram.org/bot"
    
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.handler = UniTradeBotHandler(bot_token, chat_id)
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._offset = 0
        
        # 支持的币种命令
        self.supported_symbols = [
            "btc", "eth", "sol", "bnb", "xrp", "doge", "ada", "avax",
            "dot", "link", "matic", "atom", "uni", "ltc", "etc",
            "arb", "op", "apt", "sui", "sei", "inj", "jup",
        ]
    
    @property
    def api_url(self) -> str:
        return f"{self.API_BASE}{self.bot_token}"
    
    async def start(self):
        """启动 Bot 服务器"""
        self._session = aiohttp.ClientSession()
        await self.handler.start()
        self._running = True
        logger.info("Telegram Bot Server started")
        logger.info(f"Supported commands: /{', /'.join(self.supported_symbols[:10])}...")
    
    async def stop(self):
        """停止 Bot 服务器"""
        self._running = False
        await self.handler.stop()
        if self._session:
            await self._session.close()
        logger.info("Telegram Bot Server stopped")
    
    async def get_updates(self, timeout: int = 30) -> list:
        """获取更新 (Long Polling)"""
        url = f"{self.api_url}/getUpdates"
        params = {
            "offset": self._offset,
            "timeout": timeout,
            "allowed_updates": ["message", "callback_query"],
        }
        
        try:
            async with self._session.get(url, params=params, timeout=timeout + 10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("ok"):
                        return data.get("result", [])
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.error(f"Get updates error: {e}")
        
        return []
    
    async def answer_callback(self, callback_id: str, text: str = ""):
        """回复回调查询"""
        url = f"{self.api_url}/answerCallbackQuery"
        data = {"callback_query_id": callback_id}
        if text:
            data["text"] = text
        
        try:
            async with self._session.post(url, json=data) as resp:
                pass
        except Exception as e:
            logger.error(f"Answer callback error: {e}")
    
    async def process_message(self, message: Dict):
        """处理用户消息"""
        text = message.get("text", "").strip().lower()
        chat_id = message.get("chat", {}).get("id")
        
        # 检查是否是命令
        if text.startswith("/"):
            command = text[1:].split("@")[0]  # 去掉 @botname
            
            # 检查是否是币种命令
            if command in self.supported_symbols:
                symbol = f"{command.upper()}USDT"
                logger.info(f"Command received: /{command} -> {symbol}")
                # 默认显示合约分析
                await self.handler.send_futures_analysis(symbol)
            
            # /start 命令
            elif command == "start":
                await self.send_welcome()
            
            # /help 命令
            elif command == "help":
                await self.send_help()
    
    async def process_callback(self, callback: Dict):
        """处理回调查询 (按钮点击)"""
        callback_id = callback.get("id")
        data = callback.get("data", "")
        
        logger.info(f"Callback received: {data}")
        
        # 解析回调数据
        parts = data.split(":")
        action = parts[0]
        param = parts[1] if len(parts) > 1 else ""
        
        # 响应回调
        await self.answer_callback(callback_id, f"正在加载 {action}...")
        
        # 处理不同的操作
        if action == "spot":
            symbol = param or "BTCUSDT"
            await self.handler.send_spot_analysis(symbol)
        
        elif action == "futures":
            symbol = param or "BTCUSDT"
            await self.handler.send_futures_analysis(symbol)
        
        elif action == "compare":
            symbol = param or "BTCUSDT"
            await self.handler.send_compare_analysis(symbol)
        
        elif action == "position":
            symbol = param or "BTCUSDT"
            await self.handler.send_position_analysis(symbol)
        
        elif action == "longshort":
            symbol = param or "BTCUSDT"
            await self.handler.send_longshort_analysis(symbol)
        
        elif action == "hot_coins":
            await self.handler.send_hot_coins()
        
        elif action == "rising_index":
            symbol = param or "BTCUSDT"
            await self.handler.send_rising_index(symbol)

        elif action == "anomaly_status":
            symbol = param or "BTCUSDT"
            await self.handler.send_anomaly_status(symbol)

        elif action == "refresh":
            symbol = param or "BTCUSDT"
            # 刷新时重新加载合约分析
            await self.handler.send_futures_analysis(symbol)
        
        elif action == "menu":
            if param == "main":
                await self.handler.send_main_menu()
        
        elif action == "close":
            # 可以选择删除消息
            pass
        
        elif action == "heatmap":
            await self.answer_callback(callback_id, "热力图功能开发中...")
    
    async def send_welcome(self):
        """发送欢迎消息"""
        text = """
<b>🤖 UniTrade Analytics Bot</b>

欢迎使用！发送币种命令查看分析：

<b>常用命令:</b>
/btc - 比特币分析
/eth - 以太坊分析
/sol - Solana分析
/bnb - BNB分析

<b>支持的币种:</b>
BTC, ETH, SOL, BNB, XRP, DOGE, ADA, AVAX, DOT, LINK, MATIC, ATOM, UNI, LTC, ARB, OP, APT, SUI...

发送 /help 获取更多帮助
"""
        await self.handler.send_menu(text, UniTradeMenus.main_menu("BTCUSDT"))
    
    async def send_help(self):
        """发送帮助消息"""
        text = """
<b>📚 帮助</b>

<b>命令格式:</b>
/币种名 - 例如 /btc /eth /sol

<b>功能说明:</b>
• 现货分析 - 机构现货 vs 散户现货
• 合约分析 - 机构合约 vs 散户合约
• 数据对比 - 合约资金 vs 现货资金
• 持仓分析 - OI 持仓量变化
• 多空分析 - 多空比历史
• 热币列表 - 涨跌幅榜

<b>数据来源:</b>
Binance API
"""
        await self.handler.send_menu(text, UniTradeMenus.main_menu("BTCUSDT"))
    
    async def run(self):
        """运行 Bot (Long Polling 模式)"""
        await self.start()
        
        print("=" * 50)
        print("🤖 UniTrade Bot 已启动")
        print("=" * 50)
        print(f"支持命令: /{', /'.join(self.supported_symbols[:8])}...")
        print("按 Ctrl+C 停止")
        print("=" * 50)
        
        try:
            while self._running:
                updates = await self.get_updates()
                
                for update in updates:
                    # 更新 offset
                    self._offset = update.get("update_id", 0) + 1
                    
                    # 处理消息
                    if "message" in update:
                        await self.process_message(update["message"])
                    
                    # 处理回调
                    if "callback_query" in update:
                        await self.process_callback(update["callback_query"])
                
        except KeyboardInterrupt:
            print("\n正在停止...")
        except Exception as e:
            logger.error(f"Bot error: {e}")
        finally:
            await self.stop()


async def main():
    """启动 Bot"""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID")

    bot = TelegramBotServer(
        bot_token=bot_token,
        chat_id=chat_id,
    )
    
    await bot.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    asyncio.run(main())
