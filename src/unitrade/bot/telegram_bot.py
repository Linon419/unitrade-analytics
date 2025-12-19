"""
Telegram Bot - 自动推送报告

功能:
1. 定时推送 EMA 雷达报告
2. 推送 OI 异动警报
3. 推送资金流报告
4. 支持命令查询
"""

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import aiohttp

from unitrade.metrics import TELEGRAM_MESSAGES_SENT, start_metrics_server

logger = logging.getLogger(__name__)


@dataclass
class TopicConfig:
    """话题频道配置 (message_thread_id 映射)
    
    获取方法: 右键话题第一条消息 → 复制消息链接 → 链接中的数字
    例如: https://t.me/c/xxxxx/123/456 中 123 为话题 ID
    """
    # WaveTrend 信号
    wavetrend_overbought_oversold: Optional[int] = None  # 波段过滤器 超买/超卖
    divergence_volume: Optional[int] = None              # 放量背离
    divergence_normal: Optional[int] = None              # 普通背离
    
    # 波动预警 (Squeeze)
    squeeze_4h_1h: Optional[int] = None                  # 4h/1h波动预警
    squeeze_daily_weekly: Optional[int] = None           # 日线/周线波动预警
    
    # EMA 雷达
    ema_flowering: Optional[int] = None                  # EMA开花
    ema_entering: Optional[int] = None                   # EMA刚进入强势/弱势
    ema_ranking: Optional[int] = None                    # EMA最强/最弱
    
    # 通用
    general: Optional[int] = None                        # General 频道
    
    def get_topic_id(self, topic_type: str) -> Optional[int]:
        """根据话题类型获取 message_thread_id"""
        return getattr(self, topic_type, None)


@dataclass
class TelegramConfig:
    """Telegram 配置"""
    bot_token: str = ""
    chat_id: str = ""
    
    # 话题配置 (超级群组 Forum Topics)
    topics: Optional[TopicConfig] = None
    
    # 定时任务
    ema_report_interval: int = 3600  # 1小时
    market_report_interval: int = 1800  # 30分钟
    
    # 启用的报告
    ema_enabled: bool = True
    market_enabled: bool = True
    alerts_enabled: bool = True


class TelegramBot:
    """
    Telegram 报告机器人
    
    自动推送:
    - EMA 趋势雷达
    - 综合市场报告
    - OI/Funding 异动警报
    """
    
    API_BASE = "https://api.telegram.org/bot"
    
    def __init__(self, config: TelegramConfig):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._metrics_runner = None
    
    @property
    def api_url(self) -> str:
        return f"{self.API_BASE}{self.config.bot_token}"
    
    async def start(self) -> None:
        """启动机器人"""
        if not self.config.bot_token or not self.config.chat_id:
            logger.error("Telegram bot token or chat_id not configured")
            return

        await self._start_metrics_if_enabled()
        
        self._session = aiohttp.ClientSession()
        self._running = True
        
        # 发送启动消息
        await self.send_message("🤖 UniTrade Bot 已启动")
        
        # 启动定时任务
        if self.config.ema_enabled:
            task = asyncio.create_task(self._ema_report_loop())
            self._tasks.append(task)
        
        if self.config.market_enabled:
            task = asyncio.create_task(self._market_report_loop())
            self._tasks.append(task)
        
        logger.info("Telegram bot started")
    
    async def stop(self) -> None:
        """停止机器人"""
        self._running = False
        
        for task in self._tasks:
            task.cancel()
        
        await self.send_message("🛑 UniTrade Bot 已停止")
        
        if self._session:
            await self._session.close()

        if self._metrics_runner:
            await self._metrics_runner.cleanup()
            self._metrics_runner = None
        
        logger.info("Telegram bot stopped")

    async def _start_metrics_if_enabled(self) -> None:
        enabled = os.getenv("UNITRADE_METRICS_ENABLED", os.getenv("METRICS_ENABLED", "true")).lower()
        if enabled in {"0", "false", "no", "off"}:
            return

        host = os.getenv("UNITRADE_METRICS_HOST", os.getenv("METRICS_HOST", "0.0.0.0"))
        port_str = os.getenv("UNITRADE_METRICS_PORT", os.getenv("METRICS_PORT", "8000")).strip()
        if not port_str:
            return

        try:
            port = int(port_str)
        except ValueError:
            logger.warning(f"Invalid METRICS_PORT='{port_str}', metrics server disabled")
            return

        if port <= 0:
            return

        try:
            self._metrics_runner = await start_metrics_server(host=host, port=port)
        except OSError as e:
            logger.warning(f"Failed to start metrics server on {host}:{port}: {e}")
    
    async def send_message(
        self, 
        text: str, 
        parse_mode: str = "HTML",
        topic_id: Optional[int] = None
    ) -> bool:
        """发送消息
        
        Args:
            text: 消息内容
            parse_mode: 解析模式 (HTML/Markdown)
            topic_id: 话题频道 ID (message_thread_id), None 表示发送到默认频道
        """
        try:
            url = f"{self.api_url}/sendMessage"
            data = {
                "chat_id": self.config.chat_id,
                "text": text,
                "parse_mode": parse_mode,
            }
            
            # 添加话题 ID (如果指定)
            if topic_id is not None:
                data["message_thread_id"] = topic_id
            
            async with self._session.post(url, json=data) as resp:
                if resp.status == 200:
                    TELEGRAM_MESSAGES_SENT.labels(result="ok").inc()
                    return True
                else:
                    error = await resp.text()
                    logger.error(f"Telegram send error: {error}")
                    TELEGRAM_MESSAGES_SENT.labels(result="error").inc()
                    return False
                    
        except Exception as e:
            logger.error(f"Telegram error: {e}")
            TELEGRAM_MESSAGES_SENT.labels(result="exception").inc()
            return False
    
    async def send_to_topic(
        self, 
        topic_type: str, 
        text: str, 
        parse_mode: str = "HTML"
    ) -> bool:
        """发送消息到指定话题频道
        
        Args:
            topic_type: 话题类型 (例如 'ema_flowering', 'divergence_volume')
            text: 消息内容
            parse_mode: 解析模式
            
        Returns:
            发送是否成功
        """
        topic_id = None
        if self.config.topics:
            topic_id = self.config.topics.get_topic_id(topic_type)
        
        if topic_id is None:
            logger.debug(f"Topic '{topic_type}' not configured, sending to default channel")
        
        return await self.send_message(text, parse_mode, topic_id)
    
    async def send_alert(self, alert_type: str, message: str) -> bool:
        """发送警报到通用频道"""
        emoji_map = {
            "oi": "🚨",
            "funding": "💰",
            "liquidation": "💥",
            "ema": "📡",
        }
        emoji = emoji_map.get(alert_type, "⚠️")
        return await self.send_message(f"{emoji} {message}")
    
    async def send_alert_to_topic(
        self, 
        topic_type: str, 
        alert_type: str, 
        message: str
    ) -> bool:
        """发送警报到指定话题频道
        
        Args:
            topic_type: 话题类型
            alert_type: 警报类型 (oi/funding/liquidation/ema)
            message: 消息内容
        """
        emoji_map = {
            "oi": "🚨",
            "funding": "💰",
            "liquidation": "💥",
            "ema": "📡",
            "wavetrend": "📊",
            "divergence": "📈",
            "squeeze": "⏰",
        }
        emoji = emoji_map.get(alert_type, "⚠️")
        return await self.send_to_topic(topic_type, f"{emoji} {message}")
    
    async def _ema_report_loop(self) -> None:
        """定时 EMA 报告 - 自动路由到对应话题频道"""
        from unitrade.scanner import EMARadar, EMARadarConfig
        
        while self._running:
            try:
                radar = EMARadar(EMARadarConfig(auto_top_n=50, top_n_results=10))
                await radar.start()
                
                results = await radar.scan("1h")
                timeframe = "1h"
                
                await radar.stop()
                
                # 1. 发送 EMA 开花信号到 ema_flowering (topic 4)
                flowering_signals = []
                for sig in results.get("new_bullish_flowering", [])[:5]:
                    flowering_signals.append(f"🌸 <b>{sig.symbol}</b> 多头开花 | 连续 {sig.flower_streak} 根 | ${sig.current_price:,.2f}")
                for sig in results.get("new_bearish_flowering", [])[:5]:
                    flowering_signals.append(f"🥀 <b>{sig.symbol}</b> 空头开花 | 连续 {sig.flower_streak} 根 | ${sig.current_price:,.2f}")
                
                if flowering_signals:
                    flowering_text = f"<b>[{timeframe.upper()}] EMA 开花信号 🌸</b>\n\n" + "\n".join(flowering_signals)
                    await self.send_to_topic("ema_flowering", flowering_text)
                    logger.info(f"EMA flowering sent: {len(flowering_signals)} signals")
                
                # 2. 发送刚进入强势/弱势信号到 ema_entering (topic 3)
                entering_signals = []
                for sig in results.get("new_uptrend", [])[:5]:
                    entering_signals.append(f"💪 <b>{sig.symbol}</b> 刚进入强势 | 连续 {sig.streak_bars} 根 | ${sig.current_price:,.2f}")
                for sig in results.get("new_downtrend", [])[:5]:
                    entering_signals.append(f"📉 <b>{sig.symbol}</b> 刚进入弱势 | 连续 {sig.streak_bars} 根 | ${sig.current_price:,.2f}")
                
                if entering_signals:
                    entering_text = f"<b>[{timeframe.upper()}] EMA 进入信号 💪</b>\n\n" + "\n".join(entering_signals)
                    await self.send_to_topic("ema_entering", entering_text)
                    logger.info(f"EMA entering sent: {len(entering_signals)} signals")
                
                # 3. 发送排行榜到 ema_ranking (topic 2)
                ranking_lines = [f"<b>[{timeframe.upper()}] EMA Trend Radar 📡</b>", ""]
                
                ranking_lines.append("🚀 <b>Consecutive Uptrend Top</b>")
                for i, sig in enumerate(results.get("uptrend", [])[:10], 1):
                    ranking_lines.append(f"{i}. {sig.format_telegram()}")
                
                ranking_lines.append("")
                ranking_lines.append("📉 <b>Consecutive Downtrend Top</b>")
                for i, sig in enumerate(results.get("downtrend", [])[:10], 1):
                    ranking_lines.append(f"{i}. {sig.format_telegram()}")
                
                ranking_text = "\n".join(ranking_lines)
                await self.send_to_topic("ema_ranking", ranking_text)
                
                logger.info("EMA report sent to topics")
                
            except Exception as e:
                logger.error(f"EMA report error: {e}")
            
            await asyncio.sleep(self.config.ema_report_interval)
    
    async def _market_report_loop(self) -> None:
        """定时市场报告"""
        from unitrade.tracker import MarketReporter
        
        while self._running:
            try:
                reporter = MarketReporter()
                await reporter.start()
                
                # BTC 报告
                report = await reporter.generate_report("BTCUSDT")
                if report:
                    history = await reporter.get_ratio_history("BTCUSDT", 5)
                    text = reporter.format_telegram_report(report, history)
                    await self.send_message(text)
                
                await reporter.stop()
                
                logger.info("Market report sent")
                
            except Exception as e:
                logger.error(f"Market report error: {e}")
            
            await asyncio.sleep(self.config.market_report_interval)
    
    async def run_forever(self) -> None:
        """持续运行"""
        await self.start()
        
        try:
            while self._running:
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()


async def main():
    """测试运行"""
    import os
    
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    
    if not token or not chat_id:
        print("请设置环境变量:")
        print("  $env:TELEGRAM_BOT_TOKEN='your_token'")
        print("  $env:TELEGRAM_CHAT_ID='your_chat_id'")
        return
    
    config = TelegramConfig(
        bot_token=token,
        chat_id=chat_id,
        ema_report_interval=60,  # 测试用 1 分钟
        market_report_interval=120,
    )
    
    bot = TelegramBot(config)
    
    print("=" * 50)
    print("🤖 Telegram Bot")
    print("=" * 50)
    print("Press Ctrl+C to stop")
    
    await bot.run_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
