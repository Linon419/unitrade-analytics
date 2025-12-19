"""
Telegram Router - 信号类型到话题频道的路由服务

负责将不同类型的信号路由到对应的 Telegram 话题频道。
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from .telegram_bot import TelegramBot, TelegramConfig, TopicConfig

logger = logging.getLogger(__name__)


class SignalType(Enum):
    """信号类型枚举"""
    # WaveTrend 信号
    WAVETREND_OVERBOUGHT = "wavetrend_overbought"
    WAVETREND_OVERSOLD = "wavetrend_oversold"
    
    # 背离信号
    DIVERGENCE_BULLISH = "divergence_bullish"
    DIVERGENCE_BEARISH = "divergence_bearish"
    DIVERGENCE_BULLISH_VOLUME = "divergence_bullish_volume"  # M+ 放量
    DIVERGENCE_BEARISH_VOLUME = "divergence_bearish_volume"  # M+ 放量
    
    # 波动预警 (Squeeze)
    SQUEEZE_4H = "squeeze_4h"
    SQUEEZE_1H = "squeeze_1h"
    SQUEEZE_DAILY = "squeeze_daily"
    SQUEEZE_WEEKLY = "squeeze_weekly"
    
    # EMA 雷达
    EMA_FLOWERING_BULLISH = "ema_flowering_bullish"
    EMA_FLOWERING_BEARISH = "ema_flowering_bearish"
    EMA_ENTERING_BULLISH = "ema_entering_bullish"
    EMA_ENTERING_BEARISH = "ema_entering_bearish"
    EMA_STRONGEST = "ema_strongest"
    EMA_WEAKEST = "ema_weakest"
    
    # 其他
    GENERAL = "general"
    OI_SPIKE = "oi_spike"
    FUNDING_ALERT = "funding_alert"
    LIQUIDATION = "liquidation"


# 信号类型到话题键的映射
SIGNAL_TO_TOPIC_MAP = {
    # WaveTrend 超买/超卖
    SignalType.WAVETREND_OVERBOUGHT: "wavetrend_overbought_oversold",
    SignalType.WAVETREND_OVERSOLD: "wavetrend_overbought_oversold",
    
    # 背离 - 区分放量和普通
    SignalType.DIVERGENCE_BULLISH_VOLUME: "divergence_volume",
    SignalType.DIVERGENCE_BEARISH_VOLUME: "divergence_volume",
    SignalType.DIVERGENCE_BULLISH: "divergence_normal",
    SignalType.DIVERGENCE_BEARISH: "divergence_normal",
    
    # 波动预警 - 区分短周期和长周期
    SignalType.SQUEEZE_4H: "squeeze_4h_1h",
    SignalType.SQUEEZE_1H: "squeeze_4h_1h",
    SignalType.SQUEEZE_DAILY: "squeeze_daily_weekly",
    SignalType.SQUEEZE_WEEKLY: "squeeze_daily_weekly",
    
    # EMA 雷达
    SignalType.EMA_FLOWERING_BULLISH: "ema_flowering",
    SignalType.EMA_FLOWERING_BEARISH: "ema_flowering",
    SignalType.EMA_ENTERING_BULLISH: "ema_entering",
    SignalType.EMA_ENTERING_BEARISH: "ema_entering",
    SignalType.EMA_STRONGEST: "ema_ranking",
    SignalType.EMA_WEAKEST: "ema_ranking",
    
    # 其他
    SignalType.GENERAL: "general",
    SignalType.OI_SPIKE: "general",
    SignalType.FUNDING_ALERT: "general",
    SignalType.LIQUIDATION: "general",
}


class TelegramRouter:
    """
    Telegram 消息路由器
    
    根据信号类型自动路由消息到对应的话题频道。
    """
    
    def __init__(self, bot: TelegramBot):
        self.bot = bot
    
    def get_topic_key(self, signal_type: SignalType) -> str:
        """获取信号类型对应的话题键"""
        return SIGNAL_TO_TOPIC_MAP.get(signal_type, "general")
    
    async def send(
        self, 
        signal_type: SignalType, 
        message: str, 
        parse_mode: str = "HTML"
    ) -> bool:
        """
        发送消息到对应话题
        
        Args:
            signal_type: 信号类型
            message: 消息内容
            parse_mode: 解析模式
            
        Returns:
            发送是否成功
        """
        topic_key = self.get_topic_key(signal_type)
        return await self.bot.send_to_topic(topic_key, message, parse_mode)
    
    async def send_wavetrend_signal(
        self, 
        symbol: str, 
        timeframe: str,
        signal_type: str,  # "overbought" or "oversold"
        level: int,
        osc_value: float,
        price: float
    ) -> bool:
        """发送 WaveTrend 超买/超卖信号"""
        if signal_type == "overbought":
            emoji = "🔴"
            direction = "超买"
            sig_type = SignalType.WAVETREND_OVERBOUGHT
        else:
            emoji = "🟢"
            direction = "超卖"
            sig_type = SignalType.WAVETREND_OVERSOLD
        
        message = (
            f"{emoji} <b>{symbol}</b> {timeframe} {direction} L{level}\n"
            f"振荡器: {osc_value:.1f} | 价格: ${price:,.2f}"
        )
        return await self.send(sig_type, message)
    
    async def send_divergence_signal(
        self,
        symbol: str,
        timeframe: str,
        divergence_type: str,  # "bullish" or "bearish"
        level: int,
        is_volume_confirmed: bool,
        price: float
    ) -> bool:
        """发送背离信号"""
        if divergence_type == "bullish":
            emoji = "📈"
            direction = "看涨背离"
            sig_type = (SignalType.DIVERGENCE_BULLISH_VOLUME 
                       if is_volume_confirmed else SignalType.DIVERGENCE_BULLISH)
        else:
            emoji = "📉"
            direction = "看跌背离"
            sig_type = (SignalType.DIVERGENCE_BEARISH_VOLUME 
                       if is_volume_confirmed else SignalType.DIVERGENCE_BEARISH)
        
        volume_tag = " M+" if is_volume_confirmed else ""
        message = (
            f"{emoji} <b>{symbol}</b> {timeframe} {direction} L{level}{volume_tag}\n"
            f"价格: ${price:,.2f}"
        )
        return await self.send(sig_type, message)
    
    async def send_squeeze_signal(
        self,
        symbol: str,
        timeframe: str,
        squeeze_level: int,  # 1 or 2
        duration: int,
        price: float
    ) -> bool:
        """发送波动预警信号"""
        # 根据时间周期选择信号类型
        tf_lower = timeframe.lower()
        if tf_lower in ["4h", "1h"]:
            sig_type = SignalType.SQUEEZE_4H if "4" in tf_lower else SignalType.SQUEEZE_1H
        elif tf_lower in ["1d", "d", "daily"]:
            sig_type = SignalType.SQUEEZE_DAILY
        else:
            sig_type = SignalType.SQUEEZE_WEEKLY
        
        emoji = "⏰" if squeeze_level == 1 else "🔔"
        message = (
            f"{emoji} <b>{symbol}</b> {timeframe} 波动预警 L{squeeze_level}\n"
            f"挤压持续: {duration} 根 | 价格: ${price:,.2f}"
        )
        return await self.send(sig_type, message)
    
    async def send_ema_flowering_signal(
        self,
        symbol: str,
        timeframe: str,
        is_bullish: bool,
        streak: int,
        price: float
    ) -> bool:
        """发送 EMA 开花信号"""
        if is_bullish:
            emoji = "🌸"
            direction = "多头开花"
            sig_type = SignalType.EMA_FLOWERING_BULLISH
        else:
            emoji = "🥀"
            direction = "空头开花"
            sig_type = SignalType.EMA_FLOWERING_BEARISH
        
        message = (
            f"{emoji} <b>{symbol}</b> {timeframe} {direction}\n"
            f"连续: {streak} 根 | 价格: ${price:,.2f}"
        )
        return await self.send(sig_type, message)
    
    async def send_ema_entering_signal(
        self,
        symbol: str,
        timeframe: str,
        is_bullish: bool,
        streak: int,
        price: float
    ) -> bool:
        """发送 EMA 刚进入强势/弱势信号"""
        if is_bullish:
            emoji = "💪"
            direction = "刚进入强势"
            sig_type = SignalType.EMA_ENTERING_BULLISH
        else:
            emoji = "📉"
            direction = "刚进入弱势"
            sig_type = SignalType.EMA_ENTERING_BEARISH
        
        message = (
            f"{emoji} <b>{symbol}</b> {timeframe} {direction}\n"
            f"连续: {streak} 根 | 价格: ${price:,.2f}"
        )
        return await self.send(sig_type, message)
    
    async def send_ema_ranking_signal(
        self,
        rankings: list,  # [(symbol, streak, price), ...]
        timeframe: str,
        is_strongest: bool
    ) -> bool:
        """发送 EMA 排行榜信号"""
        if is_strongest:
            emoji = "💪"
            title = "EMA 最强"
            sig_type = SignalType.EMA_STRONGEST
        else:
            emoji = "📉"
            title = "EMA 最弱"
            sig_type = SignalType.EMA_WEAKEST
        
        lines = [f"{emoji} <b>{title}</b> ({timeframe})"]
        for i, (symbol, streak, price) in enumerate(rankings[:10], 1):
            lines.append(f"{i}. {symbol}: {streak} 根 | ${price:,.2f}")
        
        message = "\n".join(lines)
        return await self.send(sig_type, message)
