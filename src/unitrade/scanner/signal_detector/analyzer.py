"""
SignalAnalyzer - 核心信号分析引擎

功能:
1. 接收 WebSocket 数据流
2. 实时计算指标 (RVOL, NetFlow, Rebound)
3. 判断是否触发信号
4. 防抖过滤后推送 Telegram
"""

import asyncio
import logging
import time
from typing import Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field

import aiohttp

from .calculators import (
    TradeData, 
    KlineData, 
    SignalResult,
    CompositeCalculator
)
from .redis_state import RedisStateManager, SignalConfig
from .websocket import BinanceWSManager, WSConfig

logger = logging.getLogger(__name__)


@dataclass
class AlertConfig:
    """报警配置"""
    # 阈值
    rvol_threshold: float = 2.0         # 量能倍数阈值
    net_flow_threshold: float = 100000  # 净流入阈值 (USDT)
    rebound_threshold: float = 5.0      # 反弹幅度阈值 (%)
    price_change_threshold: float = 3.0 # 涨跌幅阈值 (%)
    
    # 阈值等级 (用于防抖升级)
    rvol_levels: List[float] = field(default_factory=lambda: [2.0, 3.0, 5.0, 10.0])
    flow_levels: List[float] = field(default_factory=lambda: [100000, 500000, 1000000, 5000000])
    
    # 防抖
    debounce_minutes: int = 5
    
    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_topic_id: Optional[int] = None


class SignalAnalyzer:
    """
    实时信号分析器
    
    架构:
    WebSocket → Calculator → Filter → Telegram
         ↓           ↓
       Redis ←→  State
    """
    
    def __init__(
        self, 
        redis_url: str = "redis://localhost:6379",
        alert_config: AlertConfig = None,
        ws_config: WSConfig = None
    ):
        self.alert_config = alert_config or AlertConfig()
        
        # 组件
        self.redis = RedisStateManager(redis_url)
        self.calculator = CompositeCalculator()
        self.ws_manager = BinanceWSManager(ws_config or WSConfig())
        
        # Telegram
        self._tg_session: Optional[aiohttp.ClientSession] = None
        
        # 状态
        self._running = False
        self._processed_count = 0
        self._alert_count = 0
        
        # 低点更新队列
        self._low_update_queue: Set[str] = set()
        
        # 防止同一币种重复创建检查任务
        self._pending_checks: Set[str] = set()
    
    async def start(self):
        """启动分析器"""
        logger.info("Starting SignalAnalyzer...")
        
        # 连接 Redis
        await self.redis.connect()
        
        # 创建 Telegram session
        self._tg_session = aiohttp.ClientSession()
        
        # 注册回调
        self.ws_manager.on_trade(self._on_trade)
        self.ws_manager.on_kline(self._on_kline)
        
        # 启动 WebSocket
        await self.ws_manager.start()
        
        # 启动后台任务
        asyncio.create_task(self._low_price_updater())
        asyncio.create_task(self._stats_reporter())
        
        self._running = True
        logger.info("SignalAnalyzer started")
    
    async def stop(self):
        """停止"""
        self._running = False
        await self.ws_manager.stop()
        await self.redis.close()
        if self._tg_session:
            await self._tg_session.close()
        logger.info("SignalAnalyzer stopped")
    
    def _on_trade(self, trade: TradeData):
        """处理交易数据 (同步回调, 快速处理)"""
        self._processed_count += 1
        
        # 更新计算器
        self.calculator.process_trade(trade)
        
        # 防止重复创建任务 (同一币种短时间内只创建一个检查任务)
        symbol = trade.symbol
        if symbol in self._pending_checks:
            return  # 已有任务在处理
        
        self._pending_checks.add(symbol)
        asyncio.create_task(self._check_signal(symbol))
    
    def _on_kline(self, kline: KlineData):
        """处理 K 线数据"""
        self.calculator.process_kline(kline)
        
        # 更新最低价
        symbol = kline.symbol
        if symbol not in self._low_update_queue:
            self._low_update_queue.add(symbol)
    
    async def _check_signal(self, symbol: str):
        """检测信号并触发报警"""
        try:
            result = self.calculator.calculate_all(symbol)
            if not result:
                return
            
            # 检查是否达到阈值
            if not self._should_alert(result):
                return
            
            # 调试日志
            logger.debug(f"Signal triggered: {symbol} rvol={result.rvol:.2f} flow={result.net_flow:.0f} pct={result.price_change_pct:.2f}")
            
            # 计算阈值等级 (用于防抖升级)
            level = self._get_alert_level(result)
            
            # 尝试获取防抖锁 (Redis 失败时跳过)
            try:
                if not await self.redis.try_acquire_lock(symbol, level):
                    return  # 被防抖
            except Exception as e:
                logger.warning(f"Redis lock failed, skipping: {e}")
            
            # 发送 Telegram
            await self._send_telegram(result)
            self._alert_count += 1
            
            logger.info(f"Alert sent: {symbol} RVOL={result.rvol:.1f} Flow={result.net_flow:.0f}")
            
            # 发送后延迟 (给防抖锁时间写入)
            await asyncio.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Check signal error for {symbol}: {e}")
        finally:
            # 清除 pending 标记 (延迟清除防止快速重复)
            await asyncio.sleep(2)  # 2秒内不再检查同一币种
            self._pending_checks.discard(symbol)
    
    def _should_alert(self, result: SignalResult) -> bool:
        """判断是否应该报警"""
        cfg = self.alert_config
        
        # 任一条件满足
        if result.rvol >= cfg.rvol_threshold:
            return True
        if abs(result.net_flow) >= cfg.net_flow_threshold:
            return True
        if result.rebound_pct >= cfg.rebound_threshold:
            return True
        if abs(result.price_change_pct) >= cfg.price_change_threshold:
            return True
        
        return False
    
    def _get_alert_level(self, result: SignalResult) -> int:
        """计算报警等级 (用于防抖升级)"""
        level = 0
        
        # RVOL 等级
        for i, threshold in enumerate(self.alert_config.rvol_levels):
            if result.rvol >= threshold:
                level = max(level, i + 1)
        
        # Flow 等级
        for i, threshold in enumerate(self.alert_config.flow_levels):
            if abs(result.net_flow) >= threshold:
                level = max(level, i + 1)
        
        return level
    
    async def _send_telegram(self, result: SignalResult):
        """发送 Telegram 消息"""
        cfg = self.alert_config
        if not cfg.telegram_bot_token or not cfg.telegram_chat_id:
            logger.debug("Telegram not configured, skip sending")
            return
        
        url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendMessage"
        
        payload = {
            "chat_id": cfg.telegram_chat_id,
            "text": result.format_message(),
            "parse_mode": "HTML",
        }
        
        if cfg.telegram_topic_id:
            payload["message_thread_id"] = cfg.telegram_topic_id
        
        try:
            async with self._tg_session.post(url, json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"Telegram send failed: {text}")
        except Exception as e:
            logger.error(f"Telegram error: {e}")
    
    async def _low_price_updater(self):
        """后台任务: 更新历史最低价"""
        while self._running:
            try:
                # 批量处理队列
                symbols = list(self._low_update_queue)[:50]
                self._low_update_queue -= set(symbols)
                
                for symbol in symbols:
                    await self._update_low_price(symbol)
                
                await asyncio.sleep(60)  # 每分钟更新
                
            except Exception as e:
                logger.error(f"Low price updater error: {e}")
                await asyncio.sleep(10)
    
    async def _update_low_price(self, symbol: str):
        """更新单个币种的历史最低价"""
        try:
            url = f"https://fapi.binance.com/fapi/v1/klines"
            
            lows = {}
            
            for days in [1, 3, 7, 14, 30]:
                params = {
                    "symbol": symbol,
                    "interval": "1d",
                    "limit": days
                }
                
                async with self._tg_session.get(url, params=params) as resp:
                    if resp.status == 200:
                        klines = await resp.json()
                        if klines:
                            low = min(float(k[3]) for k in klines)  # index 3 = low
                            lows[days] = low
            
            # 更新计算器
            self.calculator.rebound.set_lows(symbol, lows)
            
            # 持久化到 Redis
            await self.redis.set_low_prices_batch(symbol, lows)
            
        except Exception as e:
            logger.debug(f"Update low price error for {symbol}: {e}")
    
    async def _stats_reporter(self):
        """后台任务: 定时报告统计"""
        while self._running:
            await asyncio.sleep(60)
            logger.info(
                f"Stats: processed={self._processed_count}, "
                f"alerts={self._alert_count}, "
                f"symbols={len(self.calculator._prices)}"
            )
            self._processed_count = 0


async def main():
    """测试入口"""
    from pathlib import Path
    import yaml
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    # 加载配置文件
    config_path = Path(__file__).parents[4] / "config" / "default.yaml"
    if not config_path.exists():
        # 尝试相对路径
        config_path = Path("config/default.yaml")
    
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        
        tg_cfg = cfg.get("telegram", {})
        bot_token = tg_cfg.get("bot_token", "")
        chat_id = tg_cfg.get("chat_id", "")
        
        # 读取 signal_detector 配置
        sd_cfg = cfg.get("signal_detector", {})
        topic_id = sd_cfg.get("topic_id", tg_cfg.get("topics", {}).get("signal_anomaly"))
        rvol_threshold = sd_cfg.get("rvol_threshold", 1.5)
        net_flow_threshold = sd_cfg.get("net_flow_threshold", 80000)
        price_change_threshold = sd_cfg.get("price_change_threshold", 2.0)
        rebound_threshold = sd_cfg.get("rebound_threshold", 5.0)
        debounce_minutes = sd_cfg.get("debounce_minutes", 5)
        min_quote_volume = sd_cfg.get("min_quote_volume_24h", 1_000_000)
        max_symbols = sd_cfg.get("max_symbols", 50)
        
        redis_url = cfg.get("database", {}).get("redis_url", "redis://localhost:6379")
    else:
        # Fallback to env
        import os
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        topic_id = 62
        rvol_threshold = 1.5
        net_flow_threshold = 80000
        price_change_threshold = 2.0
        rebound_threshold = 5.0
        debounce_minutes = 5
        min_quote_volume = 1_000_000
        max_symbols = 50
        redis_url = "redis://localhost:6379"
    
    # 配置
    alert_config = AlertConfig(
        telegram_bot_token=bot_token,
        telegram_chat_id=chat_id,
        telegram_topic_id=topic_id,
        rvol_threshold=rvol_threshold,
        net_flow_threshold=net_flow_threshold,
        price_change_threshold=price_change_threshold,
        rebound_threshold=rebound_threshold,
        debounce_minutes=debounce_minutes,
    )
    
    ws_config = WSConfig(
        min_quote_volume_24h=min_quote_volume,
        max_symbols=max_symbols,
    )
    
    # 启动
    analyzer = SignalAnalyzer(
        redis_url=redis_url,
        alert_config=alert_config,
        ws_config=ws_config
    )
    
    await analyzer.start()
    
    print("=" * 50)
    print("🚀 Signal Analyzer Running")
    print(f"📡 Telegram: {chat_id[:10]}... topic={topic_id}")
    print("=" * 50)
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        await analyzer.stop()


if __name__ == "__main__":
    asyncio.run(main())
