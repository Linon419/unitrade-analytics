"""
统一市场扫描器

组合所有扫描功能:
- OI 飙升检测
- Funding Rate 异常
- 清算监控
- 信号生成
"""

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional

import aiohttp

from .squeeze_scanner import BinanceScanner, ScannerConfig, OISpikeAlert
from .funding_scanner import FundingRateScanner, FundingRateAlert
from .liquidation_monitor import LiquidationMonitor, LiquidationEvent, LiquidationAlert

logger = logging.getLogger(__name__)


@dataclass
class MarketScannerConfig:
    """统一扫描器配置"""
    # OI 扫描
    oi_enabled: bool = True
    oi_threshold: float = 1.15
    oi_min_volume: float = 5_000_000
    
    # Funding Rate 扫描
    funding_enabled: bool = True
    funding_threshold: float = 0.0005  # 0.05%
    
    # 清算监控
    liquidation_enabled: bool = True
    liquidation_large_threshold: float = 100_000
    liquidation_cascade_threshold: float = 1_000_000
    
    # 通用
    ignore_list: List[str] = None
    scan_interval_minutes: int = 5
    
    # Telegram
    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_topic_id: Optional[int] = None  # message_thread_id for topic channels
    
    def __post_init__(self):
        if self.ignore_list is None:
            self.ignore_list = ["BTCUSDT", "ETHUSDT"]


class MarketScanner:
    """
    统一市场扫描器
    
    同时运行:
    1. OI 飙升检测
    2. Funding Rate 异常检测
    3. 清算实时监控
    """
    
    def __init__(self, config: Optional[MarketScannerConfig] = None):
        self.config = config or MarketScannerConfig()
        
        # 子扫描器
        self._oi_scanner: Optional[BinanceScanner] = None
        self._funding_scanner: Optional[FundingRateScanner] = None
        self._liquidation_monitor: Optional[LiquidationMonitor] = None
        
        # 回调
        self._on_oi_spike: Optional[Callable] = None
        self._on_funding_alert: Optional[Callable] = None
        self._on_liquidation: Optional[Callable] = None
        self._on_liquidation_alert: Optional[Callable] = None
        
        # HTTP Session for Telegram
        self._session: Optional[aiohttp.ClientSession] = None
        
        # 运行状态
        self._running = False
        self._scan_task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """启动所有扫描器"""
        logger.info("Starting Market Scanner...")
        self._session = aiohttp.ClientSession()
        self._running = True
        
        # 初始化 OI 扫描器
        if self.config.oi_enabled:
            oi_config = ScannerConfig(
                min_volume_usdt=self.config.oi_min_volume,
                spike_threshold=self.config.oi_threshold,
                ignore_list=self.config.ignore_list,
            )
            self._oi_scanner = BinanceScanner(oi_config)
            await self._oi_scanner.start()
        
        # 初始化 Funding 扫描器
        if self.config.funding_enabled:
            self._funding_scanner = FundingRateScanner(
                threshold=self.config.funding_threshold,
                ignore_list=self.config.ignore_list,
            )
            await self._funding_scanner.start()
        
        # 初始化清算监控
        if self.config.liquidation_enabled:
            self._liquidation_monitor = LiquidationMonitor(
                large_threshold=self.config.liquidation_large_threshold,
                cascade_threshold=self.config.liquidation_cascade_threshold,
            )
            self._liquidation_monitor.set_liquidation_callback(self._handle_liquidation)
            self._liquidation_monitor.set_alert_callback(self._handle_liquidation_alert)
            await self._liquidation_monitor.start()
        
        logger.info("Market Scanner started")
    
    async def stop(self) -> None:
        """停止所有扫描器"""
        logger.info("Stopping Market Scanner...")
        self._running = False
        
        if self._scan_task:
            self._scan_task.cancel()
        
        if self._oi_scanner:
            await self._oi_scanner.stop()
        if self._funding_scanner:
            await self._funding_scanner.stop()
        if self._liquidation_monitor:
            await self._liquidation_monitor.stop()
        if self._session:
            await self._session.close()
        
        logger.info("Market Scanner stopped")
    
    def set_oi_callback(self, callback: Callable) -> None:
        self._on_oi_spike = callback
    
    def set_funding_callback(self, callback: Callable) -> None:
        self._on_funding_alert = callback
    
    def set_liquidation_callback(self, callback: Callable) -> None:
        self._on_liquidation = callback
    
    def set_liquidation_alert_callback(self, callback: Callable) -> None:
        self._on_liquidation_alert = callback
    
    async def scan_once(self) -> dict:
        """执行单次完整扫描"""
        results = {
            "timestamp": datetime.now().isoformat(),
            "oi_spikes": [],
            "funding_alerts": [],
        }
        
        # OI 扫描
        if self._oi_scanner:
            oi_alerts = await self._oi_scanner.scan()
            results["oi_spikes"] = oi_alerts
            
            for alert in oi_alerts:
                if self._on_oi_spike:
                    await self._safe_callback(self._on_oi_spike, alert)
                await self._send_telegram(alert.to_telegram_message())
        
        # Funding 扫描
        if self._funding_scanner:
            funding_alerts = await self._funding_scanner.scan()
            results["funding_alerts"] = funding_alerts
            
            for alert in funding_alerts[:5]:  # 只取前5个
                if self._on_funding_alert:
                    await self._safe_callback(self._on_funding_alert, alert)
                await self._send_telegram(alert.to_telegram_message())
        
        return results
    
    async def run_continuous(self) -> None:
        """持续扫描模式"""
        while self._running:
            try:
                results = await self.scan_once()
                
                oi_count = len(results["oi_spikes"])
                funding_count = len(results["funding_alerts"])
                
                logger.info(
                    f"Scan complete: {oi_count} OI spikes, "
                    f"{funding_count} funding alerts"
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scan error: {e}")
            
            await asyncio.sleep(self.config.scan_interval_minutes * 60)
    
    async def _handle_liquidation(self, event: LiquidationEvent) -> None:
        """处理清算事件"""
        if self._on_liquidation:
            await self._safe_callback(self._on_liquidation, event)
    
    async def _handle_liquidation_alert(self, alert: LiquidationAlert) -> None:
        """处理清算警报"""
        if self._on_liquidation_alert:
            await self._safe_callback(self._on_liquidation_alert, alert)
        await self._send_telegram(alert.to_telegram_message())
    
    async def _send_telegram(self, message: str) -> None:
        """发送 Telegram 消息"""
        if not self.config.telegram_enabled:
            return
        if not self.config.telegram_bot_token or not self.config.telegram_chat_id:
            return
        
        try:
            url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
            payload = {
                "chat_id": self.config.telegram_chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }
            if self.config.telegram_topic_id:
                payload["message_thread_id"] = self.config.telegram_topic_id
            async with self._session.post(url, json=payload) as resp:
                if resp.status != 200:
                    logger.error(f"Telegram error: {await resp.text()}")
        except Exception as e:
            logger.error(f"Telegram send error: {e}")
    
    async def _safe_callback(self, callback: Callable, data) -> None:
        """安全执行回调"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            logger.error(f"Callback error: {e}")


async def run_market_scanner(
    config: Optional[MarketScannerConfig] = None,
    continuous: bool = True,
) -> None:
    """运行市场扫描器"""
    scanner = MarketScanner(config)
    
    # 设置回调
    async def on_oi(alert: OISpikeAlert):
        print(f"🚨 OI SPIKE: {alert.symbol} +{alert.oi_spike_pct:.1f}%")
    
    async def on_funding(alert: FundingRateAlert):
        print(f"💰 FUNDING: {alert.symbol} {alert.funding_rate_pct:+.4f}%")
    
    async def on_liq(event: LiquidationEvent):
        d = "LONG" if event.is_long_liquidation else "SHORT"
        print(f"💥 LIQ: {event.symbol} {d} ${float(event.notional):,.0f}")
    
    scanner.set_oi_callback(on_oi)
    scanner.set_funding_callback(on_funding)
    scanner.set_liquidation_callback(on_liq)
    
    await scanner.start()
    
    try:
        if continuous:
            await scanner.run_continuous()
        else:
            await scanner.scan_once()
    finally:
        await scanner.stop()


async def main():
    """主函数"""
    config = MarketScannerConfig(
        oi_threshold=1.10,  # 10% OI spike
        funding_threshold=0.0003,  # 0.03%
        liquidation_large_threshold=50_000,
        ignore_list=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
        scan_interval_minutes=5,
        telegram_enabled=bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
    )
    
    print("=" * 60)
    print("🔍 UniTrade Market Scanner")
    print("=" * 60)
    print(f"OI Threshold: {(config.oi_threshold - 1) * 100:.0f}%")
    print(f"Funding Threshold: {config.funding_threshold * 100:.2f}%")
    print(f"Liquidation Threshold: ${config.liquidation_large_threshold:,.0f}")
    print(f"Telegram: {'Enabled' if config.telegram_enabled else 'Disabled'}")
    print("=" * 60)
    print()
    
    # 单次扫描演示
    await run_market_scanner(config, continuous=False)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    asyncio.run(main())
