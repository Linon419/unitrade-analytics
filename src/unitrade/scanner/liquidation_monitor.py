"""
清算数据监控器

监控大额清算事件:
- 通过 WebSocket 实时接收清算流
- 计算清算强度指标
- 检测清算级联风险
"""

import asyncio
import json
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Callable, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class LiquidationEvent:
    """清算事件"""
    symbol: str
    side: str  # "BUY" (空头被爆) 或 "SELL" (多头被爆)
    price: Decimal
    quantity: Decimal
    notional: Decimal  # 清算金额
    timestamp: int
    
    @property
    def is_long_liquidation(self) -> bool:
        """是否为多头清算"""
        return self.side == "SELL"
    
    @property
    def is_short_liquidation(self) -> bool:
        """是否为空头清算"""
        return self.side == "BUY"


@dataclass
class LiquidationAlert:
    """清算警报"""
    symbol: str
    alert_type: str  # "large_single", "cascade_risk", "volume_spike"
    total_notional: float
    long_liquidations: float
    short_liquidations: float
    event_count: int
    time_window_seconds: int
    details: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_telegram_message(self) -> str:
        emoji = "💥" if self.alert_type == "large_single" else "🌊"
        direction = "LONGS" if self.long_liquidations > self.short_liquidations else "SHORTS"
        
        return f"""{emoji} LIQUIDATION ALERT {emoji}
Symbol: ${self.symbol.replace('USDT', '')}
Type: {self.alert_type.upper()}
Total: ${self.total_notional:,.0f}
Direction: {direction}
Events: {self.event_count}
Window: {self.time_window_seconds}s
{self.details}
"""


class LiquidationMonitor:
    """
    清算监控器
    
    功能:
    1. 实时接收清算流
    2. 计算清算强度
    3. 检测级联风险
    """
    
    WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"
    
    def __init__(
        self,
        large_threshold: float = 100_000,  # 大额清算阈值 (USDT)
        cascade_threshold: float = 1_000_000,  # 级联阈值 (5分钟累计)
        window_seconds: int = 300,  # 5 分钟窗口
    ):
        self.large_threshold = large_threshold
        self.cascade_threshold = cascade_threshold
        self.window_seconds = window_seconds
        
        # 清算历史
        self._history: Dict[str, deque] = {}
        
        # 回调
        self._on_liquidation: Optional[Callable] = None
        self._on_alert: Optional[Callable] = None
        
        # WebSocket
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """启动监控"""
        self._session = aiohttp.ClientSession()
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info("Liquidation monitor started")
    
    async def stop(self) -> None:
        """停止监控"""
        self._running = False
        if self._task:
            self._task.cancel()
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        logger.info("Liquidation monitor stopped")
    
    def set_liquidation_callback(self, callback: Callable) -> None:
        """设置清算事件回调"""
        self._on_liquidation = callback
    
    def set_alert_callback(self, callback: Callable) -> None:
        """设置警报回调"""
        self._on_alert = callback
    
    async def _run(self) -> None:
        """运行主循环"""
        while self._running:
            try:
                self._ws = await self._session.ws_connect(self.WS_URL)
                logger.info("Connected to liquidation stream")
                
                async for msg in self._ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_message(json.loads(msg.data))
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Liquidation stream error: {e}")
                await asyncio.sleep(5)
    
    async def _handle_message(self, msg: Dict) -> None:
        """处理清算消息"""
        order = msg.get("o", {})
        
        event = LiquidationEvent(
            symbol=order.get("s", ""),
            side=order.get("S", ""),
            price=Decimal(order.get("p", "0")),
            quantity=Decimal(order.get("q", "0")),
            notional=Decimal(order.get("p", "0")) * Decimal(order.get("q", "0")),
            timestamp=order.get("T", 0),
        )
        
        # 记录历史
        if event.symbol not in self._history:
            self._history[event.symbol] = deque(maxlen=1000)
        self._history[event.symbol].append(event)
        
        # 清算回调
        if self._on_liquidation:
            await self._safe_callback(self._on_liquidation, event)
        
        # 检测警报
        alerts = await self._check_alerts(event)
        for alert in alerts:
            if self._on_alert:
                await self._safe_callback(self._on_alert, alert)
    
    async def _check_alerts(self, event: LiquidationEvent) -> List[LiquidationAlert]:
        """检查是否触发警报"""
        alerts = []
        
        # 1. 大额单笔清算
        if float(event.notional) >= self.large_threshold:
            alerts.append(LiquidationAlert(
                symbol=event.symbol,
                alert_type="large_single",
                total_notional=float(event.notional),
                long_liquidations=float(event.notional) if event.is_long_liquidation else 0,
                short_liquidations=float(event.notional) if event.is_short_liquidation else 0,
                event_count=1,
                time_window_seconds=0,
                details=f"Single liquidation: ${float(event.notional):,.0f}",
            ))
        
        # 2. 级联风险检测
        cascade_alert = self._check_cascade(event.symbol)
        if cascade_alert:
            alerts.append(cascade_alert)
        
        return alerts
    
    def _check_cascade(self, symbol: str) -> Optional[LiquidationAlert]:
        """检查级联风险"""
        if symbol not in self._history:
            return None
        
        now = int(datetime.now().timestamp() * 1000)
        window_ms = self.window_seconds * 1000
        
        long_total = Decimal("0")
        short_total = Decimal("0")
        count = 0
        
        for event in self._history[symbol]:
            if now - event.timestamp <= window_ms:
                if event.is_long_liquidation:
                    long_total += event.notional
                else:
                    short_total += event.notional
                count += 1
        
        total = float(long_total + short_total)
        
        if total >= self.cascade_threshold:
            return LiquidationAlert(
                symbol=symbol,
                alert_type="cascade_risk",
                total_notional=total,
                long_liquidations=float(long_total),
                short_liquidations=float(short_total),
                event_count=count,
                time_window_seconds=self.window_seconds,
                details=f"Cascade risk: ${total:,.0f} in {self.window_seconds}s",
            )
        
        return None
    
    async def _safe_callback(self, callback: Callable, data) -> None:
        """安全执行回调"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            logger.error(f"Callback error: {e}")
    
    def get_stats(self, symbol: str, window_seconds: int = 300) -> Dict:
        """获取清算统计"""
        if symbol not in self._history:
            return {"long": 0, "short": 0, "total": 0, "count": 0}
        
        now = int(datetime.now().timestamp() * 1000)
        window_ms = window_seconds * 1000
        
        long_total = Decimal("0")
        short_total = Decimal("0")
        count = 0
        
        for event in self._history[symbol]:
            if now - event.timestamp <= window_ms:
                if event.is_long_liquidation:
                    long_total += event.notional
                else:
                    short_total += event.notional
                count += 1
        
        return {
            "long": float(long_total),
            "short": float(short_total),
            "total": float(long_total + short_total),
            "count": count,
        }


async def main():
    """测试运行 (10秒)"""
    monitor = LiquidationMonitor(
        large_threshold=50_000,  # 降低阈值用于测试
    )
    
    async def on_liq(event: LiquidationEvent):
        direction = "LONG" if event.is_long_liquidation else "SHORT"
        print(f"💥 {event.symbol} {direction} ${float(event.notional):,.0f}")
    
    async def on_alert(alert: LiquidationAlert):
        print(f"\n🚨 ALERT: {alert.alert_type}")
        print(alert.to_telegram_message())
    
    monitor.set_liquidation_callback(on_liq)
    monitor.set_alert_callback(on_alert)
    
    print("=" * 60)
    print("💥 Liquidation Monitor (10 seconds)")
    print("=" * 60)
    
    await monitor.start()
    await asyncio.sleep(10)
    await monitor.stop()
    
    print("\nDone!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
