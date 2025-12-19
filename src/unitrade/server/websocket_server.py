"""
WebSocket 数据服务器

向客户端实时推送:
- 市场指标 (OBI, CVD, OI)
- 交易信号
- 扫描器警报
"""

import asyncio
import json
import logging
from dataclasses import asdict
from datetime import datetime
from typing import Dict, Optional, Set

from aiohttp import web, WSMsgType

logger = logging.getLogger(__name__)


class DataWebSocketServer:
    """
    WebSocket 数据服务器
    
    向连接的客户端实时推送市场数据和警报
    """
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8765):
        self.host = host
        self.port = port
        
        # 客户端管理
        self._clients: Set[web.WebSocketResponse] = set()
        self._subscriptions: Dict[web.WebSocketResponse, Set[str]] = {}
        
        # 服务器
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
    
    async def start(self) -> None:
        """启动 WebSocket 服务器"""
        self._app = web.Application()
        self._app.router.add_get("/ws", self._websocket_handler)
        self._app.router.add_get("/health", self._health_handler)
        
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()
        
        logger.info(f"WebSocket server started on ws://{self.host}:{self.port}/ws")
    
    async def stop(self) -> None:
        """停止服务器"""
        # 关闭所有客户端连接
        for ws in list(self._clients):
            await ws.close()
        
        if self._runner:
            await self._runner.cleanup()
        
        logger.info("WebSocket server stopped")
    
    async def _websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        """处理 WebSocket 连接"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        # 注册客户端
        self._clients.add(ws)
        self._subscriptions[ws] = {"all"}  # 默认订阅所有
        
        client_ip = request.remote
        logger.info(f"Client connected: {client_ip}")
        
        # 发送欢迎消息
        await ws.send_json({
            "type": "connected",
            "message": "Welcome to UniTrade WebSocket Server",
            "timestamp": datetime.now().isoformat(),
        })
        
        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    await self._handle_message(ws, json.loads(msg.data))
                elif msg.type == WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {ws.exception()}")
        finally:
            self._clients.discard(ws)
            self._subscriptions.pop(ws, None)
            logger.info(f"Client disconnected: {client_ip}")
        
        return ws
    
    async def _handle_message(self, ws: web.WebSocketResponse, data: Dict) -> None:
        """处理客户端消息"""
        msg_type = data.get("type")
        
        if msg_type == "subscribe":
            channels = data.get("channels", [])
            self._subscriptions[ws] = set(channels)
            await ws.send_json({
                "type": "subscribed",
                "channels": channels,
            })
        
        elif msg_type == "unsubscribe":
            channels = data.get("channels", [])
            current = self._subscriptions.get(ws, set())
            self._subscriptions[ws] = current - set(channels)
            await ws.send_json({
                "type": "unsubscribed",
                "channels": channels,
            })
        
        elif msg_type == "ping":
            await ws.send_json({"type": "pong", "timestamp": datetime.now().isoformat()})
    
    async def _health_handler(self, request: web.Request) -> web.Response:
        """健康检查"""
        return web.json_response({
            "status": "ok",
            "clients": len(self._clients),
            "timestamp": datetime.now().isoformat(),
        })
    
    async def broadcast(self, channel: str, data: Dict) -> None:
        """
        向所有订阅该频道的客户端广播消息
        
        频道:
        - metrics: 市场指标
        - signals: 交易信号
        - alerts: 扫描器警报
        - liquidations: 清算事件
        """
        message = {
            "type": "data",
            "channel": channel,
            "data": data,
            "timestamp": datetime.now().isoformat(),
        }
        
        dead_clients = []
        
        for ws in self._clients:
            subs = self._subscriptions.get(ws, set())
            if "all" in subs or channel in subs:
                try:
                    await ws.send_json(message)
                except Exception as e:
                    logger.error(f"Broadcast error: {e}")
                    dead_clients.append(ws)
        
        # 清理断开的连接
        for ws in dead_clients:
            self._clients.discard(ws)
            self._subscriptions.pop(ws, None)
    
    async def send_metrics(
        self,
        symbol: str,
        exchange: str,
        obi: float,
        cvd: float,
        volatility: Optional[float] = None,
        open_interest: Optional[float] = None,
        regime: Optional[str] = None,
    ) -> None:
        """发送市场指标"""
        await self.broadcast("metrics", {
            "symbol": symbol,
            "exchange": exchange,
            "obi": obi,
            "cvd": cvd,
            "volatility": volatility,
            "open_interest": open_interest,
            "regime": regime,
        })
    
    async def send_signal(self, signal) -> None:
        """发送交易信号"""
        data = signal.to_dict() if hasattr(signal, 'to_dict') else signal
        await self.broadcast("signals", data)
    
    async def send_alert(self, alert) -> None:
        """发送警报"""
        data = alert.to_dict() if hasattr(alert, 'to_dict') else alert
        await self.broadcast("alerts", data)
    
    async def send_liquidation(self, event) -> None:
        """发送清算事件"""
        await self.broadcast("liquidations", {
            "symbol": event.symbol,
            "side": event.side,
            "price": str(event.price),
            "quantity": str(event.quantity),
            "notional": str(event.notional),
            "is_long": event.is_long_liquidation,
        })
    
    @property
    def client_count(self) -> int:
        return len(self._clients)


async def main():
    """测试运行"""
    server = DataWebSocketServer(port=8765)
    await server.start()
    
    print("=" * 60)
    print("📡 WebSocket Server Running")
    print("=" * 60)
    print(f"URL: ws://localhost:8765/ws")
    print(f"Health: http://localhost:8765/health")
    print()
    print("Connect with: wscat -c ws://localhost:8765/ws")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    
    # 模拟数据推送
    import random
    try:
        while True:
            await asyncio.sleep(2)
            
            # 模拟指标
            await server.send_metrics(
                symbol="BTCUSDT",
                exchange="binance",
                obi=round(random.uniform(-0.5, 0.5), 3),
                cvd=round(random.uniform(-1000, 1000), 2),
                volatility=round(random.uniform(20, 50), 1),
            )
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent metrics, clients: {server.client_count}")
            
    except KeyboardInterrupt:
        pass
    finally:
        await server.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
