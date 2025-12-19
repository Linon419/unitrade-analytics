"""
Prometheus 指标模块

提供系统监控指标:
- 连接状态
- 消息处理速率
- 延迟统计
- OI 飙升计数
"""

import asyncio
import logging
import os
import time
from prometheus_client import Counter, Gauge, Histogram, Info, generate_latest, CONTENT_TYPE_LATEST
from aiohttp import web

logger = logging.getLogger(__name__)


# ============ 连接指标 ============
CONNECTION_STATUS = Gauge(
    'unitrade_connection_status',
    'Exchange connection status (1=connected, 0=disconnected)',
    ['exchange']
)

RECONNECT_COUNT = Counter(
    'unitrade_reconnect_total',
    'Total number of reconnection attempts',
    ['exchange']
)

# ============ 消息指标 ============
MESSAGES_RECEIVED = Counter(
    'unitrade_messages_received_total',
    'Total messages received from exchanges',
    ['exchange', 'message_type']
)

MESSAGE_PROCESSING_TIME = Histogram(
    'unitrade_message_processing_seconds',
    'Time spent processing messages',
    ['exchange', 'message_type'],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
)

# ============ 分析指标 ============
OBI_VALUE = Gauge(
    'unitrade_obi',
    'Current Order Book Imbalance',
    ['exchange', 'symbol']
)

CVD_VALUE = Gauge(
    'unitrade_cvd',
    'Cumulative Volume Delta',
    ['exchange', 'symbol']
)

VOLATILITY_VALUE = Gauge(
    'unitrade_volatility',
    'Realized volatility (annualized %)',
    ['exchange', 'symbol']
)

OPEN_INTEREST = Gauge(
    'unitrade_open_interest',
    'Current Open Interest',
    ['exchange', 'symbol']
)

MARKET_REGIME = Gauge(
    'unitrade_market_regime',
    'Market regime (1=long_build, 2=short_build, 3=long_unwind, 4=short_cover, 0=neutral)',
    ['exchange', 'symbol']
)

# ============ Scanner 指标 ============
OI_SPIKES_DETECTED = Counter(
    'unitrade_oi_spikes_total',
    'Total OI spikes detected',
    ['symbol']
)

SCAN_DURATION = Histogram(
    'unitrade_scan_duration_seconds',
    'Time spent on each scan cycle',
    buckets=(1, 5, 10, 30, 60, 120)
)

SYMBOLS_SCANNED = Gauge(
    'unitrade_symbols_scanned',
    'Number of symbols scanned in last cycle'
)

# ============ 系统信息 ============
SYSTEM_INFO = Info(
    'unitrade',
    'UniTrade Analytics Gateway information'
)

# 设置系统信息
SYSTEM_INFO.info({
    'version': '1.0.0',
    'python_version': '3.10+',
})

# ============ Telegram Bot 指标 ============
TELEGRAM_MESSAGES_SENT = Counter(
    'unitrade_telegram_messages_sent_total',
    'Total Telegram messages sent',
    ['result']
)


def update_connection_status(exchange: str, connected: bool) -> None:
    """更新连接状态"""
    CONNECTION_STATUS.labels(exchange=exchange).set(1 if connected else 0)


def record_reconnect(exchange: str) -> None:
    """记录重连"""
    RECONNECT_COUNT.labels(exchange=exchange).inc()


def record_message(exchange: str, message_type: str) -> None:
    """记录消息"""
    MESSAGES_RECEIVED.labels(exchange=exchange, message_type=message_type).inc()


class MessageTimer:
    """消息处理计时器"""
    
    def __init__(self, exchange: str, message_type: str):
        self.exchange = exchange
        self.message_type = message_type
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, *args):
        duration = time.perf_counter() - self.start_time
        MESSAGE_PROCESSING_TIME.labels(
            exchange=self.exchange,
            message_type=self.message_type
        ).observe(duration)


def update_analytics(
    exchange: str,
    symbol: str,
    obi: float = None,
    cvd: float = None,
    volatility: float = None,
    open_interest: float = None,
    regime: str = None
) -> None:
    """更新分析指标"""
    if obi is not None:
        OBI_VALUE.labels(exchange=exchange, symbol=symbol).set(obi)
    
    if cvd is not None:
        CVD_VALUE.labels(exchange=exchange, symbol=symbol).set(cvd)
    
    if volatility is not None:
        VOLATILITY_VALUE.labels(exchange=exchange, symbol=symbol).set(volatility)
    
    if open_interest is not None:
        OPEN_INTEREST.labels(exchange=exchange, symbol=symbol).set(open_interest)
    
    if regime is not None:
        regime_map = {
            'long_build': 1,
            'short_build': 2,
            'long_unwind': 3,
            'short_cover': 4,
            'neutral': 0
        }
        MARKET_REGIME.labels(exchange=exchange, symbol=symbol).set(
            regime_map.get(regime, 0)
        )


def record_oi_spike(symbol: str) -> None:
    """记录 OI 飙升"""
    OI_SPIKES_DETECTED.labels(symbol=symbol).inc()


# ============ HTTP 端点 ============

async def metrics_handler(request: web.Request) -> web.Response:
    """Prometheus metrics endpoint"""
    return web.Response(
        body=generate_latest(),
        headers={"Content-Type": CONTENT_TYPE_LATEST},
    )


async def health_handler(request: web.Request) -> web.Response:
    """Health check endpoint"""
    return web.Response(text="OK", status=200)


def create_metrics_app() -> web.Application:
    """创建 metrics HTTP 应用"""
    app = web.Application()
    app.router.add_get('/metrics', metrics_handler)
    app.router.add_get('/health', health_handler)
    app.router.add_get('/healthz', health_handler)
    return app


async def start_metrics_server(host: str = "0.0.0.0", port: int = 8000) -> web.AppRunner:
    """启动 metrics 服务器"""
    app = create_metrics_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info(f"Metrics server running at http://{host}:{port}/metrics")
    return runner


async def main() -> None:
    """单独启动 Prometheus metrics 服务（用于长期运行/监控）"""
    host = os.getenv("UNITRADE_METRICS_HOST", os.getenv("METRICS_HOST", "0.0.0.0"))
    port = int(os.getenv("UNITRADE_METRICS_PORT", os.getenv("METRICS_PORT", "8000")))

    runner = await start_metrics_server(host=host, port=port)

    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
