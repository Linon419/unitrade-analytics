"""
UniTrade Analytics 主入口

当前项目默认以 SQLite + Web Dashboard 为核心运行形态（无需 TimescaleDB）。
"""

import asyncio
import logging
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """
    默认启动 Dashboard。

    端口优先级:
    1) $PORT
    2) $DASHBOARD_PORT
    3) 8080
    """
    from .web import WebDashboard, DashboardConfig

    host = os.getenv("DASHBOARD_HOST", "0.0.0.0")
    port = int(os.getenv("PORT", os.getenv("DASHBOARD_PORT", "8080")))

    dashboard = WebDashboard(DashboardConfig(host=host, port=port))
    await dashboard.start()

    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        await dashboard.stop()


if __name__ == "__main__":
    asyncio.run(main())
