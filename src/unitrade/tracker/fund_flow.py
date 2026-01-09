"""
资金流向追踪器

功能:
1. 实时追踪合约逐笔成交
2. 计算 CVD (Cumulative Volume Delta)
3. 存储到 SQLite 供历史查询
4. 生成资金流向报告
"""

import asyncio
import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)

# 数据目录
DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"


@dataclass
class FundFlowConfig:
    """资金流向追踪配置"""
    # 追踪的币种
    symbols: List[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT"])
    
    # 快照间隔 (秒)
    snapshot_interval: int = 60  # 每分钟快照
    
    # 数据库路径
    db_path: str = ""
    
    def __post_init__(self):
        if not self.db_path:
            DATA_DIR.mkdir(exist_ok=True)
            self.db_path = str(DATA_DIR / "fundflow.db")


@dataclass
class FlowSnapshot:
    """资金流快照"""
    symbol: str
    timestamp: datetime
    buy_volume: float
    sell_volume: float
    cvd: float  # 累计净流入
    price: float
    trade_count: int


class FundFlowDB:
    """SQLite 持久化"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """初始化数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 分钟级快照
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS flow_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                buy_volume REAL NOT NULL,
                sell_volume REAL NOT NULL,
                cvd REAL NOT NULL,
                price REAL NOT NULL,
                trade_count INTEGER NOT NULL,
                UNIQUE(symbol, timestamp)
            )
        """)
        
        # 小时级聚合
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hourly_flow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                hour TEXT NOT NULL,
                buy_volume REAL NOT NULL,
                sell_volume REAL NOT NULL,
                net_flow REAL NOT NULL,
                trade_count INTEGER NOT NULL,
                UNIQUE(symbol, hour)
            )
        """)
        
        # 日级聚合
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_flow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                buy_volume REAL NOT NULL,
                sell_volume REAL NOT NULL,
                net_flow REAL NOT NULL,
                trade_count INTEGER NOT NULL,
                UNIQUE(symbol, date)
            )
        """)
        
        # 创建索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_symbol ON flow_snapshot(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_time ON flow_snapshot(timestamp)")
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized: {self.db_path}")
    
    def save_snapshot(self, snapshot: FlowSnapshot):
        """保存快照"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        ts = int(snapshot.timestamp.timestamp())
        
        cursor.execute("""
            INSERT OR REPLACE INTO flow_snapshot 
            (symbol, timestamp, buy_volume, sell_volume, cvd, price, trade_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot.symbol, ts, snapshot.buy_volume, snapshot.sell_volume,
            snapshot.cvd, snapshot.price, snapshot.trade_count
        ))
        
        conn.commit()
        conn.close()
    
    def get_hourly_flow(self, symbol: str, hours: int = 24) -> List[Dict]:
        """获取小时级资金流"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        since = int((datetime.now() - timedelta(hours=hours)).timestamp())
        
        # 注意: flow_snapshot 存的是“当日累计值”，不能用 SUM 聚合；应使用每小时首尾差值。
        cursor.execute("""
            WITH grouped AS (
                SELECT
                    strftime('%Y-%m-%d %H:00', datetime(timestamp, 'unixepoch', 'localtime')) AS hour,
                    MIN(timestamp) AS ts_start,
                    MAX(timestamp) AS ts_end
                FROM flow_snapshot
                WHERE symbol = ? AND timestamp >= ?
                GROUP BY hour
            ),
            starts AS (
                SELECT
                    g.hour AS hour,
                    fs.buy_volume AS buy_start,
                    fs.sell_volume AS sell_start,
                    fs.cvd AS cvd_start,
                    fs.trade_count AS trade_start
                FROM grouped g
                JOIN flow_snapshot fs
                  ON fs.symbol = ? AND fs.timestamp = g.ts_start
            ),
            ends AS (
                SELECT
                    g.hour AS hour,
                    fs.buy_volume AS buy_end,
                    fs.sell_volume AS sell_end,
                    fs.cvd AS cvd_end,
                    fs.trade_count AS trade_end
                FROM grouped g
                JOIN flow_snapshot fs
                  ON fs.symbol = ? AND fs.timestamp = g.ts_end
            )
            SELECT
                g.hour,
                (e.buy_end - s.buy_start) AS buy,
                (e.sell_end - s.sell_start) AS sell,
                (e.cvd_end - s.cvd_start) AS net,
                (e.trade_end - s.trade_start) AS trades
            FROM grouped g
            JOIN starts s ON s.hour = g.hour
            JOIN ends e ON e.hour = g.hour
            ORDER BY g.hour DESC
        """, (symbol, since, symbol, symbol))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                "hour": row[0],
                "buy": row[1],
                "sell": row[2],
                "net_flow": row[3],
                "trade_count": row[4],
            })
        
        conn.close()
        return results
    
    def get_daily_flow(self, symbol: str, days: int = 30) -> List[Dict]:
        """获取日级资金流"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        since = int((datetime.now() - timedelta(days=days)).timestamp())
        
        # 注意: flow_snapshot 存的是“当日累计值”，不能用 SUM 聚合；应使用每日首尾差值。
        cursor.execute("""
            WITH grouped AS (
                SELECT
                    strftime('%m%d', datetime(timestamp, 'unixepoch', 'localtime')) AS day,
                    MIN(timestamp) AS ts_start,
                    MAX(timestamp) AS ts_end
                FROM flow_snapshot
                WHERE symbol = ? AND timestamp >= ?
                GROUP BY day
            ),
            starts AS (
                SELECT
                    g.day AS day,
                    fs.buy_volume AS buy_start,
                    fs.sell_volume AS sell_start,
                    fs.cvd AS cvd_start,
                    fs.trade_count AS trade_start
                FROM grouped g
                JOIN flow_snapshot fs
                  ON fs.symbol = ? AND fs.timestamp = g.ts_start
            ),
            ends AS (
                SELECT
                    g.day AS day,
                    fs.buy_volume AS buy_end,
                    fs.sell_volume AS sell_end,
                    fs.cvd AS cvd_end,
                    fs.trade_count AS trade_end
                FROM grouped g
                JOIN flow_snapshot fs
                  ON fs.symbol = ? AND fs.timestamp = g.ts_end
            )
            SELECT
                g.day,
                (e.buy_end - s.buy_start) AS buy,
                (e.sell_end - s.sell_start) AS sell,
                (e.cvd_end - s.cvd_start) AS net,
                (e.trade_end - s.trade_start) AS trades
            FROM grouped g
            JOIN starts s ON s.day = g.day
            JOIN ends e ON e.day = g.day
            ORDER BY g.day DESC
        """, (symbol, since, symbol, symbol))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                "date": row[0],
                "buy": row[1],
                "sell": row[2],
                "net_flow": row[3],
                "trade_count": row[4],
            })
        
        conn.close()
        return results
    
    def get_latest_snapshot(self, symbol: str) -> Optional[Dict]:
        """获取最新快照"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT timestamp, buy_volume, sell_volume, cvd, price, trade_count
            FROM flow_snapshot
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                "timestamp": datetime.fromtimestamp(row[0]),
                "buy_volume": row[1],
                "sell_volume": row[2],
                "cvd": row[3],
                "price": row[4],
                "trade_count": row[5],
            }
        return None


class FundFlowTracker:
    """
    资金流向追踪器
    
    实时追踪多个币种的逐笔成交，计算 CVD 并存储
    """
    
    WS_URL = "wss://fstream.binance.com/ws"
    
    def __init__(self, config: Optional[FundFlowConfig] = None):
        self.config = config or FundFlowConfig()
        self.db = FundFlowDB(self.config.db_path)
        
        # 每个币种的累计数据
        self._accumulators: Dict[str, Dict] = {}
        
        # WebSocket
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []
    
    async def start(self) -> None:
        """启动追踪"""
        self._session = aiohttp.ClientSession()
        self._running = True
        
        # 初始化累计器
        for symbol in self.config.symbols:
            self._accumulators[symbol] = {
                "buy_volume": 0.0,
                "sell_volume": 0.0,
                "trade_count": 0,
                "last_price": 0.0,
                "day_start": datetime.now().replace(hour=0, minute=0, second=0),
            }
        
        # 启动 WebSocket
        ws_task = asyncio.create_task(self._run_websocket())
        snapshot_task = asyncio.create_task(self._snapshot_loop())
        self._tasks = [ws_task, snapshot_task]
        
        logger.info(f"Fund flow tracker started for {self.config.symbols}")
    
    async def stop(self) -> None:
        """停止追踪"""
        self._running = False
        
        for task in self._tasks:
            task.cancel()
        
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        
        logger.info("Fund flow tracker stopped")
    
    async def _run_websocket(self) -> None:
        """运行 WebSocket 连接"""
        # 构建多币种订阅流
        streams = [f"{s.lower()}@aggTrade" for s in self.config.symbols]
        stream_str = "/".join(streams)
        url = f"{self.WS_URL}/{stream_str}"
        
        while self._running:
            try:
                self._ws = await self._session.ws_connect(url)
                logger.info(f"WebSocket connected: {len(streams)} streams")
                
                async for msg in self._ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        self._handle_trade(data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await asyncio.sleep(5)
    
    def _handle_trade(self, data: Dict) -> None:
        """处理逐笔成交"""
        symbol = data.get("s", "")
        if symbol not in self._accumulators:
            return
        
        # 检查是否跨天：先重置再计入当笔成交，避免“新的一天第一笔被清零”
        now = datetime.now()
        acc = self._accumulators[symbol]
        if now.date() > acc["day_start"].date():
            acc["buy_volume"] = 0.0
            acc["sell_volume"] = 0.0
            acc["trade_count"] = 0
            acc["day_start"] = now.replace(hour=0, minute=0, second=0)

        qty = float(data.get("q", 0))
        price = float(data.get("p", 0))
        is_buyer_maker = data.get("m", False)
        
        # 判断买卖方向
        if is_buyer_maker:
            # Buyer is maker = Sell (taker is selling)
            acc["sell_volume"] += qty * price  # 转换为 USDT
        else:
            # Seller is maker = Buy (taker is buying)
            acc["buy_volume"] += qty * price
        
        acc["trade_count"] += 1
        acc["last_price"] = price
    
    async def _snapshot_loop(self) -> None:
        """定时快照"""
        while self._running:
            try:
                await asyncio.sleep(self.config.snapshot_interval)
                
                for symbol, acc in self._accumulators.items():
                    snapshot = FlowSnapshot(
                        symbol=symbol,
                        timestamp=datetime.now(),
                        buy_volume=acc["buy_volume"],
                        sell_volume=acc["sell_volume"],
                        cvd=acc["buy_volume"] - acc["sell_volume"],
                        price=acc["last_price"],
                        trade_count=acc["trade_count"],
                    )
                    self.db.save_snapshot(snapshot)
                
                logger.debug(f"Saved snapshots for {len(self._accumulators)} symbols")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Snapshot error: {e}")
    
    def get_current_stats(self, symbol: str) -> Optional[Dict]:
        """获取当前累计统计"""
        if symbol not in self._accumulators:
            return None
        
        acc = self._accumulators[symbol]
        cvd = acc["buy_volume"] - acc["sell_volume"]
        
        return {
            "symbol": symbol,
            "buy_volume": acc["buy_volume"],
            "sell_volume": acc["sell_volume"],
            "cvd": cvd,
            "trade_count": acc["trade_count"],
            "price": acc["last_price"],
        }
    
    async def run_forever(self) -> None:
        """持续运行"""
        await self.start()
        
        try:
            while self._running:
                await asyncio.sleep(10)
                
                # 打印状态
                for symbol in self.config.symbols:
                    stats = self.get_current_stats(symbol)
                    if stats:
                        cvd = stats["cvd"]
                        direction = "📈" if cvd > 0 else "📉"
                        print(
                            f"{direction} {symbol}: "
                            f"Buy {stats['buy_volume']/1e6:.2f}M | "
                            f"Sell {stats['sell_volume']/1e6:.2f}M | "
                            f"CVD {cvd/1e6:+.2f}M"
                        )
        except KeyboardInterrupt:
            pass
        finally:
            await self.stop()


def format_flow(value: float) -> str:
    """格式化资金流 (万/亿)"""
    if abs(value) >= 1e8:
        return f"{value/1e8:.2f}亿"
    else:
        return f"{value/1e4:.0f}万"


async def main():
    """测试运行"""
    tracker = FundFlowTracker(FundFlowConfig(
        symbols=["BTCUSDT", "ETHUSDT"],
        snapshot_interval=30,  # 30秒快照
    ))
    
    print("=" * 60)
    print("📊 Fund Flow Tracker")
    print("=" * 60)
    print("Tracking: BTCUSDT, ETHUSDT")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    
    await tracker.run_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
