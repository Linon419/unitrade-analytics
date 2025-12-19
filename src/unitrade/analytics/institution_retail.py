"""
机构 vs 散户分析服务

实现三种区分方法:
1. 订单大小分析 - 大单(>$50k)=机构, 小单=散户
2. Binance 大户 API - topLongShortPositionRatio, topLongShortAccountRatio
3. WebSocket 逐笔成交实时分析 - 按单笔金额分类

数据结构:
- institution_buy: 机构买入量 (USDT)
- institution_sell: 机构卖出量 (USDT)
- retail_buy: 散户买入量 (USDT)
- retail_sell: 散户卖出量 (USDT)
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger(__name__)


# ===== 配置 =====

@dataclass
class InstitutionRetailConfig:
    """机构/散户分析配置"""
    # 机构订单阈值 (USDT)
    institution_threshold: float = 50000  # >$50k = 机构
    
    # 中等订单阈值 (可选分级)
    medium_threshold: float = 10000  # $10k-$50k = 中等
    
    # 追踪的币种
    symbols: List[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT"])
    
    # 数据聚合周期 (秒)
    aggregation_interval: int = 60


# ===== Binance 大户 API =====

class BigTraderAPI:
    """Binance 大户数据 API"""
    
    BASE_URL = "https://fapi.binance.com"
    
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def start(self):
        self._session = aiohttp.ClientSession()
    
    async def stop(self):
        if self._session:
            await self._session.close()
    
    async def get_top_trader_ratios(self, symbol: str, periods: List[str] = None) -> Dict:
        """
        获取大户多空比数据
        
        返回:
        - top_account_ratio: 大户账户多空比
        - top_position_ratio: 大户持仓多空比
        - global_ratio: 全市场账户多空比
        """
        if periods is None:
            periods = ["5m", "15m", "30m", "1h", "4h", "1d"]
        
        result = {}
        
        for period in periods:
            try:
                # 大户账户多空比
                url = f"{self.BASE_URL}/futures/data/topLongShortAccountRatio"
                async with self._session.get(url, params={"symbol": symbol, "period": period, "limit": 1}) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data:
                            result[f"top_account_{period}"] = {
                                "ratio": float(data[0].get("longShortRatio", 0)),
                                "long_pct": float(data[0].get("longAccount", 0)) * 100,
                                "short_pct": float(data[0].get("shortAccount", 0)) * 100,
                            }
                
                # 大户持仓多空比
                url = f"{self.BASE_URL}/futures/data/topLongShortPositionRatio"
                async with self._session.get(url, params={"symbol": symbol, "period": period, "limit": 1}) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data:
                            result[f"top_position_{period}"] = {
                                "ratio": float(data[0].get("longShortRatio", 0)),
                                "long_pct": float(data[0].get("longAccount", 0)) * 100,
                                "short_pct": float(data[0].get("shortAccount", 0)) * 100,
                            }
                
                # 全市场账户多空比
                url = f"{self.BASE_URL}/futures/data/globalLongShortAccountRatio"
                async with self._session.get(url, params={"symbol": symbol, "period": period, "limit": 1}) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data:
                            result[f"global_{period}"] = {
                                "ratio": float(data[0].get("longShortRatio", 0)),
                                "long_pct": float(data[0].get("longAccount", 0)) * 100,
                                "short_pct": float(data[0].get("shortAccount", 0)) * 100,
                            }
                            
            except Exception as e:
                logger.error(f"Error getting ratios for {symbol} {period}: {e}")
        
        return result
    
    async def get_taker_volume(self, symbol: str, period: str = "5m", limit: int = 12) -> List[Dict]:
        """
        获取主动买卖成交量
        
        返回 Taker Buy/Sell Volume 历史
        """
        url = f"{self.BASE_URL}/futures/data/takerlongshortRatio"
        
        try:
            async with self._session.get(url, params={
                "symbol": symbol, 
                "period": period, 
                "limit": limit
            }) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return [
                        {
                            "time": datetime.fromtimestamp(item["timestamp"]/1000).strftime("%H:%M"),
                            "buy_sell_ratio": float(item.get("buySellRatio", 0)),
                            "buy_vol": float(item.get("buyVol", 0)),
                            "sell_vol": float(item.get("sellVol", 0)),
                        }
                        for item in data
                    ]
        except Exception as e:
            logger.error(f"Error getting taker volume: {e}")
        
        return []


# ===== WebSocket 逐笔成交分析 =====

@dataclass
class TradeStats:
    """交易统计"""
    institution_buy: float = 0.0
    institution_sell: float = 0.0
    retail_buy: float = 0.0
    retail_sell: float = 0.0
    institution_count: int = 0
    retail_count: int = 0
    
    @property
    def institution_net(self) -> float:
        return self.institution_buy - self.institution_sell
    
    @property
    def retail_net(self) -> float:
        return self.retail_buy - self.retail_sell
    
    @property
    def total_buy(self) -> float:
        return self.institution_buy + self.retail_buy
    
    @property
    def total_sell(self) -> float:
        return self.institution_sell + self.retail_sell
    
    def reset(self):
        self.institution_buy = 0.0
        self.institution_sell = 0.0
        self.retail_buy = 0.0
        self.retail_sell = 0.0
        self.institution_count = 0
        self.retail_count = 0


class InstitutionRetailTracker:
    """
    机构/散户实时追踪器
    
    通过 WebSocket 监听逐笔成交，按订单大小分类
    """
    
    WS_URL = "wss://fstream.binance.com/ws"
    
    def __init__(self, config: Optional[InstitutionRetailConfig] = None):
        self.config = config or InstitutionRetailConfig()
        
        # 每个币种的统计
        self._stats: Dict[str, TradeStats] = {}
        
        # 历史快照 (分钟级)
        self._history: Dict[str, List[Dict]] = {}
        
        # WebSocket
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []
    
    async def start(self):
        """启动追踪"""
        self._session = aiohttp.ClientSession()
        self._running = True
        
        # 初始化统计
        for symbol in self.config.symbols:
            self._stats[symbol] = TradeStats()
            self._history[symbol] = []
        
        # 启动 WebSocket
        ws_task = asyncio.create_task(self._run_websocket())
        snapshot_task = asyncio.create_task(self._snapshot_loop())
        self._tasks = [ws_task, snapshot_task]
        
        logger.info(f"Institution/Retail tracker started for {self.config.symbols}")
    
    async def stop(self):
        """停止追踪"""
        self._running = False
        
        for task in self._tasks:
            task.cancel()
        
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        
        logger.info("Institution/Retail tracker stopped")
    
    async def _run_websocket(self):
        """运行 WebSocket 连接"""
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
    
    def _handle_trade(self, data: Dict):
        """处理逐笔成交 - 按金额分类机构/散户"""
        symbol = data.get("s", "")
        if symbol not in self._stats:
            return
        
        qty = float(data.get("q", 0))
        price = float(data.get("p", 0))
        is_buyer_maker = data.get("m", False)
        
        # 计算成交金额 (USDT)
        trade_value = qty * price
        
        stats = self._stats[symbol]
        
        # 判断是否为机构订单
        is_institution = trade_value >= self.config.institution_threshold
        
        # 判断买卖方向
        if is_buyer_maker:
            # Buyer is maker = Sell (taker is selling)
            if is_institution:
                stats.institution_sell += trade_value
                stats.institution_count += 1
            else:
                stats.retail_sell += trade_value
                stats.retail_count += 1
        else:
            # Seller is maker = Buy (taker is buying)
            if is_institution:
                stats.institution_buy += trade_value
                stats.institution_count += 1
            else:
                stats.retail_buy += trade_value
                stats.retail_count += 1
    
    async def _snapshot_loop(self):
        """定时快照"""
        while self._running:
            try:
                await asyncio.sleep(self.config.aggregation_interval)
                
                now = datetime.now()
                
                for symbol, stats in self._stats.items():
                    # 保存快照
                    snapshot = {
                        "time": now.strftime("%H:%M"),
                        "timestamp": now.timestamp(),
                        "institution_buy": stats.institution_buy,
                        "institution_sell": stats.institution_sell,
                        "retail_buy": stats.retail_buy,
                        "retail_sell": stats.retail_sell,
                        "institution_net": stats.institution_net,
                        "retail_net": stats.retail_net,
                        "institution_count": stats.institution_count,
                        "retail_count": stats.retail_count,
                    }
                    
                    self._history[symbol].append(snapshot)
                    
                    # 只保留最近 1440 条 (24小时)
                    if len(self._history[symbol]) > 1440:
                        self._history[symbol] = self._history[symbol][-1440:]
                    
                    # 重置统计
                    stats.reset()
                
                logger.debug(f"Saved snapshots for {len(self._stats)} symbols")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Snapshot error: {e}")
    
    def get_current_stats(self, symbol: str) -> Optional[Dict]:
        """获取当前累计统计"""
        if symbol not in self._stats:
            return None
        
        stats = self._stats[symbol]
        
        return {
            "symbol": symbol,
            "institution_buy": stats.institution_buy,
            "institution_sell": stats.institution_sell,
            "institution_net": stats.institution_net,
            "retail_buy": stats.retail_buy,
            "retail_sell": stats.retail_sell,
            "retail_net": stats.retail_net,
            "institution_count": stats.institution_count,
            "retail_count": stats.retail_count,
        }
    
    def get_history(self, symbol: str, periods: int = 60) -> List[Dict]:
        """获取历史数据"""
        if symbol not in self._history:
            return []
        
        return self._history[symbol][-periods:]
    
    def get_aggregated(self, symbol: str, periods: List[str] = None) -> Dict[str, Dict]:
        """
        获取聚合数据 (按时间周期)
        
        periods: ["1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "24h", ...]
        """
        if periods is None:
            periods = ["1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "24h"]
        
        if symbol not in self._history:
            return {}
        
        history = self._history[symbol]
        result = {}
        
        period_minutes = {
            "1m": 1, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "4h": 240, "8h": 480, "12h": 720, "24h": 1440,
            "2d": 2880, "3d": 4320, "5d": 7200, "7d": 10080,
        }
        
        for period in periods:
            minutes = period_minutes.get(period, 0)
            if minutes == 0:
                continue
            
            # 取最近 N 分钟的数据
            recent = history[-minutes:] if len(history) >= minutes else history
            
            if not recent:
                result[period] = {
                    "institution_buy": 0, "institution_sell": 0, "institution_net": 0,
                    "retail_buy": 0, "retail_sell": 0, "retail_net": 0,
                }
                continue
            
            # 聚合
            inst_buy = sum(s["institution_buy"] for s in recent)
            inst_sell = sum(s["institution_sell"] for s in recent)
            retail_buy = sum(s["retail_buy"] for s in recent)
            retail_sell = sum(s["retail_sell"] for s in recent)
            
            result[period] = {
                "institution_buy": inst_buy,
                "institution_sell": inst_sell,
                "institution_net": inst_buy - inst_sell,
                "retail_buy": retail_buy,
                "retail_sell": retail_sell,
                "retail_net": retail_buy - retail_sell,
            }
        
        return result


# ===== 综合分析服务 =====

class InstitutionRetailAnalyzer:
    """
    机构/散户综合分析服务
    
    整合三种数据源:
    1. BigTraderAPI - Binance 大户比例
    2. InstitutionRetailTracker - WebSocket 实时追踪
    3. 订单大小分析 - 基于阈值分类
    """
    
    def __init__(self, config: Optional[InstitutionRetailConfig] = None):
        self.config = config or InstitutionRetailConfig()
        self.big_trader_api = BigTraderAPI()
        self.realtime_tracker: Optional[InstitutionRetailTracker] = None
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def start(self):
        """启动服务"""
        self._session = aiohttp.ClientSession()
        await self.big_trader_api.start()
        
        # 可选: 启动实时追踪
        # self.realtime_tracker = InstitutionRetailTracker(self.config)
        # await self.realtime_tracker.start()
        
        logger.info("InstitutionRetailAnalyzer started")
    
    async def stop(self):
        """停止服务"""
        await self.big_trader_api.stop()
        if self.realtime_tracker:
            await self.realtime_tracker.stop()
        if self._session:
            await self._session.close()
    
    async def get_full_analysis(self, symbol: str) -> Dict:
        """
        获取完整分析
        
        包含:
        - big_trader: Binance 大户数据
        - taker_volume: 主动买卖量
        - realtime: 实时追踪数据 (如已启动)
        """
        result = {
            "symbol": symbol,
            "timestamp": datetime.now().isoformat(),
        }
        
        # 1. 大户多空比
        big_trader = await self.big_trader_api.get_top_trader_ratios(symbol)
        result["big_trader"] = big_trader
        
        # 2. 主动买卖量
        taker_vol = await self.big_trader_api.get_taker_volume(symbol)
        result["taker_volume"] = taker_vol
        
        # 3. 实时追踪 (如已启动)
        if self.realtime_tracker:
            result["realtime"] = self.realtime_tracker.get_current_stats(symbol)
            result["realtime_history"] = self.realtime_tracker.get_aggregated(symbol)
        
        return result
    
    def format_telegram_report(self, analysis: Dict) -> str:
        """格式化 Telegram 报告"""
        symbol = analysis.get("symbol", "UNKNOWN")
        base = symbol.replace("USDT", "")
        
        lines = [
            f"<b>🏛️ {base} 机构 vs 散户分析</b>",
            f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "━" * 20,
            "",
            "<b>📊 大户多空比:</b>",
        ]
        
        big_trader = analysis.get("big_trader", {})
        
        for period in ["5m", "15m", "1h", "4h"]:
            top_pos = big_trader.get(f"top_position_{period}", {})
            if top_pos:
                ratio = top_pos.get("ratio", 0)
                long_pct = top_pos.get("long_pct", 0)
                lines.append(f"  {period}: 多空比 {ratio:.2f} (多{long_pct:.1f}%)")
        
        lines.append("")
        lines.append("<b>📈 主动买卖量:</b>")
        
        taker_vol = analysis.get("taker_volume", [])
        for item in taker_vol[:5]:
            ratio = item.get("buy_sell_ratio", 0)
            lines.append(f"  {item['time']}: 买卖比 {ratio:.2f}")
        
        return "\n".join(lines)


# ===== 测试 =====

async def test():
    """测试"""
    print("=" * 50)
    print("🏛️ Institution vs Retail Analyzer")
    print("=" * 50)
    
    analyzer = InstitutionRetailAnalyzer()
    await analyzer.start()
    
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        print(f"\n{symbol}:")
        analysis = await analyzer.get_full_analysis(symbol)
        report = analyzer.format_telegram_report(analysis)
        print(report)
    
    await analyzer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test())
