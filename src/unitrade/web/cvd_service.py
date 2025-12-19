import asyncio
import logging
from typing import Dict, List, Optional
import aiohttp
import time

logger = logging.getLogger(__name__)

class CVDAnalysisService:
    """
    CVD 分析服务
    
    对比现货(Spot)与合约(Futures)在不同时间周期下的累计买入/卖出净额 (USDT)。
    使用 K线数据 (TakerBuyQuoteVolume) 进行估算:
    Net = (2 * TakerBuyQuoteVol) - TotalQuoteVol
    """
    
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        # 时间窗口定义 (分钟)
        self.windows = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
            "4h": 240,
            "8h": 480,
            "12h": 720,
            "24h": 1440,
            "2d": 2 * 1440,
            "3d": 3 * 1440,
            "5d": 5 * 1440,
            "7d": 7 * 1440,
            "10d": 10 * 1440,
            "15d": 15 * 1440,
            "30d": 30 * 1440,
            "60d": 60 * 1440,
        }
        
    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_cvd_analysis(self, symbol: str) -> Dict[str, Dict[str, float]]:
        """
        获取指定币种的 Spot vs Futures CVD 分析
        
        返回每个周期的:
        - spot_buy: 现货买入量
        - spot_sell: 现货卖出量
        - futures_buy: 合约买入量
        - futures_sell: 合约卖出量
        """
        await self._ensure_session()
        symbol = symbol.upper()
        
        # 并行获取四组数据
        tasks = [
            self._fetch_klines(symbol, "spot", "1m", 1500),
            self._fetch_klines(symbol, "spot", "1d", 62),
            self._fetch_klines(symbol, "futures", "1m", 1500),
            self._fetch_klines(symbol, "futures", "1d", 62),
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        spot_1m, spot_1d, fut_1m, fut_1d = results
        
        # 处理异常
        if isinstance(spot_1m, Exception): spot_1m = []
        if isinstance(spot_1d, Exception): spot_1d = []
        if isinstance(fut_1m, Exception): fut_1m = []
        if isinstance(fut_1d, Exception): fut_1d = []
        
        # 计算买入量和卖出量 (分开)
        spot_buy_1m, spot_sell_1m = self._calc_buy_sell_flows(spot_1m)
        spot_buy_1d, spot_sell_1d = self._calc_buy_sell_flows(spot_1d)
        fut_buy_1m, fut_sell_1m = self._calc_buy_sell_flows(fut_1m)
        fut_buy_1d, fut_sell_1d = self._calc_buy_sell_flows(fut_1d)
        
        analysis = {}
        
        for name, minutes in self.windows.items():
            is_daily = minutes > 1440
            
            # 选择数据源
            sb = spot_buy_1d if is_daily else spot_buy_1m
            ss = spot_sell_1d if is_daily else spot_sell_1m
            fb = fut_buy_1d if is_daily else fut_buy_1m
            fs = fut_sell_1d if is_daily else fut_sell_1m
            
            analysis[name] = {
                "spot_buy": self._sum_recent(sb, minutes, is_daily),
                "spot_sell": self._sum_recent(ss, minutes, is_daily),
                "futures_buy": self._sum_recent(fb, minutes, is_daily),
                "futures_sell": self._sum_recent(fs, minutes, is_daily),
                # 保留旧的兼容字段
                "spot": self._sum_recent(sb, minutes, is_daily) - self._sum_recent(ss, minutes, is_daily),
                "futures": self._sum_recent(fb, minutes, is_daily) - self._sum_recent(fs, minutes, is_daily),
            }
            
        return analysis

    async def _fetch_klines(self, symbol: str, market: str, interval: str, limit: int) -> List[list]:
        """获取 K 线数据"""
        base_url = "https://api.binance.com" if market == "spot" else "https://fapi.binance.com"
        endpoint = "/api/v3/klines" if market == "spot" else "/fapi/v1/klines"
        url = f"{base_url}{endpoint}"
        
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        try:
            async with self._session.get(url, params=params, timeout=5) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.warning(f"Failed to fetch {market} {symbol} {interval}: {resp.status}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching {market} {symbol}: {e}")
            return []

    def _calc_net_flows(self, klines: List[list]) -> List[float]:
        """从 K 线计算净买入量 (Quote Asset)"""
        # Kline format:
        # 0: Open time
        # ...
        # 5: Volume (Base)
        # 7: Quote Asset Volume
        # 10: Taker Buy Quote Asset Volume
        nets = []
        for k in klines:
            try:
                quote_vol = float(k[7])
                taker_buy_quote = float(k[10])
                # Net = Buy - Sell
                # TakerBuy = Buy
                # Sell = Total - Buy
                # Net = Buy - (Total - Buy) = 2*Buy - Total
                net = 2 * taker_buy_quote - quote_vol
                nets.append(net)
            except (IndexError, ValueError):
                nets.append(0.0)
        return nets
    
    def _calc_buy_sell_flows(self, klines: List[list]) -> tuple:
        """
        从 K 线分别计算买入量和卖出量
        
        返回: (buy_list, sell_list)
        """
        buys = []
        sells = []
        for k in klines:
            try:
                quote_vol = float(k[7])
                taker_buy_quote = float(k[10])
                taker_sell_quote = quote_vol - taker_buy_quote
                buys.append(taker_buy_quote)
                sells.append(taker_sell_quote)
            except (IndexError, ValueError):
                buys.append(0.0)
                sells.append(0.0)
        return buys, sells

    def _sum_recent(self, net_flows: List[float], minutes: int, is_daily: bool) -> float:
        """
        计算最近 N 分钟/天 的累积和
        net_flows 是按时间正序排列的 (旧->新)
        """
        if not net_flows:
            return 0.0
            
        count = minutes
        if is_daily:
            count = minutes // 1440
        else:
            # 如果是分钟线，count 就是 minutes
            # 但要注意：1m K线列表可能包含 1440个。
            # "24h" 需要 1440 个 1m K线。
            pass
            
        if count > len(net_flows):
            # 数据不足，返回现有的总和 (或者 None?)
            # 用户可能接受近似值
            count = len(net_flows)
            
        # 取切片 [-count:]
        subset = net_flows[-count:]
        return sum(subset)
