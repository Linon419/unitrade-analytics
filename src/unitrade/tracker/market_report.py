"""
综合数据报告生成器

生成类似 Coinglass 风格的多维度数据报告
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class SymbolReport:
    """币种综合报告"""
    symbol: str
    timestamp: datetime
    
    # 价格
    price: float
    price_change_pct: float
    
    # 资金费率
    funding_rate: float
    
    # 持仓量
    oi_quantity: float  # 币数量
    oi_value: float     # USDT 价值
    oi_change_pct: float
    
    # 多空比
    long_short_ratio: float        # 账户多空比
    top_trader_ratio: float        # 大户账户多空比
    top_position_ratio: float      # 大户持仓多空比


class MarketReporter:
    """
    市场数据报告生成器
    
    获取并格式化综合市场数据
    """
    
    BASE_URL = "https://fapi.binance.com"
    
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
    
    async def stop(self) -> None:
        if self._session:
            await self._session.close()
    
    async def generate_report(self, symbol: str) -> Optional[SymbolReport]:
        """生成综合报告"""
        try:
            # 并行获取所有数据
            price_data, oi_data, ratios = await asyncio.gather(
                self._get_price_funding(symbol),
                self._get_oi(symbol),
                self._get_ratios(symbol),
            )
            
            if not price_data:
                return None
            
            return SymbolReport(
                symbol=symbol,
                timestamp=datetime.now(),
                price=price_data["price"],
                price_change_pct=price_data["change_pct"],
                funding_rate=price_data["funding_rate"],
                oi_quantity=oi_data.get("quantity", 0),
                oi_value=oi_data.get("value", 0),
                oi_change_pct=oi_data.get("change_pct", 0),
                long_short_ratio=ratios.get("ls_ratio", 0),
                top_trader_ratio=ratios.get("top_account", 0),
                top_position_ratio=ratios.get("top_position", 0),
            )
            
        except Exception as e:
            logger.error(f"Report generation error: {e}")
            return None
    
    async def _get_price_funding(self, symbol: str) -> Dict:
        """获取价格和资金费率"""
        url = f"{self.BASE_URL}/fapi/v1/premiumIndex"
        
        async with self._session.get(url, params={"symbol": symbol}) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
        
        price = float(data.get("markPrice", 0))
        funding = float(data.get("lastFundingRate", 0))
        
        # 获取24h变化
        ticker_url = f"{self.BASE_URL}/fapi/v1/ticker/24hr"
        async with self._session.get(ticker_url, params={"symbol": symbol}) as resp:
            if resp.status == 200:
                ticker = await resp.json()
                change_pct = float(ticker.get("priceChangePercent", 0))
            else:
                change_pct = 0
        
        return {
            "price": price,
            "funding_rate": funding,
            "change_pct": change_pct,
        }
    
    async def _get_oi(self, symbol: str) -> Dict:
        """获取持仓量"""
        url = f"{self.BASE_URL}/fapi/v1/openInterest"
        
        async with self._session.get(url, params={"symbol": symbol}) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
        
        quantity = float(data.get("openInterest", 0))
        
        # 获取当前价格计算价值
        price_data = await self._get_price_funding(symbol)
        price = price_data.get("price", 0)
        value = quantity * price
        
        return {
            "quantity": quantity,
            "value": value,
            "change_pct": 0,  # 需要历史数据才能计算
        }
    
    async def _get_ratios(self, symbol: str) -> Dict:
        """获取多空比数据"""
        result = {}
        
        # 账户多空比
        url = f"{self.BASE_URL}/futures/data/globalLongShortAccountRatio"
        async with self._session.get(url, params={"symbol": symbol, "period": "1h", "limit": 1}) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data:
                    result["ls_ratio"] = float(data[0].get("longShortRatio", 0))
        
        # 大户账户多空比
        url = f"{self.BASE_URL}/futures/data/topLongShortAccountRatio"
        async with self._session.get(url, params={"symbol": symbol, "period": "1h", "limit": 1}) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data:
                    result["top_account"] = float(data[0].get("longShortRatio", 0))
        
        # 大户持仓多空比
        url = f"{self.BASE_URL}/futures/data/topLongShortPositionRatio"
        async with self._session.get(url, params={"symbol": symbol, "period": "1h", "limit": 1}) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data:
                    result["top_position"] = float(data[0].get("longShortRatio", 0))
        
        return result
    
    async def get_ratio_history(self, symbol: str, hours: int = 5) -> List[Dict]:
        """获取多空比历史"""
        results = []
        
        url = f"{self.BASE_URL}/futures/data/globalLongShortAccountRatio"
        params = {"symbol": symbol, "period": "1h", "limit": hours}
        
        async with self._session.get(url, params=params) as resp:
            if resp.status != 200:
                return []
            ls_data = await resp.json()
        
        url = f"{self.BASE_URL}/futures/data/topLongShortAccountRatio"
        async with self._session.get(url, params=params) as resp:
            if resp.status == 200:
                top_account_data = await resp.json()
            else:
                top_account_data = []
        
        url = f"{self.BASE_URL}/futures/data/topLongShortPositionRatio"
        async with self._session.get(url, params=params) as resp:
            if resp.status == 200:
                top_pos_data = await resp.json()
            else:
                top_pos_data = []
        
        for i, item in enumerate(ls_data):
            ts = datetime.fromtimestamp(item["timestamp"] / 1000)
            results.append({
                "time": ts.strftime("%H:%M"),
                "ls_ratio": float(item["longShortRatio"]),
                "top_account": float(top_account_data[i]["longShortRatio"]) if i < len(top_account_data) else 0,
                "top_position": float(top_pos_data[i]["longShortRatio"]) if i < len(top_pos_data) else 0,
            })
        
        return results
    
    def format_telegram_report(self, report: SymbolReport, ratio_history: List[Dict] = None) -> str:
        """生成 Telegram 格式报告"""
        base_symbol = report.symbol.replace("USDT", "")
        
        lines = [
            f"#{base_symbol} 综合数据报告",
            "━" * 20,
            f"当前价格 ₮{report.price:,.1f} ({report.price_change_pct:+.2f}%)",
            f"资金费率 {report.funding_rate*100:+.4f}%",
            f"持仓数量 {report.oi_quantity/1e4:.1f}万 ({report.oi_change_pct:+.1f}%)",
            f"持仓价值 {report.oi_value/1e8:.2f}亿U",
            "",
            f"#{base_symbol} 多空比 | 大户账户 | 大户持仓",
        ]
        
        if ratio_history:
            for item in ratio_history:
                lines.append(
                    f"{item['time']}: {item['ls_ratio']:.2f} | "
                    f"{item['top_account']:.2f} | {item['top_position']:.2f}"
                )
        else:
            lines.append(
                f"当前: {report.long_short_ratio:.2f} | "
                f"{report.top_trader_ratio:.2f} | {report.top_position_ratio:.2f}"
            )
        
        return "\n".join(lines)


async def main():
    """测试运行"""
    reporter = MarketReporter()
    await reporter.start()
    
    print("=" * 50)
    print("📊 Market Report Generator")
    print("=" * 50)
    
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        report = await reporter.generate_report(symbol)
        if report:
            history = await reporter.get_ratio_history(symbol, 5)
            print(reporter.format_telegram_report(report, history))
            print()
    
    await reporter.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
