"""
Funding Rate 扫描器

扫描极端 Funding Rate 的交易对:
- 高正 Funding: 多头过度杠杆
- 高负 Funding: 空头过度杠杆 (潜在挤压)
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class FundingRateAlert:
    """Funding Rate 警报"""
    symbol: str
    funding_rate: float
    funding_rate_pct: float  # 百分比
    predicted_rate: Optional[float] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    next_funding_time: Optional[datetime] = None
    alert_type: str = ""  # "extreme_positive", "extreme_negative"
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_telegram_message(self) -> str:
        emoji = "🔴" if self.funding_rate > 0 else "🟢"
        direction = "LONGS PAY" if self.funding_rate > 0 else "SHORTS PAY"
        
        return f"""{emoji} EXTREME FUNDING {emoji}
Symbol: ${self.symbol.replace('USDT', '')}
Funding: {self.funding_rate_pct:+.4f}% ({direction})
Mark Price: {self.mark_price:.4f}
Next Funding: {self.next_funding_time.strftime('%H:%M UTC') if self.next_funding_time else 'N/A'}
-------------------
⚠️ High funding = potential reversal
"""


class FundingRateScanner:
    """
    Funding Rate 扫描器
    
    扫描逻辑:
    1. 获取所有 USDT 交易对的 funding rate
    2. 筛选极端值 (> threshold 或 < -threshold)
    3. 发送警报
    """
    
    BASE_URL = "https://fapi.binance.com"
    
    def __init__(
        self,
        threshold: float = 0.001,  # 0.1% = 极端
        min_volume_usdt: float = 5_000_000,
        ignore_list: Optional[List[str]] = None,
    ):
        self.threshold = threshold
        self.min_volume_usdt = min_volume_usdt
        self.ignore_list = ignore_list or []
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
    
    async def stop(self) -> None:
        if self._session:
            await self._session.close()
    
    async def scan(self) -> List[FundingRateAlert]:
        """扫描极端 funding rate"""
        alerts = []
        
        try:
            # 获取所有 premium index (包含 funding rate)
            url = f"{self.BASE_URL}/fapi/v1/premiumIndex"
            
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    logger.error(f"Failed to get funding rates: {resp.status}")
                    return []
                
                data = await resp.json()
            
            # 获取 24h volume 用于过滤
            volumes = await self._get_volumes()
            
            for item in data:
                symbol = item["symbol"]
                
                # 过滤
                if not symbol.endswith("USDT"):
                    continue
                if symbol in self.ignore_list:
                    continue
                if volumes.get(symbol, 0) < self.min_volume_usdt:
                    continue
                
                funding_rate = float(item.get("lastFundingRate", 0))
                
                # 检测极端值
                if abs(funding_rate) >= self.threshold:
                    alert_type = "extreme_positive" if funding_rate > 0 else "extreme_negative"
                    
                    next_funding = None
                    if item.get("nextFundingTime"):
                        next_funding = datetime.fromtimestamp(
                            int(item["nextFundingTime"]) / 1000
                        )
                    
                    alert = FundingRateAlert(
                        symbol=symbol,
                        funding_rate=funding_rate,
                        funding_rate_pct=funding_rate * 100,
                        mark_price=float(item.get("markPrice", 0)),
                        index_price=float(item.get("indexPrice", 0)),
                        next_funding_time=next_funding,
                        alert_type=alert_type,
                    )
                    alerts.append(alert)
            
            # 按 funding rate 绝对值排序
            alerts.sort(key=lambda x: abs(x.funding_rate), reverse=True)
            
        except Exception as e:
            logger.error(f"Funding rate scan error: {e}")
        
        return alerts
    
    async def _get_volumes(self) -> dict:
        """获取 24h 成交量"""
        volumes = {}
        
        try:
            url = f"{self.BASE_URL}/fapi/v1/ticker/24hr"
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data:
                        volumes[item["symbol"]] = float(item.get("quoteVolume", 0))
        except Exception as e:
            logger.error(f"Error getting volumes: {e}")
        
        return volumes
    
    async def get_extreme_funding(
        self,
        top_n: int = 10
    ) -> tuple[List[FundingRateAlert], List[FundingRateAlert]]:
        """
        获取极端 funding rate
        
        Returns:
            (最高正 funding 列表, 最高负 funding 列表)
        """
        all_alerts = await self.scan()
        
        positive = [a for a in all_alerts if a.funding_rate > 0]
        negative = [a for a in all_alerts if a.funding_rate < 0]
        
        return positive[:top_n], negative[:top_n]


async def main():
    """测试运行"""
    scanner = FundingRateScanner(
        threshold=0.0005,  # 0.05%
        ignore_list=["BTCUSDT", "ETHUSDT"],
    )
    
    await scanner.start()
    
    print("=" * 60)
    print("🔍 Funding Rate Scanner")
    print("=" * 60)
    
    positive, negative = await scanner.get_extreme_funding(top_n=5)
    
    print("\n🔴 Highest Positive Funding (Longs Pay):")
    for alert in positive:
        print(f"  {alert.symbol}: {alert.funding_rate_pct:+.4f}%")
    
    print("\n🟢 Highest Negative Funding (Shorts Pay):")
    for alert in negative:
        print(f"  {alert.symbol}: {alert.funding_rate_pct:+.4f}%")
    
    await scanner.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
