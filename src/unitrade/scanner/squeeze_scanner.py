"""
Short Squeeze Scanner - OI 飙升检测器

扫描所有 USDT 交易对的 Open Interest 变化，检测潜在的空头挤压信号。

功能:
1. 获取所有活跃 USDT 交易对
2. 过滤低成交量币种
3. 使用 asyncio.Semaphore 批量获取 OI 历史
4. 检测 OI 飙升 (短期 MA > 长期 MA * threshold)
5. 发送 Telegram 警报
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Set

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class ScannerConfig:
    """扫描器配置"""
    # API 配置
    base_url: str = "https://fapi.binance.com"
    
    # API 模式: "public" 或 "private"
    api_mode: str = "public"
    api_key: str = ""
    api_secret: str = ""
    
    # 币种筛选 (方案C)
    # 最终扫描 = Top N + 额外白名单 - 忽略列表
    auto_top_n: int = 100  # 自动包含交易量前 N 名
    extra_whitelist: List[str] = field(default_factory=list)  # 额外白名单
    ignore_list: List[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "BNBUSDT"])
    
    # OI 分析配置
    oi_period: str = "5m"  # OI 历史周期
    oi_limit: int = 30     # 获取 30 个周期
    ma_short_periods: int = 3   # 短期 MA 周期
    ma_long_periods: int = 30   # 长期 MA 周期
    spike_threshold: float = 1.2  # 飙升阈值 (20%)
    
    # 并发控制
    max_concurrent_requests: int = 10
    request_delay: float = 0.1  # 请求间隔 (秒)
    
    # Telegram 配置
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_enabled: bool = False
    telegram_topic_id: Optional[int] = None  # message_thread_id

    @property
    def is_private(self) -> bool:
        """是否使用私有 API"""
        return self.api_mode == "private" and self.api_key and self.api_secret


@dataclass
class OISpikeAlert:
    """OI 飙升警报"""
    symbol: str
    oi_spike_pct: float      # OI 飙升百分比
    current_oi: float
    ma_short: float
    ma_long: float
    funding_rate: Optional[float] = None
    current_price: Optional[float] = None
    price_change_pct: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_telegram_message(self) -> str:
        """生成 Telegram 消息"""
        funding_str = f"{self.funding_rate * 100:.4f}%" if self.funding_rate is not None else "N/A"
        price_change_str = f"{self.price_change_pct:+.2f}%" if self.price_change_pct is not None else "N/A"
        price_str = f"{self.current_price:.4f}" if self.current_price is not None else "N/A"
        
        return f"""⚠️ OI SURGE DETECTED ⚠️
Symbol: ${self.symbol.replace('USDT', '')}
OI Spike: +{self.oi_spike_pct:.1f}% vs Avg
Current Funding: {funding_str}
Price: {price_str} ({price_change_str})
-------------------
🔗 [TradingView](https://www.tradingview.com/chart/?symbol=BINANCE:{self.symbol}.P)
"""


class BinanceScanner:
    """
    Binance 空头挤压扫描器
    
    扫描所有 USDT 交易对的 OI 变化，检测潜在的空头挤压信号。
    支持公共和私有 API 模式。
    """
    
    def __init__(self, config: Optional[ScannerConfig] = None):
        self.config = config or ScannerConfig()
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        
        # 缓存
        self._active_symbols: List[str] = []
        self._alerts: List[OISpikeAlert] = []
    
    async def start(self) -> None:
        """启动扫描器"""
        self._session = aiohttp.ClientSession()
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        
        mode = "private" if self.config.is_private else "public"
        logger.info(f"Scanner started (API mode: {mode})")
    
    async def stop(self) -> None:
        """停止扫描器"""
        if self._session:
            await self._session.close()
        logger.info("Scanner stopped")
    
    def _get_headers(self) -> Dict:
        """获取请求头"""
        headers = {}
        if self.config.is_private:
            headers["X-MBX-APIKEY"] = self.config.api_key
        return headers
    
    def _sign_request(self, params: Dict) -> Dict:
        """签名请求 (私有 API)"""
        if not self.config.is_private:
            return params
        
        import hmac
        import hashlib
        import time
        
        params["timestamp"] = int(time.time() * 1000)
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        signature = hmac.new(
            self.config.api_secret.encode(),
            query_string.encode(),
            hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
        return params
    
    async def scan(self) -> List[OISpikeAlert]:
        """
        执行完整扫描
        
        Returns:
            检测到的 OI 飙升警报列表
        """
        logger.info("Starting OI spike scan...")
        self._alerts = []
        
        # Step 1: 获取活跃交易对
        symbols = await self._discover_symbols()
        logger.info(f"Found {len(symbols)} active symbols after filtering")
        
        if not symbols:
            logger.warning("No symbols to scan")
            return []
        
        # Step 2: 批量分析 OI
        await self._analyze_oi_batch(symbols)
        
        # Step 3: 为检测到的警报获取额外信息
        for alert in self._alerts:
            await self._enrich_alert(alert)
        
        logger.info(f"Scan complete. Found {len(self._alerts)} OI spikes")
        
        return self._alerts
    
    async def _discover_symbols(self) -> List[str]:
        """
        Step 1: 获取要扫描的交易对 (方案C)
        
        逻辑:
        最终扫描 = Top N 交易量币种 + 额外白名单 - 忽略列表
        """
        final_symbols = set()
        
        try:
            # 获取 24h ticker 数据 (包含成交量)
            url = f"{self.config.base_url}/fapi/v1/ticker/24hr"
            
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    logger.error(f"Failed to get ticker data: {resp.status}")
                    return []
                
                tickers = await resp.json()
            
            # 过滤出 USDT 交易对并按交易量排序
            usdt_tickers = [
                t for t in tickers 
                if t["symbol"].endswith("USDT")
            ]
            usdt_tickers.sort(
                key=lambda x: float(x.get("quoteVolume", 0)), 
                reverse=True
            )
            
            # Step 1: 添加 Top N 交易量币种
            if self.config.auto_top_n > 0:
                top_symbols = [t["symbol"] for t in usdt_tickers[:self.config.auto_top_n]]
                final_symbols.update(top_symbols)
                logger.info(f"Added Top {self.config.auto_top_n} by volume: {len(top_symbols)} symbols")
            
            # Step 2: 添加额外白名单
            if self.config.extra_whitelist:
                for symbol in self.config.extra_whitelist:
                    # 验证白名单中的币种是否存在
                    if any(t["symbol"] == symbol for t in usdt_tickers):
                        final_symbols.add(symbol)
                        logger.debug(f"Added from whitelist: {symbol}")
                    else:
                        logger.warning(f"Whitelist symbol not found: {symbol}")
            
            # Step 3: 移除忽略列表
            for symbol in self.config.ignore_list:
                final_symbols.discard(symbol)
            
            logger.info(
                f"Symbol discovery: {len(usdt_tickers)} total USDT pairs -> "
                f"{len(final_symbols)} after filtering "
                f"(Top {self.config.auto_top_n} + {len(self.config.extra_whitelist)} whitelist "
                f"- {len(self.config.ignore_list)} ignored)"
            )
            
        except Exception as e:
            logger.error(f"Symbol discovery error: {e}")
        
        return list(final_symbols)
    
    async def _analyze_oi_batch(self, symbols: List[str]) -> None:
        """
        Step 2: 批量分析所有交易对的 OI
        
        使用 asyncio.Semaphore 限制并发请求数
        """
        tasks = []
        
        for symbol in symbols:
            task = asyncio.create_task(self._analyze_single_oi(symbol))
            tasks.append(task)
        
        # 等待所有任务完成
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error analyzing {symbols[i]}: {result}")
    
    async def _analyze_single_oi(self, symbol: str) -> Optional[OISpikeAlert]:
        """
        分析单个交易对的 OI 历史
        
        使用 Semaphore 限制并发
        """
        async with self._semaphore:
            try:
                # 请求间隔，避免速率限制
                await asyncio.sleep(self.config.request_delay)
                
                # 获取 OI 历史
                url = f"{self.config.base_url}/futures/data/openInterestHist"
                params = {
                    "symbol": symbol,
                    "period": self.config.oi_period,
                    "limit": self.config.oi_limit
                }
                
                async with self._session.get(url, params=params) as resp:
                    if resp.status == 429:
                        # 速率限制
                        retry_after = int(resp.headers.get("Retry-After", 60))
                        logger.warning(f"Rate limited for {symbol}, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        return None
                    
                    if resp.status != 200:
                        logger.debug(f"No OI data for {symbol}: {resp.status}")
                        return None
                    
                    data = await resp.json()
                
                if not data or len(data) < self.config.ma_long_periods:
                    return None
                
                # 提取 OI 值
                oi_values = [float(d["sumOpenInterest"]) for d in data]
                
                # 计算 MA
                ma_short = sum(oi_values[-self.config.ma_short_periods:]) / self.config.ma_short_periods
                ma_long = sum(oi_values) / len(oi_values)
                
                # 检测飙升
                threshold = ma_long * self.config.spike_threshold
                
                if ma_short > threshold:
                    spike_pct = ((ma_short - ma_long) / ma_long) * 100
                    
                    alert = OISpikeAlert(
                        symbol=symbol,
                        oi_spike_pct=spike_pct,
                        current_oi=oi_values[-1],
                        ma_short=ma_short,
                        ma_long=ma_long
                    )
                    
                    self._alerts.append(alert)
                    logger.info(f"🚨 OI Spike detected: {symbol} +{spike_pct:.1f}%")
                    
                    return alert
                
            except Exception as e:
                logger.error(f"Error analyzing OI for {symbol}: {e}")
            
            return None
    
    async def _enrich_alert(self, alert: OISpikeAlert) -> None:
        """
        Step 3: 为警报添加额外信息 (Funding Rate, Price)
        """
        try:
            async with self._semaphore:
                # 获取 Funding Rate
                url = f"{self.config.base_url}/fapi/v1/premiumIndex"
                params = {"symbol": alert.symbol}
                
                async with self._session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        alert.funding_rate = float(data.get("lastFundingRate", 0))
                        alert.current_price = float(data.get("markPrice", 0))
                
                # 获取价格变化
                url = f"{self.config.base_url}/fapi/v1/ticker/24hr"
                params = {"symbol": alert.symbol}
                
                async with self._session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        alert.price_change_pct = float(data.get("priceChangePercent", 0))
                        
        except Exception as e:
            logger.error(f"Error enriching alert for {alert.symbol}: {e}")
    
    async def send_telegram_alerts(self, alerts: List[OISpikeAlert]) -> None:
        """发送 Telegram 警报"""
        if not self.config.telegram_enabled:
            logger.debug("Telegram alerts disabled")
            return
        
        if not self.config.telegram_bot_token or not self.config.telegram_chat_id:
            logger.warning("Telegram credentials not configured")
            return
        
        url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
        
        for alert in alerts:
            try:
                payload = {
                    "chat_id": self.config.telegram_chat_id,
                    "text": alert.to_telegram_message(),
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True
                }
                if self.config.telegram_topic_id is not None:
                    payload["message_thread_id"] = self.config.telegram_topic_id
                
                async with self._session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        logger.error(f"Telegram send failed: {await resp.text()}")
                    else:
                        logger.info(f"Telegram alert sent for {alert.symbol}")
                        
            except Exception as e:
                logger.error(f"Telegram error: {e}")


async def run_scanner(
    config: Optional[ScannerConfig] = None,
    interval_minutes: int = 5,
    continuous: bool = False
) -> List[OISpikeAlert]:
    """
    运行扫描器
    
    Args:
        config: 扫描器配置
        interval_minutes: 扫描间隔 (分钟)
        continuous: 是否持续运行
    
    Returns:
        检测到的警报列表
    """
    scanner = BinanceScanner(config)
    await scanner.start()
    
    try:
        all_alerts = []
        
        while True:
            alerts = await scanner.scan()
            all_alerts.extend(alerts)
            
            # 发送 Telegram 警报
            if alerts:
                await scanner.send_telegram_alerts(alerts)
                
                # 打印到控制台
                print("\n" + "=" * 50)
                print(f"🔍 Scan Complete - {datetime.now().strftime('%H:%M:%S')}")
                print(f"📊 Found {len(alerts)} OI Spikes")
                print("=" * 50)
                
                for alert in alerts:
                    print(alert.to_telegram_message())
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] No OI spikes detected")
            
            if not continuous:
                break
            
            # 等待下一次扫描
            await asyncio.sleep(interval_minutes * 60)
            
    finally:
        await scanner.stop()
    
    return all_alerts


async def main():
    """主函数 - 快速测试"""
    import os
    
    # 配置
    config = ScannerConfig(
        min_volume_usdt=5_000_000,
        ignore_list=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
        spike_threshold=1.15,  # 15% 飙升
        max_concurrent_requests=10,
        
        # Telegram (从环境变量读取)
        telegram_enabled=bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
    )
    
    print("=" * 60)
    print("🔍 Short Squeeze Scanner - OI Spike Detector")
    print("=" * 60)
    print(f"Min Volume: ${config.min_volume_usdt:,.0f}")
    print(f"Spike Threshold: {(config.spike_threshold - 1) * 100:.0f}%")
    print(f"Ignore List: {config.ignore_list}")
    print(f"Max Concurrent: {config.max_concurrent_requests}")
    print("=" * 60)
    print()
    
    # 单次扫描
    alerts = await run_scanner(config, continuous=False)
    
    print()
    print("=" * 60)
    print(f"📊 Total Alerts: {len(alerts)}")
    print("=" * 60)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    asyncio.run(main())
