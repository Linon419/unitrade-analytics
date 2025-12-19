"""
仓位对账器

职责:
1. 定期从交易所拉取实际仓位
2. 与本地状态对比
3. 发现差异时触发警报或自动修复
"""

import asyncio
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """仓位数据"""
    symbol: str
    side: str  # "LONG" or "SHORT"
    quantity: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal
    leverage: int
    timestamp: int


@dataclass
class PositionMismatch:
    """仓位差异"""
    symbol: str
    local_qty: Decimal
    exchange_qty: Decimal
    difference: Decimal
    severity: str  # "INFO", "WARNING", "CRITICAL"


class PositionReconciler:
    """
    仓位对账器
    
    定期对比本地仓位状态与交易所实际仓位，
    发现差异时触发警报。
    """
    
    def __init__(
        self,
        exchange: str = "binance",
        api_key: str = "",
        api_secret: str = "",
        testnet: bool = False,
        reconcile_interval: float = 60.0,  # 对账间隔 (秒)
        tolerance: Decimal = Decimal("0.0001"),  # 容差
    ):
        self.exchange = exchange
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.reconcile_interval = reconcile_interval
        self.tolerance = tolerance
        
        # 本地仓位状态
        self._local_positions: Dict[str, Position] = {}
        
        # 回调
        self._on_mismatch: Optional[Callable] = None
        
        # 运行状态
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        
        # URL
        if exchange == "binance":
            self._base_url = (
                "https://testnet.binancefuture.com" if testnet 
                else "https://fapi.binance.com"
            )
        elif exchange == "bybit":
            self._base_url = (
                "https://api-testnet.bybit.com" if testnet
                else "https://api.bybit.com"
            )
    
    async def start(self) -> None:
        """启动对账器"""
        self._session = aiohttp.ClientSession()
        self._running = True
        self._task = asyncio.create_task(self._reconcile_loop())
        logger.info(f"Position reconciler started for {self.exchange}")
    
    async def stop(self) -> None:
        """停止对账器"""
        self._running = False
        if self._task:
            self._task.cancel()
        if self._session:
            await self._session.close()
        logger.info("Position reconciler stopped")
    
    def set_mismatch_callback(self, callback: Callable) -> None:
        """设置差异回调"""
        self._on_mismatch = callback
    
    def update_local_position(self, position: Position) -> None:
        """更新本地仓位状态"""
        self._local_positions[position.symbol] = position
    
    def remove_local_position(self, symbol: str) -> None:
        """移除本地仓位"""
        self._local_positions.pop(symbol, None)
    
    async def _reconcile_loop(self) -> None:
        """对账循环"""
        while self._running:
            try:
                mismatches = await self.reconcile()
                
                if mismatches:
                    logger.warning(f"Found {len(mismatches)} position mismatches")
                    for mismatch in mismatches:
                        await self._handle_mismatch(mismatch)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Reconciliation error: {e}")
            
            await asyncio.sleep(self.reconcile_interval)
    
    async def reconcile(self) -> List[PositionMismatch]:
        """
        执行一次对账
        
        Returns:
            发现的仓位差异列表
        """
        mismatches = []
        
        # 获取交易所仓位
        exchange_positions = await self._fetch_exchange_positions()
        
        # 对比本地仓位
        all_symbols = set(self._local_positions.keys()) | set(exchange_positions.keys())
        
        for symbol in all_symbols:
            local_pos = self._local_positions.get(symbol)
            exchange_pos = exchange_positions.get(symbol)
            
            local_qty = local_pos.quantity if local_pos else Decimal("0")
            exchange_qty = exchange_pos.quantity if exchange_pos else Decimal("0")
            
            difference = abs(local_qty - exchange_qty)
            
            if difference > self.tolerance:
                severity = self._classify_severity(local_qty, exchange_qty, difference)
                
                mismatches.append(PositionMismatch(
                    symbol=symbol,
                    local_qty=local_qty,
                    exchange_qty=exchange_qty,
                    difference=difference,
                    severity=severity
                ))
        
        return mismatches
    
    def _classify_severity(
        self,
        local_qty: Decimal,
        exchange_qty: Decimal,
        difference: Decimal
    ) -> str:
        """分类差异严重程度"""
        # 一方为 0，另一方不为 0 -> CRITICAL
        if (local_qty == 0) != (exchange_qty == 0):
            return "CRITICAL"
        
        # 差异超过 10% -> WARNING
        base = max(local_qty, exchange_qty)
        if base > 0 and difference / base > Decimal("0.1"):
            return "WARNING"
        
        return "INFO"
    
    async def _fetch_exchange_positions(self) -> Dict[str, Position]:
        """从交易所获取实际仓位"""
        positions = {}
        
        try:
            if self.exchange == "binance":
                positions = await self._fetch_binance_positions()
            elif self.exchange == "bybit":
                positions = await self._fetch_bybit_positions()
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
        
        return positions
    
    async def _fetch_binance_positions(self) -> Dict[str, Position]:
        """获取 Binance 仓位"""
        import hmac
        import hashlib
        import time
        
        positions = {}
        
        timestamp = int(time.time() * 1000)
        query = f"timestamp={timestamp}"
        signature = hmac.new(
            self.api_secret.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()
        
        url = f"{self._base_url}/fapi/v2/positionRisk?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}
        
        async with self._session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                
                for pos in data:
                    qty = Decimal(pos["positionAmt"])
                    if qty != 0:
                        positions[pos["symbol"]] = Position(
                            symbol=pos["symbol"],
                            side="LONG" if qty > 0 else "SHORT",
                            quantity=abs(qty),
                            entry_price=Decimal(pos["entryPrice"]),
                            unrealized_pnl=Decimal(pos["unRealizedProfit"]),
                            leverage=int(pos["leverage"]),
                            timestamp=int(pos["updateTime"])
                        )
        
        return positions
    
    async def _fetch_bybit_positions(self) -> Dict[str, Position]:
        """获取 Bybit 仓位"""
        import hmac
        import hashlib
        import time
        
        positions = {}
        
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        
        param_str = f"{timestamp}{self.api_key}{recv_window}"
        signature = hmac.new(
            self.api_secret.encode(),
            param_str.encode(),
            hashlib.sha256
        ).hexdigest()
        
        url = f"{self._base_url}/v5/position/list"
        params = {"category": "linear", "settleCoin": "USDT"}
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
        }
        
        async with self._session.get(url, params=params, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                
                if data.get("retCode") == 0:
                    for pos in data.get("result", {}).get("list", []):
                        qty = Decimal(pos["size"])
                        if qty != 0:
                            positions[pos["symbol"]] = Position(
                                symbol=pos["symbol"],
                                side=pos["side"].upper(),
                                quantity=qty,
                                entry_price=Decimal(pos["avgPrice"]),
                                unrealized_pnl=Decimal(pos["unrealisedPnl"]),
                                leverage=int(pos["leverage"]),
                                timestamp=int(pos["updatedTime"])
                            )
        
        return positions
    
    async def _handle_mismatch(self, mismatch: PositionMismatch) -> None:
        """处理仓位差异"""
        log_msg = (
            f"Position mismatch [{mismatch.severity}]: {mismatch.symbol} - "
            f"Local: {mismatch.local_qty}, Exchange: {mismatch.exchange_qty}, "
            f"Diff: {mismatch.difference}"
        )
        
        if mismatch.severity == "CRITICAL":
            logger.critical(log_msg)
        elif mismatch.severity == "WARNING":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)
        
        # 触发回调
        if self._on_mismatch:
            try:
                if asyncio.iscoroutinefunction(self._on_mismatch):
                    await self._on_mismatch(mismatch)
                else:
                    self._on_mismatch(mismatch)
            except Exception as e:
                logger.error(f"Mismatch callback error: {e}")
