"""
订单路由器

职责:
1. 将统一 OrderRequest 转换为交易所特定格式
2. 验证订单参数
3. 管理 ClientOrderId
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Optional

from .id_manager import OrderIdManager


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    LIMIT = "limit"
    MARKET = "market"
    STOP_MARKET = "stop_market"
    STOP_LIMIT = "stop_limit"


class TimeInForce(Enum):
    GTC = "gtc"  # Good Till Cancel
    IOC = "ioc"  # Immediate or Cancel
    FOK = "fok"  # Fill or Kill
    POST_ONLY = "post_only"


@dataclass
class OrderRequest:
    """统一订单请求"""
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    time_in_force: TimeInForce = TimeInForce.GTC
    reduce_only: bool = False
    client_order_id: Optional[str] = None
    algo_id: Optional[str] = None
    
    def validate(self) -> None:
        """验证订单参数"""
        if self.quantity <= 0:
            raise ValueError("Quantity must be positive")
        
        if self.order_type == OrderType.LIMIT and self.price is None:
            raise ValueError("Limit order requires price")
        
        if self.order_type in (OrderType.STOP_MARKET, OrderType.STOP_LIMIT) and self.stop_price is None:
            raise ValueError("Stop order requires stop_price")
        
        if self.order_type == OrderType.STOP_LIMIT and self.price is None:
            raise ValueError("Stop limit order requires price")


class OrderRouter:
    """
    订单路由器
    
    职责:
    1. 将统一 OrderRequest 转换为交易所特定格式
    2. 验证订单参数
    3. 管理 ClientOrderId
    """
    
    def __init__(self, id_manager: Optional[OrderIdManager] = None):
        self.id_manager = id_manager or OrderIdManager()
    
    def route(self, exchange: str, request: OrderRequest) -> Dict[str, Any]:
        """路由订单到指定交易所"""
        # 验证
        request.validate()
        
        # 生成 ClientOrderId
        if not request.client_order_id:
            request.client_order_id = self.id_manager.generate(
                algo_id=request.algo_id or "MANUAL",
                exchange=exchange,
                symbol=request.symbol
            )
        
        if exchange == "binance":
            return self._to_binance(request)
        elif exchange == "bybit":
            return self._to_bybit(request)
        else:
            raise ValueError(f"Unknown exchange: {exchange}")
    
    def _to_binance(self, req: OrderRequest) -> Dict[str, Any]:
        """
        转换为 Binance USDT-M Futures 格式
        
        Endpoint: POST /fapi/v1/order
        """
        payload = {
            "symbol": req.symbol,
            "side": req.side.name.upper(),
            "type": self._map_type_binance(req.order_type),
            "quantity": str(req.quantity),
            "newClientOrderId": req.client_order_id
        }
        
        if req.price:
            payload["price"] = str(req.price)
        
        if req.stop_price:
            payload["stopPrice"] = str(req.stop_price)
        
        # Limit 订单需要 timeInForce
        if req.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
            payload["timeInForce"] = self._map_tif_binance(req.time_in_force)
        
        if req.reduce_only:
            payload["reduceOnly"] = "true"
        
        return payload
    
    def _to_bybit(self, req: OrderRequest) -> Dict[str, Any]:
        """
        转换为 Bybit V5 格式
        
        Endpoint: POST /v5/order/create
        """
        payload = {
            "category": "linear",
            "symbol": req.symbol,
            "side": "Buy" if req.side == OrderSide.BUY else "Sell",
            "orderType": self._map_type_bybit(req.order_type),
            "qty": str(req.quantity),
            "orderLinkId": req.client_order_id
        }
        
        if req.price:
            payload["price"] = str(req.price)
        
        if req.stop_price:
            payload["triggerPrice"] = str(req.stop_price)
        
        if req.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
            payload["timeInForce"] = self._map_tif_bybit(req.time_in_force)
        
        if req.reduce_only:
            payload["reduceOnly"] = True
        
        return payload
    
    @staticmethod
    def _map_type_binance(ot: OrderType) -> str:
        return {
            OrderType.LIMIT: "LIMIT",
            OrderType.MARKET: "MARKET",
            OrderType.STOP_MARKET: "STOP_MARKET",
            OrderType.STOP_LIMIT: "STOP",
        }[ot]
    
    @staticmethod
    def _map_type_bybit(ot: OrderType) -> str:
        return {
            OrderType.LIMIT: "Limit",
            OrderType.MARKET: "Market",
            OrderType.STOP_MARKET: "Market",
            OrderType.STOP_LIMIT: "Limit",
        }[ot]
    
    @staticmethod
    def _map_tif_binance(tif: TimeInForce) -> str:
        return {
            TimeInForce.GTC: "GTC",
            TimeInForce.IOC: "IOC",
            TimeInForce.FOK: "FOK",
            TimeInForce.POST_ONLY: "GTX",
        }[tif]
    
    @staticmethod
    def _map_tif_bybit(tif: TimeInForce) -> str:
        return {
            TimeInForce.GTC: "GTC",
            TimeInForce.IOC: "IOC",
            TimeInForce.FOK: "FOK",
            TimeInForce.POST_ONLY: "PostOnly",
        }[tif]
