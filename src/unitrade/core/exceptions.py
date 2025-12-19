"""
UniTrade 自定义异常

提供层次化的异常类，用于精确的错误处理。
"""

from typing import Optional


class UniTradeError(Exception):
    """UniTrade 基础异常类"""
    
    def __init__(self, message: str, code: Optional[str] = None):
        self.message = message
        self.code = code
        super().__init__(self.message)


class ConnectionError(UniTradeError):
    """WebSocket 连接相关错误"""
    
    def __init__(
        self, 
        message: str, 
        exchange: str = "", 
        reconnect_attempts: int = 0
    ):
        super().__init__(message, code="CONNECTION_ERROR")
        self.exchange = exchange
        self.reconnect_attempts = reconnect_attempts


class RateLimitError(UniTradeError):
    """速率限制错误"""
    
    def __init__(
        self, 
        message: str, 
        exchange: str = "",
        retry_after: float = 0,
        endpoint: str = ""
    ):
        super().__init__(message, code="RATE_LIMIT_ERROR")
        self.exchange = exchange
        self.retry_after = retry_after
        self.endpoint = endpoint


class OrderError(UniTradeError):
    """订单执行错误"""
    
    def __init__(
        self, 
        message: str,
        exchange: str = "",
        order_id: str = "",
        error_code: Optional[str] = None
    ):
        super().__init__(message, code="ORDER_ERROR")
        self.exchange = exchange
        self.order_id = order_id
        self.error_code = error_code


class DataSyncError(UniTradeError):
    """数据同步错误 (订单簿、仓位不一致)"""
    
    def __init__(
        self, 
        message: str,
        data_type: str = "",
        symbol: str = ""
    ):
        super().__init__(message, code="DATA_SYNC_ERROR")
        self.data_type = data_type
        self.symbol = symbol


class AuthenticationError(UniTradeError):
    """认证错误 (API Key/Secret 无效)"""
    
    def __init__(self, message: str, exchange: str = ""):
        super().__init__(message, code="AUTH_ERROR")
        self.exchange = exchange


class ValidationError(UniTradeError):
    """参数验证错误"""
    
    def __init__(self, message: str, field: str = ""):
        super().__init__(message, code="VALIDATION_ERROR")
        self.field = field
