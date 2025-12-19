"""
UniTrade Analytics Gateway

加密货币交易分析系统，提供实时市场数据、指标计算和持久化存储。
"""

__version__ = "1.0.0"
__author__ = "UniTrade Team"

# Lazy imports to avoid circular dependencies
def __getattr__(name):
    if name == "connection":
        from . import connection
        return connection
    elif name == "analytics":
        from . import analytics
        return analytics
    elif name == "order":
        from . import order
        return order
    elif name == "scanner":
        from . import scanner
        return scanner
    elif name == "strategy":
        from . import strategy
        return strategy
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "__version__",
    "connection",
    "analytics",
    "order",
    "scanner",
    "strategy",
]

