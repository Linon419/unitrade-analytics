"""Backtesting module."""

from .backtester import (
    Backtester,
    BacktestResult,
    Trade,
    MarketData,
    example_strategy,
)

__all__ = [
    "Backtester",
    "BacktestResult",
    "Trade",
    "MarketData",
    "example_strategy",
]
