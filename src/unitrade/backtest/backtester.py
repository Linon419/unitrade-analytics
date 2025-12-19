"""
简易回测框架

功能:
1. 加载历史数据
2. 模拟信号生成
3. 计算收益指标
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """交易记录"""
    symbol: str
    side: str  # "long" or "short"
    entry_price: Decimal
    exit_price: Optional[Decimal] = None
    quantity: Decimal = Decimal("1")
    entry_time: datetime = None
    exit_time: Optional[datetime] = None
    pnl: Optional[Decimal] = None
    pnl_pct: Optional[float] = None
    status: str = "open"  # "open", "closed", "stopped"
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    
    def close(self, exit_price: Decimal, exit_time: datetime) -> None:
        """关闭交易"""
        self.exit_price = exit_price
        self.exit_time = exit_time
        self.status = "closed"
        
        if self.side == "long":
            self.pnl = (exit_price - self.entry_price) * self.quantity
        else:
            self.pnl = (self.entry_price - exit_price) * self.quantity
        
        self.pnl_pct = float(self.pnl / (self.entry_price * self.quantity) * 100)


@dataclass
class BacktestResult:
    """回测结果"""
    symbol: str
    start_time: datetime
    end_time: datetime
    initial_capital: Decimal
    final_capital: Decimal
    
    # 交易统计
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    
    # 收益指标
    total_pnl: Decimal = Decimal("0")
    total_pnl_pct: float = 0.0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    
    # 交易记录
    trades: List[Trade] = field(default_factory=list)
    
    def calculate_metrics(self) -> None:
        """计算所有指标"""
        if not self.trades:
            return
        
        closed_trades = [t for t in self.trades if t.status == "closed"]
        self.total_trades = len(closed_trades)
        
        if self.total_trades == 0:
            return
        
        # 胜负统计
        winning = [t for t in closed_trades if t.pnl and t.pnl > 0]
        losing = [t for t in closed_trades if t.pnl and t.pnl < 0]
        
        self.winning_trades = len(winning)
        self.losing_trades = len(losing)
        
        # 胜率
        self.win_rate = self.winning_trades / self.total_trades * 100
        
        # 平均盈亏
        if winning:
            self.avg_win = float(sum(t.pnl for t in winning) / len(winning))
        if losing:
            self.avg_loss = float(abs(sum(t.pnl for t in losing)) / len(losing))
        
        # 盈亏比
        total_win = sum(t.pnl for t in winning) if winning else Decimal("0")
        total_loss = abs(sum(t.pnl for t in losing)) if losing else Decimal("1")
        self.profit_factor = float(total_win / total_loss) if total_loss > 0 else 0
        
        # 总收益
        self.total_pnl = sum(t.pnl for t in closed_trades if t.pnl)
        self.final_capital = self.initial_capital + self.total_pnl
        self.total_pnl_pct = float(self.total_pnl / self.initial_capital * 100)
        
        # 最大回撤
        self._calculate_max_drawdown()
    
    def _calculate_max_drawdown(self) -> None:
        """计算最大回撤"""
        equity = float(self.initial_capital)
        peak = equity
        max_dd = 0.0
        
        for trade in self.trades:
            if trade.pnl:
                equity += float(trade.pnl)
                if equity > peak:
                    peak = equity
                dd = (peak - equity) / peak * 100
                if dd > max_dd:
                    max_dd = dd
        
        self.max_drawdown = max_dd
    
    def summary(self) -> str:
        """生成摘要"""
        return f"""
========================================
📊 回测结果: {self.symbol}
========================================
时间范围: {self.start_time} - {self.end_time}
初始资金: ${float(self.initial_capital):,.2f}
最终资金: ${float(self.final_capital):,.2f}

📈 收益指标
----------------------------------------
总收益: ${float(self.total_pnl):,.2f} ({self.total_pnl_pct:+.2f}%)
最大回撤: {self.max_drawdown:.2f}%
盈亏比: {self.profit_factor:.2f}

📊 交易统计
----------------------------------------
总交易: {self.total_trades}
盈利: {self.winning_trades} | 亏损: {self.losing_trades}
胜率: {self.win_rate:.1f}%
平均盈利: ${self.avg_win:,.2f}
平均亏损: ${self.avg_loss:,.2f}
========================================
"""


@dataclass
class MarketData:
    """市场数据点"""
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    
    # 可选指标
    obi: Optional[float] = None
    cvd: Optional[float] = None
    oi_delta: Optional[float] = None
    regime: Optional[str] = None


class Backtester:
    """
    回测引擎
    
    使用方法:
    1. 加载历史数据
    2. 定义策略回调
    3. 运行回测
    4. 分析结果
    """
    
    def __init__(
        self,
        symbol: str,
        initial_capital: Decimal = Decimal("10000"),
        commission: float = 0.0004,  # 0.04%
    ):
        self.symbol = symbol
        self.initial_capital = initial_capital
        self.commission = commission
        
        self._data: List[MarketData] = []
        self._trades: List[Trade] = []
        self._current_position: Optional[Trade] = None
        self._capital = initial_capital
        
        # 策略回调
        self._strategy = None
    
    def load_data(self, data: List[MarketData]) -> None:
        """加载历史数据"""
        self._data = sorted(data, key=lambda x: x.timestamp)
        logger.info(f"Loaded {len(data)} data points")
    
    def set_strategy(self, strategy_func) -> None:
        """
        设置策略函数
        
        策略函数签名:
        def strategy(data: MarketData, position: Optional[Trade]) -> Optional[str]
        
        返回: "long", "short", "close", None
        """
        self._strategy = strategy_func
    
    def run(self) -> BacktestResult:
        """运行回测"""
        if not self._data:
            raise ValueError("No data loaded")
        if not self._strategy:
            raise ValueError("No strategy set")
        
        logger.info(f"Running backtest on {len(self._data)} bars...")
        
        for i, bar in enumerate(self._data):
            # 检查止损/止盈
            if self._current_position:
                self._check_stops(bar)
            
            # 获取策略信号
            signal = self._strategy(bar, self._current_position)
            
            if signal == "long" and not self._current_position:
                self._open_position("long", bar)
            elif signal == "short" and not self._current_position:
                self._open_position("short", bar)
            elif signal == "close" and self._current_position:
                self._close_position(bar)
        
        # 强制关闭未平仓位
        if self._current_position:
            self._close_position(self._data[-1])
        
        # 生成结果
        result = BacktestResult(
            symbol=self.symbol,
            start_time=self._data[0].timestamp,
            end_time=self._data[-1].timestamp,
            initial_capital=self.initial_capital,
            final_capital=self._capital,
            trades=self._trades.copy(),
        )
        result.calculate_metrics()
        
        return result
    
    def _open_position(self, side: str, bar: MarketData) -> None:
        """开仓"""
        # 计算仓位大小 (使用全部资金)
        quantity = self._capital / bar.close * Decimal("0.95")  # 95%仓位
        
        self._current_position = Trade(
            symbol=self.symbol,
            side=side,
            entry_price=bar.close,
            quantity=quantity,
            entry_time=bar.timestamp,
        )
        
        # 扣除手续费
        commission = float(bar.close * quantity) * self.commission
        self._capital -= Decimal(str(commission))
    
    def _close_position(self, bar: MarketData) -> None:
        """平仓"""
        if not self._current_position:
            return
        
        self._current_position.close(bar.close, bar.timestamp)
        
        # 更新资金
        self._capital += self._current_position.pnl
        
        # 扣除手续费
        commission = float(bar.close * self._current_position.quantity) * self.commission
        self._capital -= Decimal(str(commission))
        
        self._trades.append(self._current_position)
        self._current_position = None
    
    def _check_stops(self, bar: MarketData) -> None:
        """检查止损/止盈"""
        if not self._current_position:
            return
        
        pos = self._current_position
        
        if pos.stop_loss:
            if pos.side == "long" and bar.low <= pos.stop_loss:
                pos.status = "stopped"
                self._close_position(bar)
            elif pos.side == "short" and bar.high >= pos.stop_loss:
                pos.status = "stopped"
                self._close_position(bar)
        
        if pos.take_profit and self._current_position:
            if pos.side == "long" and bar.high >= pos.take_profit:
                self._close_position(bar)
            elif pos.side == "short" and bar.low <= pos.take_profit:
                self._close_position(bar)


def example_strategy(data: MarketData, position: Optional[Trade]) -> Optional[str]:
    """
    示例策略: OBI + Regime
    
    规则:
    - OBI > 0.3 且 regime = long_build → 做多
    - OBI < -0.3 且 regime = short_build → 做空
    - 反向信号时平仓
    """
    if data.obi is None or data.regime is None:
        return None
    
    if not position:
        if data.obi > 0.3 and data.regime == "long_build":
            return "long"
        elif data.obi < -0.3 and data.regime == "short_build":
            return "short"
    else:
        # 反向信号平仓
        if position.side == "long" and data.obi < -0.2:
            return "close"
        elif position.side == "short" and data.obi > 0.2:
            return "close"
    
    return None


async def main():
    """示例运行"""
    import random
    from datetime import timedelta
    
    # 生成模拟数据
    data = []
    price = Decimal("50000")
    base_time = datetime(2024, 1, 1)
    
    for i in range(1000):
        # 随机价格变动
        change = Decimal(str(random.uniform(-0.02, 0.02)))
        price = price * (1 + change)
        
        obi = random.uniform(-0.5, 0.5)
        regime = random.choice(["long_build", "short_build", "neutral"])
        
        data.append(MarketData(
            timestamp=base_time + timedelta(hours=i),
            open=price,
            high=price * Decimal("1.005"),
            low=price * Decimal("0.995"),
            close=price,
            volume=Decimal(str(random.randint(100, 1000))),
            obi=obi,
            regime=regime,
        ))
    
    # 运行回测
    bt = Backtester("BTCUSDT", initial_capital=Decimal("10000"))
    bt.load_data(data)
    bt.set_strategy(example_strategy)
    
    result = bt.run()
    print(result.summary())


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
