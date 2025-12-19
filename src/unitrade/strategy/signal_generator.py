"""
交易信号生成器

基于多指标组合生成交易信号:
- OBI (订单簿不平衡)
- CVD (累积成交量差)
- OI Delta (持仓量变化)
- Market Regime (市场状态)
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class SignalType(Enum):
    """信号类型"""
    STRONG_LONG = "strong_long"
    LONG = "long"
    NEUTRAL = "neutral"
    SHORT = "short"
    STRONG_SHORT = "strong_short"


class SignalReason(Enum):
    """信号原因"""
    OBI_BULLISH = "obi_bullish"
    OBI_BEARISH = "obi_bearish"
    CVD_DIVERGENCE_BULL = "cvd_divergence_bull"
    CVD_DIVERGENCE_BEAR = "cvd_divergence_bear"
    OI_LONG_BUILD = "oi_long_build"
    OI_SHORT_BUILD = "oi_short_build"
    OI_LONG_UNWIND = "oi_long_unwind"
    OI_SHORT_COVER = "oi_short_cover"
    SQUEEZE_POTENTIAL = "squeeze_potential"


@dataclass
class TradingSignal:
    """交易信号"""
    symbol: str
    exchange: str
    signal_type: SignalType
    strength: float  # 0.0 - 1.0
    reasons: List[SignalReason]
    
    # 指标快照
    obi: Optional[float] = None
    cvd: Optional[float] = None
    oi_delta_pct: Optional[float] = None
    volatility: Optional[float] = None
    funding_rate: Optional[float] = None
    
    # 元数据
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "signal": self.signal_type.value,
            "strength": self.strength,
            "reasons": [r.value for r in self.reasons],
            "metrics": {
                "obi": self.obi,
                "cvd": self.cvd,
                "oi_delta_pct": self.oi_delta_pct,
                "volatility": self.volatility,
                "funding_rate": self.funding_rate,
            },
            "timestamp": self.timestamp.isoformat()
        }


class SignalGenerator:
    """
    多指标信号生成器
    
    组合以下指标生成交易信号:
    1. OBI: 订单簿压力
    2. CVD: 净买卖力量
    3. OI + Price: 市场状态
    4. Volatility: 波动率过滤
    """
    
    # 阈值配置
    OBI_THRESHOLD = 0.3      # OBI 绝对值 > 0.3 为显著
    CVD_THRESHOLD = 1000     # CVD 变化阈值
    OI_DELTA_THRESHOLD = 3.0 # OI 变化 > 3% 为显著
    VOLATILITY_MAX = 80      # 波动率 > 80% 时过滤信号
    
    def __init__(self):
        self._signal_history: Dict[str, List[TradingSignal]] = {}
    
    def generate_signal(
        self,
        symbol: str,
        exchange: str,
        obi: float,
        cvd: float,
        oi_delta_pct: float,
        market_regime: str,
        volatility: Optional[float] = None,
        funding_rate: Optional[float] = None,
        price_change_pct: Optional[float] = None,
    ) -> TradingSignal:
        """
        生成交易信号
        
        核心逻辑:
        1. OBI > 0.3 + CVD 上升 + Long Build = 强多信号
        2. OBI < -0.3 + CVD 下降 + Short Build = 强空信号
        3. 低 Funding + OI 飙升 = 潜在挤压
        """
        reasons = []
        bullish_score = 0
        bearish_score = 0
        
        # 1. OBI 分析
        if obi > self.OBI_THRESHOLD:
            reasons.append(SignalReason.OBI_BULLISH)
            bullish_score += 1
        elif obi < -self.OBI_THRESHOLD:
            reasons.append(SignalReason.OBI_BEARISH)
            bearish_score += 1
        
        # 2. CVD 背离分析
        if price_change_pct is not None:
            if cvd > self.CVD_THRESHOLD and price_change_pct < 0:
                # CVD 上升但价格下跌 = 潜在反转做多
                reasons.append(SignalReason.CVD_DIVERGENCE_BULL)
                bullish_score += 1.5
            elif cvd < -self.CVD_THRESHOLD and price_change_pct > 0:
                # CVD 下降但价格上涨 = 潜在反转做空
                reasons.append(SignalReason.CVD_DIVERGENCE_BEAR)
                bearish_score += 1.5
        
        # 3. 市场状态分析
        if market_regime == "long_build":
            reasons.append(SignalReason.OI_LONG_BUILD)
            bullish_score += 1
        elif market_regime == "short_build":
            reasons.append(SignalReason.OI_SHORT_BUILD)
            bearish_score += 1
        elif market_regime == "short_cover":
            reasons.append(SignalReason.OI_SHORT_COVER)
            bullish_score += 0.5  # 空头回补，短期看多
        elif market_regime == "long_unwind":
            reasons.append(SignalReason.OI_LONG_UNWIND)
            bearish_score += 0.5  # 多头平仓，短期看空
        
        # 4. 挤压潜力检测
        if funding_rate is not None and abs(oi_delta_pct) > self.OI_DELTA_THRESHOLD:
            if funding_rate < -0.01 and oi_delta_pct > 0:
                # 负 funding + OI 上升 = 空头挤压潜力
                reasons.append(SignalReason.SQUEEZE_POTENTIAL)
                bullish_score += 2
            elif funding_rate > 0.01 and oi_delta_pct > 0:
                # 正 funding + OI 上升 = 多头挤压潜力
                reasons.append(SignalReason.SQUEEZE_POTENTIAL)
                bearish_score += 2
        
        # 5. 波动率过滤
        if volatility and volatility > self.VOLATILITY_MAX:
            # 高波动率时降低信号强度
            bullish_score *= 0.5
            bearish_score *= 0.5
        
        # 计算信号
        net_score = bullish_score - bearish_score
        
        if net_score >= 3:
            signal_type = SignalType.STRONG_LONG
            strength = min(net_score / 5, 1.0)
        elif net_score >= 1.5:
            signal_type = SignalType.LONG
            strength = 0.6 + (net_score - 1.5) / 5
        elif net_score <= -3:
            signal_type = SignalType.STRONG_SHORT
            strength = min(abs(net_score) / 5, 1.0)
        elif net_score <= -1.5:
            signal_type = SignalType.SHORT
            strength = 0.6 + (abs(net_score) - 1.5) / 5
        else:
            signal_type = SignalType.NEUTRAL
            strength = 0.0
        
        signal = TradingSignal(
            symbol=symbol,
            exchange=exchange,
            signal_type=signal_type,
            strength=min(strength, 1.0),
            reasons=reasons,
            obi=obi,
            cvd=cvd,
            oi_delta_pct=oi_delta_pct,
            volatility=volatility,
            funding_rate=funding_rate,
        )
        
        # 记录历史
        key = f"{exchange}:{symbol}"
        if key not in self._signal_history:
            self._signal_history[key] = []
        self._signal_history[key].append(signal)
        
        # 保留最近 100 个信号
        if len(self._signal_history[key]) > 100:
            self._signal_history[key] = self._signal_history[key][-100:]
        
        return signal
    
    def get_signal_history(
        self,
        exchange: str,
        symbol: str,
        limit: int = 10
    ) -> List[TradingSignal]:
        """获取信号历史"""
        key = f"{exchange}:{symbol}"
        history = self._signal_history.get(key, [])
        return history[-limit:]
    
    def get_active_signals(
        self,
        min_strength: float = 0.5
    ) -> List[TradingSignal]:
        """获取当前活跃信号"""
        active = []
        
        for key, signals in self._signal_history.items():
            if signals:
                latest = signals[-1]
                if latest.strength >= min_strength and \
                   latest.signal_type != SignalType.NEUTRAL:
                    active.append(latest)
        
        # 按强度排序
        active.sort(key=lambda s: s.strength, reverse=True)
        return active
