"""Scanner module for market analysis."""

from .squeeze_scanner import (
    BinanceScanner,
    ScannerConfig,
    OISpikeAlert,
    run_scanner,
)
from .funding_scanner import (
    FundingRateScanner,
    FundingRateAlert,
)
from .liquidation_monitor import (
    LiquidationMonitor,
    LiquidationEvent,
    LiquidationAlert,
)
from .market_scanner import (
    MarketScanner,
    MarketScannerConfig,
    run_market_scanner,
)
from .big_trade_monitor import (
    BigTradeMonitor,
    BigTradeMonitorConfig,
)
from .oi_price_monitor import (
    OIPriceMonitor,
    OIPriceMonitorConfig,
)
from .ema_radar import (
    EMARadar,
    EMARadarConfig,
    EMATrendSignal,
)

# Optional scanners (may require extra dependencies like pandas_ta)
try:
    from .wavetrend_scanner import (
        WaveTrendScanner,
        WaveTrendConfig,
        WaveTrendSignal,
    )
except ModuleNotFoundError:  # pragma: no cover
    WaveTrendScanner = None
    WaveTrendConfig = None
    WaveTrendSignal = None

try:
    from .squeeze_momentum_scanner import (
        SqueezeMomentumScanner,
        SqueezeConfig,
        SqueezeSignal,
        run_squeeze_scanner,
    )
except ModuleNotFoundError:  # pragma: no cover
    SqueezeMomentumScanner = None
    SqueezeConfig = None
    SqueezeSignal = None
    run_squeeze_scanner = None
from .signal_detector import (
    AnomalyDetector,
    AnomalyConfig,
    BreakoutSignal,
    RisingIndex,
    RisingIndexConfig,
    RisingScore,
    run_anomaly_detector,
    get_rising_ranking,
    scheduled_ranking_push,
)

__all__ = [
    # OI Squeeze Scanner
    "BinanceScanner",
    "ScannerConfig",
    "OISpikeAlert",
    "run_scanner",
    # Funding Rate Scanner
    "FundingRateScanner",
    "FundingRateAlert",
    # Liquidation Monitor
    "LiquidationMonitor",
    "LiquidationEvent",
    "LiquidationAlert",
    # Unified Market Scanner
    "MarketScanner",
    "MarketScannerConfig",
    "run_market_scanner",
    # Big Trade Monitor
    "BigTradeMonitor",
    "BigTradeMonitorConfig",
    # OI + Price Monitor
    "OIPriceMonitor",
    "OIPriceMonitorConfig",
    # EMA Trend Radar
    "EMARadar",
    "EMARadarConfig",
    "EMATrendSignal",
    # WaveTrend Scanner (optional)
    "WaveTrendScanner",
    "WaveTrendConfig",
    "WaveTrendSignal",
    # Squeeze Momentum Scanner (optional)
    "SqueezeMomentumScanner",
    "SqueezeConfig",
    "SqueezeSignal",
    "run_squeeze_scanner",
    # Anomaly Detector
    "AnomalyDetector",
    "AnomalyConfig",
    "BreakoutSignal",
    "RisingIndex",
    "RisingIndexConfig",
    "RisingScore",
    "run_anomaly_detector",
    "get_rising_ranking",
    "scheduled_ranking_push",
]

# Hide optional scanners from `__all__` if dependencies are missing.
if WaveTrendScanner is None:
    for name in ("WaveTrendScanner", "WaveTrendConfig", "WaveTrendSignal"):
        if name in __all__:
            __all__.remove(name)

if SqueezeMomentumScanner is None:
    for name in ("SqueezeMomentumScanner", "SqueezeConfig", "SqueezeSignal", "run_squeeze_scanner"):
        if name in __all__:
            __all__.remove(name)
