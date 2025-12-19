"""
UniTrade 配置管理

支持 YAML 配置文件和环境变量覆盖。
"""

import os
from dataclasses import dataclass, field, fields as dataclass_fields
from typing import Dict, List, Optional
from pathlib import Path
import yaml


@dataclass
class ExchangeConfig:
    """交易所连接配置"""
    name: str
    api_mode: str = "public"
    api_key: str = ""
    api_secret: str = ""
    testnet: bool = False
    
    # WebSocket 配置
    ws_url: str = ""
    ws_private_url: str = ""
    rest_url: str = ""
    
    # 连接池配置
    max_connections: int = 5
    heartbeat_interval: int = 30
    reconnect_max_attempts: int = 10
    reconnect_base_delay: float = 1.0
    reconnect_max_delay: float = 60.0
    
    # 订阅配置
    symbols: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """根据交易所名称设置默认 URL"""
        if self.name == "binance":
            if not self.ws_url:
                self.ws_url = "wss://fstream.binance.com/stream" if not self.testnet else "wss://stream.binancefuture.com/stream"
            if not self.rest_url:
                self.rest_url = "https://fapi.binance.com" if not self.testnet else "https://testnet.binancefuture.com"
        elif self.name == "bybit":
            if not self.ws_url:
                self.ws_url = "wss://stream.bybit.com/v5/public/linear" if not self.testnet else "wss://stream-testnet.bybit.com/v5/public/linear"
            if not self.ws_private_url:
                self.ws_private_url = "wss://stream.bybit.com/v5/private" if not self.testnet else "wss://stream-testnet.bybit.com/v5/private"
            if not self.rest_url:
                self.rest_url = "https://api.bybit.com" if not self.testnet else "https://api-testnet.bybit.com"


@dataclass
class DatabaseConfig:
    """数据库配置"""
    # Redis
    redis_url: str = "redis://localhost:6379"
    redis_password: str = ""
    redis_db: int = 0
    
    # SQLite
    sqlite_path: str = "data/unitrade.db"
    data_retention_days: int = 30


@dataclass
class AnalyticsConfig:
    """分析引擎配置"""
    # OBI 计算
    obi_depth_levels: int = 10
    
    # CVD 计算
    cvd_history_size: int = 1000
    
    # 波动率计算
    volatility_window_seconds: int = 60
    volatility_sample_interval: float = 1.0
    
    # OI 分析
    oi_poll_interval: float = 5.0  # Binance OI 轮询间隔
    oi_history_size: int = 60
    oi_change_threshold: float = 0.005  # 0.5%
    price_change_threshold: float = 0.001  # 0.1%


@dataclass
class Config:
    """UniTrade 主配置"""
    # 交易所配置
    exchanges: Dict[str, ExchangeConfig] = field(default_factory=dict)
    
    # 数据库配置
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    
    # 分析引擎配置
    analytics: AnalyticsConfig = field(default_factory=AnalyticsConfig)
    
    # 日志配置
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Realtime Service Config (Added dynamically)
    realtime: Dict = field(default_factory=dict)

    # Telegram (global) config (YAML passthrough)
    telegram: Dict = field(default_factory=dict)
    
    # Scanner 配置 (币种筛选规则)
    scanner: Dict = field(default_factory=dict)

    # Module configs (YAML passthrough)
    anomaly_detector: Dict = field(default_factory=dict)
    squeeze_scanner: Dict = field(default_factory=dict)
    oi_spike_scanner: Dict = field(default_factory=dict)
    signal_detector: Dict = field(default_factory=dict)
    
    # EMA Radar 配置
    ema_radar: Dict = field(default_factory=dict)
    
    # WaveTrend Scanner 配置
    wavetrend_scanner: Dict = field(default_factory=dict)
    
    @classmethod
    def from_yaml(cls, path: str) -> "Config":
        """从 YAML 文件加载配置"""
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        
        return cls._from_dict(data)
    
    @classmethod
    def from_env(cls) -> "Config":
        """从环境变量加载配置"""
        config = cls()
        
        # Redis
        if redis_url := os.getenv("REDIS_URL"):
            config.database.redis_url = redis_url
        
        # Binance
        if os.getenv("BINANCE_API_KEY"):
            config.exchanges["binance"] = ExchangeConfig(
                name="binance",
                api_key=os.getenv("BINANCE_API_KEY", ""),
                api_secret=os.getenv("BINANCE_API_SECRET", ""),
                testnet=os.getenv("BINANCE_TESTNET", "").lower() == "true",
                symbols=os.getenv("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT").split(","),
            )
        
        # Bybit
        if os.getenv("BYBIT_API_KEY"):
            config.exchanges["bybit"] = ExchangeConfig(
                name="bybit",
                api_key=os.getenv("BYBIT_API_KEY", ""),
                api_secret=os.getenv("BYBIT_API_SECRET", ""),
                testnet=os.getenv("BYBIT_TESTNET", "").lower() == "true",
                symbols=os.getenv("BYBIT_SYMBOLS", "BTCUSDT,ETHUSDT").split(","),
            )
        
        # Log level
        if log_level := os.getenv("LOG_LEVEL"):
            config.log_level = log_level
        
        return config
    
    @classmethod
    def _from_dict(cls, data: Dict) -> "Config":
        """从字典创建配置"""
        config = cls()

        def _filter_kwargs(dc_cls, raw: dict) -> dict:
            allowed = {f.name for f in dataclass_fields(dc_cls) if f.init}
            return {k: v for k, v in raw.items() if k in allowed}
        
        # 解析交易所配置
        for name, exc_data in data.get("exchanges", {}).items():
            if not isinstance(exc_data, dict):
                continue
            if exc_data.get("enabled", True) is False:
                continue
            kwargs = _filter_kwargs(ExchangeConfig, exc_data)
            kwargs.pop("name", None)
            config.exchanges[name] = ExchangeConfig(name=name, **kwargs)
        
        # 解析数据库配置
        if db_data := data.get("database"):
            config.database = DatabaseConfig(**db_data)
        
        # 解析分析配置
        if analytics_data := data.get("analytics"):
            config.analytics = AnalyticsConfig(**analytics_data)

        # 解析实时数据配置
        config.realtime = data.get("realtime", {})

        # Telegram (pass-through; modules can interpret enabled switches)
        config.telegram = data.get("telegram", {})
        
        # 解析 Scanner 配置
        config.scanner = data.get("scanner", {})

        # Module configs (pass-through)
        config.anomaly_detector = data.get("anomaly_detector", {})
        config.squeeze_scanner = data.get("squeeze_scanner", {})
        config.oi_spike_scanner = data.get("oi_spike_scanner", {})
        config.signal_detector = data.get("signal_detector", {})
        
        # 解析 EMA Radar 配置
        config.ema_radar = data.get("ema_radar", {})
        
        # 解析 WaveTrend Scanner 配置
        config.wavetrend_scanner = data.get("wavetrend_scanner", {})
        
        # 日志配置
        config.log_level = data.get("log_level", "INFO")
        
        return config
    
    def get_exchange(self, name: str) -> Optional[ExchangeConfig]:
        """获取交易所配置"""
        return self.exchanges.get(name)


def load_config(config_path: Optional[str] = None) -> Config:
    """
    加载配置
    
    优先级: 环境变量 > 配置文件 > 默认值
    """
    config = Config()
    resolved_path = resolve_config_path(config_path)
    if resolved_path is not None:
        config = Config.from_yaml(str(resolved_path))
    
    # 环境变量覆盖
    env_config = Config.from_env()
    
    # 合并交易所配置
    for name, exc in env_config.exchanges.items():
        config.exchanges[name] = exc
    
    # 合并数据库配置
    if os.getenv("REDIS_URL"):
        config.database.redis_url = env_config.database.redis_url
    
    return config


def resolve_config_path(config_path: Optional[str] = None) -> Optional[Path]:
    """
    Resolve config file path robustly (supports running from outside project root).

    Priority:
      1) env UNITRADE_CONFIG
      2) given config_path (absolute/relative)
      3) cwd config/default.yaml
      4) project_root/config/default.yaml (relative to this module)
    """
    candidates: list[Path] = []

    env_path = os.getenv("UNITRADE_CONFIG")
    if env_path:
        candidates.append(Path(env_path))

    if config_path:
        p = Path(config_path)
        candidates.append(p)
        if not p.is_absolute():
            project_root = Path(__file__).resolve().parents[3]
            candidates.append(project_root / p)

    candidates.append(Path("config/default.yaml"))
    candidates.append(Path(__file__).resolve().parents[3] / "config" / "default.yaml")

    for p in candidates:
        try:
            if p.exists():
                return p
        except Exception:
            continue

    return None
