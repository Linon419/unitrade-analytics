"""
Redis State Manager - 高性能状态存储

Key Design:
- signal:vol:{symbol}:1m    - Sorted Set (滑动窗口成交量)
- signal:flow:{symbol}:5m   - Hash (资金流)
- signal:low:{symbol}:{n}d  - String (N日最低价)
- signal:lock:{symbol}      - String (防抖锁)
"""

import asyncio
import time
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


@dataclass
class SignalConfig:
    """信号配置"""
    # 滑动窗口
    rvol_window_minutes: int = 60  # RVOL 计算窗口 (60分钟)
    flow_window_minutes: int = 5   # 资金流窗口 (5分钟)
    
    # 低点缓存
    rebound_days: List[int] = None  # 反弹计算天数 [1, 3, 7, 14, 30]
    
    # 防抖
    debounce_minutes: int = 5  # 同币种防抖时间
    
    # 阈值
    rvol_threshold: float = 2.0     # 量能倍数阈值
    flow_threshold: float = 100000  # 净流入阈值 (USDT)
    rebound_threshold: float = 5.0  # 反弹幅度阈值 (%)
    
    def __post_init__(self):
        if self.rebound_days is None:
            self.rebound_days = [1, 3, 7, 14, 30]


class RedisStateManager:
    """
    Redis 状态管理器
    
    使用 Pipeline 批量操作提高性能
    Redis 不可用时使用内存 fallback
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379", config: SignalConfig = None):
        self.redis_url = redis_url
        self.config = config or SignalConfig()
        self._redis: Optional[aioredis.Redis] = None
        self._connected = False
        # 内存 fallback 用于防抖锁
        self._memory_locks: Dict[str, float] = {}
        self._memory_thresholds: Dict[str, int] = {}
        self._memory_first_ranked: Dict[str, Tuple[float, float]] = {}
    
    async def connect(self):
        """连接 Redis (失败时使用内存 fallback)"""
        try:
            self._redis = await aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=3
            )
            # 测试连接
            await self._redis.ping()
            self._connected = True
            logger.info(f"Redis connected: {self.redis_url}")
        except Exception as e:
            logger.warning(f"Redis unavailable ({e}), using in-memory fallback")
            self._connected = False
    
    async def close(self):
        """关闭连接"""
        if self._redis and self._connected:
            await self._redis.close()
    
    async def try_acquire_lock(self, symbol: str, threshold_level: int = 0) -> bool:
        """
        尝试获取防抖锁 (Redis 或内存)
        """
        if self._connected:
            return await self._redis_try_lock(symbol, threshold_level)
        else:
            return self._memory_try_lock(symbol, threshold_level)
    
    def _memory_try_lock(self, symbol: str, threshold_level: int) -> bool:
        """内存防抖锁"""
        import time
        now = time.time()
        
        # 检查现有锁
        if symbol in self._memory_locks:
            lock_time = self._memory_locks[symbol]
            if now - lock_time < self.config.debounce_minutes * 60:
                # 检查阈值
                current_level = self._memory_thresholds.get(symbol, 0)
                if current_level >= threshold_level:
                    return False  # 被防抖
        
        # 获取锁
        self._memory_locks[symbol] = now
        self._memory_thresholds[symbol] = threshold_level
        return True
    
    async def _redis_try_lock(self, symbol: str, threshold_level: int) -> bool:
        """Redis 防抖锁"""
        lock_key = f"signal:lock:{symbol}"
        threshold_key = f"signal:alert:{symbol}:threshold"
        
        # 检查是否已有锁
        current_threshold = await self._redis.get(threshold_key)
        if current_threshold is not None:
            if int(current_threshold) >= threshold_level:
                return False  # 未突破更高阈值
        
        # 尝试获取锁
        acquired = await self._redis.set(
            lock_key, 
            "1", 
            nx=True, 
            ex=self.config.debounce_minutes * 60
        )
        
        if acquired:
            # 记录已触发的阈值等级
            await self._redis.set(threshold_key, str(threshold_level), ex=3600)
            return True
        
        return False
    
    # ========== 成交量滑动窗口 ==========
    
    async def add_volume(self, symbol: str, volume: float, timestamp: float = None):
        """
        添加成交量到滑动窗口
        
        使用 Sorted Set, score = timestamp, value = volume
        """
        if timestamp is None:
            timestamp = time.time()
        
        key = f"signal:vol:{symbol}:1m"
        
        pipe = self._redis.pipeline()
        # 添加新数据
        pipe.zadd(key, {f"{timestamp}:{volume}": timestamp})
        # 移除过期数据 (窗口外)
        cutoff = timestamp - (self.config.rvol_window_minutes * 60)
        pipe.zremrangebyscore(key, "-inf", cutoff)
        # 设置 TTL
        pipe.expire(key, 86400)  # 24小时
        
        await pipe.execute()
    
    async def get_volume_history(self, symbol: str, minutes: int = 60) -> List[Tuple[float, float]]:
        """
        获取历史成交量
        
        Returns: [(timestamp, volume), ...]
        """
        key = f"signal:vol:{symbol}:1m"
        cutoff = time.time() - (minutes * 60)
        
        data = await self._redis.zrangebyscore(key, cutoff, "+inf")
        
        result = []
        for item in data:
            try:
                ts_str, vol_str = item.split(":")
                result.append((float(ts_str), float(vol_str)))
            except ValueError:
                continue
        
        return result
    
    async def get_avg_volume(self, symbol: str, minutes: int = 60) -> float:
        """获取平均成交量"""
        history = await self.get_volume_history(symbol, minutes)
        if not history:
            return 0.0
        return sum(v for _, v in history) / len(history)
    
    # ========== 资金流 ==========
    
    async def update_flow(self, symbol: str, buy_vol: float, sell_vol: float):
        """
        更新资金流
        
        使用 Hash 存储: {buy_vol, sell_vol, net, updated_at}
        """
        key = f"signal:flow:{symbol}:5m"
        now = time.time()
        
        # 获取现有数据
        existing = await self._redis.hgetall(key)
        
        # 检查是否需要重置 (超过窗口时间)
        last_update = float(existing.get("updated_at", 0))
        if now - last_update > self.config.flow_window_minutes * 60:
            # 重置
            existing = {"buy_vol": "0", "sell_vol": "0"}
        
        # 累加
        new_buy = float(existing.get("buy_vol", 0)) + buy_vol
        new_sell = float(existing.get("sell_vol", 0)) + sell_vol
        net = new_buy - new_sell
        
        # 保存
        await self._redis.hset(key, mapping={
            "buy_vol": str(new_buy),
            "sell_vol": str(new_sell),
            "net": str(net),
            "updated_at": str(now)
        })
        await self._redis.expire(key, 600)  # 10分钟过期
        
        return {"buy_vol": new_buy, "sell_vol": new_sell, "net": net}
    
    async def get_flow(self, symbol: str) -> Dict[str, float]:
        """获取资金流"""
        key = f"signal:flow:{symbol}:5m"
        data = await self._redis.hgetall(key)
        
        return {
            "buy_vol": float(data.get("buy_vol", 0)),
            "sell_vol": float(data.get("sell_vol", 0)),
            "net": float(data.get("net", 0))
        }
    
    # ========== 历史低点缓存 ==========
    
    async def set_low_price(self, symbol: str, days: int, price: float):
        """设置 N 日最低价"""
        key = f"signal:low:{symbol}:{days}d"
        await self._redis.set(key, str(price), ex=86400)
    
    async def get_low_price(self, symbol: str, days: int) -> Optional[float]:
        """获取 N 日最低价"""
        key = f"signal:low:{symbol}:{days}d"
        val = await self._redis.get(key)
        return float(val) if val else None
    
    async def set_low_prices_batch(self, symbol: str, lows: Dict[int, float]):
        """批量设置最低价"""
        pipe = self._redis.pipeline()
        for days, price in lows.items():
            key = f"signal:low:{symbol}:{days}d"
            pipe.set(key, str(price), ex=86400)
        await pipe.execute()
    
    # ========== 防抖锁 ==========
    
    async def try_acquire_lock(self, symbol: str, threshold_level: int = 0) -> bool:
        """
        尝试获取防抖锁
        
        使用 SET NX EX 原子操作
        threshold_level: 阈值等级, 只有突破更高等级才允许重复通知
        """
        lock_key = f"signal:lock:{symbol}"
        threshold_key = f"signal:alert:{symbol}:threshold"
        
        # 检查是否已有锁
        current_threshold = await self._redis.get(threshold_key)
        if current_threshold is not None:
            if int(current_threshold) >= threshold_level:
                return False  # 未突破更高阈值
        
        # 尝试获取锁
        acquired = await self._redis.set(
            lock_key, 
            "1", 
            nx=True, 
            ex=self.config.debounce_minutes * 60
        )
        
        if acquired:
            # 记录已触发的阈值等级
            await self._redis.set(threshold_key, str(threshold_level), ex=3600)
            return True
        
        return False
    
    async def release_lock(self, symbol: str):
        """释放锁"""
        await self._redis.delete(f"signal:lock:{symbol}")
    
    # ========== 批量操作 ==========
    
    async def get_symbol_state(self, symbol: str) -> Dict:
        """获取币种完整状态 (Pipeline 批量查询)"""
        pipe = self._redis.pipeline()
        
        # 成交量
        vol_key = f"signal:vol:{symbol}:1m"
        cutoff = time.time() - 3600
        pipe.zrangebyscore(vol_key, cutoff, "+inf")
        
        # 资金流
        flow_key = f"signal:flow:{symbol}:5m"
        pipe.hgetall(flow_key)
        
        # 低点
        for days in self.config.rebound_days:
            pipe.get(f"signal:low:{symbol}:{days}d")
        
        results = await pipe.execute()
        
        return {
            "volume_history": results[0],
            "flow": results[1],
            "lows": {
                days: float(results[2 + i]) if results[2 + i] else None
                for i, days in enumerate(self.config.rebound_days)
            }
        }
    
    # ========== Anomaly Detector Integration ==========
    
    async def store_breakout_signal(
        self, 
        symbol: str, 
        oi_change_pct: float,
        rvol: float,
        price: float,
        ema200: float,
        timeframe: str,
        prefix: str = "anomaly",
        ttl_days: int = 5
    ) -> None:
        """
        存储突破信号到 Redis (用于上涨指数排行)
        
        Args:
            symbol: 交易对
            oi_change_pct: OI 变化百分比
            rvol: 放量倍数
            price: 当前价格
            ema200: EMA200 值
            timeframe: 时间周期
            prefix: Redis key 前缀
            ttl_days: 过期天数
        """
        if not self._connected:
            return
        
        key = f"{prefix}:signals:{symbol}"
        now = time.time()
        signal_data = f"{oi_change_pct}|{rvol}|{price}|{ema200}|{timeframe}"
        
        ttl = ttl_days * 24 * 3600
        
        pipe = self._redis.pipeline()
        pipe.zadd(key, {signal_data: now})
        pipe.expire(key, ttl)
        pipe.zremrangebyscore(key, "-inf", now - ttl)
        await pipe.execute()
    
    async def get_breakout_signals(
        self,
        symbol: str,
        prefix: str = "anomaly"
    ) -> List[Tuple[str, float]]:
        """
        获取币种的历史突破信号
        
        Returns: [(signal_data, timestamp), ...]
        """
        if not self._connected:
            return []
        
        key = f"{prefix}:signals:{symbol}"
        return await self._redis.zrangebyscore(key, "-inf", "+inf", withscores=True)
    
    async def scan_signal_keys(self, prefix: str = "anomaly") -> List[str]:
        """扫描所有有信号的币种 key"""
        if not self._connected:
            return []
        
        pattern = f"{prefix}:signals:*"
        keys = []
        async for key in self._redis.scan_iter(match=pattern):
            keys.append(key)
        return keys

    # ========== Rising Index Metadata ==========

    @staticmethod
    def _first_ranked_key(symbol: str, prefix: str = "anomaly") -> str:
        return f"{prefix}:rising_index:first_ranked:{symbol}"

    @staticmethod
    def _encode_first_ranked(ts: float, price: float) -> str:
        return f"{float(ts)}|{float(price)}"

    @staticmethod
    def _decode_first_ranked(value: str) -> Optional[Tuple[float, float]]:
        try:
            parts = str(value).split("|")
            if len(parts) != 2:
                return None
            return float(parts[0]), float(parts[1])
        except Exception:
            return None

    async def get_or_set_first_ranked(
        self,
        symbol: str,
        ts: float,
        price: float,
        prefix: str = "anomaly",
    ) -> Tuple[float, float]:
        """
        Return the first time this symbol entered the rising index ranking.

        Stored as a string value: \"<ts>|<price>\".
        - Uses Redis when available (persistent across restarts).
        - Falls back to in-memory storage when Redis is unavailable.
        """
        key = self._first_ranked_key(symbol=symbol, prefix=prefix)

        if not self._connected or not self._redis:
            existing = self._memory_first_ranked.get(key)
            if existing is not None:
                return existing
            self._memory_first_ranked[key] = (float(ts), float(price))
            return self._memory_first_ranked[key]

        value = self._encode_first_ranked(ts, price)
        try:
            created = await self._redis.set(key, value, nx=True)
            if created:
                return float(ts), float(price)

            existing = await self._redis.get(key)
            if existing:
                decoded = self._decode_first_ranked(existing)
                if decoded is not None:
                    return decoded
        except Exception:
            existing = self._memory_first_ranked.get(key)
            if existing is not None:
                return existing
            self._memory_first_ranked[key] = (float(ts), float(price))
            return self._memory_first_ranked[key]

        # Unexpected (empty/malformed Redis value) -> reset to provided values.
        try:
            await self._redis.set(key, value)
        except Exception:
            pass
        return float(ts), float(price)
    
    async def check_anomaly_cooldown(
        self,
        symbol: str,
        timeframe: str,
        cooldown_seconds: int = 1800,
        prefix: str = "anomaly"
    ) -> bool:
        """
        检查并设置异动信号冷却锁
        
        Returns: True 表示可以发送信号, False 表示在冷却中
        """
        if not self._connected:
            # 内存 fallback
            lock_key = f"{prefix}:{symbol}:{timeframe}"
            now = time.time()
            if lock_key in self._memory_locks:
                if now - self._memory_locks[lock_key] < cooldown_seconds:
                    return False
            self._memory_locks[lock_key] = now
            return True
        
        key = f"{prefix}:cooldown:{symbol}:{timeframe}"
        acquired = await self._redis.set(
            key, "1",
            ex=cooldown_seconds,
            nx=True
        )
        return acquired is not None

