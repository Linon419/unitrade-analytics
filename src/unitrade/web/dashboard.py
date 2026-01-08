"""
Web Dashboard - 增强版 HTML 仪表板

功能:
1. Top 10 币种卡片
2. OI 变化趋势
3. 资金流向 (CVD)
4. EMA 趋势雷达
5. 多空比历史图表
6. 最近清算事件
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web
from unitrade.web.cvd_service import CVDAnalysisService
from unitrade.analytics.report import MarketReportGenerator
from unitrade.order import BinanceRateLimiter

logger = logging.getLogger(__name__)


@dataclass
class DashboardConfig:
    """Dashboard 配置"""
    host: str = "0.0.0.0"
    port: int = 8080
    symbols: List[str] = None
    enable_realtime: bool = True

    # Binance REST 安全阈值 (防止触发 429/418)
    binance_cache_ttl_seconds: int = 60
    binance_max_inflight: int = 5
    
    def __post_init__(self):
        if self.symbols is None:
            self.symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", 
                           "XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT"]


class WebDashboard:
    """增强版 Web 仪表板 (集成实时 OBI/CVD/波动率)"""
    
    def __init__(self, config: Optional[DashboardConfig] = None):
        self.config = config or DashboardConfig()
        self.app = web.Application()
        self._runner: Optional[web.AppRunner] = None
        self._session: Optional[aiohttp.ClientSession] = None  # 共享 session

        # Binance REST 防封禁: 全局限速 + 并发限制 + 断路器 + 缓存
        self._binance_rate_limiter = BinanceRateLimiter()
        self._binance_inflight = asyncio.Semaphore(max(1, int(self.config.binance_max_inflight)))
        self._binance_blocked_until: float = 0.0
        self._binance_block_reason: str = ""
        self._cache: Dict[str, Tuple[float, Any]] = {}
        self._ema_lock = asyncio.Lock()
        self._wavetrend_lock = asyncio.Lock()

        self._realtime_service = None  # 实时数据服务
        self.cvd_service = CVDAnalysisService()
        self._setup_routes()

    def _cache_get(self, key: str) -> Optional[Any]:
        entry = self._cache.get(key)
        if not entry:
            return None
        expires_at, value = entry
        if time.time() >= expires_at:
            self._cache.pop(key, None)
            return None
        return value

    def _cache_set(self, key: str, value: Any, ttl_seconds: int) -> Any:
        self._cache[key] = (time.time() + max(0, int(ttl_seconds)), value)
        return value

    def _binance_cache_key(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
        params = params or {}
        items = "&".join(f"{k}={params[k]}" for k in sorted(params))
        return f"binance:{path}?{items}"

    async def _binance_get_json(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        cache_ttl_seconds: Optional[int] = None,
    ) -> Any:
        """
        统一 Binance 公共 REST 请求入口 (防 418/429):
        - 全局权重限速 (BinanceRateLimiter)
        - 并发限制 (binance_max_inflight)
        - 断路器 (418 后在 Retry-After 时间内不再请求)
        - 响应缓存 (TTL)
        """
        if not self._session:
            raise web.HTTPServiceUnavailable(text="Session not ready")

        # Respect config switch: exchanges.binance.enabled
        try:
            from pathlib import Path
            from unitrade.core.config import load_config

            if Path("config/default.yaml").exists():
                conf = load_config("config/default.yaml")
                if conf.get_exchange("binance") is None:
                    raise web.HTTPServiceUnavailable(text="Binance disabled (exchanges.binance.enabled=false)")
        except web.HTTPException:
            raise
        except Exception:
            # If config can't be loaded, keep backward-compatible behavior (assume enabled)
            pass

        now = time.time()
        if now < self._binance_blocked_until:
            raise web.HTTPServiceUnavailable(
                text=f"Binance IP banned/backoff ({self._binance_block_reason}), retry in {int(self._binance_blocked_until - now)}s"
            )

        params = params or {}
        ttl = int(cache_ttl_seconds if cache_ttl_seconds is not None else self.config.binance_cache_ttl_seconds)
        cache_key = self._binance_cache_key(path, params)
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        url = f"https://fapi.binance.com{path}"

        async with self._binance_inflight:
            await self._binance_rate_limiter.acquire(path, params=params)

            async with self._session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return self._cache_set(cache_key, data, ttl)

                if resp.status in (418, 429):
                    retry_after = int(resp.headers.get("Retry-After", "60") or "60")
                    self._binance_rate_limiter.handle_429(retry_after)
                    self._binance_blocked_until = time.time() + retry_after
                    self._binance_block_reason = f"HTTP {resp.status}"
                    raise web.HTTPServiceUnavailable(text=f"Binance rate limited/banned: HTTP {resp.status}, retry_after={retry_after}s")

                text = await resp.text()
                raise web.HTTPBadGateway(text=f"Binance error: HTTP {resp.status}: {text}")
    
    def _setup_routes(self):
        """设置路由"""
        self.app.router.add_get("/", self._handle_index)
        self.app.router.add_get("/health", self._handle_health)
        self.app.router.add_get("/api/market/{symbol}", self._handle_market)
        self.app.router.add_get("/api/markets", self._handle_markets)
        self.app.router.add_get("/api/ema", self._handle_ema)
        self.app.router.add_get("/api/ratios/{symbol}", self._handle_ratios)
        self.app.router.add_get("/api/oi/{symbol}", self._handle_oi_history)
        self.app.router.add_get("/api/top", self._handle_top_coins)
        self.app.router.add_get("/api/oi-spikes", self._handle_oi_spikes)
        # 实时数据 API
        self.app.router.add_get("/api/realtime/obi", self._handle_realtime_obi)
        self.app.router.add_get("/api/realtime/cvd", self._handle_realtime_cvd)
        self.app.router.add_get("/api/realtime/volatility", self._handle_realtime_volatility)
        self.app.router.add_get("/api/realtime/all", self._handle_realtime_all)
        self.app.router.add_get("/api/cvd/analysis", self._handle_cvd_analysis)
        self.app.router.add_get("/api/report", self._handle_report)
        self.app.router.add_get("/api/wavetrend", self._handle_wavetrend)
        self.app.router.add_get("/api/wavetrend/scatter", self._handle_wavetrend_scatter)
        self.app.router.add_get("/wavetrend", self._handle_wavetrend_page)
        # TradingView 图表页面
        self.app.router.add_get("/chart", self._handle_chart_page)
        self.app.router.add_get("/api/chart/klines", self._handle_chart_klines)
        # 上涨指数排行
        self.app.router.add_get("/api/rising-index", self._handle_rising_index)
    
    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
        try:
            if self.config.enable_realtime:
                from unitrade.web.realtime_service import start_realtime_service

                self._realtime_service = await start_realtime_service()
                self.report_gen = MarketReportGenerator(self._session, self._realtime_service, self.cvd_service)
                if self._realtime_service is not None:
                    logger.info("Realtime data service started")
                else:
                    logger.info("Realtime data service disabled (config)")
            else:
                self._realtime_service = None
                self.report_gen = MarketReportGenerator(self._session, None, self.cvd_service)
                logger.info("Realtime data service disabled (cli)")
        except Exception as e:
            logger.warning(f"Failed to start realtime service: {e}")

        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.config.host, self.config.port)
        await site.start()
        logger.info(f"Dashboard running at http://localhost:{self.config.port}")
    
    async def stop(self) -> None:
        # 停止实时数据服务
        if self._realtime_service:
            try:
                from unitrade.web.realtime_service import stop_realtime_service
                await stop_realtime_service()
            except Exception as e:
                logger.warning(f"Failed to stop realtime service: {e}")
        
        if self.cvd_service:
            await self.cvd_service.close()
        
        if self._session:
            await self._session.close()  # 关闭共享 session
        if self._runner:
            await self._runner.cleanup()
        logger.info("Dashboard stopped")
    
    async def _handle_index(self, request: web.Request) -> web.Response:
        html = self._generate_html()
        return web.Response(text=html, content_type="text/html")

    async def _handle_health(self, request: web.Request) -> web.Response:
        realtime_running = False
        if self._realtime_service is not None:
            realtime_running = bool(getattr(self._realtime_service, "is_running", False))

        return web.json_response({
            "status": "ok",
            "realtime_running": realtime_running,
            "timestamp": datetime.now().isoformat(),
        })
    
    async def _handle_market(self, request: web.Request) -> web.Response:
        from unitrade.tracker import MarketReporter
        
        symbol = request.match_info.get("symbol", "BTCUSDT").upper()

        cache_key = f"api:market:{symbol}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return web.json_response(cached)

        reporter = MarketReporter()
        await reporter.start()
        
        try:
            report = await reporter.generate_report(symbol)
            if report:
                payload = {
                    "symbol": report.symbol,
                    "price": report.price,
                    "price_change": report.price_change_pct,
                    "funding_rate": report.funding_rate * 100,
                    "oi_quantity": report.oi_quantity,
                    "oi_value": report.oi_value,
                    "long_short_ratio": report.long_short_ratio,
                    "top_trader_ratio": report.top_trader_ratio,
                    "top_position_ratio": report.top_position_ratio,
                    "timestamp": datetime.now().isoformat(),
                }
                self._cache_set(cache_key, payload, ttl_seconds=60)
                return web.json_response(payload)
            return web.json_response({"error": "Failed"}, status=500)
        finally:
            await reporter.stop()
    
    async def _handle_markets(self, request: web.Request) -> web.Response:
        """批量获取多个币种数据"""
        symbols = self.config.symbols
        results = []

        tickers = await self._binance_get_json("/fapi/v1/ticker/24hr", cache_ttl_seconds=30)
        ticker_map = {t["symbol"]: t for t in tickers} if isinstance(tickers, list) else {}

        for symbol in symbols:
            if symbol in ticker_map:
                t = ticker_map[symbol]
                results.append({
                    "symbol": symbol,
                    "price": float(t["lastPrice"]),
                    "change": float(t["priceChangePercent"]),
                    "volume": float(t["quoteVolume"]),
                })

        return web.json_response({"markets": results})
    
    async def _handle_ema(self, request: web.Request) -> web.Response:
        from unitrade.scanner import EMARadar, EMARadarConfig
        from unitrade.core.config import load_config
        
        timeframe = request.query.get("tf", "1h")
        cache_key = f"api:ema:{timeframe}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return web.json_response(cached)
        
        # 从全局配置加载 EMA Radar 设置
        try:
            config = load_config("config/default.yaml")
            ema_radar_enabled = bool((config.ema_radar or {}).get("enabled", True))
            radar_config = EMARadarConfig.from_config(config)
        except Exception as e:
            logger.warning(f"Failed to load config, using defaults: {e}")
            ema_radar_enabled = True
            radar_config = EMARadarConfig()

        if not ema_radar_enabled:
            payload = {
                "disabled": True,
                "reason": "ema_radar.enabled=false",
                "timeframe": timeframe,
                "uptrend": [],
                "downtrend": [],
                "new_uptrend": [],
                "new_downtrend": [],
                "new_bullish_flowering": [],
                "new_bearish_flowering": [],
                "cached_at": datetime.now().isoformat(),
            }
            self._cache_set(cache_key, payload, ttl_seconds=300)
            return web.json_response(payload)
        
        async with self._ema_lock:
            cached = self._cache_get(cache_key)
            if cached is not None:
                return web.json_response(cached)

            radar = EMARadar(radar_config)
            await radar.start()
            try:
                results = await radar.scan(timeframe)
                payload = {
                    "timeframe": timeframe,
                    "uptrend": [s.to_dict() for s in results["uptrend"]],
                    "downtrend": [s.to_dict() for s in results["downtrend"]],
                    "new_uptrend": [s.to_dict() for s in results.get("new_uptrend", [])],
                    "new_downtrend": [s.to_dict() for s in results.get("new_downtrend", [])],
                    "new_bullish_flowering": [s.to_dict() for s in results.get("new_bullish_flowering", [])],
                    "new_bearish_flowering": [s.to_dict() for s in results.get("new_bearish_flowering", [])],
                    "cached_at": datetime.now().isoformat(),
                }
                self._cache_set(cache_key, payload, ttl_seconds=300)
                return web.json_response(payload)
            finally:
                await radar.stop()
    
    async def _handle_wavetrend(self, request: web.Request) -> web.Response:
        """WaveTrend 波段信号扫描 API"""
        from unitrade.scanner import WaveTrendScanner, WaveTrendConfig
        from unitrade.core.config import load_config
        
        timeframe = request.query.get("tf", "1h")
        cache_key = f"api:wavetrend:{timeframe}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return web.json_response(cached)
        
        # 从配置加载
        try:
            config = load_config("config/default.yaml")
            wavetrend_enabled = bool((config.wavetrend_scanner or {}).get("enabled", True))
            wt_config = WaveTrendConfig.from_config(config)
        except Exception as e:
            logger.warning(f"Failed to load WaveTrend config: {e}")
            wavetrend_enabled = True
            wt_config = WaveTrendConfig()

        if not wavetrend_enabled:
            payload = {
                "disabled": True,
                "reason": "wavetrend_scanner.enabled=false",
                "timeframe": timeframe,
                "overbought": [],
                "oversold": [],
                "exit_overbought": [],
                "exit_oversold": [],
                "squeeze": [],
                "divergence": [],
                "cached_at": datetime.now().isoformat(),
            }
            self._cache_set(cache_key, payload, ttl_seconds=300)
            return web.json_response(payload)
        
        async with self._wavetrend_lock:
            cached = self._cache_get(cache_key)
            if cached is not None:
                return web.json_response(cached)

            scanner = WaveTrendScanner(wt_config)
            await scanner.start()
            try:
                results = await scanner.scan(timeframe)
                payload = {
                    "timeframe": timeframe,
                    "overbought": [s.to_dict() for s in results.get("overbought", [])],
                    "oversold": [s.to_dict() for s in results.get("oversold", [])],
                    "exit_overbought": [s.to_dict() for s in results.get("exit_overbought", [])],
                    "exit_oversold": [s.to_dict() for s in results.get("exit_oversold", [])],
                    "squeeze": [s.to_dict() for s in results.get("squeeze", [])],
                    "divergence": [s.to_dict() for s in results.get("divergence", [])],
                    "cached_at": datetime.now().isoformat(),
                }
                self._cache_set(cache_key, payload, ttl_seconds=300)
                return web.json_response(payload)
            except Exception as e:
                logger.error(f"WaveTrend scan error: {e}", exc_info=True)
                return web.json_response({"error": str(e)}, status=500)
            finally:
                await scanner.stop()
    
    async def _handle_wavetrend_scatter(self, request: web.Request) -> web.Response:
        """WaveTrend 散点图数据 API - 返回所有币种的 OSC 值"""
        from unitrade.scanner import WaveTrendScanner, WaveTrendConfig
        from unitrade.core.config import load_config
        import numpy as np
        
        timeframe = request.query.get("tf", "4h")
        
        try:
            config = load_config("config/default.yaml")
            wavetrend_enabled = bool((config.wavetrend_scanner or {}).get("enabled", True))
            wt_config = WaveTrendConfig.from_config(config)
        except Exception:
            wavetrend_enabled = True
            wt_config = WaveTrendConfig()

        if not wavetrend_enabled:
            return web.json_response(
                {"disabled": True, "reason": "wavetrend_scanner.enabled=false", "timeframe": timeframe, "points": []}
            )
        
        scanner = WaveTrendScanner(wt_config)
        await scanner.start()
        
        try:
            # 获取币种列表
            symbols = await scanner._discover_symbols()
            
            # 使用并发处理加速
            async def process_symbol(symbol):
                try:
                    klines = await scanner._fetch_klines(symbol, timeframe, 150)
                    if len(klines) >= 50:
                        opens = np.array([float(k[1]) for k in klines])
                        highs = np.array([float(k[2]) for k in klines])
                        lows = np.array([float(k[3]) for k in klines])
                        closes = np.array([float(k[4]) for k in klines])
                        volumes = np.array([float(k[5]) for k in klines])
                        
                        # 使用 Heikin Ashi 数据 (与 TradingView 指标一致)
                        if wt_config.use_ha:
                            ha_open, ha_high, ha_low, ha_close = scanner._calc_heikin_ashi(opens, highs, lows, closes)
                            hlc3 = (ha_high + ha_low + ha_close) / 3
                            src_open = ha_open
                            src_close = ha_close
                        else:
                            hlc3 = (highs + lows + closes) / 3
                            src_open = opens
                            src_close = closes
                        
                        osc = scanner._calc_wavetrend(hlc3, closes, volumes)
                        
                        # 检测背离 (带蜡烛确认，使用 HA 数据判断阴阳线)
                        div_signals = scanner._detect_divergence(osc, highs, lows, volumes, symbol, timeframe, float(closes[-1]), src_open, src_close)
                        
                        div_info = None
                        for sig in div_signals:
                            if sig.signal_type == "bull_div":
                                div_info = {"type": "多", "level": int(sig.level) if sig.level else 1, "m_plus": bool(sig.is_m_plus)}
                            elif sig.signal_type == "bear_div":
                                div_info = {"type": "空", "level": int(sig.level) if sig.level else 1, "m_plus": bool(sig.is_m_plus)}
                        
                        # 🆕 计算当前连续超买/超卖 K 线数
                        zone_type, zone_streak = scanner._calc_ob_os_streak(osc, wt_config.lvl_1)
                        
                        # 🆕 计算连对次数 (连续正确预测)
                        accuracy = scanner._calc_signal_accuracy(osc, closes, wt_config.lvl_1)
                        
                        return {
                            "symbol": symbol.replace("USDT", ""),
                            "osc": round(float(osc[-1]), 1),
                            "div": div_info,
                            "zone_type": zone_type,  # "ob" / "os" / "neutral"
                            "zone_streak": zone_streak,  # 连续 K 线数
                            "accuracy": accuracy,  # 连对次数
                        }
                except Exception as e:
                    logger.debug(f"Error processing {symbol}: {e}")
                return None
            
            # 使用配置中的 top_n 值
            scan_limit = wt_config.auto_top_n
            tasks = [process_symbol(s) for s in symbols[:scan_limit]]
            results = await asyncio.gather(*tasks)
            scatter_data = [r for r in results if r is not None]
            
            return web.json_response({
                "timeframe": timeframe,
                "data": scatter_data,
            })
        except Exception as e:
            logger.error(f"Scatter API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)
        finally:
            await scanner.stop()
    
    async def _handle_wavetrend_page(self, request: web.Request) -> web.Response:
        """WaveTrend 波段过滤器可视化页面"""
        html = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>🌊 波段过滤器 | WaveTrend Scanner</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); 
            color: #fff; 
            font-family: 'Segoe UI', system-ui, sans-serif;
            min-height: 100vh;
        }
        .header {
            padding: 20px;
            text-align: center;
            background: rgba(0,0,0,0.3);
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .header h1 { font-size: 24px; margin-bottom: 10px; }
        .controls {
            display: flex;
            justify-content: center;
            gap: 10px;
            margin-top: 10px;
        }
        .controls select, .controls button {
            background: rgba(255,255,255,0.1);
            border: 1px solid rgba(255,255,255,0.2);
            color: #fff;
            padding: 8px 16px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 14px;
        }
        .controls button:hover { background: rgba(255,255,255,0.2); }
        .chart-container {
            position: relative;
            width: 100%;
            height: calc(100vh - 120px);
            padding: 20px;
        }
        #scatter-chart {
            width: 100%;
            height: 100%;
            display: block;
        }
        .legend {
            position: absolute;
            right: 40px;
            top: 50%;
            transform: translateY(-50%);
            font-size: 12px;
            line-height: 1.8;
        }
        .legend-item { display: flex; align-items: center; gap: 8px; }
        .legend-color { width: 16px; height: 16px; border-radius: 3px; }
        .loading {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            font-size: 18px;
            color: rgba(255,255,255,0.6);
        }
        .back-link {
            position: absolute;
            top: 20px;
            left: 20px;
            color: #888;
            text-decoration: none;
            font-size: 14px;
        }
        .back-link:hover { color: #fff; }
    </style>
</head>
<body>
    <a href="/" class="back-link">← 返回主页</a>
    <div class="header">
        <h1>🔍 加密市场 波段过滤器</h1>
        <div class="controls">
            <select id="tf-select">
                <option value="1h">1小时</option>
                <option value="4h" selected>4小时</option>
                <option value="1d">日线</option>
            </select>
            <button onclick="loadScatterData()">🔄 刷新</button>
        </div>
    </div>
    <div class="chart-container">
        <canvas id="scatter-chart"></canvas>
        <div id="loading" class="loading">加载中...</div>
        <div class="legend">
            <div class="legend-item"><div class="legend-color" style="background:#8B0000"></div> 超买 (>45)</div>
            <div class="legend-item"><div class="legend-color" style="background:#4a2c2c"></div> 强势 (20~45)</div>
            <div class="legend-item"><div class="legend-color" style="background:#3a3a3a"></div> 中性 (-20~20)</div>
            <div class="legend-item"><div class="legend-color" style="background:#1a3a3a"></div> 弱势 (-45~-20)</div>
            <div class="legend-item"><div class="legend-color" style="background:#006666"></div> 超卖 (<-45)</div>
            <hr style="border-color:#444;margin:8px 0">
            <div style="font-size:11px;color:#aaa">
                <div>多1-4 = 多头背离 L1-L4</div>
                <div>空1-4 = 空头背离 L1-L4</div>
                <div style="color:#ff9800">M+ = 放量确认</div>
                <div style="margin-top:4px">⭕ = 有背离信号</div>
            </div>
        </div>
    </div>
    <script>
        const canvas = document.getElementById('scatter-chart');
        const ctx = canvas.getContext('2d');
        let scatterData = [];
        
        function resizeCanvas() {
            const dpr = window.devicePixelRatio || 1;
            const rect = canvas.getBoundingClientRect();
            
            // 设置实际绘制尺寸 (高分辨率)
            canvas.width = rect.width * dpr;
            canvas.height = rect.height * dpr;
            
            // 缩放到 CSS 尺寸
            ctx.scale(dpr, dpr);
            
            if (scatterData.length) drawChart();
        }
        window.addEventListener('resize', resizeCanvas);
        
        function getZoneColor(osc) {
            if (osc >= 45) return '#ff4444';
            if (osc >= 20) return '#cc6666';
            if (osc >= -20) return '#aaaaaa';
            if (osc >= -45) return '#66cccc';
            return '#00cccc';
        }
        
        // 主流币种符号和品牌颜色
        const coinSymbols = {
            'BTC': { symbol: '₿', color: '#F7931A' },
            'ETH': { symbol: 'Ξ', color: '#627EEA' },
            'SOL': { symbol: '◎', color: '#00FFA3' },
            'BNB': { symbol: '◆', color: '#F3BA2F' },
        };
        
        function drawChart() {
            const dpr = window.devicePixelRatio || 1;
            const rect = canvas.getBoundingClientRect();
            const w = rect.width;
            const h = rect.height;
            const padding = { left: 60, right: 120, top: 120, bottom: 120 };
            const chartW = w - padding.left - padding.right;
            const chartH = h - padding.top - padding.bottom;
            
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            
            // Y轴范围: -70 到 +70 (更大的垂直空间)
            const yMin = -70, yMax = 70, yRange = yMax - yMin;
            
            // 绘制背景区域
            const zones = [
                { y1: 70, y2: 45, color: '#5c1a1a' },
                { y1: 45, y2: 20, color: '#3d2020' },
                { y1: 20, y2: -20, color: '#2a2a2a' },
                { y1: -20, y2: -45, color: '#1a3535' },
                { y1: -45, y2: -70, color: '#0a4040' },
            ];
            
            zones.forEach(zone => {
                const y1 = padding.top + (yMax - zone.y1) / yRange * chartH;
                const y2 = padding.top + (yMax - zone.y2) / yRange * chartH;
                ctx.fillStyle = zone.color;
                ctx.fillRect(padding.left, y1, chartW, y2 - y1);
            });
            
            // Y轴刻度
            ctx.fillStyle = '#666';
            ctx.font = '12px sans-serif';
            ctx.textAlign = 'right';
            [-60, -45, -20, 0, 20, 45, 60].forEach(val => {
                const y = padding.top + (yMax - val) / yRange * chartH;
                ctx.fillText(val.toString(), padding.left - 10, y + 4);
                ctx.strokeStyle = 'rgba(255,255,255,0.1)';
                ctx.beginPath();
                ctx.moveTo(padding.left, y);
                ctx.lineTo(padding.left + chartW, y);
                ctx.stroke();
            });
            
            // 右侧标签
            ctx.textAlign = 'left';
            ctx.fillStyle = '#ff6666';
            ctx.fillText('超买', w - 110, padding.top + chartH * 0.05);
            ctx.fillStyle = '#cc9999';
            ctx.fillText('强势', w - 110, padding.top + chartH * 0.25);
            ctx.fillStyle = '#999';
            ctx.fillText('中性', w - 110, padding.top + chartH * 0.5);
            ctx.fillStyle = '#66aaaa';
            ctx.fillText('弱势', w - 110, padding.top + chartH * 0.75);
            ctx.fillStyle = '#00cccc';
            ctx.fillText('超卖', w - 110, padding.top + chartH * 0.95);
            
            // 散点
            const numPoints = scatterData.length;
            scatterData.forEach((d, i) => {
                const x = padding.left + (i + 0.5) / numPoints * chartW;
                const y = padding.top + (70 - d.osc) / 140 * chartH;
                
                // 绘制点 (主流币种用品牌色+符号，其他用圆点)
                const coinInfo = coinSymbols[d.symbol];
                if (coinInfo) {
                    // 主流币种：绘制带品牌颜色的圆 + Unicode符号
                    ctx.beginPath();
                    ctx.arc(x, y, 12, 0, Math.PI * 2);
                    ctx.fillStyle = coinInfo.color;
                    ctx.fill();
                    ctx.strokeStyle = 'rgba(0,0,0,0.5)';
                    ctx.lineWidth = 1;
                    ctx.stroke();
                    
                    // 绘制币种符号
                    ctx.fillStyle = '#fff';
                    ctx.font = 'bold 14px sans-serif';
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'middle';
                    ctx.fillText(coinInfo.symbol, x, y);
                    ctx.textBaseline = 'alphabetic';
                    
                    // 背离边框 (围绕图标)
                    if (d.div) {
                        ctx.beginPath();
                        ctx.arc(x, y, 16, 0, Math.PI * 2);
                        ctx.strokeStyle = d.div.m_plus ? '#ff9800' : '#fff';
                        ctx.lineWidth = d.div.m_plus ? 3 : 2;
                        ctx.stroke();
                    }
                } else {
                    // 背离边框
                    if (d.div) {
                        ctx.beginPath();
                        ctx.arc(x, y, 12, 0, Math.PI * 2);
                        ctx.strokeStyle = d.div.m_plus ? '#ff9800' : '#fff';
                        ctx.lineWidth = d.div.m_plus ? 3 : 2;
                        ctx.stroke();
                    }
                    
                    // 普通圆点
                    ctx.beginPath();
                    ctx.arc(x, y, 8, 0, Math.PI * 2);
                    ctx.fillStyle = getZoneColor(d.osc);
                    ctx.fill();
                    ctx.strokeStyle = 'rgba(255,255,255,0.3)';
                    ctx.lineWidth = 1;
                    ctx.stroke();
                }
                
                // 标签 - 错开显示避免重叠
                ctx.fillStyle = '#fff';
                ctx.font = '9px sans-serif';
                ctx.textAlign = 'center';
                
                // 标签垂直偏移：基于索引奇偶错开显示
                const labelOffset = (i % 3 === 0) ? -18 : (i % 3 === 1) ? -28 : -38;
                
                // 构建标签: 币种 + 超买/超卖连续数 + 背离 + 连对次数
                let label = d.symbol;
                
                // 超买/超卖连续 K 线数 (↑3 = 超买3根, ↓5 = 超卖5根)
                if (d.zone_type === 'ob' && d.zone_streak > 0) {
                    label += '↑' + d.zone_streak;
                } else if (d.zone_type === 'os' && d.zone_streak > 0) {
                    label += '↓' + d.zone_streak;
                }
                
                // 背离信号
                if (d.div) {
                    label += d.div.type[0] + d.div.level;
                    if (d.div.m_plus) label += '+';
                }
                
                // 连对次数 (格式: /6✓)
                if (d.accuracy && d.accuracy > 0) {
                    label += '/' + d.accuracy + '✓';
                }
                
                ctx.fillText(label, x, y + labelOffset);
            });
            
            document.getElementById('loading').style.display = 'none';
        }
        
        async function loadScatterData() {
            const loadingEl = document.getElementById('loading');
            loadingEl.style.display = 'block';
            loadingEl.textContent = '加载中...';
            const tf = document.getElementById('tf-select').value;
            try {
                loadingEl.textContent = '正在获取数据...';
                const resp = await fetch(`/api/wavetrend/scatter?tf=${tf}`);
                if (!resp.ok) {
                    throw new Error(`HTTP ${resp.status}`);
                }
                loadingEl.textContent = '解析数据...';
                const data = await resp.json();
                if (!data.data || data.data.length === 0) {
                    loadingEl.textContent = '无数据';
                    return;
                }
                loadingEl.textContent = `渲染 ${data.data.length} 个币种...`;
                scatterData = data.data.sort((a, b) => b.osc - a.osc);
                drawChart();
            } catch (e) {
                console.error('Load error:', e);
                loadingEl.textContent = '加载失败: ' + e.message;
            }
        }
        
        resizeCanvas();
        loadScatterData();
    </script>
</body>
</html>'''
        return web.Response(text=html, content_type='text/html')
    
    async def _handle_ratios(self, request: web.Request) -> web.Response:
        from unitrade.tracker import MarketReporter
        
        symbol = request.match_info.get("symbol", "BTCUSDT").upper()
        hours = int(request.query.get("hours", "12"))

        cache_key = f"api:ratios:{symbol}:{hours}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return web.json_response(cached)
        
        reporter = MarketReporter()
        await reporter.start()
        
        try:
            history = await reporter.get_ratio_history(symbol, hours)
            payload = {"symbol": symbol, "history": history, "cached_at": datetime.now().isoformat()}
            self._cache_set(cache_key, payload, ttl_seconds=300)
            return web.json_response(payload)
        finally:
            await reporter.stop()
    
    async def _handle_oi_history(self, request: web.Request) -> web.Response:
        """获取 OI 历史"""
        symbol = request.match_info.get("symbol", "BTCUSDT").upper()

        data = await self._binance_get_json(
            "/futures/data/openInterestHist",
            params={"symbol": symbol, "period": "1h", "limit": 24},
            cache_ttl_seconds=120,
        )

        history = []
        for item in (data or []):
            history.append({
                "time": datetime.fromtimestamp(item["timestamp"] / 1000).strftime("%H:%M"),
                "oi": float(item["sumOpenInterest"]),
                "value": float(item["sumOpenInterestValue"]),
            })
        return web.json_response({"symbol": symbol, "history": history})
    
    async def _handle_top_coins(self, request: web.Request) -> web.Response:
        """获取 Top 涨跌幅"""
        tickers = await self._binance_get_json("/fapi/v1/ticker/24hr", cache_ttl_seconds=30)
        usdt = [t for t in (tickers or []) if t.get("symbol", "").endswith("USDT")]

        usdt = [t for t in usdt if float(t.get("quoteVolume", 0) or 0) > 1e8]

        gainers = sorted(usdt, key=lambda x: float(x.get("priceChangePercent", 0) or 0), reverse=True)[:5]
        losers = sorted(usdt, key=lambda x: float(x.get("priceChangePercent", 0) or 0))[:5]

        return web.json_response({
            "gainers": [
                {"symbol": t["symbol"], "change": float(t["priceChangePercent"]), "price": float(t["lastPrice"])}
                for t in gainers
            ],
            "losers": [
                {"symbol": t["symbol"], "change": float(t["priceChangePercent"]), "price": float(t["lastPrice"])}
                for t in losers
            ],
        })
    
    async def _handle_oi_spikes(self, request: web.Request) -> web.Response:
        """
        OI 异动检测 - 多重确认逻辑
        
        过滤假信号:
        1. OI 变化 >= 5% (简单变化率)
        2. 成交量放大 >= 30%
        3. OI 价值 >= 500万U
        4. 信号质量分级 (强/中/弱)
        """
        # 配置参数
        oi_threshold = float(request.query.get("oi_threshold", "5"))    # OI 变化阈值 %
        vol_threshold = float(request.query.get("vol_threshold", "30")) # 成交量变化阈值 %
        min_oi_value = float(request.query.get("min_oi", "5000000"))    # 最小 OI 价值 U
        max_symbols = int(request.query.get("limit", "20"))             # 默认只扫描前 20，避免触发封禁

        cache_key = f"api:oi-spikes:{oi_threshold}:{vol_threshold}:{min_oi_value}:{max_symbols}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return web.json_response(cached)

        # Respect config switch: oi_spike_scanner.enabled
        try:
            from pathlib import Path
            from unitrade.core.config import load_config

            if Path("config/default.yaml").exists():
                conf = load_config("config/default.yaml")
                if not bool((conf.oi_spike_scanner or {}).get("enabled", True)):
                    payload = {
                        "disabled": True,
                        "reason": "oi_spike_scanner.enabled=false",
                        "config": {
                            "oi_threshold": oi_threshold,
                            "vol_threshold": vol_threshold,
                            "min_oi_value": min_oi_value,
                            "limit": max_symbols,
                        },
                        "spikes": [],
                        "timestamp": datetime.now().isoformat(),
                    }
                    self._cache_set(cache_key, payload, ttl_seconds=60)
                    return web.json_response(payload)
        except Exception:
            pass
        
        spikes = []

        tickers = await self._binance_get_json("/fapi/v1/ticker/24hr", cache_ttl_seconds=30)
        usdt = [t for t in (tickers or []) if t.get("symbol", "").endswith("USDT")]

        # 过滤: 成交量 > 5000万U
        usdt = [t for t in usdt if float(t.get("quoteVolume", 0) or 0) > 5e7]
        usdt = sorted(usdt, key=lambda x: float(x.get("quoteVolume", 0) or 0), reverse=True)[:100]

        async def analyze_symbol(ticker: dict) -> Optional[dict]:
            symbol = ticker.get("symbol")
            if not symbol:
                return None

            try:
                oi_data = await self._binance_get_json(
                    "/futures/data/openInterestHist",
                    params={"symbol": symbol, "period": "5m", "limit": 2},
                    cache_ttl_seconds=60,
                )
                if not oi_data or len(oi_data) < 2:
                    return None

                oi_old = float(oi_data[0]["sumOpenInterest"])
                oi_new = float(oi_data[1]["sumOpenInterest"])
                oi_value = float(oi_data[1]["sumOpenInterestValue"])

                if oi_old <= 0:
                    return None

                oi_change = ((oi_new - oi_old) / oi_old) * 100
                if abs(oi_change) < oi_threshold:
                    return None

                if oi_value < min_oi_value:
                    return None

                price = float(ticker.get("lastPrice", 0) or 0)
                price_change = float(ticker.get("priceChangePercent", 0) or 0)
                volume = float(ticker.get("quoteVolume", 0) or 0)

                avg_vol = volume / 24 if volume > 0 else 0

                quality_score = 0
                quality_reasons: List[str] = []

                if oi_change > 0 and price_change > 0:
                    quality_score += 2
                    quality_reasons.append("做多加仓")
                elif oi_change > 0 and price_change < 0:
                    quality_score += 2
                    quality_reasons.append("空头加仓")
                elif oi_change < 0 and price_change > 0:
                    quality_score += 1
                    quality_reasons.append("空头止损")
                elif oi_change < 0 and price_change < 0:
                    quality_score += 1
                    quality_reasons.append("多头止损")

                if avg_vol > 0 and volume > avg_vol * 1.5:
                    quality_score += 1
                    quality_reasons.append("量能放大")

                if abs(oi_change) >= 10:
                    quality_score += 1
                    quality_reasons.append("变化剧烈")

                if quality_score >= 3:
                    signal_level = "强"
                    signal_emoji = "🟢"
                elif quality_score >= 2:
                    signal_level = "中"
                    signal_emoji = "🟡"
                else:
                    signal_level = "弱"
                    signal_emoji = "🔴"

                return {
                    "symbol": symbol,
                    "oi_change": round(oi_change, 2),
                    "oi_value": round(oi_value, 0),
                    "price": price,
                    "price_change": round(price_change, 2),
                    "signal_level": signal_level,
                    "signal_emoji": signal_emoji,
                    "reasons": quality_reasons,
                    "score": quality_score,
                }
            except Exception:
                return None

        scan_list = usdt[: max(1, min(max_symbols, 100))]
        results = await asyncio.gather(*[analyze_symbol(t) for t in scan_list], return_exceptions=True)
        for r in results:
            if isinstance(r, dict):
                spikes.append(r)
        
        # 按信号质量 + OI 变化排序
        spikes = sorted(spikes, key=lambda x: (x["score"], abs(x["oi_change"])), reverse=True)[:10]
        
        payload = {
            "config": {
                "oi_threshold": oi_threshold,
                "vol_threshold": vol_threshold,
                "min_oi_value": min_oi_value,
                "limit": max_symbols,
            },
            "spikes": spikes,
            "timestamp": datetime.now().isoformat(),
        }
        self._cache_set(cache_key, payload, ttl_seconds=60)
        return web.json_response(payload)
    
    async def _handle_realtime_obi(self, request: web.Request) -> web.Response:
        """获取实时 OBI (Order Book Imbalance) - 支持多周期"""
        symbol = request.query.get("symbol", "BTCUSDT").upper()
        timeframe = request.query.get("tf", "1m")  # 默认 1 分钟
        
        if not self._realtime_service:
            return web.json_response({"error": "Realtime service not running"}, status=503)
        
        data = self._realtime_service.get_obi(symbol, timeframe)
        if not data:
            return web.json_response({"error": "No data available", "symbol": symbol}, status=404)
        
        return web.json_response(data)
    
    async def _handle_realtime_cvd(self, request: web.Request) -> web.Response:
        """获取实时 CVD (Cumulative Volume Delta) - 支持多周期"""
        symbol = request.query.get("symbol", "BTCUSDT").upper()
        timeframe = request.query.get("tf", "1m")  # 默认 1 分钟
        
        if not self._realtime_service:
            return web.json_response({"error": "Realtime service not running"}, status=503)
        
        data = self._realtime_service.get_cvd(symbol, timeframe)
        if not data:
            return web.json_response({"error": "No data available", "symbol": symbol}, status=404)
        
        return web.json_response(data)
    
    async def _handle_realtime_volatility(self, request: web.Request) -> web.Response:
        """获取实时波动率"""
        symbol = request.query.get("symbol", "BTCUSDT").upper()
        
        if not self._realtime_service:
            return web.json_response({"error": "Realtime service not running"}, status=503)
        
        data = self._realtime_service.get_volatility(symbol)
        if not data:
            return web.json_response({"error": "No data available", "symbol": symbol}, status=404)
        
        return web.json_response(data)
    
    async def _handle_realtime_all(self, request: web.Request) -> web.Response:
        """获取所有实时数据 (所有周期)"""
        symbol = request.query.get("symbol", "")
        
        if not self._realtime_service:
            return web.json_response({"error": "Realtime service not running"}, status=503)
        
        if symbol:
            # 返回指定币种的所有周期
            data = self._realtime_service.get_all_timeframes(symbol.upper())
        else:
            # 返回所有币种
            data = self._realtime_service.get_all_metrics()
        
        return web.json_response(data)

    async def _handle_cvd_analysis(self, request: web.Request) -> web.Response:
        """获取 CVD 深度分析 (Spot vs Futures)"""
        symbol = request.query.get("symbol", "BTCUSDT").upper()
        try:
            data = await self.cvd_service.get_cvd_analysis(symbol)
            return web.json_response(data)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_report(self, request: web.Request) -> web.Response:
        """生成市场报告"""
        symbol = request.query.get("symbol", "BTCUSDT").upper()
        try:
            data = await self.report_gen.generate_report(symbol)
            return web.json_response(data)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)
    
    async def _handle_rising_index(self, request: web.Request) -> web.Response:
        """
        上涨指数排行 API
        
        返回基于异动信号的上涨潜力排行榜
        - 价格结构 35%
        - 资金流入 30%
        - 动量新鲜度 20%
        - 成交量持续性 15%
        """
        # Respect config switch: anomaly_detector.enabled + anomaly_detector.rising_index.enabled
        rising_index_cfg = {}
        try:
            from pathlib import Path
            from unitrade.core.config import load_config

            if Path("config/default.yaml").exists():
                conf = load_config("config/default.yaml")
                ad = conf.anomaly_detector or {}
                rising_index_cfg = (ad.get("rising_index") or {}) if isinstance(ad, dict) else {}
                ad_enabled = bool(ad.get("enabled", True))
                rising_enabled = bool((rising_index_cfg or {}).get("enabled", True))
                if not (ad_enabled and rising_enabled):
                    return web.json_response(
                        {
                            "disabled": True,
                            "reason": "anomaly_detector.rising_index disabled",
                            "rankings": [],
                            "cached": False,
                            "updated_at": "",
                        }
                    )
        except Exception:
            pass

        import os
        import redis.asyncio as aioredis
        
        try:
            top_n = int(request.query.get("limit", "20"))
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
            
            # 检查缓存 (1小时有效)
            redis_client = await aioredis.from_url(redis_url, encoding="utf-8", decode_responses=True)
            cache_key = "anomaly:rising_index_cache"
            cached = await redis_client.get(cache_key)
            
            if cached:
                import json
                data = json.loads(cached)
                await redis_client.close()
                return web.json_response({
                    "rankings": data[:top_n],
                    "cached": True,
                    "updated_at": await redis_client.get(f"{cache_key}:time") or "",
                })
            
            # 计算排行
            from unitrade.scanner.signal_detector import RisingIndex, RisingIndexConfig

            allowed = set(RisingIndexConfig.__dataclass_fields__.keys())
            overrides = rising_index_cfg if isinstance(rising_index_cfg, dict) else {}
            kwargs = {k: v for k, v in overrides.items() if k in allowed and k not in ("redis_url", "enabled")}
            config = RisingIndexConfig(redis_url=redis_url, **kwargs)
            index = RisingIndex(config)
            await index.connect()
            
            try:
                scores = await index.get_ranking(top_n=50)
                
                results = []
                for s in scores:
                    results.append({
                        "symbol": s.symbol,
                        "score": round(s.total_score, 1),
                        "price_score": round(s.price_structure_score, 1),
                        "oi_score": round(s.oi_flow_score, 1),
                        "recency_score": round(s.recency_score, 1),
                        "volume_score": round(s.volume_score, 1),
                        "signal_count": s.signal_count,
                        "oi_change": round(s.cumulative_oi_change, 4),
                        "price_change_5d": round(s.price_change_5d, 4),
                        "trend": s.ema_alignment,
                    })
                
                # 缓存 1 小时
                import json
                from datetime import datetime
                await redis_client.set(cache_key, json.dumps(results), ex=3600)
                await redis_client.set(f"{cache_key}:time", datetime.now().isoformat(), ex=3600)
                
            finally:
                await index.close()
            
            await redis_client.close()
            
            return web.json_response({
                "rankings": results[:top_n],
                "cached": False,
                "updated_at": datetime.now().isoformat(),
            })
            
        except Exception as e:
            logger.error(f"Rising index error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_chart_klines(self, request: web.Request) -> web.Response:
        """?? K??? + WaveTrend ???? Pine ???? TradingView"""
        import numpy as np

        try:
            import pandas as pd
            import pandas_ta as pta
        except ModuleNotFoundError:
            return web.json_response(
                {
                    "error": "Optional dependencies missing: pandas/pandas_ta required for WaveTrend chart endpoint.",
                    "hint": "Install with `pip install -e \".[ta]\"` (or install pandas/pandas_ta manually).",
                },
                status=503,
            )
        from unitrade.core.config import load_config
        from unitrade.scanner import WaveTrendScanner, WaveTrendConfig
        
        symbol = request.query.get("symbol", "BTCUSDT").upper()
        interval = request.query.get("interval", "30m")
        limit = min(int(request.query.get("limit", "500")), 1000)
        
        try:
            klines_raw = await self._binance_get_json(
                "/fapi/v1/klines",
                params={"symbol": symbol, "interval": interval, "limit": limit},
                cache_ttl_seconds=10,
            )
            
            if len(klines_raw) < 20:
                return web.json_response({"error": "Not enough data"}, status=400)
            
            df = pd.DataFrame(klines_raw, columns=[
                'time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_base', 
                'taker_buy_quote', 'ignore'
            ])
            df['time'] = df['time'].astype(int) // 1000
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['close'] = df['close'].astype(float)
            df['volume'] = df['volume'].astype(float)
            df['taker_buy_base'] = df['taker_buy_base'].astype(float)
            
            try:
                cfg_obj = load_config("config/default.yaml")
                wt_cfg = WaveTrendConfig.from_config(cfg_obj)
            except Exception as e:
                logger.warning(f"Load WaveTrend config failed, using defaults: {e}")
                wt_cfg = WaveTrendConfig()
            
            scanner = WaveTrendScanner(wt_cfg)
            df_ha = scanner._calc_heikin_ashi_df(df)
            osc = scanner._calc_wavetrend_pandas(df, df_ha)
            src_df = df_ha if wt_cfg.use_ha else df
            
            atr_val = pta.atr(df['high'], df['low'], df['close'], length=wt_cfg.atr_len)
            kc_mid = pta.ema(df['close'], length=wt_cfg.squeeze_len)
            kc_top = kc_mid + wt_cfg.kc_mult * atr_val
            kc_bot = kc_mid - wt_cfg.kc_mult * atr_val
            bb_mid = pta.sma(df['close'], length=wt_cfg.squeeze_len)
            bb_std = pta.stdev(df['close'], length=wt_cfg.squeeze_len)
            bb_top = bb_mid + wt_cfg.bb_mult * bb_std
            bb_bot = bb_mid - wt_cfg.bb_mult * bb_std
            
            is_squeeze = (bb_top < kc_top) & (bb_bot > kc_bot)
            squeeze_levels = []
            streak = 0
            for i in range(len(df)):
                if bool(is_squeeze.iloc[i]):
                    streak += 1
                else:
                    streak = 0
                level = None
                if streak >= wt_cfg.min_squeeze_duration:
                    kc_width = kc_top.iloc[i] - kc_bot.iloc[i]
                    bb_width = bb_top.iloc[i] - bb_bot.iloc[i]
                    tightness = bb_width / kc_width if kc_width != 0 else 1.0
                    if not pd.isna(tightness):
                        level = "l2" if tightness <= wt_cfg.tightness_l2_ratio else "l1"
                squeeze_levels.append(level)
            
            vol_mean = df['volume'].rolling(wt_cfg.vol_len).mean()
            vol_std = df['volume'].rolling(wt_cfg.vol_len).std()
            vol_z = (df['volume'] - vol_mean) / vol_std.replace(0, np.nan)
            vol_z = vol_z.fillna(0)
            effective_vol = (vol_z - wt_cfg.vol_trigger).clip(lower=0)
            lvl2_dynamic = (wt_cfg.lvl_2_base - effective_vol * wt_cfg.vol_weight).clip(lower=wt_cfg.lvl_2_floor)
            lvl3_dynamic = (wt_cfg.lvl_3_base - effective_vol * wt_cfg.vol_weight).clip(lower=wt_cfg.lvl_3_floor)
            
            adx_series = None
            if wt_cfg.use_adx_filter_div:
                dmi = pta.dmi(df['high'], df['low'], df['close'], length=wt_cfg.adx_len)
                if dmi is not None and f"ADX_{wt_cfg.adx_len}" in dmi:
                    adx_series = dmi[f"ADX_{wt_cfg.adx_len}"]
            
            markers = []
            lb_left = wt_cfg.lb_left
            lb_right = wt_cfg.lb_right
            lvl_1 = wt_cfg.lvl_1
            search_limit = wt_cfg.search_limit
            bear_seq = 0
            bull_seq = 0
            last_bear_idx = -10_000
            last_bull_idx = -10_000
            
            for i in range(lb_left, len(df) - lb_right):
                curr_osc = osc.iloc[i]
                # Pivot High
                is_ph = True
                for k in range(1, lb_left + 1):
                    if curr_osc <= osc.iloc[i - k]:
                        is_ph = False
                        break
                if is_ph:
                    for k in range(1, lb_right + 1):
                        if curr_osc <= osc.iloc[i + k]:
                            is_ph = False
                            break
                if is_ph and curr_osc >= lvl_1:
                    allow_div = True
                    if adx_series is not None:
                        allow_div = adx_series.iloc[i] <= wt_cfg.adx_limit
                    if allow_div and ((not wt_cfg.require_candle_conf) or (src_df['close'].iloc[i] < src_df['open'].iloc[i])):
                        for j in range(1, search_limit):
                            check_idx = i - j
                            if check_idx < 1:
                                break
                            check_osc = osc.iloc[check_idx]
                            is_peak = (check_osc >= osc.iloc[check_idx - 1] if check_idx > 0 else True) and                                       (check_osc >= osc.iloc[check_idx + 1] if check_idx < len(df) - 1 else True)
                            if is_peak and check_osc >= lvl_1:
                                if df['high'].iloc[i] > df['high'].iloc[check_idx] and curr_osc <= check_osc:
                                    level = 1 if j <= 15 else 2 if j <= 35 else 3 if j <= 55 else 4
                                    is_m_plus = vol_z.iloc[i] > wt_cfg.vol_trigger
                                    # 序列计数（与 Pine 类似）
                                    if (i - last_bear_idx) > search_limit:
                                        bear_seq = 1
                                    else:
                                        bear_seq += 1
                                    last_bear_idx = i
                                    txt = f"空{level}"
                                    if bear_seq > 1:
                                        txt += f" x{bear_seq}"
                                    if is_m_plus:
                                        txt += " M+"
                                    markers.append({
                                        "time": int(df['time'].iloc[i]),
                                        "position": "aboveBar",
                                        "color": "#ff9800" if is_m_plus else "#ef5350",
                                        "shape": "arrowDown",
                                        "text": txt,
                                        "size": 2
                                    })
                                break
                # Pivot Low
                is_pl = True
                for k in range(1, lb_left + 1):
                    if curr_osc >= osc.iloc[i - k]:
                        is_pl = False
                        break
                if is_pl:
                    for k in range(1, lb_right + 1):
                        if curr_osc >= osc.iloc[i + k]:
                            is_pl = False
                            break
                if is_pl and curr_osc <= -lvl_1:
                    allow_div = True
                    if adx_series is not None:
                        allow_div = adx_series.iloc[i] <= wt_cfg.adx_limit
                    if allow_div and ((not wt_cfg.require_candle_conf) or (src_df['close'].iloc[i] >= src_df['open'].iloc[i])):
                        for j in range(1, search_limit):
                            check_idx = i - j
                            if check_idx < 1:
                                break
                            check_osc = osc.iloc[check_idx]
                            is_bottom = (check_osc <= osc.iloc[check_idx - 1] if check_idx > 0 else True) and                                         (check_osc <= osc.iloc[check_idx + 1] if check_idx < len(df) - 1 else True)
                            if is_bottom and check_osc <= -lvl_1:
                                if df['low'].iloc[i] < df['low'].iloc[check_idx] and curr_osc >= check_osc:
                                    level = 1 if j <= 15 else 2 if j <= 35 else 3 if j <= 55 else 4
                                    is_m_plus = vol_z.iloc[i] > wt_cfg.vol_trigger
                                    # 序列计数
                                    if (i - last_bull_idx) > search_limit:
                                        bull_seq = 1
                                    else:
                                        bull_seq += 1
                                    last_bull_idx = i
                                    txt = f"多{level}"
                                    if bull_seq > 1:
                                        txt += f" x{bull_seq}"
                                    if is_m_plus:
                                        txt += " M+"
                                    markers.append({
                                        "time": int(df['time'].iloc[i]),
                                        "position": "belowBar",
                                        "color": "#ff9800" if is_m_plus else "#26a69a",
                                        "shape": "arrowUp",
                                        "text": txt,
                                        "size": 2
                                    })
                                break
            
            klines_out = []
            wavetrend_data = []
            
            for i in range(len(df)):
                klines_out.append({
                    "time": int(df['time'].iloc[i]),
                    "open": float(df['open'].iloc[i]),
                    "high": float(df['high'].iloc[i]),
                    "low": float(df['low'].iloc[i]),
                    "close": float(df['close'].iloc[i]),
                    "volume": float(df['volume'].iloc[i]),
                    "buyVolume": float(df['taker_buy_base'].iloc[i]),
                    "sellVolume": float(df['volume'].iloc[i] - df['taker_buy_base'].iloc[i]),
                })
                wavetrend_data.append({
                    "time": int(df['time'].iloc[i]),
                    "osc": round(float(osc.iloc[i]) if pd.notna(osc.iloc[i]) else 0, 2),
                    "squeeze_level": squeeze_levels[i],
                    "lvl2_dyn": float(lvl2_dynamic.iloc[i]) if len(lvl2_dynamic) > i else None,
                    "lvl3_dyn": float(lvl3_dynamic.iloc[i]) if len(lvl3_dynamic) > i else None,
                })
            
            return web.json_response({
                "symbol": symbol,
                "interval": interval,
                "klines": klines_out,
                "wavetrend": wavetrend_data,
                "markers": markers,
                "levels": {
                    "lvl1": wt_cfg.lvl_1,
                    "lvl2": wt_cfg.lvl_2_base,
                    "lvl3": wt_cfg.lvl_3_base,
                    "lvl2_dyn": float(lvl2_dynamic.iloc[-1]) if len(lvl2_dynamic) else wt_cfg.lvl_2_base,
                    "lvl3_dyn": float(lvl3_dynamic.iloc[-1]) if len(lvl3_dynamic) else wt_cfg.lvl_3_base,
                },
            })
            
        except Exception as e:
            logger.error(f"Chart klines error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)
    async def _handle_chart_page(self, request: web.Request) -> web.Response:
        """TradingView Lightweight Charts 图表页面"""
        html = '''<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>📈 K线图表 | UniTrade</title>
    <script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', system-ui, sans-serif;
            background: #131722;
            color: #d1d4dc;
            height: 100vh;
            display: flex;
            flex-direction: column;
        }
        .header {
            background: #1e222d;
            padding: 12px 20px;
            display: flex;
            align-items: center;
            gap: 20px;
            border-bottom: 1px solid #2a2e39;
        }
        .header h1 { font-size: 18px; font-weight: 600; }
        .controls { display: flex; gap: 12px; flex: 1; }
        .controls input, .controls select {
            background: #2a2e39;
            border: 1px solid #363a45;
            color: #d1d4dc;
            padding: 8px 12px;
            border-radius: 4px;
            font-size: 14px;
        }
        .controls input { width: 160px; }
        .controls button {
            background: #2962ff;
            color: #fff;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
        }
        .controls button:hover { background: #1e88e5; }
        .tf-buttons { display: flex; gap: 4px; }
        .tf-btn {
            background: #2a2e39;
            border: 1px solid #363a45;
            color: #d1d4dc;
            padding: 6px 12px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 13px;
        }
        .tf-btn:hover { background: #363a45; }
        .tf-btn.active { background: #2962ff; border-color: #2962ff; }
        .chart-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            padding: 10px;
        }
        #main-chart { flex: 3; min-height: 300px; }
        #indicator-chart { flex: 1; min-height: 150px; border-top: 1px solid #2a2e39; }
        .back-link {
            color: #787b86;
            text-decoration: none;
            font-size: 14px;
        }
        .back-link:hover { color: #d1d4dc; }
        .loading {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            font-size: 16px;
            color: #787b86;
        }
        .symbol-info {
            font-size: 14px;
            color: #787b86;
        }
        .price-up { color: #26a69a; }
        .price-down { color: #ef5350; }
    </style>
</head>
<body>
    <div class="header">
        <a href="/" class="back-link">← 返回</a>
        <h1>📈 K线图表</h1>
        <div class="controls">
            <input type="text" id="symbol-input" placeholder="输入币种 (如 BTC)" value="BTC">
            <div class="tf-buttons">
                <button class="tf-btn" data-tf="3m">3m</button>
                <button class="tf-btn" data-tf="5m">5m</button>
                <button class="tf-btn" data-tf="15m">15m</button>
                <button class="tf-btn active" data-tf="30m">30m</button>
                <button class="tf-btn" data-tf="1h">1H</button>
                <button class="tf-btn" data-tf="4h">4H</button>
                <button class="tf-btn" data-tf="1d">D</button>
                <button class="tf-btn" data-tf="1w">W</button>
            </div>
            <button onclick="loadChart()">刷新</button>
        </div>
        <div class="symbol-info" id="price-info"></div>
    </div>
    <div class="chart-container">
        <div id="main-chart"></div>
        <div id="indicator-chart"></div>
    </div>
    <div id="loading" class="loading">加载中...</div>
    
    <script>
        let mainChart, indicatorChart;
        let candleSeries, volumeSeries, oscSeries, squeezeSeries, energySeries;
        let currentTf = '30m';
        
        function initCharts() {
            const mainContainer = document.getElementById('main-chart');
            const indicatorContainer = document.getElementById('indicator-chart');
            
            // 主图 - K线 + 成交量
            mainChart = LightweightCharts.createChart(mainContainer, {
                layout: {
                    background: { type: 'solid', color: '#131722' },
                    textColor: '#d1d4dc',
                },
                grid: {
                    vertLines: { color: '#1e222d' },
                    horzLines: { color: '#1e222d' },
                },
                crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
                rightPriceScale: { borderColor: '#2a2e39', scaleMargins: { top: 0.1, bottom: 0.2 } },
                timeScale: { borderColor: '#2a2e39', timeVisible: true },
            });
            
            // K线
            candleSeries = mainChart.addCandlestickSeries({
                upColor: '#26a69a',
                downColor: '#ef5350',
                borderUpColor: '#26a69a',
                borderDownColor: '#ef5350',
                wickUpColor: '#26a69a',
                wickDownColor: '#ef5350',
            });
            
            // 成交量
            volumeSeries = mainChart.addHistogramSeries({
                priceFormat: { type: 'volume' },
                priceScaleId: '',
                scaleMargins: { top: 0.8, bottom: 0 },
            });
            
            // 指标图 - WaveTrend OSC
            indicatorChart = LightweightCharts.createChart(indicatorContainer, {
                layout: {
                    background: { type: 'solid', color: '#131722' },
                    textColor: '#d1d4dc',
                },
                grid: {
                    vertLines: { color: '#1e222d' },
                    horzLines: { color: '#1e222d' },
                },
                rightPriceScale: { borderColor: '#2a2e39' },
                timeScale: { borderColor: '#2a2e39', visible: false },
            });
            
            // Squeeze 背景区域 (橙色，细条形)
            squeezeSeries = indicatorChart.addHistogramSeries({
                color: 'rgba(255, 152, 0, 0.4)',
                priceFormat: { type: 'price' },
                priceScaleId: 'right',
                base: 0,
            });
            
            // ??? (Histogram, share scale with OSC)
            energySeries = indicatorChart.addHistogramSeries({
                priceFormat: { type: 'price' },
                priceScaleId: 'right',
                color: '#5d606b',
                base: 0,
            });

            // OSC 线 (阶梯线样式)
            oscSeries = indicatorChart.addLineSeries({
                color: '#2962ff',
                lineWidth: 2,
                lineType: 1,  // LineType.WithSteps
                priceScaleId: 'right',
            });
            
            // 零轴 (实线，灰色)
            window.zeroLineSeries = indicatorChart.addLineSeries({
                color: '#787b86',
                lineWidth: 1,
                lineStyle: 0, // Solid
                priceScaleId: 'right',
                lastValueVisible: false,
                priceLineVisible: false,
            });
            
            // 一级上轨 (虚线 45)
            window.topLineSeries = indicatorChart.addLineSeries({
                color: 'rgba(255, 255, 255, 0.3)',
                lineWidth: 1,
                lineStyle: 2, // Dashed
                priceScaleId: 'right',
                lastValueVisible: true,
                priceLineVisible: false,
            });
            
            // 一级下轨 (虚线 -45)
            window.botLineSeries = indicatorChart.addLineSeries({
                color: 'rgba(255, 255, 255, 0.3)',
                lineWidth: 1,
                lineStyle: 2, // Dashed
                priceScaleId: 'right',
                lastValueVisible: true,
                priceLineVisible: false,
            });
            
            // 双向同步时间轴
            let isSyncingLeft = false;
            let isSyncingRight = false;

            function syncTimeScale(source, target) {
                const sourceTimeScale = source.timeScale();
                const targetTimeScale = target.timeScale();

                sourceTimeScale.subscribeVisibleLogicalRangeChange((range) => {
                    if (isSyncingLeft) return;
                    isSyncingRight = true;
                    if (range) targetTimeScale.setVisibleLogicalRange(range);
                    isSyncingRight = false;
                });
            }

            // 主图 -> 指标图
            mainChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
                if (isSyncingRight) return;
                isSyncingLeft = true;
                if (range) indicatorChart.timeScale().setVisibleLogicalRange(range);
                isSyncingLeft = false;
            });

            // 指标图 -> 主图
            indicatorChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
                if (isSyncingLeft) return;
                isSyncingRight = true;
                if (range) mainChart.timeScale().setVisibleLogicalRange(range);
                isSyncingRight = false;
            });
            
            // 响应式
            const resizeObserver = new ResizeObserver(() => {
                mainChart.applyOptions({ width: mainContainer.clientWidth, height: mainContainer.clientHeight });
                indicatorChart.applyOptions({ width: indicatorContainer.clientWidth, height: indicatorContainer.clientHeight });
            });
            resizeObserver.observe(mainContainer);
            resizeObserver.observe(indicatorContainer);
        }
        
        async function loadChart() {
            const loading = document.getElementById('loading');
            loading.style.display = 'block';
            
            const symbolInput = document.getElementById('symbol-input').value.toUpperCase().trim();
            const symbol = symbolInput.endsWith('USDT') ? symbolInput : symbolInput + 'USDT';
            
            try {
                const resp = await fetch(`/api/chart/klines?symbol=${symbol}&interval=${currentTf}&limit=300`);
                if (!resp.ok) {
                    const text = await resp.text();
                    throw new Error(`API error ${resp.status}: ${text}`);
                }
                const data = await resp.json();
                
                if (data.error) throw new Error(data.error);
                
                console.log('Chart data received:', data);

                // 1. 更新 K 线
                const candleData = data.klines.map(k => ({
                    time: k.time,
                    open: k.open,
                    high: k.high,
                    low: k.low,
                    close: k.close,
                }));
                candleSeries.setData(candleData);
                
                // 2. 更新成交量
                const volumeData = data.klines.map(k => ({
                    time: k.time,
                    value: k.volume,
                    color: k.close >= k.open ? 'rgba(38, 166, 154, 0.5)' : 'rgba(239, 83, 80, 0.5)',
                }));
                volumeSeries.setData(volumeData);
                
                // 3. WaveTrend 阈值（含动态 L2/L3 默认）
                const levels = data.levels || {};
                const lvl1 = levels.lvl1 ?? 45;
                const lvl2Default = levels.lvl2_dyn ?? levels.lvl2 ?? 60;
                const lvl3Default = levels.lvl3_dyn ?? levels.lvl3 ?? 68;
                
                // 4. 更新 WaveTrend OSC (深灰色)
                const oscData = data.wavetrend.map(w => ({
                    time: w.time,
                    value: w.osc,
                    color: '#5d606b' 
                }));
                oscSeries.setData(oscData);
                
                // 5. 更新 Squeeze 高亮 (L1/L2)
                const squeezeData = data.wavetrend.map(w => {
                    let val = null;
                    let color = 'transparent';
                    if (w.squeeze_level === 'l2') { val = 2 * lvl1; color = 'rgba(255, 152, 0, 0.55)'; }
                    else if (w.squeeze_level === 'l1') { val = 1.6 * lvl1; color = 'rgba(120, 123, 134, 0.35)'; }
                    return { time: w.time, value: val, color };
                });
                squeezeSeries.applyOptions({ base: -lvl1 });
                squeezeSeries.setData(squeezeData);
                
                // 6. 能量柱（使用动态 L2/L3）
                const energyData = data.wavetrend.map(w => {
                    const l2 = (w.lvl2_dyn !== null && w.lvl2_dyn !== undefined) ? w.lvl2_dyn : lvl2Default;
                    const l3 = (w.lvl3_dyn !== null && w.lvl3_dyn !== undefined) ? w.lvl3_dyn : lvl3Default;
                    const absVal = Math.abs(w.osc);
                    let color = '#5d606b';
                    if (absVal >= l3) color = '#2962ff';
                    else if (absVal >= l2) color = '#fdd835';
                    else if (absVal >= lvl1) color = w.osc > 0 ? '#ef5350' : '#26a69a';
                    return { time: w.time, value: w.osc, color };
                });
                energySeries.setData(energyData);

                // 5.  更新 Markers (背离标签与Pivot圆圈)
                if (data.markers && Array.isArray(data.markers)) {
                    try {
                        // 按时间排序
                        data.markers.sort((a, b) => a.time - b.time);
                        
                        const markers = data.markers.map(m => {
                            if (m.shape === 'circle') {
                                return { 
                                    time: m.time, 
                                    position: m.position, 
                                    color: m.color, 
                                    shape: undefined, 
                                    text: '○',
                                    size: 1,
                                    id: m.id
                                }; 
                            }
                            return {
                               time: m.time, 
                               position: m.position, 
                               color: m.color, 
                               shape: m.shape, 
                               text: m.text,
                               size: 2,
                               id: m.id
                            };
                        });
                        oscSeries.setMarkers(markers);
                        console.log('Markers set:', markers.length);
                    } catch (err) {
                        console.error('Error applying markers:', err);
                    }
                } else {
                    oscSeries.setMarkers([]);
                }
                
                // 6. 为辅助线填充数据
                const lineData0 = data.wavetrend.map(w => ({ time: w.time, value: 0 }));
                const lineDataTop = data.wavetrend.map(w => ({ time: w.time, value: 45 }));
                const lineDataBot = data.wavetrend.map(w => ({ time: w.time, value: -45 }));
                
                if (window.zeroLineSeries) window.zeroLineSeries.setData(lineData0);
                if (window.topLineSeries) window.topLineSeries.setData(lineDataTop);
                if (window.botLineSeries) window.botLineSeries.setData(lineDataBot);
                
                // 7. 更新价格信息
                if (data.klines.length > 0) {
                    const lastK = data.klines[data.klines.length - 1];
                    const firstK = data.klines[0];
                    const priceChange = ((lastK.close - firstK.open) / firstK.open * 100).toFixed(2);
                    const priceClass = lastK.close >= firstK.open ? 'price-up' : 'price-down';
                    document.getElementById('price-info').innerHTML = 
                        `<span class="${priceClass}">${symbol} $${lastK.close.toFixed(4)} (${priceChange}%)</span>`;
                }
                
                mainChart.timeScale().fitContent();
                
            } catch (e) {
                console.error('Load chart error:', e);
            } finally {
                loading.style.display = 'none';
            }
        }
        
        // 时间周期切换
        document.querySelectorAll('.tf-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentTf = btn.dataset.tf;
                loadChart();
            });
        });
        
        // 搜索
        document.getElementById('symbol-input').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') loadChart();
        });
        
        // 初始化
        initCharts();
        loadChart();
    </script>
</body>
</html>'''
        return web.Response(text=html, content_type='text/html')
    
    def _generate_html(self) -> str:
        return '''<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>UniTrade Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
            background: linear-gradient(135deg, #0f0f23 0%, #1a1a3e 100%);
            color: #f0f0f0;
            min-height: 100vh;
            padding: 24px;
            font-size: 15px;
            line-height: 1.6;
        }
        .container { max-width: 1600px; margin: 0 auto; }
        h1 { 
            text-align: center; 
            margin-bottom: 24px;
            font-size: 2.2em;
            font-weight: 700;
            background: linear-gradient(to right, #00d4ff, #7b2cbf);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        /* 币种卡片网格 */
        .coin-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 14px;
            margin-bottom: 24px;
        }
        .coin-card {
            background: rgba(255,255,255,0.06);
            border-radius: 14px;
            padding: 18px;
            text-align: center;
            border: 1px solid rgba(255,255,255,0.1);
            transition: transform 0.2s;
        }
        .coin-card:hover { transform: translateY(-3px); }
        .coin-symbol { font-size: 14px; color: #aaa; font-weight: 500; letter-spacing: 0.5px; }
        .coin-price { font-size: 20px; font-weight: 700; margin: 10px 0; }
        .coin-change { font-size: 16px; font-weight: 600; }
        .positive { color: #00ff88; }
        .negative { color: #ff5555; }
        
        /* 主网格 */
        .main-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 18px;
        }
        @media (max-width: 1200px) { .main-grid { grid-template-columns: repeat(2, 1fr); } }
        @media (max-width: 800px) { .main-grid { grid-template-columns: 1fr; } }
        
        .card {
            background: rgba(255,255,255,0.06);
            border-radius: 16px;
            padding: 22px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .card h2 {
            font-size: 16px;
            font-weight: 600;
            margin-bottom: 18px;
            color: #00d4ff;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        /* 详情卡片 */
        .detail-card { grid-column: span 1; }
        .metric {
            display: flex;
            justify-content: space-between;
            padding: 12px 0;
            border-bottom: 1px solid rgba(255,255,255,0.08);
            font-size: 15px;
        }
        .metric:last-child { border-bottom: none; }
        .metric-label { color: #aaa; }
        .metric-value { font-weight: 600; color: #fff; }
        
        /* 列表 */
        .list-item {
            padding: 10px 0;
            border-bottom: 1px solid rgba(255,255,255,0.06);
            font-size: 14px;
            display: flex;
            justify-content: space-between;
        }
        .list-item:last-child { border-bottom: none; }
        
        /* 涨跌榜 */
        .top-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
        .top-title { font-size: 13px; color: #aaa; margin-bottom: 10px; font-weight: 500; }
        
        /* 图表 */
        .chart-container { height: 200px; margin-top: 12px; }
        
        /* EMA */
        .ema-item { 
            padding: 8px 0;
            border-bottom: 1px solid rgba(255,255,255,0.06);
            font-size: 14px;
        }
        .flower { color: #ff69b4; }
        .flower-bear { color: #ff6b6b; }
        
        /* 底部 */
        .footer {
            text-align: center;
            margin-top: 20px;
            color: #666;
            font-size: 0.85em;
        }
        .refresh-btn {
            background: linear-gradient(to right, #00d4ff, #7b2cbf);
            border: none;
            padding: 10px 20px;
            border-radius: 8px;
            color: white;
            cursor: pointer;
            font-size: 0.95em;
        }
        .loading { text-align: center; padding: 30px; color: #666; }
        
        /* 搜索框 */
        .search-box {
            display: flex;
            justify-content: center;
            gap: 10px;
            margin-bottom: 20px;
        }
        .search-box input {
            width: 300px;
            padding: 12px 16px;
            border: 1px solid rgba(255,255,255,0.2);
            border-radius: 8px;
            background: rgba(255,255,255,0.05);
            color: #fff;
            font-size: 1em;
        }
        .search-box input::placeholder { color: #666; }
        .search-box input:focus {
            outline: none;
            border-color: #00d4ff;
            box-shadow: 0 0 10px rgba(0,212,255,0.3);
        }
        .search-box button {
            padding: 12px 20px;
            background: linear-gradient(to right, #00d4ff, #7b2cbf);
            border: none;
            border-radius: 8px;
            color: white;
            cursor: pointer;
            font-size: 1em;
        }
        .search-result {
            margin-bottom: 20px;
        }

        /* CVD 分析模态框 */
        .modal {
            display: none; 
            position: fixed; 
            z-index: 1000; 
            left: 0;
            top: 0;
            width: 100%; 
            height: 100%; 
            overflow: auto; 
            background-color: rgba(0,0,0,0.8); 
            backdrop-filter: blur(5px);
        }
        .modal-content {
            background-color: #1a1a3e;
            margin: 5% auto; 
            padding: 20px; 
            border: 1px solid #444; 
            width: 80%; 
            max-width: 800px;
            border-radius: 12px;
            box-shadow: 0 0 20px rgba(0,0,0,0.5);
        }
        .close-btn {
            color: #aaa;
            float: right;
            font-size: 28px;
            font-weight: bold;
            cursor: pointer;
        }
        .close-btn:hover { color: #fff; }
        
        .cvd-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            font-family: monospace;
        }
        .cvd-table th, .cvd-table td {
            text-align: right;
            padding: 12px 15px;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .cvd-table th {
            text-align: right;
            color: #888;
            font-weight: 600;
        }
        .cvd-table tr:hover { background: rgba(255,255,255,0.05); }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 UniTrade Dashboard</h1>
        
        <!-- 导航链接 -->
        <div style="text-align:center; margin-bottom:20px;">
            <a href="/wavetrend" style="color:#9C27B0;text-decoration:none;padding:8px 16px;border:1px solid #9C27B0;border-radius:8px;margin:0 5px;">🌊 波段过滤器</a>
        </div>
        
        <!-- 搜索框 -->
        <div class="search-box">
            <input type="text" id="search-input" placeholder="搜索币种 (如: SOL, DOGE, PEPE...)" />
            <button onclick="searchCoin()">🔍 搜索</button>
            <button onclick="openReportModal()" style="margin-left:10px; padding:12px 20px; background:#333; border:1px solid #555; border-radius:8px; color:#fff; cursor:pointer;" title="生成报告">📄 报告</button>
        </div>
        
        <!-- 搜索结果 -->
        <div id="search-result" class="search-result" style="display:none;">
            <div class="card" style="background: rgba(0,212,255,0.1); border-color: #00d4ff;">
                <h2 id="search-title">🔍 搜索结果</h2>
                <div id="search-content"></div>
                <button onclick="closeSearch()" style="margin-top:10px; padding:8px 16px; background:#444; border:none; color:#fff; border-radius:4px; cursor:pointer;">关闭</button>
            </div>
        </div>
        
        <!-- 币种卡片 -->
        <div class="coin-grid" id="coin-grid">
            <div class="loading">加载币种数据...</div>
        </div>
        
        <!-- 主网格 -->
        <div class="main-grid">
            <!-- BTC 详情 -->
            <div class="card detail-card">
                <h2>₿ BTC 详情</h2>
                <div id="btc-detail" class="loading">加载中...</div>
            </div>
            
            <!-- OI 图表 -->
            <div class="card">
                <h2>📈 BTC 持仓量变化 (24H)</h2>
                <div class="chart-container">
                    <canvas id="oi-chart"></canvas>
                </div>
            </div>
            
            <!-- 涨跌榜 -->
            <div class="card">
                <h2>🔥 涨跌榜 (24H)</h2>
                <div class="top-grid" id="top-grid">
                    <div class="loading">加载中...</div>
                </div>
            </div>
            
            <!-- OI 异动警报 -->
            <div class="card">
                <h2>🚨 OI 异动 (1H)</h2>
                <div id="oi-spikes" class="loading">加载中...</div>
            </div>
            
            <!-- EMA 雷达 -->
            <div class="card">
                <h2>📡 EMA Trend Radar
                    <select id="ema-tf" onchange="loadEMAData()" style="margin-left:auto;background:#333;color:#fff;border:1px solid #555;padding:4px 8px;border-radius:4px;font-size:12px">
                        <option value="15m">15m</option>
                        <option value="1h" selected>1H</option>
                        <option value="4h">4H</option>
                        <option value="1d">1D</option>
                    </select>
                </h2>
                <div id="ema-data" class="loading">加载中...</div>
            </div>
            
            <!-- WaveTrend 波段信号 -->
            <div class="card" style="border-color: #9C27B0;">
                <h2>🌊 波段信号雷达
                    <select id="wt-tf" onchange="loadWaveTrend()" style="margin-left:auto;background:#333;color:#fff;border:1px solid #555;padding:4px 8px;border-radius:4px;font-size:12px">
                        <option value="1h" selected>1H</option>
                        <option value="4h">4H</option>
                        <option value="1d">1D</option>
                    </select>
                </h2>
                <div id="wt-data" class="loading">加载中...</div>
            </div>
            
            <!-- 多空比图表 -->
            <div class="card">
                <h2>📊 BTC 多空比 (12H)</h2>
                <div class="chart-container">
                    <canvas id="ratio-chart"></canvas>
                </div>
            </div>
            
            <!-- 多空比数据 -->
            <div class="card">
                <h2>📋 多空比详情</h2>
                <div id="ratio-data" class="loading">加载中...</div>
            </div>
            
            <!-- 实时盘口 OBI -->
            <!-- 实时盘口 OBI -->
            <div class="card" style="border-color: #00d4ff;">
                <h2>📊 实时盘口 OBI
                    <span style="font-size:0.6em; color:#888; cursor:help; margin-left:5px;" title="Order Book Imbalance (OBI): 衡量买卖盘口压力差异。&#10;> 0: 买单墙更厚 (支撑强)&#10;< 0: 卖单墙更厚 (阻力强)">ℹ️</span>
                    <select id="obi-symbol" onchange="loadRealtimeOBI()" style="margin-left:auto;background:#333;color:#fff;border:1px solid #555;padding:4px 8px;border-radius:4px;font-size:12px">
                        <option value="BTCUSDT">Loading...</option>
                    </select>
                </h2>
                <div id="realtime-obi" class="loading">连接 WebSocket...</div>
            </div>
            
            <!-- 实时 CVD -->
            <div class="card" style="border-color: #00ff88;">
                <h2>📈 实时 CVD
                    <span style="font-size:0.6em; color:#888; cursor:help; margin-left:5px;" title="此处 CVD 为 Binance 合约逐笔成交推导的净买卖额差(USDT 口径)。&#10;每个周期展示的是该周期内的 Δ；下方“累积 CVD”为服务启动以来累积。&#10;如需 Spot vs Futures 的 USDT 口径对比，请点“深度分析”。">?</span>
                    <div style="margin-left:auto;display:flex;gap:10px;align-items:center">
                        <button onclick="showCVDAnalysis()" style="background:transparent;border:1px solid #00ff88;color:#00ff88;border-radius:4px;padding:2px 8px;cursor:pointer;font-size:12px">深度分析</button>
                        <select id="cvd-symbol" onchange="loadRealtimeCVD()" style="background:#333;color:#fff;border:1px solid #555;padding:4px 8px;border-radius:4px;font-size:12px">
                            <option value="BTCUSDT">Loading...</option>
                        </select>
                    </div>
                </h2>
                <div id="realtime-cvd" class="loading">连接 WebSocket...</div>
            </div>
            
            <!-- 实时波动率 -->
            <div class="card" style="border-color: #ff69b4;">
                <h2>📉 实时波动率</h2>
                <div id="realtime-volatility" class="loading">连接 WebSocket...</div>
            </div>
        </div>
        
        <div class="footer">
            <button class="refresh-btn" onclick="refreshAll()">🔄 刷新</button>
            <span id="last-update" style="margin-left: 20px;"></span>
        </div>
    </div>
    
    <!-- CVD Modal -->
    <div id="cvd-modal" class="modal">
        <div class="modal-content">
            <span class="close-btn" onclick="closeCVDModal()">&times;</span>
            <h2 id="cvd-modal-title" style="color:#00ff88;margin-bottom:10px">CVD 深度分析</h2>
            <div id="cvd-modal-body" class="loading">加载数据中...</div>
        </div>
    </div>
    
    <!-- Report Modal -->
    <div id="report-modal" class="modal">
        <div class="modal-content" style="max-width: 600px;">
            <span class="close-btn" onclick="closeReportModal()">&times;</span>
            <h2 style="color:#00ff88;margin-bottom:10px">📄 市场报告</h2>
            <div style="margin-bottom:15px;display:flex;gap:10px">
                <input type="text" id="report-symbol" placeholder="BTCUSDT" value="BTCUSDT" style="flex:1;background:#333;color:#fff;border:1px solid #555;padding:8px;border-radius:4px" />
                <button onclick="generateReport()" style="background:#00ff88;color:#000;border:none;padding:8px 16px;border-radius:4px;cursor:pointer;font-weight:bold">生成</button>
            </div>
            <textarea id="report-content" style="width:100%;height:300px;background:#000;color:#0f0;border:1px solid #333;padding:10px;font-family:monospace;resize:vertical" readonly></textarea>
            <button onclick="copyReport()" style="margin-top:10px;width:100%;background:#444;color:#fff;border:none;padding:10px;border-radius:4px;cursor:pointer">📋 复制到剪贴板</button>
        </div>
    </div>
    
    <script>
        let oiChart = null;
        let ratioChart = null;
        
        function formatNumber(n) {
            if (Math.abs(n) >= 1e8) return (n/1e8).toFixed(2) + '亿';
            if (Math.abs(n) >= 1e4) return (n/1e4).toFixed(2) + '万';
            return n.toFixed(2);
        }

        function formatCompact(num) {
            // 如: -3.16m, 11.12b
            if (num === null || num === undefined) return '-';
            const abs = Math.abs(num);
            if (abs >= 1e9) return (num / 1e9).toFixed(2) + 'b';
            if (abs >= 1e6) return (num / 1e6).toFixed(2) + 'm';
            if (abs >= 1e3) return (num / 1e3).toFixed(2) + 'k';
            return num.toFixed(2);
        }
        
        function formatChange(n) {
            const cls = n >= 0 ? 'positive' : 'negative';
            const sign = n >= 0 ? '+' : '';
            return `<span class="${cls}">${sign}${n.toFixed(2)}%</span>`;
        }
        
        async function loadCoinGrid() {
            try {
                const resp = await fetch('/api/markets');
                const data = await resp.json();
                
                let html = '';
                data.markets.forEach(m => {
                    const cls = m.change >= 0 ? 'positive' : 'negative';
                    const sign = m.change >= 0 ? '+' : '';
                    html += `
                        <div class="coin-card">
                            <div class="coin-symbol">${m.symbol.replace('USDT', '')}</div>
                            <div class="coin-price">$${formatNumber(m.price)}</div>
                            <div class="coin-change ${cls}">${sign}${m.change.toFixed(2)}%</div>
                        </div>
                    `;
                });
                document.getElementById('coin-grid').innerHTML = html;
            } catch (e) {
                document.getElementById('coin-grid').innerHTML = '加载失败';
            }
        }
        
        async function loadBTCDetail() {
            try {
                const resp = await fetch('/api/market/BTCUSDT');
                const d = await resp.json();
                
                document.getElementById('btc-detail').innerHTML = `
                    <div class="metric"><span class="metric-label">价格</span><span class="metric-value">$${formatNumber(d.price)}</span></div>
                    <div class="metric"><span class="metric-label">24h变化</span><span class="metric-value">${formatChange(d.price_change)}</span></div>
                    <div class="metric"><span class="metric-label">资金费率</span><span class="metric-value">${d.funding_rate >= 0 ? '+' : ''}${d.funding_rate.toFixed(4)}%</span></div>
                    <div class="metric"><span class="metric-label">持仓量</span><span class="metric-value">${formatNumber(d.oi_quantity)} BTC</span></div>
                    <div class="metric"><span class="metric-label">持仓价值</span><span class="metric-value">${formatNumber(d.oi_value)} U</span></div>
                    <div class="metric"><span class="metric-label">多空比</span><span class="metric-value">${d.long_short_ratio.toFixed(2)}</span></div>
                    <div class="metric"><span class="metric-label">大户持仓比</span><span class="metric-value">${d.top_position_ratio.toFixed(2)}</span></div>
                `;
            } catch (e) {
                document.getElementById('btc-detail').innerHTML = '加载失败';
            }
        }
        
        async function loadOIChart() {
            try {
                const resp = await fetch('/api/oi/BTCUSDT');
                const data = await resp.json();
                
                const labels = data.history.map(h => h.time);
                const values = data.history.map(h => h.oi / 10000);
                
                const ctx = document.getElementById('oi-chart').getContext('2d');
                
                if (oiChart) oiChart.destroy();
                
                oiChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: 'OI (万BTC)',
                            data: values,
                            borderColor: '#00d4ff',
                            backgroundColor: 'rgba(0, 212, 255, 0.1)',
                            fill: true,
                            tension: 0.4,
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { grid: { color: 'rgba(255,255,255,0.1)' }, ticks: { color: '#888' } },
                            y: { grid: { color: 'rgba(255,255,255,0.1)' }, ticks: { color: '#888' } }
                        }
                    }
                });
            } catch (e) {
                console.error('OI chart error:', e);
            }
        }
        
        async function loadTopCoins() {
            try {
                const resp = await fetch('/api/top');
                const data = await resp.json();
                
                let html = '<div><div class="top-title">🚀 涨幅榜</div>';
                data.gainers.forEach(c => {
                    html += `<div class="list-item"><span>${c.symbol.replace('USDT','')}</span><span class="positive">+${c.change.toFixed(2)}%</span></div>`;
                });
                html += '</div><div><div class="top-title">📉 跌幅榜</div>';
                data.losers.forEach(c => {
                    html += `<div class="list-item"><span>${c.symbol.replace('USDT','')}</span><span class="negative">${c.change.toFixed(2)}%</span></div>`;
                });
                html += '</div>';
                
                document.getElementById('top-grid').innerHTML = html;
            } catch (e) {
                document.getElementById('top-grid').innerHTML = '加载失败';
            }
        }
        
        async function loadEMAData() {
            try {
                const tf = document.getElementById('ema-tf').value;
                document.getElementById('ema-data').innerHTML = '<div class="loading">扫描中...</div>';
                
                const resp = await fetch(`/api/ema?tf=${tf}&top=50`);
                const data = await resp.json();

                if (data.disabled) {
                    emaDisabled = true;
                    document.getElementById('ema-data').innerHTML = '<div style="color:#666">EMA 雷达已禁用 (ema_radar.enabled=false)</div>';
                    return;
                }
                
                const tfLabel = tf.toUpperCase();
                let html = `<div><strong>🚀 最强 Uptrend (${tfLabel})</strong>`;
                data.uptrend.slice(0, 5).forEach((s, i) => {
                    const flower = s.flowering && s.flower_type === 'bullish' ? '<span class="flower">🌸</span>' : '';
                    const near = s.near_ema ? `<small style="color:#666">[${s.near_ema}]</small>` : '';
                    html += `<div class="ema-item">${i+1}. ${flower}${s.symbol.replace('USDT','')} (${s.streak} bars) ${near}</div>`;
                });
                
                html += `</div><div style="margin-top:12px"><strong>📉 最弱 Downtrend (${tfLabel})</strong>`;
                data.downtrend.slice(0, 5).forEach((s, i) => {
                    const flower = s.flowering && s.flower_type === 'bearish' ? '<span class="flower-bear">🥀</span>' : '';
                    const near = s.near_ema ? `<small style="color:#666">[${s.near_ema}]</small>` : '';
                    html += `<div class="ema-item">${i+1}. ${flower}${s.symbol.replace('USDT','')} (${s.streak} bars) ${near}</div>`;
                });
                
                // 刚进入趋势的币种
                if (data.new_uptrend && data.new_uptrend.length > 0) {
                    html += `</div><div style="margin-top:12px;border-top:1px solid #eee;padding-top:8px"><strong>🆕 刚进入强势 (${tfLabel})</strong>`;
                    data.new_uptrend.slice(0, 5).forEach((s, i) => {
                        const flower = s.flowering && s.flower_type === 'bullish' ? '<span class="flower">🌸</span>' : '';
                        html += `<div class="ema-item" style="color:#4CAF50">${i+1}. ${flower}${s.symbol.replace('USDT','')} (${s.streak} bars)</div>`;
                    });
                }
                
                if (data.new_downtrend && data.new_downtrend.length > 0) {
                    html += `</div><div style="margin-top:12px"><strong>🆕 刚进入弱势 (${tfLabel})</strong>`;
                    data.new_downtrend.slice(0, 5).forEach((s, i) => {
                        const flower = s.flowering && s.flower_type === 'bearish' ? '<span class="flower-bear">🥀</span>' : '';
                        html += `<div class="ema-item" style="color:#F44336">${i+1}. ${flower}${s.symbol.replace('USDT','')} (${s.streak} bars)</div>`;
                    });
                }
                
                // 🆕 刚进入多头排列/空头排列
                if (data.new_bullish_flowering && data.new_bullish_flowering.length > 0) {
                    html += `</div><div style="margin-top:12px;border-top:1px solid #333;padding-top:8px"><strong>🌸 刚进多头排列 (${tfLabel})</strong>`;
                    data.new_bullish_flowering.slice(0, 5).forEach((s, i) => {
                        html += `<div class="ema-item" style="color:#FFD700">${i+1}. 🌸 ${s.symbol.replace('USDT','')} (排列${s.flower_streak}根)</div>`;
                    });
                }
                
                if (data.new_bearish_flowering && data.new_bearish_flowering.length > 0) {
                    html += `</div><div style="margin-top:12px"><strong>🥀 刚进空头排列 (${tfLabel})</strong>`;
                    data.new_bearish_flowering.slice(0, 5).forEach((s, i) => {
                        html += `<div class="ema-item" style="color:#FF6B6B">${i+1}. 🥀 ${s.symbol.replace('USDT','')} (排列${s.flower_streak}根)</div>`;
                    });
                }
                html += '</div>';
                
                document.getElementById('ema-data').innerHTML = html;
            } catch (e) {
                document.getElementById('ema-data').innerHTML = '加载失败';
            }
        }
        
        async function loadWaveTrend() {
            try {
                const tf = document.getElementById('wt-tf').value;
                document.getElementById('wt-data').innerHTML = '<div class="loading">扫描中...</div>';
                
                const resp = await fetch(`/api/wavetrend?tf=${tf}`);
                const data = await resp.json();
                
                const tfLabel = tf.toUpperCase();
                let html = '';
                
                // 超买/超卖信号
                if (data.overbought.length > 0 || data.oversold.length > 0) {
                    html += '<div><strong>🎯 超买超卖</strong>';
                    data.overbought.slice(0, 3).forEach(s => {
                        html += `<div style="color:#F44336">🔴 ${s.symbol.replace('USDT','')} 超买 OSC=${s.osc}</div>`;
                    });
                    data.oversold.slice(0, 3).forEach(s => {
                        html += `<div style="color:#4CAF50">🟢 ${s.symbol.replace('USDT','')} 超卖 OSC=${s.osc}</div>`;
                    });
                    html += '</div>';
                }
                
                // 波动预警
                if (data.squeeze.length > 0) {
                    html += '<div style="margin-top:10px"><strong>⚠️ 波动预警</strong>';
                    data.squeeze.slice(0, 5).forEach(s => {
                        const isL2 = s.signal_type === 'squeeze_l2';
                        const emoji = isL2 ? '🔥' : '⚠️';
                        const level = isL2 ? 'L2' : 'L1';
                        html += `<div style="color:${isL2 ? '#FF9800' : '#FFC107'}">${emoji} ${s.symbol.replace('USDT','')} ${level} (${s.squeeze_bars}bars)</div>`;
                    });
                    html += '</div>';
                }
                
                // 背离信号
                if (data.divergence.length > 0) {
                    html += '<div style="margin-top:10px"><strong>🔀 背离信号</strong>';
                    data.divergence.slice(0, 5).forEach(s => {
                        const isBull = s.signal_type === 'bull_div';
                        const emoji = isBull ? '📈' : '📉';
                        const type = isBull ? '多头' : '空头';
                        const mPlus = s.m_plus ? ' M+' : '';
                        html += `<div style="color:${isBull ? '#4CAF50' : '#F44336'}">${emoji} ${s.symbol.replace('USDT','')} ${type}背离 L${s.level}${mPlus}</div>`;
                    });
                    html += '</div>';
                }
                
                if (!html) {
                    html = '<div style="color:#888">暂无信号</div>';
                }
                
                document.getElementById('wt-data').innerHTML = html;
            } catch (e) {
                document.getElementById('wt-data').innerHTML = '加载失败';
            }
        }
        
        async function loadRatioChart() {
            try {
                const resp = await fetch('/api/ratios/BTCUSDT?hours=12');
                const data = await resp.json();
                
                const labels = data.history.map(h => h.time);
                const lsRatio = data.history.map(h => h.ls_ratio);
                const topPos = data.history.map(h => h.top_position);
                
                // 表格数据
                let tableHtml = '<div style="font-size:0.85em"><small>时间 | 多空比 | 大户持仓</small>';
                data.history.slice(0, 8).forEach(h => {
                    tableHtml += `<div class="list-item"><span>${h.time}</span><span>${h.ls_ratio.toFixed(2)} | ${h.top_position.toFixed(2)}</span></div>`;
                });
                tableHtml += '</div>';
                document.getElementById('ratio-data').innerHTML = tableHtml;
                
                // 图表
                const ctx = document.getElementById('ratio-chart').getContext('2d');
                
                if (ratioChart) ratioChart.destroy();
                
                ratioChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: '多空比',
                            data: lsRatio,
                            borderColor: '#00ff88',
                            tension: 0.4,
                        }, {
                            label: '大户持仓比',
                            data: topPos,
                            borderColor: '#ff69b4',
                            tension: 0.4,
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { labels: { color: '#888' } } },
                        scales: {
                            x: { grid: { color: 'rgba(255,255,255,0.1)' }, ticks: { color: '#888' } },
                            y: { grid: { color: 'rgba(255,255,255,0.1)' }, ticks: { color: '#888' } }
                        }
                    }
                });
            } catch (e) {
                console.error('Ratio chart error:', e);
            }
        }
        
        async function loadOISpikes() {
            try {
                // 多重确认: OI变化>=5%, OI价值>=500万U, 过滤新币
                const resp = await fetch('/api/oi-spikes?oi_threshold=5');
                const data = await resp.json();

                if (data.disabled) {
                    oiSpikesDisabled = true;
                    document.getElementById('oi-spikes').innerHTML = '<div style="color:#666">OI Scanner 已禁用 (oi_spike_scanner.enabled=false)</div>';
                    return;
                }
                
                if (!data.spikes || data.spikes.length === 0) {
                    document.getElementById('oi-spikes').innerHTML = '<div style="color:#666">暂无异动 (阈值5%, 过滤新币)</div>';
                    return;
                }
                
                let html = '<div class="ema-list">';
                data.spikes.forEach(s => {
                    const sign = s.oi_change >= 0 ? '+' : '';
                    const reasons = s.reasons.join(' ');
                    html += `<div class="list-item" style="flex-wrap:wrap">
                        <span>${s.signal_emoji} ${s.symbol.replace('USDT','')}</span>
                        <span>OI ${sign}${s.oi_change.toFixed(1)}% | ${s.signal_level}</span>
                    </div>
                    <div style="font-size:0.8em;color:#888;margin-bottom:6px">${reasons}</div>`;
                });
                html += '</div>';
                
                document.getElementById('oi-spikes').innerHTML = html;
            } catch (e) {
                document.getElementById('oi-spikes').innerHTML = '<div style="color:#666">加载失败</div>';
            }
        }
        
        async function loadRealtimeOBI() {
            try {
                const symbol = document.getElementById('obi-symbol').value;
                const resp = await fetch(`/api/realtime/all?symbol=${symbol}`);
                const data = await resp.json();
                
                if (data.error) {
                    document.getElementById('realtime-obi').innerHTML = '<div style="color:#666">等待 WebSocket 连接...</div>';
                    return;
                }
                
                // 显示多周期 OBI
                const coinName = symbol.replace('USDT','');
                const timeframes = ['1m', '5m', '15m', '1h'];
                let html = `<div style="font-size:13px;color:#888;margin-bottom:8px">${coinName} 盘口不平衡</div>`;
                html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;text-align:center">';
                
                timeframes.forEach(tf => {
                    const tfData = data[tf];
                    if (tfData && tfData.current) {
                        const obi = tfData.current.obi_avg;
                        const cls = obi >= 0 ? 'positive' : 'negative';
                        const sign = obi >= 0 ? '+' : '';
                        html += `<div style="background:rgba(255,255,255,0.05);padding:8px;border-radius:6px">
                            <div style="color:#888;font-size:12px">${tf}</div>
                            <div class="${cls}" style="font-weight:600">${sign}${obi.toFixed(1)}%</div>
                        </div>`;
                    }
                });
                html += '</div>';
                
                document.getElementById('realtime-obi').innerHTML = html;
            } catch (e) {
                document.getElementById('realtime-obi').innerHTML = '<div style="color:#666">加载失败</div>';
            }
        }
        
        async function loadRealtimeCVD() {
            try {
                const symbol = document.getElementById('cvd-symbol').value;
                const resp = await fetch(`/api/realtime/all?symbol=${symbol}`);
                const data = await resp.json();
                
                if (data.error) {
                    document.getElementById('realtime-cvd').innerHTML = '<div style="color:#666">等待 WebSocket 连接...</div>';
                    return;
                }
                
                // 显示多周期 CVD
                const coinName = symbol.replace('USDT','');
                const timeframes = ['1m', '5m', '15m', '1h'];
                let html = `<div style="font-size:13px;color:#888;margin-bottom:8px">${coinName} 成交量差 Δ (USDT)</div>`;
                html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;text-align:center">';
                
                timeframes.forEach(tf => {
                    const tfData = data[tf];
                    if (tfData && tfData.current) {
                        const cvd = tfData.current.cvd;
                        const cls = cvd >= 0 ? 'positive' : 'negative';
                        const sign = cvd >= 0 ? '+' : '';
                        html += `<div style="background:rgba(255,255,255,0.05);padding:8px;border-radius:6px">
                            <div style="color:#888;font-size:12px">${tf}</div>
                            <div class="${cls}" style="font-weight:600">${sign}${cvd.toFixed(2)}</div>
                        </div>`;
                    }
                });
                html += '</div>';
                
                // 累积 CVD
                if (data['1m'] && data['1m'].cumulative_cvd !== undefined) {
                    const cumCvd = data['1m'].cumulative_cvd;
                    const cls = cumCvd >= 0 ? 'positive' : 'negative';
                    const sign = cumCvd >= 0 ? '+' : '';
                    const emoji = cumCvd >= 0 ? '🟢 净买入' : '🔴 净卖出';
                    html += `<div class="metric" style="margin-top:10px"><span class="metric-label">累积 CVD (启动以来)</span><span class="metric-value ${cls}">${sign}${cumCvd.toFixed(2)} ${emoji}</span></div>`;
                }
                
                document.getElementById('realtime-cvd').innerHTML = html;
            } catch (e) {
                document.getElementById('realtime-cvd').innerHTML = '<div style="color:#666">加载失败</div>';
            }
        }
        
        async function loadRealtimeVolatility() {
            try {
                const btcResp = await fetch('/api/realtime/volatility?symbol=BTCUSDT');
                const ethResp = await fetch('/api/realtime/volatility?symbol=ETHUSDT');
                
                const btc = await btcResp.json();
                const eth = await ethResp.json();
                
                if (btc.error && eth.error) {
                    document.getElementById('realtime-volatility').innerHTML = '<div style="color:#666">等待 WebSocket 连接...</div>';
                    return;
                }
                
                const formatVol = (d) => {
                    if (!d || d.error) return '<div style="color:#666">无数据</div>';
                    const vol = d.volatility_pct;
                    let level = '低';
                    let emoji = '🟢';
                    if (vol > 50) { level = '高'; emoji = '🔴'; }
                    else if (vol > 20) { level = '中'; emoji = '🟡'; }
                    return `<div class="metric"><span class="metric-label">${d.symbol.replace('USDT','')}</span><span class="metric-value">${vol.toFixed(1)}% ${emoji} ${level}</span></div>`;
                };
                
                document.getElementById('realtime-volatility').innerHTML = formatVol(btc) + formatVol(eth);
            } catch (e) {
                document.getElementById('realtime-volatility').innerHTML = '<div style="color:#666">加载失败</div>';
            }
        }
        
        let emaDisabled = false;
        let oiSpikesDisabled = false;
        let lastHeavyRefreshTs = 0;
        async function refreshAll() {
            const now = Date.now();
            const doHeavy = (now - lastHeavyRefreshTs) > 5 * 60 * 1000;
            
            const tasks = [
                loadCoinGrid(),
                loadBTCDetail(),
                loadOIChart(),
                loadTopCoins(),
                loadRealtimeOBI(),
                loadRealtimeCVD(),
                loadRealtimeVolatility(),
            ];
            
            // Heavy endpoints (Binance REST intensive): run every 5 minutes
            if (doHeavy) {
                tasks.push(loadRatioChart());
                if (!oiSpikesDisabled) tasks.push(loadOISpikes());
                if (!emaDisabled) tasks.push(loadEMAData());
                lastHeavyRefreshTs = now;
            }
            
            await Promise.all(tasks);
            document.getElementById('last-update').textContent = '更新: ' + new Date().toLocaleTimeString();
        }
        
        // 搜索功能
        async function searchCoin() {
            const input = document.getElementById('search-input').value.trim().toUpperCase();
            if (!input) return;
            
            const symbol = input.endsWith('USDT') ? input : input + 'USDT';
            
            document.getElementById('search-content').innerHTML = '<div class="loading">搜索中...</div>';
            document.getElementById('search-result').style.display = 'block';
            document.getElementById('search-title').textContent = '🔍 ' + symbol.replace('USDT', '') + ' 详情';
            
            try {
                const resp = await fetch(`/api/market/${symbol}`);
                const d = await resp.json();
                
                if (d.error) {
                    document.getElementById('search-content').innerHTML = '<div style="color:#ff4444">未找到该币种</div>';
                    return;
                }
                
                document.getElementById('search-content').innerHTML = `
                    <div class="metric"><span class="metric-label">价格</span><span class="metric-value">$${formatNumber(d.price)}</span></div>
                    <div class="metric"><span class="metric-label">24h变化</span><span class="metric-value">${formatChange(d.price_change)}</span></div>
                    <div class="metric"><span class="metric-label">资金费率</span><span class="metric-value">${d.funding_rate >= 0 ? '+' : ''}${d.funding_rate.toFixed(4)}%</span></div>
                    <div class="metric"><span class="metric-label">持仓量</span><span class="metric-value">${formatNumber(d.oi_quantity)}</span></div>
                    <div class="metric"><span class="metric-label">持仓价值</span><span class="metric-value">${formatNumber(d.oi_value)} U</span></div>
                    <div class="metric"><span class="metric-label">多空比</span><span class="metric-value">${d.long_short_ratio.toFixed(2)}</span></div>
                    <div class="metric"><span class="metric-label">大户账户比</span><span class="metric-value">${d.top_trader_ratio.toFixed(2)}</span></div>
                    <div class="metric"><span class="metric-label">大户持仓比</span><span class="metric-value">${d.top_position_ratio.toFixed(2)}</span></div>
                `;
            } catch (e) {
                document.getElementById('search-content').innerHTML = '<div style="color:#ff4444">查询失败</div>';
            }
        }
        
        function closeSearch() {
            document.getElementById('search-result').style.display = 'none';
            document.getElementById('search-input').value = '';
        }
        
        // 回车键搜索
        document.getElementById('search-input').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') searchCoin();
        });
        
        async function initSymbolSelectors() {
            try {
                const resp = await fetch('/api/realtime/all');
                const data = await resp.json();
                
                if (data.error) return;
                
                const symbols = Object.keys(data).sort();
                if (symbols.length === 0) return;
                
                ['obi-symbol', 'cvd-symbol'].forEach(id => {
                    const select = document.getElementById(id);
                    // 保存当前选择 (如果已有)
                    const current = select.value;
                    
                    select.innerHTML = '';
                    
                    symbols.forEach(sym => {
                        const opt = document.createElement('option');
                        opt.value = sym;
                        opt.text = sym.replace('USDT', '');
                        select.appendChild(opt);
                    });
                    
                    // 恢复选择或默认
                    if (symbols.includes(current)) {
                        select.value = current;
                    } else if (symbols.includes('BTCUSDT')) {
                        select.value = 'BTCUSDT';
                    }
                });
                
            } catch (e) {
                console.error("Failed to init symbols", e);
            }
        }

        // CVD Analysis Functions
        function closeCVDModal() {
            document.getElementById('cvd-modal').style.display = "none";
        }
        
        window.onclick = function(event) {
            const modal = document.getElementById('cvd-modal');
            if (event.target == modal) {
                modal.style.display = "none";
            }
        }

        async function showCVDAnalysis() {
            const symbol = document.getElementById('cvd-symbol').value;
            const modal = document.getElementById('cvd-modal');
            const title = document.getElementById('cvd-modal-title');
            const body = document.getElementById('cvd-modal-body');
            
            modal.style.display = "block";
            title.textContent = `📊 CVD 深度分析: ${symbol.replace('USDT','')}`;
            body.innerHTML = '<div class="loading">正在从 K 线计算 Spot vs Futures 数据...</div>';
            
            try {
                const resp = await fetch(`/api/cvd/analysis?symbol=${symbol}`);
                const data = await resp.json();
                
                if (data.error) {
                    body.innerHTML = `<div style="color:red">Error: ${data.error}</div>`;
                    return;
                }
                
                // 固定顺序
                const order = [
                    "1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "24h", 
                    "2d", "3d", "5d", "7d", "10d", "15d", "30d", "60d"
                ];
                
                let html = `
                    <table class="cvd-table">
                        <thead>
                            <tr>
                                <th style="text-align:left">Timeframe</th>
                                <th>Futures CVD (U)</th>
                                <th>Spot CVD (U)</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                order.forEach(tf => {
                    const row = data[tf];
                    if (row) {
                        const spotCls = row.spot >= 0 ? 'positive' : 'negative';
                        const futCls = row.futures >= 0 ? 'positive' : 'negative';
                        
                        html += `
                            <tr>
                                <td style="text-align:left;color:#fff">${tf}</td>
                                <td class="${futCls}">${formatCompact(row.futures)}</td>
                                <td class="${spotCls}">${formatCompact(row.spot)}</td>
                            </tr>
                        `;
                    }
                });
                
                html += '</tbody></table>';
                body.innerHTML = html;
                
            } catch (e) {
                console.error(e);
                body.innerHTML = '<div style="color:red">加载失败</div>';
            }
        }

        // Report Functions
        function openReportModal() {
            document.getElementById('report-modal').style.display = 'block';
        }
        function closeReportModal() {
            document.getElementById('report-modal').style.display = 'none';
        }
        
        async function generateReport() {
            const symbol = document.getElementById('report-symbol').value;
            const textarea = document.getElementById('report-content');
            textarea.value = "正在生成报告 (获取 K 线、现货、期货数据)...";
            
            try {
                const resp = await fetch(`/api/report?symbol=${symbol}`);
                const data = await resp.json();
                
                if (data.error) {
                    textarea.value = "Error: " + data.error;
                    return;
                }
                
                textarea.value = data.text;
            } catch (e) {
                textarea.value = "Request Failed: " + e;
            }
        }
        
        function copyReport() {
            const textarea = document.getElementById('report-content');
            textarea.select();
            document.execCommand('copy');
            alert("已复制到剪贴板!");
        }

        // 初始加载
        initSymbolSelectors().then(() => {
            loadRealtimeOBI();
            loadRealtimeCVD();
        });
        refreshAll();
        
        // 全局刷新 (60秒)
        setInterval(refreshAll, 60000);
        
        // 实时数据也跟随这个 60秒节奏，或者你可以手动保留一个较慢的独立刷新
        // 既然用户要求 1min，那我们在 refreshAll 里已经有了 loadRealtimeOBI/CVD 的调用?
        // 检查 refreshAll 定义: 
        // 行 1068: loadRealtimeOBI(), loadRealtimeCVD(), loadRealtimeVolatility()
        // 所以只要删掉下面的独立定时器即可。
    </script>
</body>
</html>'''


async def main():
    dashboard = WebDashboard(DashboardConfig(port=8080))
    await dashboard.start()
    
    print("=" * 50)
    print("📊 UniTrade Web Dashboard")
    print("=" * 50)
    print(f"Open: http://localhost:8080")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        await dashboard.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
