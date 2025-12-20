"""
UniTrade CLI 入口

用法:
    unitrade scan         # 运行市场扫描器
    unitrade ema          # EMA 趋势雷达
    unitrade serve        # Bot + Dashboard 一键启动
    unitrade server       # 启动 WebSocket 服务器
    unitrade dashboard    # 启动 Web Dashboard
    unitrade bot          # 启动 Telegram Bot
    unitrade backtest     # 运行回测示例
    unitrade validate     # 验证指标计算
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

import yaml

# 确保模块路径正确
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _load_yaml(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _resolve_default_config() -> dict:
    candidates: list[Path] = []

    if env_path := os.getenv("UNITRADE_CONFIG"):
        candidates.append(Path(env_path))

    candidates.append(Path("config/default.yaml"))
    candidates.append(Path(__file__).resolve().parents[2] / "config" / "default.yaml")

    for path in candidates:
        if path.exists():
            return _load_yaml(path)

    return {}


def _load_telegram_settings() -> tuple[str, str, dict]:
    """
    获取 Telegram 配置（优先级: CLI/env > config/default.yaml）。

    Returns:
        (bot_token, chat_id, topics_dict)
    """
    cfg = _resolve_default_config()
    tg = cfg.get("telegram", {}) if isinstance(cfg, dict) else {}
    topics = tg.get("topics", {}) if isinstance(tg, dict) else {}

    if isinstance(tg, dict) and tg.get("enabled", True) is False:
        return "", "", {}

    bot_token = str(tg.get("bot_token") or "").strip()
    chat_id = str(tg.get("chat_id") or "").strip()

    return bot_token, chat_id, topics if isinstance(topics, dict) else {}


def setup_logging(debug: bool = False):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


async def cmd_scan(args):
    """运行市场扫描器"""
    from unitrade.scanner import MarketScanner, MarketScannerConfig
    
    config = MarketScannerConfig(
        oi_enabled=not args.no_oi,
        oi_threshold=1 + args.oi_threshold,
        funding_enabled=not args.no_funding,
        funding_threshold=args.funding_threshold,
        liquidation_enabled=not args.no_liq,
        scan_interval_minutes=args.interval,
        telegram_enabled=bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
    )
    
    scanner = MarketScanner(config)
    
    async def on_oi(alert):
        print(f"🚨 OI: {alert.symbol} +{alert.oi_spike_pct:.1f}%")
    
    async def on_funding(alert):
        print(f"💰 FR: {alert.symbol} {alert.funding_rate_pct:+.4f}%")
    
    async def on_liq(event):
        d = "L" if event.is_long_liquidation else "S"
        print(f"💥 LIQ: {event.symbol} {d} ${float(event.notional):,.0f}")
    
    scanner.set_oi_callback(on_oi)
    scanner.set_funding_callback(on_funding)
    scanner.set_liquidation_callback(on_liq)
    
    await scanner.start()
    
    try:
        if args.continuous:
            print("Running continuously... (Ctrl+C to stop)")
            await scanner.run_continuous()
        else:
            await scanner.scan_once()
    except KeyboardInterrupt:
        pass
    finally:
        await scanner.stop()


async def cmd_ema(args):
    """EMA 趋势雷达"""
    from unitrade.scanner import EMARadar, EMARadarConfig
    
    config = EMARadarConfig(
        auto_top_n=args.top,
        timeframes=[args.timeframe],
        top_n_results=args.results,
    )
    
    radar = EMARadar(config)
    await radar.start()
    
    print(f"📡 Scanning {args.top} symbols on {args.timeframe}...")
    
    try:
        results = await radar.scan(args.timeframe)
        print(radar.format_telegram_report(results, args.timeframe))
    finally:
        await radar.stop()


async def cmd_server(args):
    """启动 WebSocket 服务器"""
    from unitrade.server import DataWebSocketServer
    
    server = DataWebSocketServer(host="0.0.0.0", port=args.port)
    await server.start()
    
    print(f"📡 WebSocket Server: ws://localhost:{args.port}/ws")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        await server.stop()


async def cmd_backtest(args):
    """运行回测示例"""
    from unitrade.backtest import Backtester, MarketData, example_strategy
    from decimal import Decimal
    from datetime import datetime, timedelta
    import random
    
    # 生成模拟数据
    data = []
    price = Decimal("50000")
    base_time = datetime(2024, 1, 1)
    
    for i in range(args.bars):
        change = Decimal(str(random.uniform(-0.02, 0.02)))
        price = price * (1 + change)
        
        data.append(MarketData(
            timestamp=base_time + timedelta(hours=i),
            open=price,
            high=price * Decimal("1.005"),
            low=price * Decimal("0.995"),
            close=price,
            volume=Decimal(str(random.randint(100, 1000))),
            obi=random.uniform(-0.5, 0.5),
            regime=random.choice(["long_build", "short_build", "neutral"]),
        ))
    
    bt = Backtester("BTCUSDT", initial_capital=Decimal(str(args.capital)))
    bt.load_data(data)
    bt.set_strategy(example_strategy)
    
    result = bt.run()
    print(result.summary())


async def cmd_validate(args):
    """验证指标计算"""
    print("Running indicator validation...")
    
    # 动态导入验证脚本的逻辑
    import aiohttp
    import json
    
    ws_url = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
    
    print(f"Connecting to {args.symbol}...")
    
    buy_volume = 0
    sell_volume = 0
    trade_count = 0
    
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url) as ws:
            start_time = asyncio.get_event_loop().time()
            
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    
                    qty = float(data["q"])
                    is_buyer_maker = data["m"]
                    
                    if is_buyer_maker:
                        sell_volume += qty
                    else:
                        buy_volume += qty
                    
                    trade_count += 1
                    
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed >= args.duration:
                        break
    
    cvd = buy_volume - sell_volume
    print(f"\n📊 Results ({args.duration}s):")
    print(f"Buy Volume: {buy_volume:.4f}")
    print(f"Sell Volume: {sell_volume:.4f}")
    print(f"CVD: {cvd:+.4f}")
    print(f"Trades: {trade_count}")


def main():
    parser = argparse.ArgumentParser(
        prog="unitrade",
        description="UniTrade Analytics Gateway CLI"
    )
    parser.add_argument("--debug", action="store_true", help="Debug mode")
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # scan command
    scan_parser = subparsers.add_parser("scan", help="Run market scanner")
    scan_parser.add_argument("-c", "--continuous", action="store_true")
    scan_parser.add_argument("-i", "--interval", type=int, default=5)
    scan_parser.add_argument("--oi-threshold", type=float, default=0.15)
    scan_parser.add_argument("--funding-threshold", type=float, default=0.0005)
    scan_parser.add_argument("--no-oi", action="store_true")
    scan_parser.add_argument("--no-funding", action="store_true")
    scan_parser.add_argument("--no-liq", action="store_true")
    
    # ema command
    ema_parser = subparsers.add_parser("ema", help="EMA Trend Radar")
    ema_parser.add_argument("-t", "--timeframe", default="1h", help="Timeframe (1h, 4h, 1d)")
    ema_parser.add_argument("--top", type=int, default=100, help="Top N symbols to scan")
    ema_parser.add_argument("--results", type=int, default=10, help="Show top N results")
    
    # report command
    report_parser = subparsers.add_parser("report", help="Generate market report")
    report_parser.add_argument("symbol", nargs="?", default="BTCUSDT", help="Symbol (e.g. BTCUSDT)")
    
    # track command
    track_parser = subparsers.add_parser("track", help="Run fund flow tracker")
    track_parser.add_argument("-s", "--symbols", default="BTCUSDT,ETHUSDT", help="Symbols to track")
    
    # server command
    server_parser = subparsers.add_parser("server", help="Start WebSocket server")
    server_parser.add_argument("-p", "--port", type=int, default=8765)
    
    # dashboard command
    dash_parser = subparsers.add_parser("dashboard", help="Start Web Dashboard")
    dash_parser.add_argument("-p", "--port", type=int, default=8080)
    dash_parser.add_argument("--no-realtime", action="store_true", help="Disable realtime WebSocket service")

    # serve command (bot + dashboard)
    serve_parser = subparsers.add_parser("serve", help="Start Bot + Dashboard")
    serve_parser.add_argument("-p", "--port", type=int, default=8080, help="Dashboard port")
    serve_parser.add_argument("--no-bot", action="store_true", help="Run dashboard only")
    serve_parser.add_argument("--token", help="Telegram bot token (or env TELEGRAM_BOT_TOKEN)")
    serve_parser.add_argument("--chat-id", help="Telegram chat ID (or env TELEGRAM_CHAT_ID)")
    serve_parser.add_argument("-i", "--interval", type=int, default=60, help="Report interval (minutes)")
    serve_parser.add_argument("--metrics-port", type=int, default=None, help="Metrics port (default 8000)")
    serve_parser.add_argument("--no-metrics", action="store_true", help="Disable metrics server")
    serve_parser.add_argument("--no-signal", action="store_true", help="Disable Signal Analyzer")
    serve_parser.add_argument("--no-market", action="store_true", help="Disable Market Scanner (OI/Funding/Liquidation)")
    serve_parser.add_argument("--no-wavetrend", action="store_true", help="Disable WaveTrend Scanner")
    serve_parser.add_argument("--no-anomaly", action="store_true", help="Disable Anomaly Detector (EMA200 breakout)")
    serve_parser.add_argument("--no-realtime", action="store_true", help="Disable realtime WebSocket service")
    
    # bot command
    bot_parser = subparsers.add_parser("bot", help="Start Telegram Bot")
    bot_parser.add_argument("--token", help="Telegram bot token")
    bot_parser.add_argument("--chat-id", help="Telegram chat ID")
    bot_parser.add_argument("-i", "--interval", type=int, default=60, help="Report interval (minutes)")
    
    # backtest command
    bt_parser = subparsers.add_parser("backtest", help="Run backtest example")
    bt_parser.add_argument("--bars", type=int, default=1000)
    bt_parser.add_argument("--capital", type=float, default=10000)
    
    # validate command
    val_parser = subparsers.add_parser("validate", help="Validate indicators")
    val_parser.add_argument("-s", "--symbol", default="BTCUSDT")
    val_parser.add_argument("-d", "--duration", type=int, default=30)
    
    args = parser.parse_args()
    setup_logging(args.debug)
    
    if args.command == "scan":
        asyncio.run(cmd_scan(args))
    elif args.command == "ema":
        asyncio.run(cmd_ema(args))
    elif args.command == "report":
        asyncio.run(cmd_report(args))
    elif args.command == "track":
        asyncio.run(cmd_track(args))
    elif args.command == "server":
        asyncio.run(cmd_server(args))
    elif args.command == "backtest":
        asyncio.run(cmd_backtest(args))
    elif args.command == "validate":
        asyncio.run(cmd_validate(args))
    elif args.command == "dashboard":
        asyncio.run(cmd_dashboard(args))
    elif args.command == "bot":
        asyncio.run(cmd_bot(args))
    elif args.command == "serve":
        asyncio.run(cmd_serve(args))
    else:
        parser.print_help()


async def cmd_report(args):
    """生成市场报告"""
    from unitrade.tracker import MarketReporter
    
    reporter = MarketReporter()
    await reporter.start()
    
    report = await reporter.generate_report(args.symbol)
    if report:
        history = await reporter.get_ratio_history(args.symbol, 5)
        print(reporter.format_telegram_report(report, history))
    else:
        print(f"Failed to generate report for {args.symbol}")
    
    await reporter.stop()


async def cmd_track(args):
    """运行资金流追踪"""
    from unitrade.tracker import FundFlowTracker, FundFlowConfig
    
    symbols = [s.strip().upper() for s in args.symbols.split(",")]
    
    config = FundFlowConfig(symbols=symbols)
    tracker = FundFlowTracker(config)
    
    print(f"📊 Fund Flow Tracker")
    print(f"Tracking: {', '.join(symbols)}")
    print("Press Ctrl+C to stop")
    print("=" * 50)
    
    await tracker.run_forever()


async def cmd_dashboard(args):
    """启动 Web 仪表板"""
    from unitrade.web import WebDashboard, DashboardConfig
    
    config = DashboardConfig(port=args.port, enable_realtime=(not getattr(args, "no_realtime", False)))
    dashboard = WebDashboard(config)
    
    await dashboard.start()
    print(f"📊 Dashboard: http://localhost:{args.port}")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        await dashboard.stop()


async def cmd_bot(args):
    """启动 Telegram Bot"""
    from unitrade.bot import TelegramBot, TelegramConfig, TopicConfig
    
    cfg = _resolve_default_config()
    tg_cfg = cfg.get("telegram", {}) if isinstance(cfg, dict) else {}
    telegram_enabled = bool(tg_cfg.get("enabled", True)) if isinstance(tg_cfg, dict) else True

    if not telegram_enabled:
        print("Telegram Bot: disabled (config telegram.enabled=false)")
        return

    scheduled = tg_cfg.get("scheduled_reports", {}) if isinstance(tg_cfg, dict) else {}
    ema_report_enabled = bool(scheduled.get("ema_report", False)) if isinstance(scheduled, dict) else False
    market_report_enabled = bool(scheduled.get("market_report", False)) if isinstance(scheduled, dict) else False

    ema_radar_enabled = cfg.get("ema_radar", {}).get("enabled", True) if isinstance(cfg, dict) else True
    ema_enabled = bool(ema_radar_enabled) and ema_report_enabled
    market_enabled = market_report_enabled

    token = args.token or os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = args.chat_id or os.getenv("TELEGRAM_CHAT_ID", "")

    topics_cfg = None
    if not token or not chat_id:
        file_token, file_chat_id, topics = _load_telegram_settings()
        token = token or file_token
        chat_id = chat_id or file_chat_id
        if topics:
            topics_cfg = TopicConfig(
                wavetrend_overbought_oversold=topics.get("wavetrend_overbought_oversold"),
                divergence_volume=topics.get("divergence_volume"),
                divergence_normal=topics.get("divergence_normal"),
                squeeze_4h_1h=topics.get("squeeze_4h_1h"),
                squeeze_daily_weekly=topics.get("squeeze_daily_weekly"),
                ema_flowering=topics.get("ema_flowering"),
                ema_entering=topics.get("ema_entering"),
                ema_ranking=topics.get("ema_ranking"),
                general=topics.get("general"),
            )
    
    if not token or not chat_id:
        print("请设置 Telegram 配置:")
        print("  $env:TELEGRAM_BOT_TOKEN='your_token'")
        print("  $env:TELEGRAM_CHAT_ID='your_chat_id'")
        print("或写入 config/default.yaml 的 telegram.bot_token / telegram.chat_id")
        print("或使用命令行参数: --token xxx --chat-id xxx")
        return
    
    config = TelegramConfig(
        bot_token=token,
        chat_id=chat_id,
        topics=topics_cfg,
        ema_report_interval=int((scheduled.get("ema_interval") if isinstance(scheduled, dict) else None) or (args.interval * 60)),
        market_report_interval=int((scheduled.get("market_interval") if isinstance(scheduled, dict) else None) or (args.interval * 60)),
        ema_enabled=ema_enabled,
        market_enabled=market_enabled,
    )
    
    bot = TelegramBot(config)
    print("🤖 Telegram Bot")
    print("Press Ctrl+C to stop")
    
    await bot.run_forever()


async def cmd_serve(args):
    """启动 Bot + Dashboard + Signal Analyzer（长期运行推荐）"""
    from unitrade.web import WebDashboard, DashboardConfig
    from unitrade.bot import TelegramBot, TelegramConfig, TopicConfig

    if args.no_metrics:
        os.environ["METRICS_ENABLED"] = "false"
    if args.metrics_port is not None:
        os.environ["METRICS_PORT"] = str(args.metrics_port)

    dashboard = WebDashboard(DashboardConfig(port=args.port, enable_realtime=(not getattr(args, "no_realtime", False))))
    await dashboard.start()
    print(f"Dashboard: http://localhost:{args.port}")

    # 加载全局配置，读取各模块 enabled 开关
    cfg = _resolve_default_config()

    tg_cfg = cfg.get("telegram", {}) if isinstance(cfg, dict) else {}
    telegram_enabled = bool(tg_cfg.get("enabled", True)) if isinstance(tg_cfg, dict) else True
    resolved_token = str((args.token or os.getenv("TELEGRAM_BOT_TOKEN", "") or (tg_cfg.get("bot_token") if isinstance(tg_cfg, dict) else "") or "")).strip()
    resolved_chat_id = str((args.chat_id or os.getenv("TELEGRAM_CHAT_ID", "") or (tg_cfg.get("chat_id") if isinstance(tg_cfg, dict) else "") or "")).strip()
    topics = tg_cfg.get("topics", {}) if isinstance(tg_cfg, dict) else {}
    topics_cfg = (
        TopicConfig(
            wavetrend_overbought_oversold=topics.get("wavetrend_overbought_oversold"),
            divergence_volume=topics.get("divergence_volume"),
            divergence_normal=topics.get("divergence_normal"),
            squeeze_4h_1h=topics.get("squeeze_4h_1h"),
            squeeze_daily_weekly=topics.get("squeeze_daily_weekly"),
            ema_flowering=topics.get("ema_flowering"),
            ema_entering=topics.get("ema_entering"),
            ema_ranking=topics.get("ema_ranking"),
            general=topics.get("general"),
        )
        if isinstance(topics, dict) and topics
        else None
    )
    scheduled = tg_cfg.get("scheduled_reports", {}) if isinstance(tg_cfg, dict) else {}
    ema_report_enabled = bool(scheduled.get("ema_report", False)) if isinstance(scheduled, dict) else False
    market_report_enabled = bool(scheduled.get("market_report", False)) if isinstance(scheduled, dict) else False

    ema_radar_enabled = cfg.get("ema_radar", {}).get("enabled", True) if isinstance(cfg, dict) else True
    ema_enabled = telegram_enabled and bool(ema_radar_enabled) and ema_report_enabled
    market_enabled = telegram_enabled and market_report_enabled

    exchanges_cfg = cfg.get("exchanges", {}) if isinstance(cfg, dict) else {}
    binance_cfg = exchanges_cfg.get("binance", {}) if isinstance(exchanges_cfg, dict) else {}
    binance_enabled = bool(binance_cfg.get("enabled", True)) if isinstance(binance_cfg, dict) else True
    if not binance_enabled:
        print("Binance: disabled (config exchanges.binance.enabled=false)")
    
    # Signal Analyzer
    signal_analyzer = None
    sd_enabled = binance_enabled and cfg.get("signal_detector", {}).get("enabled", True)
    if sd_enabled and not getattr(args, 'no_signal', False):
        try:
            from unitrade.scanner.signal_detector.analyzer import SignalAnalyzer
            from unitrade.scanner.signal_detector.config import AlertConfig, WSConfig
            
            tg_cfg = cfg.get("telegram", {}) if telegram_enabled else {}
            sd_cfg = cfg.get("signal_detector", {})
            db_cfg = cfg.get("database", {})
            
            # 配置
            alert_config = AlertConfig(
                telegram_bot_token=resolved_token if telegram_enabled else "",
                telegram_chat_id=resolved_chat_id if telegram_enabled else "",
                telegram_topic_id=sd_cfg.get("topic_id", tg_cfg.get("topics", {}).get("signal_anomaly")),
                rvol_threshold=sd_cfg.get("rvol_threshold", 1.5),
                net_flow_threshold=sd_cfg.get("net_flow_threshold", 80000),
                price_change_threshold=sd_cfg.get("price_change_threshold", 2.0),
                rebound_threshold=sd_cfg.get("rebound_threshold", 5.0),
                debounce_minutes=sd_cfg.get("debounce_minutes", 5),
            )
            
            ws_config = WSConfig(
                min_quote_volume_24h=sd_cfg.get("min_quote_volume_24h", 1_000_000),
                max_symbols=sd_cfg.get("max_symbols", 50),
            )
            
            redis_url = db_cfg.get("redis_url", "redis://localhost:6379")
            
            signal_analyzer = SignalAnalyzer(
                redis_url=redis_url,
                alert_config=alert_config,
                ws_config=ws_config
            )
            await signal_analyzer.start()
            print("Signal Analyzer: running")
        except Exception as e:
            print(f"Signal Analyzer: failed to start ({e})")
            signal_analyzer = None
    elif not sd_enabled:
        print("Signal Analyzer: disabled (config)")

    bot = None
    if not args.no_bot and telegram_enabled:
        token = resolved_token
        chat_id = resolved_chat_id

        if token and chat_id:
            if not ema_enabled and not market_enabled:
                print("Telegram Bot: reports disabled (config telegram.scheduled_reports)")
            else:
                bot = TelegramBot(
                    TelegramConfig(
                        bot_token=token,
                        chat_id=chat_id,
                        topics=topics_cfg,
                        ema_report_interval=int((scheduled.get("ema_interval") if isinstance(scheduled, dict) else None) or (args.interval * 60)),
                        market_report_interval=int((scheduled.get("market_interval") if isinstance(scheduled, dict) else None) or (args.interval * 60)),
                        ema_enabled=ema_enabled,
                        market_enabled=market_enabled,
                    )
                )
                await bot.start()
                print("Telegram Bot: running (reports)")
        else:
            print("Telegram Bot: disabled (missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID)")
    elif not args.no_bot and not telegram_enabled:
        print("Telegram Bot: disabled (config telegram.enabled=false)")

    # TelegramBotServer for command handling (/btc, /eth, etc.)
    bot_server = None
    bot_server_task = None
    if not args.no_bot and telegram_enabled:
        token = resolved_token
        chat_id = resolved_chat_id
        
        if token and chat_id:
            try:
                from unitrade.bot.bot_handler import TelegramBotServer
                
                bot_server = TelegramBotServer(bot_token=token, chat_id=chat_id)
                
                # 创建后台任务运行命令处理
                async def run_bot_server():
                    await bot_server.start()
                    try:
                        while bot_server._running:
                            updates = await bot_server.get_updates()
                            for update in updates:
                                bot_server._offset = update.get("update_id", 0) + 1
                                if "message" in update:
                                    await bot_server.process_message(update["message"])
                                if "callback_query" in update:
                                    await bot_server.process_callback(update["callback_query"])
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        logging.error(f"Bot server error: {e}")
                
                bot_server_task = asyncio.create_task(run_bot_server())
                print("Telegram Bot: running (commands: /btc, /eth, ...)")
            except Exception as e:
                print(f"Telegram Bot Server: failed to start ({e})")
        else:
            print("Telegram Bot Server: disabled (missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID)")

    # AnomalyDetector (EMA200 突破 + OI 异动 + 放量) - 使用 anomaly_detector 配置
    anomaly_detector = None
    anomaly_task = None
    ranking_push_task = None
    ad_enabled = binance_enabled and (not getattr(args, "no_anomaly", False)) and cfg.get("anomaly_detector", {}).get("enabled", True)
    if ad_enabled:
        try:
            from unitrade.scanner.signal_detector import AnomalyDetector, AnomalyConfig
            
            ad_config = AnomalyConfig.from_config(cfg)
            timeframes = ", ".join(ad_config.timeframes) if ad_config.timeframes else "none"
            print(f"Anomaly Detector timeframes: {timeframes}")
            anomaly_detector = AnomalyDetector(ad_config)
            await anomaly_detector.start()
            anomaly_task = asyncio.create_task(anomaly_detector.run())
            print("Anomaly Detector: running (EMA200 breakout + OI + volume)")
        except Exception as e:
            print(f"Anomaly Detector: failed to start ({e})")
            anomaly_detector = None
    else:
        print("Anomaly Detector: disabled (config)")

    # Rising Index ranking push (anomaly_detector.telegram_push)
    try:
        ad_cfg = cfg.get("anomaly_detector", {}) if isinstance(cfg, dict) else {}
        push_cfg = ad_cfg.get("telegram_push", {}) if isinstance(ad_cfg, dict) else {}
        rising_enabled = bool((ad_cfg.get("rising_index") or {}).get("enabled", True)) if isinstance(ad_cfg, dict) else True

        if ad_enabled and rising_enabled and telegram_enabled and isinstance(push_cfg, dict) and bool(push_cfg.get("enabled", False)):
            bot_token = resolved_token
            chat_id = resolved_chat_id
            db_cfg = cfg.get("database", {}) if isinstance(cfg, dict) else {}
            redis_url = db_cfg.get("redis_url", "redis://localhost:6379") if isinstance(db_cfg, dict) else "redis://localhost:6379"

            if bot_token and chat_id:
                from unitrade.scanner.signal_detector import scheduled_ranking_push

                interval_hours = int(push_cfg.get("interval_hours", 4))
                top_n = int(push_cfg.get("top_n", 10))

                ranking_push_task = asyncio.create_task(
                    scheduled_ranking_push(
                        bot_token=bot_token,
                        chat_id=chat_id,
                        redis_url=redis_url,
                        interval_hours=interval_hours,
                        top_n=top_n,
                    )
                )
                print(f"Rising Index Push: running (every {interval_hours}h, top {top_n})")
            else:
                print("Rising Index Push: disabled (missing telegram.bot_token/chat_id)")
    except Exception as e:
        print(f"Rising Index Push: failed to start ({e})")

    # MarketScanner (OI + Funding + Liquidation) - 使用 oi_spike_scanner 配置
    market_scanner = None
    market_scan_task = None
    oi_enabled = binance_enabled and cfg.get("oi_spike_scanner", {}).get("enabled", True)
    if oi_enabled and not getattr(args, 'no_market', False):
        try:
            from unitrade.scanner import MarketScanner, MarketScannerConfig
            
            scanner_cfg = cfg.get("oi_spike_scanner", {})
            
            market_config = MarketScannerConfig(
                oi_enabled=scanner_cfg.get("oi_enabled", True),
                oi_threshold=scanner_cfg.get("oi_threshold", 1.15),
                funding_enabled=scanner_cfg.get("funding_enabled", True),
                funding_threshold=scanner_cfg.get("funding_threshold", 0.0005),
                liquidation_enabled=scanner_cfg.get("liquidation_enabled", True),
                scan_interval_minutes=scanner_cfg.get("scan_interval", 5),
                telegram_enabled=telegram_enabled and bool(resolved_token and resolved_chat_id),
                telegram_bot_token=resolved_token,
                telegram_chat_id=resolved_chat_id,
                telegram_topic_id=(topics.get("signal_anomaly") if isinstance(topics, dict) else None),  # topic 62
            )
            
            market_scanner = MarketScanner(market_config)
            await market_scanner.start()
            market_scan_task = asyncio.create_task(market_scanner.run_continuous())
            print("Market Scanner: running (OI/Funding/Liquidation)")
        except Exception as e:
            print(f"Market Scanner: failed to start ({e})")
            market_scanner = None
    elif not oi_enabled:
        print("Market Scanner: disabled (config)")

    # WaveTrend Scanner - 使用 wavetrend_scanner 配置
    wavetrend_scanner = None
    wavetrend_task = None
    squeeze_momentum_scanner = None
    squeeze_momentum_task = None
    wt_enabled = binance_enabled and cfg.get("wavetrend_scanner", {}).get("enabled", True)
    squeeze_enabled = cfg.get("squeeze_scanner", {}).get("enabled", True)
    if wt_enabled and telegram_enabled and not getattr(args, 'no_wavetrend', False):
        try:
            from unitrade.scanner import WaveTrendScanner, WaveTrendConfig
            
            wt_cfg = cfg.get("wavetrend_scanner", {})

            if not resolved_token or not resolved_chat_id:
                print("WaveTrend Scanner: disabled (missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID)")
                raise RuntimeError("Missing telegram credentials")
            
            wt_config = WaveTrendConfig(
                auto_top_n=wt_cfg.get("top_n", 100),
                timeframes=wt_cfg.get("timeframes", ["1h", "4h"]),
            )
            
            wavetrend_scanner = WaveTrendScanner(wt_config)
            await wavetrend_scanner.start()
            
            # 启动定时扫描任务
            async def wavetrend_loop():
                bot_token = resolved_token
                chat_id = resolved_chat_id
                wt_topic = topics.get("wavetrend_overbought_oversold")  # topic 9
                div_volume_topic = topics.get("divergence_volume")  # topic 8 - 放量背离
                div_normal_topic = topics.get("divergence_normal")  # topic 7 - 普通背离
                
                import aiohttp
                
                async def send_to_topic(session, message: str, topic_id: int = None):
                    if not bot_token or not chat_id:
                        return
                    try:
                        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                        payload = {
                            "chat_id": chat_id,
                            "text": message,
                            "parse_mode": "HTML",
                            "disable_web_page_preview": True,
                        }
                        if topic_id:
                            payload["message_thread_id"] = topic_id
                        async with session.post(url, json=payload) as resp:
                            if resp.status != 200:
                                logging.error(f"Telegram error: {await resp.text()}")
                    except Exception as e:
                        logging.error(f"Telegram send error: {e}")
                
                async with aiohttp.ClientSession() as session:
                    while True:
                        try:
                            for tf in wt_config.timeframes:
                                signals = await wavetrend_scanner.scan(tf)
                                
                                # 超买超卖信号
                                ob_signals = signals.get("overbought", [])
                                os_signals = signals.get("oversold", [])
                                div_signals = signals.get("divergence", [])
                                squeeze_signals = []
                                
                                # 发送超买信号
                                for sig in ob_signals[:5]:  # 最多5个
                                    msg = f"🔴 <b>超买</b> {sig.symbol} ({tf})\nOSC: {sig.osc_value:.1f} | 价格: ${sig.price:,.2f}"
                                    await send_to_topic(session, msg, wt_topic)
                                
                                # 发送超卖信号
                                for sig in os_signals[:5]:
                                    msg = f"🟢 <b>超卖</b> {sig.symbol} ({tf})\nOSC: {sig.osc_value:.1f} | 价格: ${sig.price:,.2f}"
                                    await send_to_topic(session, msg, wt_topic)
                                
                                # 发送背离信号 - 区分放量和普通
                                for sig in div_signals[:5]:
                                    if sig.is_m_plus:
                                        # 放量背离 (M+) -> divergence_volume topic
                                        emoji = "📈" if sig.signal_type == "bull_div" else "📉"
                                        div_type = "多头" if sig.signal_type == "bull_div" else "空头"
                                        msg = f"{emoji} <b>放量{div_type}背离 M+</b> {sig.symbol} ({tf})\nL{sig.level} | OSC: {sig.osc_value:.1f} | 价格: ${sig.price:,.2f}"
                                        await send_to_topic(session, msg, div_volume_topic)
                                    else:
                                        # 普通背离 -> divergence_normal topic
                                        emoji = "📈" if sig.signal_type == "bull_div" else "📉"
                                        div_type = "多头" if sig.signal_type == "bull_div" else "空头"
                                        msg = f"{emoji} <b>{div_type}背离</b> {sig.symbol} ({tf})\nL{sig.level} | OSC: {sig.osc_value:.1f} | 价格: ${sig.price:,.2f}"
                                        await send_to_topic(session, msg, div_normal_topic)
                                
                                total = len(ob_signals) + len(os_signals) + len(div_signals)
                                if total > 0:
                                    logging.info(f"WaveTrend {tf}: {len(ob_signals)} OB, {len(os_signals)} OS, {len(div_signals)} DIV")
                                    
                        except Exception as e:
                            logging.error(f"WaveTrend scan error: {e}")
                        
                        await asyncio.sleep(30 * 60)  # 每30分钟扫描一次
            
            wavetrend_task = asyncio.create_task(wavetrend_loop())
            print("WaveTrend Scanner: running")
        except Exception as e:
            if str(e) != "Missing telegram credentials":
                print(f"WaveTrend Scanner: failed to start ({e})")
            wavetrend_scanner = None
    elif not wt_enabled:
        print("WaveTrend Scanner: disabled (config)")
    elif wt_enabled and not telegram_enabled:
        print("WaveTrend Scanner: disabled (telegram.enabled=false)")

    # Squeeze Momentum Scanner - 使用 squeeze_scanner 配置（仅发送“波动开始”/释放信号）
    if squeeze_enabled and binance_enabled and telegram_enabled:
        bot_token = resolved_token
        chat_id = resolved_chat_id

        if not bot_token or not chat_id:
            print("Squeeze Momentum Scanner: disabled (missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID)")
        else:
            try:
                from unitrade.scanner import SqueezeMomentumScanner, SqueezeConfig

                sq_cfg = SqueezeConfig.from_config(cfg)
                squeeze_cfg = cfg.get("squeeze_scanner", {}) if isinstance(cfg, dict) else {}
                scan_interval_minutes = int((squeeze_cfg.get("scan_interval_minutes") if isinstance(squeeze_cfg, dict) else None) or 30)
                max_per_timeframe = int((squeeze_cfg.get("max_push_per_timeframe") if isinstance(squeeze_cfg, dict) else None) or 10)

                squeeze_momentum_scanner = SqueezeMomentumScanner(sq_cfg)
                await squeeze_momentum_scanner.start()

                async def squeeze_momentum_loop():
                    squeeze_4h_1h_topic = topics.get("squeeze_4h_1h") if isinstance(topics, dict) else None
                    squeeze_daily_topic = topics.get("squeeze_daily_weekly") if isinstance(topics, dict) else None

                    import aiohttp

                    async def send_to_topic(session, message: str, topic_id: int = None):
                        try:
                            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                            payload = {
                                "chat_id": chat_id,
                                "text": message,
                                "parse_mode": "HTML",
                                "disable_web_page_preview": True,
                            }
                            if topic_id:
                                payload["message_thread_id"] = topic_id
                            async with session.post(url, json=payload) as resp:
                                if resp.status != 200:
                                    logging.error(f"Telegram error: {await resp.text()}")
                        except Exception as e:
                            logging.error(f"Telegram send error: {e}")

                    async with aiohttp.ClientSession() as session:
                        while True:
                            try:
                                results = await squeeze_momentum_scanner.scan_all_timeframes()
                                for tf, tf_result in results.items():
                                    releases = tf_result.get("squeeze_release", []) if isinstance(tf_result, dict) else []
                                    if not releases:
                                        continue

                                    if tf in ["4h", "1h", "30m", "15m", "5m"]:
                                        topic_id = squeeze_4h_1h_topic
                                    else:
                                        topic_id = squeeze_daily_topic

                                    for sig in releases[:max_per_timeframe]:
                                        direction = "📈" if sig.momentum_direction == "up" else "📉" if sig.momentum_direction == "down" else ""
                                        msg = (
                                            f"🚀 <b>波动开始</b> {sig.symbol} ({tf}) {direction}\n"
                                            f"挤压: {sig.squeeze_duration} 根 | tightness: {sig.tightness_ratio:.2f} | 动量: {sig.momentum:.2f}\n"
                                            f"价格: ${sig.price:,.4f}"
                                        )
                                        await send_to_topic(session, msg, topic_id)
                            except Exception as e:
                                logging.error(f"Squeeze Momentum scan error: {e}")

                            await asyncio.sleep(max(60, scan_interval_minutes * 60))

                squeeze_momentum_task = asyncio.create_task(squeeze_momentum_loop())
                print(f"Squeeze Momentum Scanner: running (interval {scan_interval_minutes}m, release-only)")
            except Exception as e:
                print(f"Squeeze Momentum Scanner: failed to start ({e})")
                squeeze_momentum_scanner = None

    print("Press Ctrl+C to stop")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        if wavetrend_task:
            wavetrend_task.cancel()
        if wavetrend_scanner:
            await wavetrend_scanner.stop()
        if squeeze_momentum_task:
            squeeze_momentum_task.cancel()
        if squeeze_momentum_scanner:
            await squeeze_momentum_scanner.stop()
        if market_scan_task:
            market_scan_task.cancel()
        if market_scanner:
            await market_scanner.stop()
        if anomaly_task:
            anomaly_task.cancel()
        if ranking_push_task:
            ranking_push_task.cancel()
        if anomaly_detector:
            await anomaly_detector.stop()
        if signal_analyzer:
            await signal_analyzer.stop()
        if bot_server_task:
            bot_server_task.cancel()
        if bot_server:
            await bot_server.stop()
        if bot:
            await bot.stop()
        await dashboard.stop()


if __name__ == "__main__":
    main()



