"""
统一市场扫描器运行脚本

同时运行:
- OI 飙升检测
- Funding Rate 异常
- 清算监控
"""

import argparse
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from unitrade.scanner import MarketScanner, MarketScannerConfig


def parse_args():
    parser = argparse.ArgumentParser(description="UniTrade Market Scanner")
    
    parser.add_argument("--continuous", "-c", action="store_true", help="持续运行")
    parser.add_argument("--interval", "-i", type=int, default=5, help="扫描间隔(分钟)")
    parser.add_argument("--oi-threshold", type=float, default=0.15, help="OI飙升阈值")
    parser.add_argument("--funding-threshold", type=float, default=0.0005, help="Funding阈值")
    parser.add_argument("--liq-threshold", type=float, default=100000, help="大额清算阈值")
    parser.add_argument("--no-oi", action="store_true", help="禁用OI扫描")
    parser.add_argument("--no-funding", action="store_true", help="禁用Funding扫描")
    parser.add_argument("--no-liq", action="store_true", help="禁用清算监控")
    parser.add_argument("--debug", action="store_true", help="调试模式")
    
    return parser.parse_args()


async def main():
    args = parse_args()
    
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    config = MarketScannerConfig(
        oi_enabled=not args.no_oi,
        oi_threshold=1 + args.oi_threshold,
        funding_enabled=not args.no_funding,
        funding_threshold=args.funding_threshold,
        liquidation_enabled=not args.no_liq,
        liquidation_large_threshold=args.liq_threshold,
        scan_interval_minutes=args.interval,
        ignore_list=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
        telegram_enabled=bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
    )
    
    print("=" * 60)
    print("🔍 UniTrade Market Scanner")
    print("=" * 60)
    print(f"OI Scan: {'✅' if config.oi_enabled else '❌'} (threshold: {args.oi_threshold*100:.0f}%)")
    print(f"Funding Scan: {'✅' if config.funding_enabled else '❌'} (threshold: {args.funding_threshold*100:.2f}%)")
    print(f"Liquidation: {'✅' if config.liquidation_enabled else '❌'} (threshold: ${args.liq_threshold:,.0f})")
    print(f"Mode: {'Continuous' if args.continuous else 'Single Scan'}")
    print(f"Telegram: {'✅' if config.telegram_enabled else '❌'}")
    print("=" * 60)
    print()
    
    scanner = MarketScanner(config)
    
    # 回调
    async def on_oi(alert):
        print(f"\n🚨 OI SPIKE: {alert.symbol} +{alert.oi_spike_pct:.1f}%")
    
    async def on_funding(alert):
        print(f"💰 FUNDING: {alert.symbol} {alert.funding_rate_pct:+.4f}%")
    
    async def on_liq(event):
        d = "LONG" if event.is_long_liquidation else "SHORT"
        print(f"💥 LIQ: {event.symbol} {d} ${float(event.notional):,.0f}")
    
    async def on_liq_alert(alert):
        print(f"\n🌊 LIQ ALERT: {alert.symbol} ${alert.total_notional:,.0f}")
    
    scanner.set_oi_callback(on_oi)
    scanner.set_funding_callback(on_funding)
    scanner.set_liquidation_callback(on_liq)
    scanner.set_liquidation_alert_callback(on_liq_alert)
    
    await scanner.start()
    
    try:
        if args.continuous:
            print("Running continuously... (Ctrl+C to stop)")
            await scanner.run_continuous()
        else:
            results = await scanner.scan_once()
            print(f"\n📊 Results: {len(results['oi_spikes'])} OI spikes, {len(results['funding_alerts'])} funding alerts")
    except KeyboardInterrupt:
        print("\n👋 Stopping...")
    finally:
        await scanner.stop()


if __name__ == "__main__":
    asyncio.run(main())
