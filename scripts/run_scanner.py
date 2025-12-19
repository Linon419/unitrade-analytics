"""
Short Squeeze Scanner 运行脚本

用法:
    python scripts/run_scanner.py                    # 单次扫描
    python scripts/run_scanner.py --continuous      # 持续扫描
    python scripts/run_scanner.py --threshold 0.15  # 自定义阈值 (15%)
"""

import argparse
import asyncio
import logging
import os
import sys

# 添加 src 到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from unitrade.scanner import BinanceScanner, ScannerConfig, run_scanner


def parse_args():
    parser = argparse.ArgumentParser(description="Short Squeeze OI Scanner")
    
    parser.add_argument(
        "--continuous", "-c",
        action="store_true",
        help="持续扫描模式"
    )
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=5,
        help="扫描间隔 (分钟), 默认 5"
    )
    parser.add_argument(
        "--threshold", "-t",
        type=float,
        default=0.20,
        help="OI 飙升阈值 (0.20 = 20%%), 默认 0.20"
    )
    parser.add_argument(
        "--min-volume", "-v",
        type=float,
        default=5_000_000,
        help="最低 24h 成交额 (USDT), 默认 5,000,000"
    )
    parser.add_argument(
        "--ignore",
        type=str,
        nargs="+",
        default=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
        help="忽略的交易对列表"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="最大并发请求数, 默认 10"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="启用调试日志"
    )
    
    return parser.parse_args()


async def main():
    args = parse_args()
    
    # 配置日志
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # 创建配置
    config = ScannerConfig(
        min_volume_usdt=args.min_volume,
        ignore_list=args.ignore,
        spike_threshold=1 + args.threshold,  # 0.20 -> 1.20
        max_concurrent_requests=args.concurrency,
        
        # Telegram (从环境变量)
        telegram_enabled=bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
    )
    
    # 打印配置
    print("=" * 60)
    print("🔍 Short Squeeze Scanner - OI Spike Detector")
    print("=" * 60)
    print(f"📊 Spike Threshold: {args.threshold * 100:.0f}%")
    print(f"💰 Min Volume: ${args.min_volume:,.0f}")
    print(f"🚫 Ignore List: {args.ignore}")
    print(f"⚡ Concurrency: {args.concurrency}")
    print(f"🔄 Mode: {'Continuous' if args.continuous else 'Single Scan'}")
    if args.continuous:
        print(f"⏰ Interval: {args.interval} minutes")
    print(f"📱 Telegram: {'Enabled' if config.telegram_enabled else 'Disabled'}")
    print("=" * 60)
    print()
    
    # 运行扫描
    try:
        await run_scanner(
            config=config,
            interval_minutes=args.interval,
            continuous=args.continuous
        )
    except KeyboardInterrupt:
        print("\n👋 Scanner stopped by user")


if __name__ == "__main__":
    asyncio.run(main())
