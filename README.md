# UniTrade Analytics Gateway

加密货币交易分析系统 - 实时市场数据分析、EMA 趋势雷达、资金流追踪。

## 功能特性

### 📡 EMA Trend Radar
- EMA 完美排列检测 (Flowering)
- 连续趋势 Bar 统计
- 接近 EMA 价位提醒

### 🔍 Short Squeeze Scanner
- OI 飙升检测 (多空比异常)
- Funding Rate 异常扫描
- 清算数据监控

### 📊 Market Reporter
- 价格/资金费率/持仓量
- 多空比 (全局/大户)
- Telegram 格式输出

### 📈 Web Dashboard
- 实时币种卡片
- OI 变化图表
- 多空比历史图表
- 涨跌榜

### 🤖 Telegram Bot
- 定时推送 EMA 报告
- 定时推送市场报告

---

## 快速开始

### 安装

```bash
pip install -e .
```

### CLI 命令

```powershell
$env:PYTHONPATH="src"

# 📡 EMA 趋势雷达
python -m unitrade.cli ema --timeframe 1h --top 100

# 🔍 OI 扫描器
python -m unitrade.cli scan --continuous --interval 5

# 📊 市场报告
python -m unitrade.cli report BTCUSDT

# 📈 Web 仪表板
python -m unitrade.cli dashboard
# 打开 http://localhost:8080

# 🚀 Bot + Dashboard (推荐)
$env:TELEGRAM_BOT_TOKEN="your_token"
$env:TELEGRAM_CHAT_ID="your_chat_id"
python -m unitrade.cli serve --port 8080 --interval 60
# Prometheus: http://localhost:8000/metrics

# 💾 资金流追踪
python -m unitrade.cli track -s BTCUSDT,ETHUSDT

# 🤖 Telegram Bot
$env:TELEGRAM_BOT_TOKEN="your_token"
$env:TELEGRAM_CHAT_ID="your_chat_id"
python -m unitrade.cli bot
```

### 运行测试

```powershell
$env:PYTHONPATH="src"
pytest tests/unit/ -v
```

---

## 项目结构

```
unitrade-analytics/
├── src/unitrade/
│   ├── scanner/        # 扫描器
│   │   ├── ema_radar.py      # EMA 趋势雷达
│   │   ├── squeeze_scanner.py  # OI 扫描
│   │   └── funding_scanner.py  # 资金费率扫描
│   ├── tracker/        # 数据追踪
│   │   ├── fund_flow.py      # 资金流追踪 (WebSocket)
│   │   └── market_report.py  # 综合报告
│   ├── web/            # Web 界面
│   │   └── dashboard.py      # 仪表板
│   ├── bot/            # Telegram Bot
│   │   └── telegram_bot.py
│   ├── data/           # 数据卫生
│   │   └── hygiene.py        # SQLite 维护
│   ├── analytics/      # 分析引擎
│   │   ├── orderbook.py      # OBI 计算
│   │   ├── trade.py          # CVD/波动率
│   │   └── open_interest.py  # OI 分析
│   ├── connection/     # WebSocket 连接
│   │   ├── binance.py
│   │   └── bybit.py
│   └── cli.py          # 命令行入口
├── config/
│   └── default.yaml    # 配置文件
├── data/               # SQLite 数据存储
└── tests/              # 单元测试
```

---

## 配置说明

编辑 `config/default.yaml`:

```yaml
# Scanner 配置
scanner:
  auto_top_n: 150        # 扫描 Top 150 交易量币种
  extra_whitelist:
    - PEPEUSDT           # 额外添加
  spike_threshold: 1.10  # 10% OI 飙升触发

# EMA 雷达配置
ema_radar:
  timeframes: ["1h", "4h"]
  ema_periods: [21, 55, 100, 200]
  near_ema_threshold: 0.01  # 1%

# 数据存储 (SQLite)
database:
  sqlite_path: "data/unitrade.db"
  data_retention_days: 30
```

---

## Telegram 设置

1. 创建 Bot: [@BotFather](https://t.me/BotFather)
2. 获取 Chat ID: [@userinfobot](https://t.me/userinfobot)
3. 设置环境变量:

```powershell
$env:TELEGRAM_BOT_TOKEN="123456:ABC-DEF..."
$env:TELEGRAM_CHAT_ID="123456789"
```

---

## Docker (可选)

```powershell
cd docker
docker compose up -d
```

服务端口:
- Dashboard: 8080
- WebSocket: 8765

---

## License

MIT
