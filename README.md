# UniTrade Analytics Gateway

面向交易的加密市场数据分析网关：聚合 Binance/Bybit 公共数据，计算 OBI/CVD/OI 变化/波动率，提供 Web Dashboard、Telegram 推送与 CLI 扫描工具。

English: A trader-focused analytics gateway for crypto market microstructure & flow signals (OBI/CVD/OI/volatility) with dashboard, Telegram notifications, and CLI scanners.

## 你能得到什么（Trader Value）

- **更干净的盘口信号**：订单簿断档会自动重拉快照，避免 OBI 漂移导致的“假信号”。
- **可解释的资金流**：实时 CVD（逐笔成交推导）+ Spot vs Futures 的 CVD 深度对比（K 线估算，USDT 口径）。
- **热点捕捉**：OI 异动、Squeeze 释放、异常检测/上涨指数等扫描器可做 Telegram 推送。
- **一键看盘**：Web Dashboard 集成核心图表与报告生成。

## 功能概览

- **Web Dashboard**：实时 OBI/CVD/波动率、CVD 深度分析、市场报告生成等。
- **Scanner**：OI Spike、Squeeze、Funding/Liquidation（按配置启用）。
- **Tracker**：资金流追踪（逐笔成交→快照→SQLite）。
- **Telegram Bot / 推送**：定时报告、信号推送（按配置启用）。
- **Prometheus Metrics**：进程/连接/扫描等指标（`/metrics`）。

## 快速开始

### 1) 安装

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -e .
```

可选（WaveTrend / Squeeze Momentum / 图表相关指标）：

```powershell
pip install -e ".[ta]"
```

### 2) 运行 Dashboard

```powershell
unitrade dashboard
# 打开 http://localhost:8080
```

### 3) 一键启动（Dashboard + Bot + 扫描/推送）

```powershell
$env:TELEGRAM_BOT_TOKEN="your_token"
$env:TELEGRAM_CHAT_ID="your_chat_id"
unitrade serve --port 8080 --interval 60

# Dashboard:   http://localhost:8080
# Prometheus:  http://localhost:8000/metrics
```

### 4) 常用 CLI

```powershell
# 市场扫描（OI / Funding / 清算等）
unitrade scan --continuous --interval 5

# EMA 趋势雷达（示例）
unitrade ema --timeframe 1h --top 100 --results 10

# 生成市场报告（REST 聚合）
unitrade report BTCUSDT

# 资金流追踪（逐笔成交→SQLite）
unitrade track -s BTCUSDT,ETHUSDT
```

如果你更喜欢 `python -m` 的方式（不依赖脚本入口）：

```powershell
$env:PYTHONPATH="src"
python -m unitrade.cli dashboard
```

## 配置

默认配置文件：`config/default.yaml`（也可通过环境变量 `UNITRADE_CONFIG` 指定）。

常用开关：

- `exchanges.binance.enabled` / `exchanges.bybit.enabled`
- `realtime.enabled`：Dashboard 实时数据服务（OBI/CVD/波动率）
- `telegram.enabled`：Telegram 相关功能总开关
- `*_scanner.enabled` / `signal_detector.enabled`：各扫描器/信号模块开关

## 数据口径（很重要）

- **Realtime Dashboard 的 CVD**：来自 Binance 合约逐笔成交推导，**USDT 口径（quote notional）**；每个周期显示的是该周期内的 Δ，`cumulative_cvd` 为服务启动以来累积（同时提供 `*_qty` 字段用于数量口径对比/调试）。
- **“CVD 深度分析”**：基于 Spot/Futures K 线的 `taker_buy_quote_volume` 估算，**USDT 口径（quote）**，用于对比 Spot vs Futures 资金流差异。
- **OBI**：Top N 深度（默认 Top 10）按买卖盘数量不平衡计算，范围 [-1, 1]。

## 测试

```powershell
$env:PYTHONPATH="src"
pytest -q
```

注意：部分扫描器依赖可选第三方库（例如 WaveTrend 需要 `pandas_ta`）。如果环境里无法安装这些依赖，请保持对应模块 `enabled: false`，不影响 Dashboard/Realtime/FundFlow 等核心能力。

## 项目结构（简版）

```
src/unitrade/
  analytics/   # OBI/CVD/OI/波动率等核心计算
  web/         # Dashboard + realtime service + cvd 深度分析
  scanner/     # OI/Squeeze/WaveTrend/异常检测等扫描器
  tracker/     # fund_flow 资金流追踪（SQLite）
  bot/         # Telegram bot / 推送
  cli.py       # unitrade 入口
config/default.yaml
tests/
```

## License

MIT
