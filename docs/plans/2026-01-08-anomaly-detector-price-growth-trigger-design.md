# Anomaly Detector: price growth trigger (post-EMA)

## Goal

Add a configurable “price growth” factor to the anomaly detector trigger so that, after a symbol is above EMA200, it must also show net positive progress over the recent N bars (slow or fast), reducing sideways/noise cases while keeping the system responsive.

## Current trigger (before)

- EMA condition: break above EMA200 (or be above EMA200 if allowed)
- Then require any 2 of:
  - OI spike
  - Volume spike (RVOL)
  - Net inflow spike

## Proposed trigger (after)

- Keep the EMA condition unchanged.
- Add a 4th factor `price_growth_spike`, then require any 2 of 4:
  - OI spike
  - Volume spike
  - Net inflow spike
  - Price growth spike

### Price growth spike (scheme B)

For the current timeframe:

- Look back `N = price_growth_lookback_bars`
- Compute overall gain over the last N bars:
  - `gain = (close_now - close_N_bars_ago) / close_N_bars_ago`
- Condition:
  - `gain >= price_growth_min_gain_pct`
  - If `price_growth_require_above_ema200=true`, additionally require the last N closes to be `>= current ema200` (a pragmatic proxy that avoids storing a full EMA history).

## Config

Add these keys under `anomaly_detector`:

- `price_growth_lookback_bars` (default `8`)
- `price_growth_min_gain_pct` (default `0.002` = 0.2%)
- `price_growth_require_above_ema200` (default `true`)

## Outputs & compatibility

- No Redis schema changes.
- Ranking/scoring remains compatible (signal fields unchanged).
- Debug logs include `price_growth` and whether it contributed.

