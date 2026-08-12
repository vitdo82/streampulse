# Design: Analytics L2 — Anomaly detection, rebalance history, throughput patterns

**Status:** Implemented · **Depends on:** `storage.md` (hourly queries, state transitions), `scraper.md` (metric names) · **Serves:** `streampulse analyze`, TUI Analytics tab

## Goal

Analytics L2 extends the L1 reports (growth, skew, retention) with three store-driven features: anomaly detection over persisted rate/lag metrics, consumer-group rebalance history, and throughput time-of-day/day-of-week patterns with a trend forecast.

## Features

### 1. Anomaly detection

- **Metrics:** `kafka.group.lag`, `kafka.topic.msg_rate`, `kafka.topic.bytes_rate` (hourly aggregates, 90d retention).
- **Detectors (per point):**
  - *Seasonal:* the point is scored against the mean/std of the same hour-of-week slot (`ProfileIndex(t, 168)`) over the window's history. Requires ≥ 3 prior samples in the slot (≈ 3 weeks of data).
  - *Rolling fallback:* scored against the previous points' mean/std (needs ≥ 2 prior points) when the seasonal bucket is too small.
  - Flat series (zero std) → z = 0, no false positives.
- **Classification:** `|z| ≥ 2` → `warning`, direction `high`/`low`; below → no flag.
- **Report:** `Anomaly{Metric, Entity, Time, Value, Expected, ZScore, Direction, Severity}`, sorted by metric/entity/time.
- **CLI:** `analyze --anomalies [lag|msg_rate|bytes_rate]` (default all). **TUI:** top-5 by |z| pane.

### 2. Rebalance history

- **Source:** persisted `kafka.group.state` samples (5s scrape; mapping 0 Empty, 1 Stable, 2 Preparing, 3 Completing, 4 Dead).
- **Detection:** count transitions into `PreparingRebalance` (2) from any other state, per group per UTC day. A single rebalance produces 1→2→3→1, so counting `To == 2` dedupes correctly.
- **Storage:** new `MetricsStore.QueryStateTransitions` — consecutive value changes via SQL `LAG(value) OVER (PARTITION BY entity_name ORDER BY ts)` (SQLite window function, modernc-supported).
- **Report:** `RebalanceReport{Group, Day, Count}`.
- **Limitation (documented):** sampling-based; rebalances shorter than the 5s scrape interval can be missed. Not a substitute for group-state event logs.

### 3. Throughput patterns

- **Source:** hourly `kafka.topic.msg_rate` / `bytes_rate`.
- **Profiles:** mean per hour-of-day (24 slots) and per weekday (7 slots); `PeakHour`/`PeakDay`; least-squares linear fit over (unix seconds, rate) → `Slope` (per second) and `Forecast7d` projection.
- **Report:** `ThroughputReport{Topic, Metric, Window, HourlyProfile, DailyProfile, PeakHour, PeakDay, Slope, Forecast7d}`.
- **CLI:** `analyze --patterns msg_rate|bytes_rate --topics orders`. **TUI:** `Bars` chart of the selected topic's hourly profile (j/k cycles topics).

## Package layout

```
internal/analytics/
  zscore.go      # meanStd, RollingZScore, SeasonalZScore, ProfileIndex
  anomalies.go   # Anomalies, sortedKeys, classifyZ
  rebalances.go  # Rebalances
  patterns.go    # Patterns, contains, peakIndex, linearFit
  report.go      # Anomaly, RebalanceReport, ThroughputReport (+ L1 structs)
internal/storage/
  store.go       # MetricsStore += QueryStateTransitions, StateTransition type
  sqlite.go      # LAG window-function implementation
```

## Failure modes

- Insufficient history → empty results, exit 0 ("no data"), never an error.
- No persisted data (daemon never ran) → same as above.
- Store unreachable → CLI exits 1 (existing analyze error path); TUI keeps last data + error line (`analyticsErr`).
- Invalid metric names for `--anomalies`/`--patterns` → usage error before store access.
- Zero-std baseline → z = 0 (no flag).
- Forecast is advisory (naive linear fit), not capacity planning.

## Testing

- Storage: transition counting with a seeded state sequence (dedupe `To == 2`, ordering, window bounds).
- Z-score helpers: golden math (spike, flat, insufficient data).
- Anomalies: seeded spike store (baseline must have variance — flat baselines hit the zero-std guard), rolling fallback, metric filter, invalid window.
- Rebalances: per-group per-day counts, group filter, non-Preparing transitions filtered, no-data, invalid window.
- Patterns: profiles/peak/slope with an asymmetric window (slope is deterministically nonzero), forecast math.
- CLI: seeded-store section output, `--json` round-trip, no-data exit 0, invalid metric errors.
- TUI: panes populate from a seeded store, 30s cache cadence, empty states, j/k selection.

## Out of scope

Per-partition drill-down, anomaly-triggered alert rules (wire `Anomalies` into the alert engine), seasonality-aware capacity forecasting, rebalance cause attribution.
