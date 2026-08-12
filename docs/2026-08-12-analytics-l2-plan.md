# Analytics L2 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Analytics L2 to StreamPulse: anomaly detection (rolling Z-score + seasonal hour-of-week baselines), consumer-group rebalance history, and throughput patterns (hour-of-day/day-of-week profiles + linear trend forecast) — surfaced through the `analyze` CLI and the TUI Analytics tab.

**Architecture:** All three features are store-driven (persisted scrape data), reusing the existing `storage.MetricsStore` queries and the `internal/analytics` package structure. One new storage method (`QueryStateTransitions`, SQL window-function based) is added for rebalance detection; the rest uses existing `QueryHourly`/`QueryRaw`. New report types are added to `internal/analytics`, wired into the existing `analyze` command flags and the TUI's 30s analytics cache. Standard library only (math, sort, time) — no new dependencies.

**Tech Stack:** Go 1.25, modernc.org/sqlite (window functions available), existing packages: `internal/analytics` (Analyzer/Client interface), `internal/storage` (MetricsStore), `internal/scraper` (metric name constants), `internal/cli` (analyze command), `internal/tui` (analytics cache + panes).

---

## Context review (what exists today)

- `internal/analytics/analyze.go`: `Analyzer{store, client}` with `Growth`/`Skew`/`Retention`; `Client` interface (`DescribeCluster`, `DescribeConfigs`); per-report error isolation.
- `internal/analytics/chart.go`: `Sparkline([]float64) string` (8 glyphs), `Bars(labels, values, width)` — reuse for profiles.
- `internal/analytics/report.go`: JSON-tagged report structs.
- Scraper metrics available (names in `internal/scraper/metric.go`): `kafka.topic.messages`, `kafka.topic.msg_rate`, `kafka.topic.bytes_rate`, `kafka.group.lag`, `kafka.group.member_count`, `kafka.group.state` (0=Empty 1=Stable 2=PreparingRebalance 3=CompletingRebalance 4=Dead).
- Storage: `QueryRaw`/`QueryHourly`/`QueryDaily` (Limit default 1000, max 10000; From inclusive, To exclusive; ascending order). Hourly aggregates kept 90d — sufficient for seasonal baselines.
- CLI: `analyze --window/--topics/--skew/--retention/--json` in `internal/cli/commands.go` (~line 481).
- TUI: `internal/tui/model.go` — `analytics []analytics.GrowthReport`, `skew`, `retention`, `analyticsErr`, `analyticsUpdated` fields; recompute when stale > 30s; injectable `now` clock.

## Key design decisions

1. **Anomaly detection input:** hourly aggregates (`QueryHourly`) of `kafka.group.lag`, `kafka.topic.msg_rate`, `kafka.topic.bytes_rate` over a configurable window (default 7d). Two detectors:
   - *Seasonal baseline:* for each point, expected value = mean of the same hour-of-week bucket over the preceding 3+ weeks of data; z = (v − mean)/std of that bucket. Requires ≥ 3 baseline samples per bucket; otherwise falls back to the rolling detector.
   - *Rolling Z-score:* z of the point against the mean/std of the previous 24 points.
   - Severity: `warning` if 2.0 ≤ |z| < 4.0, `critical` if |z| ≥ 4.0. Direction `high`/`low`.
   - Points with fewer than 2 preceding samples are skipped (insufficient data).
2. **Rebalance detection:** count transitions into `PreparingRebalance` (state 2) from any other state, per group per day, via the new `QueryStateTransitions` storage method. A single rebalance produces one 1→2 transition followed by 2→3→1, so counting `prev != 2 && value == 2` dedupes correctly. Limitation (documented): detection is sampling-based (5s scrape); sub-5s rebalances may be missed.
3. **Throughput patterns:** hourly profiles (24 buckets, mean of `bytes_rate`/`msg_rate` per hour-of-day), daily profiles (7 buckets), peak hour/day, and a least-squares linear fit slope + 7-day forecast from hourly points.
4. **No new dependencies.** Window functions are used inside SQL (modernc supports them); regression math is stdlib.

## File map

| File | Responsibility |
|------|----------------|
| `internal/storage/store.go` | Add `QueryStateTransitions` to `MetricsStore` interface |
| `internal/storage/sqlite.go` | `QueryStateTransitions` implementation (window function) |
| `internal/storage/sqlite_test.go` | Transition-count tests |
| `internal/analytics/zscore.go` | **new** — rolling Z-score + seasonal baseline helpers |
| `internal/analytics/zscore_test.go` | **new** — golden math tests |
| `internal/analytics/anomalies.go` | **new** — `Anomalies` report |
| `internal/analytics/anomalies_test.go` | **new** |
| `internal/analytics/rebalances.go` | **new** — `Rebalances` report |
| `internal/analytics/rebalances_test.go` | **new** |
| `internal/analytics/patterns.go` | **new** — `Patterns` report + linear fit |
| `internal/analytics/patterns_test.go` | **new** |
| `internal/analytics/report.go` | Add `Anomaly`, `RebalanceReport`, `ThroughputReport` structs |
| `internal/cli/commands.go` | `--anomalies`, `--rebalances`, `--patterns` flags + output |
| `internal/cli/analyze_test.go` | flag/output tests |
| `internal/tui/model.go` | anomaly + rebalance panes, patterns for selected topic |
| `internal/tui/model_test.go` | pane tests |
| `internal/scraper/scraper_test.go`, `internal/analytics/analyze_test.go` | add `QueryStateTransitions` stubs to fakes |
| `docs/design/analytics-l2.md` | **new** — design doc |

---

### Task 1: Storage — `QueryStateTransitions`

**Files:**
- Modify: `internal/storage/store.go` (interface), `internal/storage/sqlite.go` (impl), `internal/storage/sqlite_test.go` (tests)
- Modify: `internal/scraper/scraper_test.go`, `internal/analytics/analyze_test.go` (fake stubs)

- [ ] **Step 1: Write the failing test** (`internal/storage/sqlite_test.go`)

```go
func TestQueryStateTransitions(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	base := time.Now().Truncate(time.Hour).Add(-48 * time.Hour)
	seq := []float64{1, 2, 3, 1, 1, 2, 1, 2, 2, 3, 1} // states: 2 rebalances into Preparing (idx1, idx5)
	metrics := make([]Metric, 0, len(seq))
	for i, v := range seq {
		metrics = append(metrics, Metric{
			TS:         base.Add(time.Duration(i) * 5 * time.Second),
			ClusterID:  "c1",
			Metric:     "kafka.group.state",
			EntityType: "consumer_group",
			EntityName: "orders-processor",
			Value:      v,
		})
	}
	require.NoError(t, s.WriteBatch(context.Background(), metrics))

	rows, err := s.QueryStateTransitions(context.Background(), QueryParams{
		Metric:     "kafka.group.state",
		EntityName: "orders-processor",
		From:       base,
		To:         base.Add(2 * time.Hour),
	})
	require.NoError(t, err)
	assert.Len(t, rows, 2) // transitions into state 2 with prev != 2
}
```

- [ ] **Step 2: Run — expect FAIL** (`QueryStateTransitions` undefined on interface).
- [ ] **Step 3: Implement**

In `internal/storage/store.go`, add to the interface:

```go
	// QueryStateTransitions returns the state-value transitions (consecutive
	// samples where the value changed) for a metric, in time order.
	QueryStateTransitions(ctx context.Context, params QueryParams) ([]StateTransition, error)
```

and the row type:

```go
// StateTransition is a consecutive value change for one entity.
type StateTransition struct {
	Time   time.Time
	Entity string
	From   float64 // previous sampled value
	To     float64 // new value
}
```

In `internal/storage/sqlite.go`:

```go
func (s *SQLiteStore) QueryStateTransitions(ctx context.Context, params QueryParams) ([]StateTransition, error) {
	where := []string{"1=1"}
	args := []any{}
	if params.Metric != "" {
		where = append(where, "metric = ?")
		args = append(args, params.Metric)
	}
	if params.EntityType != "" {
		where = append(where, "entity_type = ?")
		args = append(args, params.EntityType)
	}
	if params.EntityName != "" {
		where = append(where, "entity_name = ?")
		args = append(args, params.EntityName)
	}
	if !params.From.IsZero() {
		where = append(where, "ts >= ?")
		args = append(args, params.From.UnixMilli())
	}
	if !params.To.IsZero() {
		where = append(where, "ts < ?")
		args = append(args, params.To.UnixMilli())
	}

	limit := params.Limit
	if limit <= 0 {
		limit = 1000
	}
	if limit > 10000 {
		limit = 10000
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT entity_name, ts, value,
		       LAG(value) OVER (PARTITION BY entity_name ORDER BY ts) AS prev
		FROM raw_metrics
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY entity_name, ts
		LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, fmt.Errorf("query state transitions: %w", err)
	}
	defer rows.Close()

	var out []StateTransition
	for rows.Next() {
		var entity string
		var ts, value, prev int64
		var prevNull sql.NullInt64
		if err := rows.Scan(&entity, &ts, &value, &prevNull); err != nil {
			return nil, fmt.Errorf("scan transition: %w", err)
		}
		if !prevNull.Valid || prevNull.Int64 == value {
			continue
		}
		out = append(out, StateTransition{
			Time:   time.UnixMilli(ts).UTC(),
			Entity: entity,
			From:   float64(prevNull.Int64),
			To:     float64(value),
		})
	}
	return out, rows.Err()
}
```

(`strings`, `database/sql` already imported in sqlite.go. Note `LAG` window function is supported by modernc.org/sqlite ≥ v1.28 — go.mod has v1.53.)

- [ ] **Step 4: Fix the fakes** — add to `internal/scraper/scraper_test.go` and `internal/analytics/analyze_test.go` fake stores:

```go
func (f *fakeStore) QueryStateTransitions(ctx context.Context, params storage.QueryParams) ([]storage.StateTransition, error) {
	return nil, nil
}
```

- [ ] **Step 5: Run — PASS.** `go build ./... && go vet ./... && go test -count=1 ./internal/storage/ ./internal/scraper/ ./internal/analytics/`
- [ ] **Step 6: Commit** — `feat: state transition query for rebalance detection`

---

### Task 2: Z-score helpers

**Files:**
- Create: `internal/analytics/zscore.go`, `internal/analytics/zscore_test.go`

- [ ] **Step 1: Write the failing tests**

```go
func TestRollingZScore(t *testing.T) {
	// last value far from the previous window mean → large z
	vals := []float64{10, 11, 9, 10.5, 9.5, 10, 40}
	z, mean, err := RollingZScore(vals)
	require.NoError(t, err)
	assert.InDelta(t, 10, mean, 0.01)
	assert.Greater(t, z, 3.0)

	// flat series → z 0, no divide-by-zero
	z, _, err = RollingZScore([]float64{5, 5, 5, 5})
	require.NoError(t, err)
	assert.Zero(t, z)

	// insufficient data
	_, _, err = RollingZScore([]float64{1})
	require.Error(t, err)
}

func TestSeasonalZScore(t *testing.T) {
	// 4 weeks of values at the same hour-of-week slot = 100, latest = 300
	vals := make([]float64, 0, 29)
	for w := 0; w < 4; w++ {
		for i := 0; i < 7; i++ {
			vals = append(vals, 100)
		}
	}
	vals = append(vals, 300) // current point last → 29 total, 28 history
	z, err := SeasonalZScore(vals)
	require.NoError(t, err)
	assert.Greater(t, z, 3.0)

	// insufficient baseline (< 3 history samples)
	_, err = SeasonalZScore([]float64{100, 100, 100})
	require.Error(t, err)
}
```

- [ ] **Step 2: Run — FAIL** (functions don't exist).
- [ ] **Step 3: Implement** (`internal/analytics/zscore.go`)

```go
package analytics

import (
	"fmt"
	"math"
	"time"
)

// meanStd returns the mean and sample standard deviation of values.
func meanStd(values []float64) (mean, std float64) {
	if len(values) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean = sum / float64(len(values))
	if len(values) < 2 {
		return mean, 0
	}
	var ss float64
	for _, v := range values {
		d := v - mean
		ss += d * d
	}
	return mean, math.Sqrt(ss / float64(len(values)-1))
}

// RollingZScore returns the z-score of the last value against the mean/std of
// the preceding values, plus that mean. Errors on fewer than 2 preceding
// samples. A zero standard deviation yields z = 0 (no anomaly signal).
func RollingZScore(values []float64) (z, mean float64, err error) {
	if len(values) < 2 {
		return 0, 0, fmt.Errorf("rolling z-score needs at least 2 samples, got %d", len(values))
	}
	v := values[len(values)-1]
	mean, std := meanStd(values[:len(values)-1])
	if std == 0 {
		return 0, mean, nil
	}
	return (v - mean) / std, mean, nil
}

// SeasonalZScore scores the last value of slotValues against its seasonal
// baseline: the preceding values of the same bucket. slotValues must contain
// the current point LAST with at least 3 history samples (4 total). A zero
// baseline standard deviation yields z = 0 (no anomaly signal).
func SeasonalZScore(slotValues []float64) (z float64, err error) {
	if len(slotValues) < 4 {
		return 0, fmt.Errorf("seasonal baseline needs at least 3 history samples, got %d", len(slotValues)-1)
	}
	history := slotValues[:len(slotValues)-1]
	mean, std := meanStd(history)
	if std == 0 {
		return 0, nil
	}
	return (slotValues[len(slotValues)-1] - mean) / std, nil
}

// ProfileIndex maps a timestamp to a slot in a cyclic profile of cycleLen
// slots. Cycle 168 = hour-of-week; cycle 24 = hour-of-day.
func ProfileIndex(t time.Time, cycleLen int) int {
	return (int(t.Weekday())*24 + t.Hour()) % cycleLen
}
```
- [ ] **Step 5: Commit** — `feat: rolling and seasonal z-score helpers`

---

### Task 3: Anomaly report

**Files:**
- Modify: `internal/analytics/report.go` (structs)
- Create: `internal/analytics/anomalies.go`, `internal/analytics/anomalies_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestAnomaliesDetectsSpike(t *testing.T) {
	store := seededAnomalyStore(t) // helper: 4 weeks of hourly kafka.group.lag = 100 for group g1, last 2 hours = 500
	a := &Analyzer{store: store, client: nil}

	anoms, err := a.Anomalies(context.Background(), []string{scraper.MetricGroupLag}, 7*24*time.Hour)
	require.NoError(t, err)
	require.Len(t, anoms, 2) // the two spike hours
	for _, an := range anoms {
		assert.Equal(t, "g1", an.Entity)
		assert.Equal(t, "high", an.Direction)
		assert.Equal(t, "warning", an.Severity) // single threshold: |z| >= 2
		assert.Greater(t, an.ZScore, 4.0)
	}

	// insufficient history → no anomalies, no error
	empty, err := (&Analyzer{store: seededEmptyStore(t)}).Anomalies(context.Background(), []string{scraper.MetricGroupLag}, 7*24*time.Hour)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func TestAnomaliesSkipsNonRequestedMetrics(t *testing.T) { ... }
```

- [ ] **Step 2: Run — FAIL.**
- [ ] **Step 3: Implement**

`internal/analytics/report.go` additions:

```go
// Anomaly flags one point that deviates from its baseline.
type Anomaly struct {
	Metric   string    `json:"metric"`
	Entity   string    `json:"entity"`
	Time     time.Time `json:"time"`
	Value    float64   `json:"value"`
	Expected float64   `json:"expected"`
	ZScore   float64   `json:"z_score"`
	Direction string   `json:"direction"` // high | low
	Severity  string   `json:"severity"`  // warning | critical
}
```

`internal/analytics/anomalies.go`:

```go
package analytics

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
)

// AnomalyMetrics are the metrics the anomaly detector evaluates.
var AnomalyMetrics = []string{
	scraper.MetricGroupLag,
	scraper.MetricTopicMsgRate,
	scraper.MetricTopicBytesRate,
}

// minSeasonalSamples is the minimum baseline samples per seasonal bucket.
const minSeasonalSamples = 3

// Anomalies scans hourly aggregates over the window for each requested metric
// and flags points whose seasonal or rolling z-score exceeds the threshold.
// A metric with insufficient history yields no anomalies (not an error).
func (a *Analyzer) Anomalies(ctx context.Context, metrics []string, window time.Duration) ([]Anomaly, error) {
	if window <= 0 {
		return nil, fmt.Errorf("invalid window %s", window)
	}
	if len(metrics) == 0 {
		metrics = AnomalyMetrics
	}

	now := time.Now()
	from := now.Add(-window)
	var out []Anomaly

	for _, metric := range metrics {
		rows, err := a.store.QueryHourly(ctx, storage.QueryParams{
			Metric: metric,
			From:   from,
			To:     now,
		})
		if err != nil {
			return nil, fmt.Errorf("query hourly %s: %w", metric, err)
		}

		byEntity := make(map[string][]storage.MetricRow)
		for _, r := range rows {
			byEntity[r.EntityName] = append(byEntity[r.EntityName], r)
		}

		for _, entity := range sortedKeys(byEntity) {
			pts := byEntity[entity]
			for i := range pts {
				z, expected, ok := a.scorePoint(metric, pts, i)
				if !ok {
					continue
				}
				sev, dir := classifyZ(z)
				if sev == "" {
					continue
				}
				out = append(out, Anomaly{
					Metric: metric, Entity: entity, Time: pts[i].TimeStart,
					Value: pts[i].Avg, Expected: expected, ZScore: z,
					Direction: dir, Severity: sev,
				})
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Metric != out[j].Metric {
			return out[i].Metric < out[j].Metric
		}
		if out[i].Entity != out[j].Entity {
			return out[i].Entity < out[j].Entity
		}
		return out[i].Time.Before(out[j].Time)
	})
	return out, nil
}

// scorePoint z-scores point i against its seasonal bucket (hour-of-week over
// the available history) when the bucket has enough baseline samples, else the
// rolling window of the preceding points.
func (a *Analyzer) scorePoint(metric string, pts []storage.MetricRow, i int) (z, expected float64, ok bool) {
	// seasonal bucket: same hour-of-week slot across the window
	slot := ProfileIndex(pts[i].TimeStart, 168)
	slotVals := []float64{}
	for j := 0; j <= i; j++ {
		if ProfileIndex(pts[j].TimeStart, 168) == slot {
			slotVals = append(slotVals, pts[j].Avg)
		}
	}
	if len(slotVals) >= minSeasonalSamples+1 {
		z, err := SeasonalZScore(slotVals)
		if err == nil {
			mean, _ := meanStd(slotVals[:len(slotVals)-1])
			return z, mean, true
		}
	}
	// fallback: rolling window over the preceding hourly points
	if i >= 2 {
		vals := make([]float64, 0, i+1)
		for j := 0; j <= i; j++ {
			vals = append(vals, pts[j].Avg)
		}
		z, mean, err := RollingZScore(vals)
		if err == nil {
			return z, mean, true
		}
	}
	return 0, 0, false
}

// classifyZ maps a z-score to severity and direction; "" severity = no flag.
func classifyZ(z float64) (severity, direction string) {
	if z >= 2.0 {
		return "warning", "high"
	}
	if z <= -2.0 {
		return "warning", "low"
	}
	return "", ""
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
```

(Single threshold decision: severity is `warning` for |z| ≥ 2, direction `high`/`low`; below 2.0 no flag. The `classifyZ` function returns `""` severity below the threshold — matches the test above.)

- [ ] **Step 4: Run — PASS.** Add a test for the rolling fallback (short history: 3 points, last spikes → anomaly flagged via rolling path).
- [ ] **Step 5: Commit** — `feat: anomaly detection with seasonal and rolling baselines`

---

### Task 4: Rebalance history report

**Files:**
- Modify: `internal/analytics/report.go`
- Create: `internal/analytics/rebalances.go`, `internal/analytics/rebalances_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestRebalancesCountsPreparingTransitions(t *testing.T) {
	store := seededTransitionStore(t) // 2 transitions into state 2 for g1 on day D, 1 for g2 on day D+1
	a := &Analyzer{store: store, client: nil}

	reports, err := a.Rebalances(context.Background(), nil, 7*24*time.Hour)
	require.NoError(t, err)
	require.Len(t, reports, 2)
	// g1/D → 2, g2/D+1 → 1 (order by group, then day)
}
```

- [ ] **Step 2: Run — FAIL.**
- [ ] **Step 3: Implement**

`internal/analytics/report.go`:

```go
// RebalanceReport counts rebalance events (transitions into PreparingRebalance)
// for one consumer group on one UTC day.
type RebalanceReport struct {
	Group string    `json:"group"`
	Day   time.Time `json:"day"` // UTC midnight of the day
	Count int       `json:"count"`
}
```

`internal/analytics/rebalances.go`:

```go
package analytics

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
)

// statePreparingRebalance is the mapped kafka.group.state value for
// PreparingRebalance (scraper mapping: 0 Empty, 1 Stable, 2 Preparing, 3 Completing, 4 Dead).
const statePreparingRebalance = 2

// Rebalances counts per-group per-day transitions into PreparingRebalance over
// the window, derived from the persisted kafka.group.state samples. Detection
// is sampling-based (5s scrape): sub-5s rebalances may be missed. Groups with
// no transitions are omitted.
func (a *Analyzer) Rebalances(ctx context.Context, groups []string, window time.Duration) ([]RebalanceReport, error) {
	if window <= 0 {
		return nil, fmt.Errorf("invalid window %s", window)
	}

	now := time.Now()
	transitions, err := a.store.QueryStateTransitions(ctx, storage.QueryParams{
		Metric:     scraper.MetricGroupState,
		EntityType: "consumer_group",
		From:       now.Add(-window),
		To:         now,
	})
	if err != nil {
		return nil, fmt.Errorf("query state transitions: %w", err)
	}

	want := make(map[string]bool, len(groups))
	for _, g := range groups {
		want[g] = true
	}

	type dayKey struct{ group string; day time.Time }
	counts := make(map[dayKey]int)
	for _, tr := range transitions {
		if tr.To != statePreparingRebalance {
			continue
		}
		if len(want) > 0 && !want[tr.Entity] {
			continue
		}
		day := tr.Time.Truncate(24 * time.Hour)
		counts[dayKey{tr.Entity, day}]++
	}

	reports := make([]RebalanceReport, 0, len(counts))
	for k, n := range counts {
		reports = append(reports, RebalanceReport{Group: k.group, Day: k.day, Count: n})
	}
	sort.Slice(reports, func(i, j int) bool {
		if reports[i].Group != reports[j].Group {
			return reports[i].Group < reports[j].Group
		}
		return reports[i].Day.Before(reports[j].Day)
	})
	return reports, nil
}
```

- [ ] **Step 4: Run — PASS.**
- [ ] **Step 5: Commit** — `feat: rebalance history per consumer group`

---

### Task 5: Throughput patterns report

**Files:**
- Modify: `internal/analytics/report.go`
- Create: `internal/analytics/patterns.go`, `internal/analytics/patterns_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestPatternsProfiles(t *testing.T) {
	store := seededPatternStore(t) // 3 days of hourly kafka.topic.msg_rate for "orders": 100 at 09:00, 10 at other hours
	a := &Analyzer{store: store, client: nil}

	reps, err := a.Patterns(ctx, []string{"orders"}, scraper.MetricTopicMsgRate, 7*24*time.Hour)
	require.NoError(t, err)
	require.Len(t, reps, 1)
	p := reps[0]
	assert.Equal(t, 9, p.PeakHour)
	assert.Greater(t, p.HourlyProfile[9], p.HourlyProfile[0])
	// slope ≈ 0 for the flat series
	assert.InDelta(t, 0, p.Slope, 1e-6)
}
```

- [ ] **Step 2: Run — FAIL.**
- [ ] **Step 3: Implement**

`internal/analytics/report.go`:

```go
// ThroughputReport describes the time-of-day/day-of-week profile and trend of
// one topic's rate metric over the window.
type ThroughputReport struct {
	Topic         string        `json:"topic"`
	Metric        string        `json:"metric"`
	Window        time.Duration `json:"window"`
	HourlyProfile [24]float64   `json:"hourly_profile"` // mean per hour-of-day
	DailyProfile  [7]float64    `json:"daily_profile"`  // mean per weekday
	PeakHour      int           `json:"peak_hour"`
	PeakDay       int           `json:"peak_day"`
	Slope         float64       `json:"slope"`      // linear fit, per second
	Forecast7d    float64       `json:"forecast_7d"` // projected rate in 7 days
}
```

`internal/analytics/patterns.go`:

```go
package analytics

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pulsedev/streampulse/internal/storage"
)

// Patterns returns throughput profiles per topic over the window. A topic
// without data is omitted. window <= 0 is an error.
func (a *Analyzer) Patterns(ctx context.Context, topics []string, metric string, window time.Duration) ([]ThroughputReport, error) {
	if window <= 0 {
		return nil, fmt.Errorf("invalid window %s", window)
	}
	if metric == "" {
		return nil, fmt.Errorf("metric required")
	}

	now := time.Now()
	rows, err := a.store.QueryHourly(ctx, storage.QueryParams{
		Metric: metric,
		From:   now.Add(-window),
		To:     now,
	})
	if err != nil {
		return nil, fmt.Errorf("query hourly %s: %w", metric, err)
	}

	byTopic := make(map[string][]storage.MetricRow)
	for _, r := range rows {
		if len(topics) > 0 && !contains(topics, r.EntityName) {
			continue
		}
		byTopic[r.EntityName] = append(byTopic[r.EntityName], r)
	}

	names := make([]string, 0, len(byTopic))
	for n := range byTopic {
		names = append(names, n)
	}
	sort.Strings(names)

	reports := make([]ThroughputReport, 0, len(names))
	for _, name := range names {
		pts := byTopic[name]
		rep := ThroughputReport{Topic: name, Metric: metric, Window: window}

		hourSums := [24]float64{}
		hourCounts := [24]int{}
		daySums := [7]float64{}
		dayCounts := [7]int{}
		for _, p := range pts {
			h := p.TimeStart.Hour()
			hourSums[h] += p.Avg
			hourCounts[h]++
			d := int(p.TimeStart.Weekday())
			daySums[d] += p.Avg
			dayCounts[d]++
		}
		for h := 0; h < 24; h++ {
			if hourCounts[h] > 0 {
				rep.HourlyProfile[h] = hourSums[h] / float64(hourCounts[h])
			}
		}
		for d := 0; d < 7; d++ {
			if dayCounts[d] > 0 {
				rep.DailyProfile[d] = daySums[d] / float64(dayCounts[d])
			}
		}
		rep.PeakHour = peakIndex(rep.HourlyProfile[:])
		rep.PeakDay = peakIndex(rep.DailyProfile[:])

		// least-squares linear fit on (unix hours, rate)
		slope, intercept := linearFit(pts)
		rep.Slope = slope
		last := pts[len(pts)-1].TimeStart
		rep.Forecast7d = slope*last.Add(7*24*time.Hour).Unix() + intercept

		reports = append(reports, rep)
	}
	return reports, nil
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

func peakIndex(vals []float64) int {
	idx := 0
	for i := 1; i < len(vals); i++ {
		if vals[i] > vals[idx] {
			idx = i
		}
	}
	return idx
}

// linearFit returns the slope (per second) and intercept of the least-squares
// line through the points' timestamps and averages.
func linearFit(pts []storage.MetricRow) (slope, intercept float64) {
	n := len(pts)
	if n < 2 {
		return 0, 0
	}
	var sx, sy, sxx, sxy float64
	for _, p := range pts {
		x := float64(p.TimeStart.Unix())
		y := p.Avg
		sx += x
		sy += y
		sxx += x * x
		sxy += x * y
	}
	denom := float64(n)*sxx - sx*sx
	if denom == 0 {
		return 0, 0
	}
	slope = (float64(n)*sxy - sx*sy) / denom
	intercept = (sy - slope*sx) / float64(n)
	return slope, intercept
}
```

- [ ] **Step 4: Run — PASS.**
- [ ] **Step 5: Commit** — `feat: throughput patterns with hourly and daily profiles`

---

### Task 6: CLI flags + output

**Files:**
- Modify: `internal/cli/commands.go` (analyze command), `internal/cli/analyze_test.go`

- [ ] **Step 1: Write the failing tests** — `analyze --anomalies` prints anomaly lines for a seeded store (lag spike); `--rebalances` prints group/day/count rows; `--patterns --topics orders` prints the hourly profile; `--json` contains all selected sections; empty data → "no data" + exit 0; invalid metric name with `--anomalies=foo` → exit 2.

- [ ] **Step 2: Run — FAIL** (flags don't exist).
- [ ] **Step 3: Implement** — extend the analyze command in `internal/cli/commands.go`:

```go
	cmd.Flags().StringSliceVar(&anomalies, "anomalies", nil, "detect anomalies for these metrics (lag, msg_rate, bytes_rate; default all)")
	cmd.Flags().StringSliceVar(&rebalanceGroups, "rebalances", nil, "show rebalance history (optional: comma-separated groups)")
	cmd.Flags().StringVar(&patternsMetric, "patterns", "", "show throughput patterns for a metric (msg_rate, bytes_rate)")
```

Section rendering order: growth (existing) → patterns → skew (existing) → retention (existing) → anomalies → rebalances. Human output:

```
ANOMALIES
metric                  entity            time        value    expected  z       dir
kafka.group.lag         orders-processor  08:00 UTC  500.00   100.00    4.12    high
```

with a `z` threshold column implied by severity. `--json` marshals `map[string]any` sections. Validate `--anomalies` values against `analytics.AnomalyMetrics` and `--patterns` against `{msg_rate, bytes_rate}` (map to `scraper.MetricTopicMsgRate/BytesRate`); unknown → usage error (exit 2 path — match how existing analyze errors exit).

- [ ] **Step 4: Run — PASS.** Full suite: `go test -race -count=1 ./...`
- [ ] **Step 5: Commit** — `feat: analyze command flags for anomalies, rebalances, and patterns`

---

### Task 7: TUI panes

**Files:**
- Modify: `internal/tui/model.go`, `internal/tui/model_test.go`

- [ ] **Step 1: Write the failing tests**
  - `loadData` (store mode) populates `m.anomalies` + `m.rebalances` from a seeded store (spike + transitions); analytics cache staleness test extended: new panes refresh with the same 30s cadence.
  - `renderAnalyticsView` contains "ANOMALIES" and "REBALANCES" sections when data exists, "no anomaly data" / "no rebalance data" otherwise.
  - Patterns: `j/k` on the Analytics tab cycles the selected topic; the pane shows the selected topic's hourly profile via `Bars`.

- [ ] **Step 2: Run — FAIL.**
- [ ] **Step 3: Implement** — in `internal/tui/model.go`:

```go
	anomalies   []analytics.Anomaly
	rebalances  []analytics.RebalanceReport
	patterns    []analytics.ThroughputReport
	patternIdx  int
```

In the analytics refresh block (where `Growth`/`Skew`/`Retention` are computed, store-mode only):
- `a.Anomalies(ctx, nil, 7*24*time.Hour)` → `m.anomalies`
- `a.Rebalances(ctx, nil, 7*24*time.Hour)` → `m.rebalances`
- `a.Patterns(ctx, topicsOf(m.topics), scraper.MetricTopicMsgRate, 7*24*time.Hour)` → `m.patterns` (topics from the live topics table; skip when none)
- keep `analyticsErr` semantics (one error line, data preserved).

`renderAnalyticsView` additions:
- ANOMALIES pane: up to 5 most severe (sort by |ZScore| desc) — `entity metric value expected z` rows, `warning`/`critical` colored via lipgloss.
- REBALANCES pane: table of group/day/count (last 10).
- PATTERNS pane (selected topic `patterns[patternIdx%len]`): `Bars(hourLabels, HourlyProfile, width)` + peak hour + 7-day forecast.
- `j/k` handler: when `m.activeTab == 5 && len(m.patterns) > 0`, move `patternIdx` (modulo) instead of table cursor.

- [ ] **Step 4: Run — PASS.** Full suite with `-race`.
- [ ] **Step 5: Commit** — `feat: TUI anomaly, rebalance, and pattern panes`

---

### Task 8: Design doc + full verification

**Files:**
- Create: `docs/design/analytics-l2.md`
- Modify: `README.md` (Analytics L2 under v0.2), `AGENTS.md` (roadmap line)

- [ ] **Step 1: Write the design doc** — capture: feature definitions, data sources (hourly aggregates, state samples), algorithms (seasonal slot z-score, rolling z, LAG-based transitions, linear fit), thresholds, failure modes (insufficient history → no data, sampling misses sub-5s rebalances), limitations (anomalies need ≥ 4 weeks for seasonal baseline, 3+ hours for rolling), and the CLI/TUI surface.
- [ ] **Step 2: Update README** v0.2 row: "Z-score anomaly detection + seasonal baselines" → mark "Analytics L2: anomaly detection, rebalance history, throughput patterns (implemented)". Keep other v0.2 rows planned. Update AGENTS.md v0.2 line similarly.
- [ ] **Step 3: Final gate** — `go build ./... && go vet ./... && go test -race -count=1 ./...` all green; `make build`; live smoke: daemon run against docker broker for ≥ 60s (produces hourly data only after an hour — so the live check verifies "no data" paths + CLI runs without panic):

```bash
bin/streampulse analyze --anomalies --json     # → [] or no-data, exit 0
bin/streampulse analyze --rebalances --json    # → [] or no-data, exit 0
bin/streampulse analyze --patterns --topics orders --json
```

- [ ] **Step 4: Commit** — `docs: analytics L2 design, roadmap, and usage docs`

---

## Failure modes & limitations (documented in the design doc)

- **Insufficient history:** seasonal baseline needs ≥ 3 prior samples in the hour-of-week bucket (≈ 3 weeks); below that the rolling fallback applies (≥ 2 prior points). No data → empty results, exit 0.
- **Sampling granularity:** rebalances shorter than the 5s scrape interval can be missed; state is mapped numeric, so a transition through 2→3 between samples counts once (both would appear as consecutive 2? No — only 1→2 counts, 2→3 does not; a fast 1→2→3 between two samples would be missed entirely — documented).
- **Flat series:** zero std → z=0, no false positives.
- **Forecast reliability:** linear fit is naive; forecasts are advisory only.
- **Store mode vs kafka mode:** all L2 features are store-driven; the TUI needs a running daemon (same as existing analytics panes). No store → panes show no-data.

## Out of scope (v0.3+)

Capacity forecasting with seasonality-aware models, per-partition throughput drill-down, anomaly alert rules wired to the alert engine (L2 follow-up), rebalance cause attribution.
