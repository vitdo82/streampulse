# Design: Analytics L1

**Status:** Design · **Depends on:** `storage.md` (queries), `scraper.md` · **Serves:** `streampulse analyze`, TUI Analytics tab

## Goal

Analytics L1: growth charts, partition skew, and retention analysis — powered by the persisted scrape data. The `analyze` command is a no-op (`commands.go:76`); the TUI Analytics tab is a static placeholder ("coming in v0.1", `model.go:666`).

## Outputs (L1 scope)

| Feature | Source | Output |
|---------|--------|--------|
| Topic growth | `QueryDaily(kafka.topic.messages)` / `msg_rate` | ASCII sparkline + delta over window (1h/24h/7d/30d) |
| Partition skew | `kafka.topic.partition_count` + leader counts per broker (`kafka.broker.leader_partitions`) | Skew ratio per topic: `max_leaders/avg_leaders`, worst topics list |
| Retention analysis | `DescribeConfigs` for `retention.ms`/`retention.bytes` + `kafka.topic.messages` rate | Estimated fill days (`bytes/rate`), retention vs. current age of oldest message |
| Consumer lag trend | `QueryDaily(kafka.group.lag)` | Sparkline per group |

## Package layout

```
internal/analytics/
  analyze.go    # Analyzer: queries store + kafka, returns reports
  chart.go      # ASCII sparkline/barchart rendering (stdlib only)
  report.go     # Report structs + JSON marshaling
```

```go
type Analyzer struct {
	store  storage.MetricsStore
	client *kafka.Client
}

type GrowthReport struct {
	Topic    string
	Window   time.Duration
	Points   []Point       // {Time time.Time, Rate float64}
	Delta    float64       // msgs/sec first→last
	Sparkline string
}

type SkewReport struct {
	Topic      string
	Leaders    map[string]int   // broker id → leader count
	Ratio      float64          // max/avg
	Balanced   bool             // ratio <= 1.5
}

type RetentionReport struct {
	Topic             string
	RetentionMS       time.Duration  // from DescribeConfigs
	EstimateFillDays  float64
	OldestOffsetAge   time.Duration  // now - oldest message timestamp
	AtRisk            bool
}
```

Queries use the existing `storage.QueryParams` (`From`, `To`, `Metric`, `EntityName`) — no new storage surface needed.

## CLI

```
streampulse analyze --window 24h --topics orders,payments [--json]
streampulse analyze --skew [--json]
streampulse analyze --retention [--json]
```

- Human output: section headers + sparklines (`▁▂▃▄▅▆▇█` glyphs from value buckets) + a compact table.
- `--json`: full report structs (used by CI dashboards; no external rendering deps).
- Default: growth for the top 10 topics by messages in the window.

## TUI Analytics tab

- Wire `renderAnalyticsView` (`model.go:644`) to real data: the model already refreshes every 2s; add an `analytics` field populated on a slower cadence (every 30s — the 2s tick recomputes only when the analytics cache is stale, mirroring the `loading` guard pattern).
- Render: growth sparkline for the selected topic (topics table selection from the Topics tab), skew table, retention warnings. Layout: reuse `lipgloss` styles; the existing chart placeholder box becomes the growth pane.
- Interaction (v0.1): topic selection cycles with `j/k` on the Analytics tab only; detailed drill-down is L2.

## Failure modes

- No persisted data (daemon never ran) → reports render "no data" + hint to run `streampulse serve`; exit 0 (not an error).
- Store unreachable → CLI exits 2 with wrapped error; TUI shows the analytics pane's error state and logs to the activity log (`Failed` flag path already handles table preservation).
- Window larger than available retention → render what exists (partial window, noted in output).
- `DescribeConfigs` unsupported by broker/ACLs → retention report marks the topic `retention: unknown`, other reports unaffected (per-report error isolation).

## Testing

- Chart rendering: golden strings for known value arrays (flat, spike, monotonic); empty input → empty sparkline.
- Growth: seed `:memory:` store with `WriteBatch` over 3 days of `kafka.topic.messages`, assert delta and point count for each window; `topics` filter respected.
- Skew: fake `DescribeCluster` leader counts → ratio/Balanced assertions (use the scraper.md fake client interface).
- Retention: fake `DescribeConfigs` values; `AtRisk` boundary tests (fill days < retention).
- CLI: golden human output + `--json` unmarshal round-trip; exit codes per failure modes above.
- TUI: analytics cache staleness (30s) unit test with a fake store.
