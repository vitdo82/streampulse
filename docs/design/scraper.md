# Design: Metrics scraper engine

**Status:** Design · **Depends on:** `configuration.md`, `security.md` · **Serves:** daemon, alerts, analytics

## Goal

Poll the Kafka cluster every `scrape_interval` (default 5s) and emit `storage.Metric` batches into the `MetricsStore` (see `storage.md`). `internal/scraper/` is currently an empty directory; this fills it.

## Architecture

```
                    ┌──────────────────────────────────────────┐
                    │               internal/scraper           │
 config ──────────► │  Scraper (orchestrator)                  │
                    │   ├─ brokerCollector  ──► kafka.Client   │
 ticker ──────────► │   ├─ topicCollector   ──► kafka.Client   │
                    │   ├─ groupCollector   ──► kafka.Client   │
                    │   └─ (future) dlqCollector               │
                    └──────────────────┬───────────────────────┘
                                       │ []storage.Metric
                              storage.MetricsStore.WriteBatch
```

Each collector is a separate struct implementing a small interface; the orchestrator runs them sequentially (rate deltas need stable ordering) within one timeout context.

```go
package scraper

// Collector scrapes one metric family.
type Collector interface {
	Collect(ctx context.Context, now time.Time) ([]storage.Metric, error)
}

type Scraper struct {
	collectors []Collector
	store      storage.MetricsStore
	clusterID  string
}
```

## Metric model

Reuses `storage.Metric` (`internal/storage/store.go:12`). Naming convention — dotted, prometheus-flavored:

| Metric | entity_type | entity_name | Value |
|--------|-------------|-------------|-------|
| `kafka.broker.leader_partitions` | broker | `host:port` | count |
| `kafka.broker.replica_partitions` | broker | `host:port` | count |
| `kafka.topic.partition_count` | topic | topic name | count |
| `kafka.topic.messages` | topic | topic name | cumulative counter |
| `kafka.topic.msg_rate` | topic | topic name | msgs/sec (delta/5s) |
| `kafka.topic.bytes_rate` | topic | topic name | bytes/sec |
| `kafka.group.lag` | consumer_group | group name | total lag |
| `kafka.group.member_count` | consumer_group | group name | count |
| `kafka.group.state` | consumer_group | group name | 0=Empty 1=Stable 2=PreparingRebalance 3=CompletingRebalance 4=Dead |

`Tags` carry partition-level detail where useful: `{"partition":"0"}`, `{"topic":"orders"}` for group lag per topic.

## Collectors

### brokerCollector
- `kafka.Client.DescribeCluster` (existing) → leader/replica counts per broker.
- New client method: `BrokerMetrics(ctx) ([]BrokerMetric, error)` wrapping `conn.Brokers()` + `conn.ReadPartitions()` — mostly a refactor of the counting logic already in `DescribeCluster` (currently only used for the TUI).

### topicCollector
- `kafka.Client.ListTopics` (existing) → partition counts.
- New: `TopicOffsets(ctx, topics) (map[string]map[int]int64, error)` — per-topic/per-partition high-watermark via `kafka.Conn.ReadLastOffsets`.
- Rate: cumulative `messages` counter; `msg_rate = (now - last) / dt` computed by the collector against its previous snapshot (kept in memory, reset on gap > 2 intervals).

### groupCollector
- `kafka.Client.ListConsumerGroups` (existing, protocol-level DescribeGroups).
- New: `GroupLag(ctx, groups) (map[string]map[string]int64, error)` — committed offsets via `kafka.Client.OffsetFetch` vs. high-watermarks from `TopicOffsets`. Lag per group per topic; total = sum.
- `group.state` mapped from the `State` string already returned by `groupsFromBroker` (`client.go`).

### dlqCollector (follow-up)
- Reuses DLQ discovery from `dlq.md` to emit `dlq.topic.message_count` + `dlq.topic.growth_rate`.

## Orchestration rules

- One `context.WithTimeout(5s)` per scrape cycle (config `scrape_interval` must be ≥ scrape timeout).
- Collectors never block the ticker: on error they return partial results + wrapped error; the orchestrator logs via `slog` and continues.
- `WriteBatch` in one transaction per cycle (`sqlite.go` already batches).
- Scrape counter exposed for Prometheus (`daemon.md`): `streampulse_scrapes_total`, `streampulse_scrape_duration_seconds`.

## Failure modes

- Cluster down → every collector errors; orchestrator logs once per cycle (rate-limited), keeps previous snapshot for rate deltas (resets after 2 missed cycles to avoid bogus spikes).
- One topic with `LeaderNotAvailable` → skipped partition in `TopicOffsets` (counted like the existing `p.Error` skip in `partitionsToTopics`).
- Partial `WriteBatch` failure → transaction rollback (existing `sqlite.go` behavior); no partial rows.
- Slow broker → context deadline; cycle skipped, next tick unaffected (mirrors the TUI `loading` guard pattern).

## Testing

- **Fake Kafka:** extract an interface from `internal/kafka.Client` for the collectors (`type Client interface { DescribeCluster(ctx) ... }` — the concrete `*kafka.Client` satisfies it), fake in tests with canned partitions/offsets/groups.
- Rate math: feed two snapshots 5s apart with known counters, assert `msg_rate` exact.
- Gap handling: skip snapshot → next scrape must reset (no rate spike).
- Group lag: fake committed offsets vs. fake high-watermarks → expected per-topic and total lag.
- Orchestrator: failing collector still yields other collectors' metrics; store receives one batch per cycle.
