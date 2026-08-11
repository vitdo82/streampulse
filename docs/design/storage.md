# Design: Storage queries, rollup, and retention

**Status:** Design · **Depends on:** `daemon.md` (runs rollup) · **Serves:** analytics, alerts, TUI store mode

## Goal

Implement the four TODO stubs in `internal/storage/sqlite.go` (`QueryRaw`/`QueryHourly`/`QueryDaily`/`Rollup`) and complete retention handling. Data lifecycle: raw (5s, 24h) → hourly (90d) → daily (365d).

## Schema additions

Migration in `Migrate` (sqlite.go) — appended, versioned via a `schema_version` table (new; the current `IF NOT EXISTS` approach gets a `PRAGMA user_version` guard so future migrations can ALTER safely):

```sql
CREATE TABLE IF NOT EXISTS hourly_metrics (
    bucket      INTEGER NOT NULL,   -- truncated-to-hour unix ms
    cluster_id  TEXT NOT NULL,
    metric      TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    tags        TEXT NOT NULL DEFAULT '{}',
    avg   REAL, min REAL, max REAL,
    p50   REAL, p95 REAL, p99 REAL,
    count INTEGER, sum REAL,
    PRIMARY KEY (bucket, cluster_id, metric, entity_type, entity_name, tags)
);
-- same shape for daily_metrics (bucket truncated to day)
CREATE INDEX IF NOT EXISTS idx_hourly_lookup ON hourly_metrics(cluster_id, metric, entity_type, entity_name, bucket);
CREATE INDEX IF NOT EXISTS idx_daily_lookup  ON daily_metrics(cluster_id, metric, entity_type, entity_name, bucket);
```

## Percentiles

SQLite has no `percentile`. Approach: `Rollup` reads a bucket's raw rows in Go, computes `avg/min/max/p50/p95/p99/count/sum` in memory (`internal/storage/percentile.go`, stdlib `sort`), then upserts the aggregate row. Bounded memory: process one (metric, entity, bucket) at a time with a SQL `GROUP BY` cursor.

Upsert (idempotent re-run):

```sql
INSERT INTO hourly_metrics (...) VALUES (...)
ON CONFLICT(bucket, cluster_id, metric, entity_type, entity_name, tags)
DO UPDATE SET avg=excluded.avg, min=excluded.min, max=excluded.max,
              p50=excluded.p50, p95=excluded.p95, p99=excluded.p99,
              count=excluded.count, sum=excluded.sum;
```

## Queries

```go
func (s *SQLiteStore) QueryRaw(ctx context.Context, params QueryParams) ([]MetricRow, error)
func (s *SQLiteStore) QueryHourly(ctx context.Context, params QueryParams) ([]MetricRow, error)
func (s *SQLiteStore) QueryDaily(ctx context.Context, params QueryParams) ([]MetricRow, error)
```

Shared behavior (one internal helper + table name):

- `WHERE` built from non-empty `QueryParams` fields (`ClusterID`, `Metric`, `EntityType`, `EntityName`), `ts/bucket >= From` and `< To`, `ORDER BY ts/bucket`, `LIMIT params.Limit` (default 1000, max 10000).
- Raw: group rows `GROUP BY ts, entity_name` → one `MetricRow` per point (count/sum/avg real; min/max real; percentiles = NULL for raw).
- Hourly/daily: rows are already aggregates; `MetricRow` fields map directly, `TimeStart = bucket`.
- `MetricRow.Tags` parsed from the stored JSON (the `encoding/json` fix in `sqlite.go` makes this safe).

## Rollup

```go
func (s *SQLiteStore) Rollup(ctx context.Context, resolution string) error // "hourly" | "daily"
```

- `hourly`: `SELECT ts, cluster_id, metric, entity_type, entity_name, tags, value FROM raw_metrics WHERE ts >= lastHourlyWatermark` — watermark from `MAX(bucket)` of hourly_metrics (re-compute the last hour fully on re-run for idempotency).
- `daily`: same from hourly_metrics.
- Runs in one transaction; batches of 500 (metric, entity, bucket) groups.
- Updates an in-memory-only watermark? No — persisted via the table itself (idempotent upsert makes watermarks unnecessary; always re-aggregate the last full bucket).

## Purge (complete retention)

Current `Purge` only handles raw (`sqlite.go:106-110`). New semantics:

```go
func (s *SQLiteStore) Purge(ctx context.Context, retention Retention) error
// retention.Raw    → DELETE FROM raw_metrics    WHERE ts < now - Raw
// retention.Hourly → DELETE FROM hourly_metrics WHERE bucket < now - Hourly
// retention.Daily  → DELETE FROM daily_metrics  WHERE bucket < now - Daily
```

All timestamps in UnixMilli (consistent with the write/purge fix). Defaults: Raw 24h, Hourly 90d, Daily 365d (config `storage.*` in `configuration.md`).

## Failure modes

- Rollup crashes mid-transaction → transaction rollback; next run re-aggregates (idempotent).
- Oversized bucket (e.g. partition skew data) → bounded by batch size; memory stays flat.
- Query with `From > To` → empty result, not an error.
- Unknown resolution string → error (`invalid rollup resolution "weekly"`).
- Stale/absent aggregate data (no hourly rows yet) → empty slice, nil error (analytics renders "no data").

## Testing

- **Percentile:** golden vectors (odd/even n, all-equal, single value) for p50/p95/p99; NaN/Inf values rejected at `WriteBatch` (new validation: reject non-finite values).
- **Rollup:** seed raw_metrics via `WriteBatch` with known values spanning two hours; run hourly rollup; assert exact aggregate rows (avg/min/max/percentiles/count/sum); run twice → identical (idempotency); partial failure (inject a bad metric name) → no partial rows.
- **Queries:** each filter dimension; time range exclusivity (From ≤ ts < To); limit; empty params (full table scan, bounded by limit); tags round-trip via JSON.
- **Purge:** per-resolution cutoff using `UnixMilli` boundary rows (strict `>` retention like the existing purge test).
- **Migration:** `Migrate` on a fresh DB creates all tables; `PRAGMA user_version` bumps correctly on upgrade path.
