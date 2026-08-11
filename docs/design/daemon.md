# Design: Daemon mode (`streampulse serve`)

**Status:** Design · **Depends on:** `configuration.md`, `security.md`, `scraper.md`, `storage.md` · **Serves:** TUI store mode, Prometheus, alerts

## Goal

Implement the currently no-op `serve` command (`internal/cli/commands.go:23`): a 24/7 process that scrapes Kafka, persists to the store, rolls up data, exposes Prometheus metrics, runs the alert engine, and shuts down gracefully.

## Process topology

```
                ┌─────────────────── streampulse serve ───────────────────┐
                │                                                          │
 SIGTERM/SIGINT │  main (cli/serve)                                        │
 ─────────────► │   ├─ config.Load()                                       │
                │   ├─ store := storage.NewStore(cfg)                      │
                │   ├─ client := kafka.NewClientWithOptions(...)           │
                │   ├─ scraper := scraper.New(...)                         │
                │   ├─ alerter := alerts.New(...)  (alerts.md)             │
                │   ├─ http.Server{ /metrics }                             │
                │   └─ goroutines:                                         │
                │        scrapeLoop  ─ ticker(interval) ─► scraper ─► store│
                │        rollupLoop  ─ cron hourly/daily ─► store.Rollup   │
                │        alertLoop   ─ every 10s ─► alerter.Evaluate       │
                │        promServer  ─ :9090/metrics                       │
                └──────────────────────────────────────────────────────────┘
```

All loops share one root `context` canceled on signal; each loop has its own timeout and error logging via `slog`.

## Lifecycle

1. `config.Load` (validation failures → exit 2, see `health-check.md`).
2. Open store (`NewStore`; SQLite `~/.streampulse/state.db` default), `defer Close` — single owner pattern from the TUI fix.
3. Connect kafka client (`defer Close` — releases transport, see `client.go`).
4. `Migrate` is already called by `NewSQLiteStore`; rollup tables added by `storage.md` migration.
5. Start loops; run Prometheus HTTP server (`chi` or stdlib `net/http`, per AGENTS.md — stdlib `http.ServeMux` suffices for one route).
6. On signal: cancel root ctx, `http.Server.Shutdown(10s)`, wait for loops via `sync.WaitGroup` with a 10s grace, exit 0. Second signal → immediate exit 1.

## Prometheus endpoint

`http://:9090/metrics` (config `prometheus.listen`/`prometheus.path`), stdlib + `prometheus/client_golang`:

- `streampulse_scrapes_total{cluster_id}`
- `streampulse_scrape_duration_seconds` (histogram)
- `streampulse_scrape_errors_total`
- `streampulse_alerts_firing{rule}` (from alerts.md)
- `streampulse_build_info{version}` (set from the `main.version` ldflag)
- Optional: expose last scrape of each metric family as gauges (`streampulse_broker_up{broker}`).

Stored metrics are NOT re-exposed wholesale; the raw scrape gauges above plus alert state are enough for external monitoring. (Full re-export is an Analytics L2 idea — see `analytics.md`.)

## Persistence & rollup

- `scrapeLoop` writes `raw_metrics` via `WriteBatch` (scraper.md).
- `rollupLoop`: hourly at `:05` past the hour, daily at `00:10`. `store.Rollup(ctx, "hourly"|"daily")` per `storage.md` (raw 5s → hourly 90d → daily 365d).
- `Purge` runs daily after rollup with the retention from `storage.md`.

## Store-mode TUI compatibility

The TUI's store mode (`tui/model.go` `loadData`, currently a stub returning empty data) becomes the daemon client:

- `streampulse` without `--brokers` → `NewModel(storePath)` (existing).
- `loadData` must query recent data via `store.QueryRaw`/`QueryDaily` (storage.md) instead of returning `DataUpdated{}`, so the Overview tables fill from persisted metrics when the daemon is running.
- Concurrency: TUI and daemon share the SQLite file. SQLite handles multi-process via WAL mode — add `PRAGMA journal_mode=WAL` + `busy_timeout=5000` in `NewSQLiteStore` (sqlite.go) as part of this feature. Single-writer is no longer true with two processes; keep `SetMaxOpenConns(1)` per process.
- If the store is unreachable, TUI shows "store offline" in the activity log (existing behavior) and keeps the last-good tables (`Failed` flag semantics already in place).

## Failure modes

- Kafka down at startup → retry with backoff (1s, 2s, 4s, … max 30s), keep serving `/metrics` (healthy:0 style gauges), log per attempt. Daemon does not exit.
- Store full/disk error → scrape loop logs and retries next cycle; alert engine can consume `scrape_errors_total` internally to fire a `scrape-failing` alert (alerts.md rule 6).
- Daemon restart → SQLite WAL recovers; rollup re-runs idempotently (`storage.md` upserts).
- Clock skew: scrape/rollup use monotonic time for intervals; wall clock only for bucketing.

## Testing

- Integration (docker compose): start daemon against the local broker for ~15s, assert `raw_metrics` rows exist, `/metrics` responds with `streampulse_scrapes_total > 0`, SIGTERM exits 0 within grace.
- Unit: signal handler table test; loop-skip-on-deadline behavior (fake store).
- Rollup correctness lives in `storage.md` tests; daemon test only asserts the loop calls `Rollup` at the right cadence (fake store recording calls).
- TUI store-mode: run daemon, run TUI headless is not feasible — covered by `loadData` unit tests with a real `:memory:` store seeded via `WriteBatch`.
