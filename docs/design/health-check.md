# Design: CI/CD health check (`streampulse check`)

**Status:** Design · **Depends on:** `security.md`, `scraper.md` (metrics for thresholds) · **Serves:** CI pipelines

## Goal

Implement the no-op `check` command (`commands.go:33`): a one-shot health gate that exits 0 (healthy) or 1 (unhealthy) for CI/CD, with machine-readable output.

## Contract

```
streampulse check \
  --brokers localhost:9093 \
  --topic orders \
  --group orders-processor \
  --min-partitions 6 \
  --max-lag 1000 \
  --timeout 10s \
  --json
```

**Exit codes** (documented in `--help`):

| Code | Meaning |
|------|---------|
| 0 | All checks passed |
| 1 | A check failed (threshold exceeded, resource unhealthy) |
| 2 | Usage/config/connectivity error (wrong flags, bad config, no brokers, auth failure) |

Distinguishing 1 vs 2 matters: CI treats 1 as "gate failed", 2 as "pipeline broken".

## Check set

Each check is a function producing `CheckResult`; checks run sequentially, all results reported even after a failure:

```go
type Check struct {
	Name    string
	Run     func(ctx context.Context, env Env) (Result, error)
}

type Result struct {
	Name    string
	Status  Status    // pass | fail | skip
	Message string
	Value   float64
}

type Env struct {
	Client *kafka.Client
	Flags  Flags
}
```

Built-in checks:

1. **connectivity** — `client.Ping` (existing). Fail → exit 2 (not 1): nothing else can run.
2. **topics** — `--topic` (repeatable): topic exists, `partitions >= --min-partitions`, no partition with `p.Error != nil` (already skipped in `ListTopics` — check uses a raw `ReadPartitions` path for this). Fail → exit 1.
3. **consumer-group** — `--group`: group exists, state is `Stable` (not `Dead`/`PreparingRebalance`), members ≥ 1, total lag `<= --max-lag` (via the lag computation from `scraper.md`'s `GroupLag`). Fail → exit 1.
4. **retention** — optional `--min-retention-hours`: topic's `retention.ms` (DescribeConfigs) ≥ threshold. Fail → exit 1.
5. **under-replication** — optional `--check-replication`: any topic with `replica_count > leader_count` (per-broker counts from `DescribeCluster`) → fail.
6. **timeout** — hard ceiling: all network calls bounded by `--timeout` (default 10s); on timeout → exit 2 with "check timed out" (pipeline liveness issue, not a cluster-health verdict).

## Output

- Human: one line per check:
  ```
  connectivity        pass  (3 brokers)
  topic orders        pass  (6 partitions, min 6)
  group orders-proc   fail  (lag 2400, max 1000)
  verdict: FAIL (exit 1)
  ```
- `--json` (CI): array of `Result` objects + `{verdict, exit_code}`.
- Order stable; `skip` for checks with unmet prerequisites (e.g. group check without `--group`).

## Wiring

- `check` command gets its own flags (not root's `--brokers` only); builds `kafka.Client` via `NewClientWithOptions` from config (auth from `security.md`); reuses config loading from `configuration.md` (PreRunE).
- `scraper.GroupLag` is extracted so both `check` and the scraper use one implementation.

## Failure modes

- Auth misconfiguration → exit 2 with sanitized error (no credentials, `security.md`).
- Topic exists but no partitions → fail (exit 1) with clear message.
- Group lag computation fails (offset fetch error) → that check reports `fail` with the error, not `skip` — a monitoring gap is itself a health problem.
- No `--topic`/`--group` flags → connectivity-only check (valid, exit 0 when cluster reachable) — makes `check` usable as a pure liveness probe.

## Testing

- Unit: exit-code table tests for each check + combinations (1 failure among 3 passes → exit 1; connectivity fail → exit 2; flag validation → exit 2).
- Integration (docker compose, `STREAMPULSE_TEST_BROKER` pattern): full pass (topic `orders` 6 partitions, group `orders-processor` exists); then `--max-lag 1` → exit 1; `--brokers localhost:1` → exit 2.
- `--json` golden output.
- Timeout: `--timeout 1s` against a black-hole address (non-routable IP) → exit 2 within ~1s (fast test).
