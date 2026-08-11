# StreamPulse Technical Design Documents

Design docs for the unimplemented v0.1 roadmap features. Each doc is self-contained and grounded in the existing codebase (see `internal/` packages and `docs/2026-08-10-bugfix-reliability-plan.md`).

## Feature areas

| # | Feature | Doc | Status |
|---|---------|-----|--------|
| 1 | Metrics scraper engine | [scraper.md](scraper.md) | Design |
| 2 | Daemon mode (`serve`, persistence, rollup, Prometheus) | [daemon.md](daemon.md) | Design |
| 3 | Alert engine (6 rules, state machine, notifications) | [alerts.md](alerts.md) | Design |
| 4 | DLQ module (discovery, inspect, replay) | [dlq.md](dlq.md) | Design |
| 5 | Analytics L1 (growth, skew, retention) | [analytics.md](analytics.md) | Design |
| 6 | CI/CD health gate (`check`) | [health-check.md](health-check.md) | Design |
| 7 | Storage queries, rollup, retention | [storage.md](storage.md) | Design |
| 8 | Configuration wiring (viper) | [configuration.md](configuration.md) | Design |
| 9 | Kafka auth (SASL/SSL/mTLS/IAM) | [security.md](security.md) | Design |

## Cross-cutting principles

- **One binary, multiple modes** — no separate daemon/TUI binaries (AGENTS.md).
- **Standard library first** — new deps only when necessary; pure-Go only (zero CGo).
- **Pluggable storage** — all persistence through the existing `storage.MetricsStore` interface (`internal/storage/store.go`).
- **Everything testable** — each doc includes a testing plan; TDD per AGENTS.md test policy.
- **Reliability baseline** — timeouts on every network call, graceful shutdown, no goroutine leaks (see the transport lifecycle fix in `internal/kafka/client.go`).

## Suggested implementation order

1. [configuration.md](configuration.md) — foundation; everything reads config
2. [security.md](security.md) — kafka client auth; needed for real clusters
3. [scraper.md](scraper.md) — produces the data everything else consumes
4. [daemon.md](daemon.md) — hosts the scraper + persistence + Prometheus
5. [storage.md](storage.md) — makes the data queryable
6. [alerts.md](alerts.md), [dlq.md](dlq.md), [analytics.md](analytics.md), [health-check.md](health-check.md) — consumers of scraped/queried data
