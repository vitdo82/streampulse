# AGENTS.md — StreamPulse Development Guide

> Instructions for AI coding agents and contributors working on this codebase.

## Project Overview

**StreamPulse** is a k9s-style terminal UI for Apache Kafka observability. Single Go binary. Zero external dependencies. `brew install streampulse`.

- **Language:** Go 1.23+
- **Binary size target:** < 10 MB
- **License:** Apache 2.0
- **Repo:** `github.com/pulsedev/streampulse`

## Quick Commands

```bash
make build          # Build binary → bin/streampulse
make run            # Build + run TUI
make test           # Run all tests (go test ./...)
make test-cover     # Tests with coverage HTML
make lint           # golangci-lint run
make fmt            # go fmt + goimports
make tidy           # go mod tidy

# Docker dev environment
docker compose up -d        # Start Kafka 3.9 KRaft + producer
docker compose logs -f kafka
docker compose down -v       # Stop + cleanup
```

## Architecture

```
cmd/streampulse/main.go          # Entry point
internal/
  cli/                           # Cobra command tree
    root.go                      # Root command (default: TUI)
    commands.go                  # serve, check, dlq, analyze, alerts
  tui/                           # Bubbletea terminal UI
    model.go                     # 6-tab dashboard (Overview, Topics, Consumers, Alerts, DLQ, Analytics)
  kafka/                         # kafka-go client wrapper
    client.go                    # Ping, DescribeCluster, consumer/producer
  storage/                       # Pluggable metrics backend
    store.go                     # MetricsStore interface
    sqlite.go                    # SQLite implementation (v0.1 default)
                                  # PostgreSQL + ClickHouse: v0.2, v0.3
  config/                        # Viper-based configuration
    config.go                    # Config struct, defaults
  scraper/                       # Metrics scraper engine (TODO)
  alerts/                        # Alert state machine (TODO)
```

## Key Design Decisions

### k9s Model: TUI + Daemon, Not Web-First
- **Mode 1 (TUI):** `streampulse` — interactive terminal dashboard (bubbletea). Read-only. Closes without affecting daemon.
- **Mode 2 (Daemon):** `streampulse serve` — persistent metrics collection, alerting, Prometheus /metrics endpoint. Runs 24/7 (systemd/launchd).
- **Mode 3 (One-shot):** `streampulse check` — CI/CD health gate. Exit 0 or 1.

### Pluggable Storage (MetricsStore Interface)
Define the interface on day 1. Ship SQLite. Add PostgreSQL + ClickHouse later.
- `WriteBatch`, `QueryRaw`, `QueryHourly`, `QueryDaily`, `Rollup`, `Purge`, `Migrate`
- Rollup: raw (5s, 24h) → hourly (90d) → daily (365d). Runs in daemon goroutine.

### Zero CGo
Use `modernc.org/sqlite` (pure Go SQLite). No `librdkafka` — use `kafka-go` (segmentio, pure Go). Single static binary, cross-compiles everywhere.

## Coding Conventions

### Go Style
- Standard library first. Add dependencies only when necessary.
- `net/http` + `chi` for HTTP. No gin/echo/fiber.
- `slog` for structured logging. No zap/zerolog/logrus.
- `testify` for test assertions. Standard `testing` package.
- Package names: lowercase, single word, no underscores.
- Error handling: always wrap with `fmt.Errorf("context: %w", err)`.

### TUI Patterns (Bubbletea)
- Use `bubbles/table` for data tables. Style with `lipgloss`.
- Use `bubbles/viewport` for scrollable logs.
- Tabs: Overview, Topics, Consumers, Alerts, DLQ, Analytics.
- Auto-refresh every 2 seconds via `tea.Tick`.
- Vim navigation: `j/k` for scroll, `tab` for tabs, `/` for search, `q` to quit.

### Kafka Client
- Use `kafka-go` from segmentio. Pure Go, no CGo.
- Connection: SASL/SSL/mTLS/IAM support from day 1.
- Consumer groups: read offsets from `__consumer_offsets` via AdminClient.
- DLQ: discover by topic name convention (`*.dlq`, `*.dead`, `*.error`, `*.failed`).

## Testing

### Test Policy
- TDD-style: write failing test → implement → verify.
- Unit tests for all packages. Table-driven tests preferred.
- Mock Kafka connections using `kafka-go`'s test utilities or interfaces.
- TUI tests: verify model state, view renders expected content.

### Running Tests
```bash
go test ./... -count=1           # All tests
go test ./internal/tui/ -v       # TUI tests only
```

## Docker Development

```yaml
# Kafka 3.9 in KRaft mode (no ZooKeeper)
# Connect: localhost:9093 (host), kafka:9092 (internal Docker network)
# Topics auto-created: orders, payments, inventory, audit + DLQ topics
# Producer generates JSON messages every 2 seconds
docker compose up -d
streampulse serve --brokers localhost:9093   # Connect StreamPulse
```

## Roadmap (What to Build Next)

```
v0.1 (DONE):  TUI + daemon + 6 alerts + DLQ module + Analytics L1 + config + auth
v0.2 (Weeks 4-6):  REST API + local web dashboard + PostgreSQL + Analytics L2
v0.3 (Weeks 7-10): Kafka Connect + Streaming (Flink/Streams) + PulseDev Cloud
```

### v0.1 (implemented — see docs/design/ and docs/2026-08-10-v0.1-roadmap-plan.md)
1. ✅ Scraper engine: poll broker/topic/consumer metrics every 5s (`internal/scraper`)
2. ✅ Daemon mode: SQLite persistence, rollup, Prometheus /metrics (`internal/daemon`)
3. ✅ TUI dashboard: 6 tabs, search, table nav, activity log, alert/DLQ/analytics data
4. ✅ Alert engine: 6 rules, state machine, Slack/Email/PagerDuty (`internal/alerts`)
5. ✅ DLQ module: discovery, inspection, replay with filters, dry run (`internal/dlq`)
6. ✅ Analytics L1: growth charts, partition skew, retention analysis (`internal/analytics`)
7. ✅ Config: viper defaults/file/env/flags (`internal/config`) + Kafka TLS/SASL/IAM auth
8. ✅ CI gate: `check` command, exit codes 0/1/2 (`internal/check`)

## Dependencies (v0.1)

| Package | Purpose |
|---------|---------|
| `github.com/charmbracelet/bubbletea` | TUI framework |
| `github.com/charmbracelet/bubbles` | TUI components (table, viewport) |
| `github.com/charmbracelet/lipgloss` | Terminal styling |
| `github.com/segmentio/kafka-go` | Kafka client (pure Go) |
| `github.com/spf13/cobra` | CLI framework |
| `github.com/spf13/viper` | Configuration (YAML + env + flags) |
| `modernc.org/sqlite` | Embedded SQLite (pure Go) |
| `github.com/prometheus/client_golang` | Prometheus metrics endpoint |
| `github.com/robfig/cron/v3` | Scheduler for scrape + rollup |
| `github.com/go-chi/chi/v5` | HTTP router (v0.2) |

## Things to Avoid

- ❌ Don't add JavaScript frameworks for the embedded web UI until v0.2 decision is made (Vue vs React TBD).
- ❌ Don't use CGo-dependent packages (librdkafka, mattn/go-sqlite3).
- ❌ Don't add features not on the v0.1 roadmap. YAGNI.
- ❌ Don't create separate binaries for TUI and daemon. One binary, multiple modes.
- ❌ Don't hardcode cluster ID or credentials. Use viper config.
