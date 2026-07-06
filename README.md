# StreamPulse — The k9s for Apache Kafka

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

**Single binary. Zero dependencies. `brew install streampulse`.**

Real-time Kafka observability in your terminal. Consumer lag, broker health, DLQ management, analytics, and 7 production alerts — all in a k9s-style TUI. Optional PulseDev Cloud for hosted dashboards and team features.

```
$ brew install streampulse
$ streampulse serve --brokers kafka:9092
$ streampulse
```

## Quick Start

```bash
# Install
brew install pulsedev/tap/streampulse

# Or: go install
go install github.com/pulsedev/streampulse/cmd/streampulse@latest

# Start daemon (metrics collection + alerts 24/7)
streampulse serve --brokers localhost:9092

# Open TUI (in another terminal)
streampulse

# One-shot health check (CI/CD)
streampulse check --topic orders --expect-max-lag 500

# DLQ management
streampulse dlq list
streampulse dlq inspect --topic orders.dlq
streampulse dlq replay --topic orders.dlq --dry-run
```

## Features

### v0.1 (Current)

| Feature | Description |
|---------|-------------|
| 🖥️ **TUI Dashboard** | k9s-style terminal dashboard — brokers, topics, consumer groups, real-time metrics |
| 🔔 **7 Production Alerts** | Lag velocity, stale consumers, under-replicated partitions, dead topics, DLQ growing, anomaly detection |
| 📂 **DLQ Management** | Auto-discover DLQ topics, inspect messages, cluster errors, replay with filters, dry run, archive, drain |
| 📈 **Analytics** | Topic growth charts, partition skew detection, rebalance history, throughput patterns |
| 📊 **Prometheus /metrics** | Native Prometheus endpoint on `:9090/metrics` |
| ✅ **CI/CD Health Gate** | `streampulse check` exits 0/1 — pipe into any deployment pipeline |

### v0.2 (Planned)

- REST API on daemon (`:9090/api`)
- PostgreSQL support for team deployments
- Z-score anomaly detection + seasonal baselines
- Embedded local web dashboard
- Redpanda native tab (Raft health, tiered storage)
- Kafka Connect module

### v0.3 (Planned)

- PulseDev Cloud (hosted dashboards, team access, SSO)
- ClickHouse analytics backend
- Kafka Streams + Flink monitoring
- Schema Registry compatibility
- Capacity forecasting + cost attribution

## Configuration

```yaml
# ~/.streampulse/config.yaml
brokers:
  - localhost:9092

scrape_interval: 5s

storage:
  type: sqlite                # default; also: postgres, clickhouse
  sqlite:
    path: ~/.streampulse/state.db

alerts:
  - name: "high-consumer-lag"
    group: "*-processor"
    condition: "lag > 10000"
    for: 30s
    notify:
      - type: slack
        webhook: "https://hooks.slack.com/..."
```

## Building from Source

```bash
git clone https://github.com/pulsedev/streampulse.git
cd streampulse
make build        # → bin/streampulse
make run          # → build + run TUI
make test         # → run tests
make lint         # → golangci-lint
```

## Tech Stack

| Layer | Choice |
|-------|--------|
| Language | Go 1.23+ |
| TUI | [bubbletea](https://github.com/charmbracelet/bubbletea) |
| Kafka | [kafka-go](https://github.com/segmentio/kafka-go) |
| Storage | SQLite ([modernc](https://modernc.org/sqlite)), PostgreSQL, ClickHouse |
| Metrics | Prometheus |

## License

Apache 2.0 — see [LICENSE](LICENSE).

---

Built by [PulseDev](https://pulsedev.dev). Also check out [ToolServe](https://toolserve.dev) — MCP server hosting.
