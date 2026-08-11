# Design: Alert engine

**Status:** Design · **Depends on:** `scraper.md`, `storage.md`, `configuration.md` · **Serves:** daemon, `streampulse alerts`, TUI Alerts tab

## Goal

Implement the alert engine in `internal/alerts/` (currently empty): 6 built-in rules with a state machine, per-rule config, and Slack/Email/PagerDuty notifications. Wire `streampulse alerts` (no-op today, `commands.go:86`) and the TUI Alerts tab (currently always empty).

## Rule set (6 built-ins)

| # | Rule | Condition (default) | entity | severity |
|---|------|--------------------|--------|----------|
| 1 | `broker-down` | `broker.up == 0` (no broker metrics this cycle) | broker | critical |
| 2 | `under-replicated` | `kafka.broker.replica_partitions > kafka.broker.leader_partitions` sustained | broker | critical |
| 3 | `consumer-lag` | `kafka.group.lag > 1000` | consumer_group | warning |
| 4 | `dlq-growth` | `dlq.topic.growth_rate > 10` msgs/s | topic (.dlq/.dead/.error/.failed) | warning |
| 5 | `partition-skew` | max/avg partition leader ratio > 1.5 (skew collector in `analytics.md`) | cluster | warning |
| 6 | `scrape-failing` | `scrape_errors_total` increments for 3 consecutive cycles | cluster | critical |

Defaults are configurable: rules in `config.alerts` override built-ins by `name` (`AlertRule` struct exists in `config.go`); unknown names in config → validation error.

## Condition language

`AlertRule.Condition` is a string, e.g. `"lag > 1000"`, `"replica > leader"`. No external expression libs (AGENTS.md: standard library first). Implement a tiny evaluator in `internal/alerts/condition.go`:

- Grammar: `metric [operator] value` where `metric` is a name from the scraper metric set (aliases: `lag`, `replica`, `leader`, `growth_rate`, `up`, `skew`), operator ∈ `> >= < <= == !=`, value a float or `0/1` for boolean metrics.
- Compiled once at startup into a struct (`metric, op, threshold`) — 40 lines, table-driven, no parser combinator.
- Unsupported syntax → `configuration.md` validation error with position.

## State machine

States: `ok → pending → firing → (resolved→ok)`.

```
                     for-duration elapsed
        ok ────────────────────────────► pending ──────────────► firing
        ▲                                  │                        │
        │            condition false        │   condition true      │
        └───────────────────────────────────┴────────────────────────┘
                (any state, on false → ok, notification "resolved")
```

- `pending`: condition true but not yet for `rule.For` (default 2m). Prevents flapping notifications.
- `firing`: notify once on entry, then re-notify at a repeat interval (default 1h, config `repeat_interval`).
- Resolution: condition false for 1 evaluation cycle → `ok`, send "resolved" notification only if it previously fired.
- Persisted in `alert_state` (already migrated, `sqlite.go:130`): `rule_name` PK, `status`, `last_fired`, `last_value`, `notify_count`.

```go
type State struct {
	RuleName   string
	Status     string // ok|pending|firing
	LastFired  time.Time
	LastValue  float64
	NotifyCount int
}

type Engine struct {
	rules     []Rule       // compiled rules with state
	store     storage.MetricsStore
	notifiers []Notifier
}

func (e *Engine) Evaluate(ctx context.Context, metrics []storage.Metric, now time.Time) error
```

`Evaluate` is pure-ish: reads rule state from `alert_state`, computes transitions, writes state back, and calls notifiers for `firing`/`resolved` events. The daemon calls it every 10s with the last scrape's metrics (`daemon.md`).

## Notifiers

`Notifier` interface; `internal/alerts/notify/`:

```go
type Notifier interface {
	Notify(ctx context.Context, n Notification) error
}

type Notification struct {
	Rule      string
	Severity  string
	Status    string // firing|resolved
	Value     float64
	Entity    string
	Message   string
	Timestamp time.Time
}
```

- **slack**: `POST webhook` with `{"text": ...}` (`AlertChannel.Webhook`, resolved from `webhook_env` at load — never stored in yaml). Stdlib HTTP, 5s timeout, retry once on 5xx.
- **email**: net/smtp, config `smtp.host/port/user/password_env/from`, `To` from `AlertChannel.To` (comma-separated). Plain text body, HTML not required (v0.1).
- **pagerduty**: `POST https://events.pagerduty.com/v2/enqueue` with the Events API v2 payload (routing_key from `webhook_env`). Only for `critical` severity by default (config `pagerduty.min_severity`).
- Notifier failures never break the engine: logged + `notify_errors_total` counter; state transition still persists (the notification is retried on next repeat interval).

## CLI + TUI surfaces

- `streampulse alerts` — reads `alert_state` from the store, prints table (name, status, last fired, value, notify count); `--json` output for scripting.
- TUI Alerts tab: `fetchFromKafka`/`loadData` gains an alert query against the store (or a `--daemon` REST-free IPC: for v0.1, TUI reads `alert_state` directly from the shared SQLite file, same as store mode; WAL makes this safe per `daemon.md`).
- `AlertRow` fields already exist in the TUI model (`model.go:95`).

## Failure modes

- Store unavailable during `Evaluate` → skip cycle, log; state kept in memory + retried.
- Condition metric missing this cycle (collector error) → rule evaluated as `false` (no firing) but marked `data_missing`; 3 consecutive missing cycles → `scrape-failing` rule fires if enabled.
- Clock skew between daemon and notifier timestamps → use daemon wall clock only.
- Duplicate notifications after daemon restart → `last_fired` + `notify_count` in `alert_state` make entry re-notification idempotent (firing already notified → only repeat-interval notifies).

## Testing

- State machine: table-driven transition tests for every arrow (ok→pending→firing→ok, pending→ok, firing repeat interval, resolved notify).
- Condition evaluator: compile errors and evaluation table tests.
- Persistence: `alert_state` round-trip via a `:memory:` store; restart-with-state test (rehydrate from store).
- Notifiers: httptest servers for slack/pagerduty; net/smtp via a fake SMTP listener; failure-path tests (5xx → retry → logged).
- `streampulse alerts`: golden output test with seeded `alert_state`.
