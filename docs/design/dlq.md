# Design: DLQ module

**Status:** Design · **Depends on:** `security.md`, `scraper.md` · **Serves:** `streampulse dlq` commands, TUI DLQ tab

## Goal

Implement the DLQ management module: auto-discover dead-letter topics by name convention, inspect messages, and replay them to the original topic. The `dlq list/inspect/replay` commands are no-ops today (`commands.go:44-66`); the TUI DLQ tab shows a static table with dead hints (ENTER/R/D/A, `model.go:640`).

## Naming conventions

Topic is a DLQ if its name matches a suffix (config `dlq.suffixes`, default `[".dlq", ".dead", ".error", ".failed"]`). Original topic = name with suffix stripped (`payments.dlq` → `payments`). Suffixes sorted by length descending so `a.dead.error` maps to `a.dead`.

## Package layout

```
internal/dlq/
  discover.go   # DLQ topic discovery (convention-based)
  inspect.go    # read last N messages
  replay.go     # replay to original topic (dry-run, filters)
```

```go
type DLQ struct {
	client *kafka.Client
	cfg    DLQConfig   // suffixes, replay batch size, timeout
}

type Topic struct {
	Name          string
	OriginalTopic string
	MessageCount  int64
	GrowthRate    float64  // from scraper dlq collector
}

type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}
```

## Commands

### `streampulse dlq list [--json]`
1. `client.ListTopics` (existing) → filter by suffixes.
2. For each DLQ: message count via `ReadLastOffsets`; growth rate from the scraper's `dlq.topic.growth_rate` metric in the store (if daemon running) else `-`.
3. Output table (name, original topic, messages, growth). TUI DLQ tab uses the same data via `ListDLQs`.

### `streampulse dlq inspect --topic payments.dlq [--limit 10]`
1. Resolve the topic's partitions (`ReadPartitions`).
2. Use a `kafka.Reader` (`kafka.ReaderConfig{Topic, Partition: -1 (all), MinBytes/MaxBytes}`) or `ReadLastOffsets` + `ReadAt` per partition, reading the last `limit` messages per topic total (round-robin across partitions).
3. Print: offset, partition, timestamp, key, value (truncated to `--max-bytes 1000`, hex for binary), headers.
4. `--json` for scripting; `--follow` tails new messages (for the TUI inspect view later).

### `streampulse dlq replay --topic payments.dlq [--dry-run] [--limit 100] [--older-than 1h] [--filter error=DB_TIMEOUT] [--skip-existing]`
1. Read messages (same reader as inspect, oldest-first within window).
2. Produce to the original topic (`kafka.Writer`, key preserved, headers preserved, added headers `x-streampulse-replayed: true`, `x-streampulse-source-offset: N`).
3. Commit offsets on the DLQ **after** successful produce per batch (at-least-once; duplicates possible on crash — documented).
4. `--dry-run`: count + print first 10, produce nothing (safe default for CI exploration).
5. `--filter key=value` matches header key/value pairs; `--older-than`/`--newer-than` filter on message timestamp.

All commands share `--brokers` from root + auth options from `configuration.md`/`security.md`.

## TUI DLQ tab

- `fetchFromKafka` gains a DLQ section (via `ListDLQs`), filling `DLQRow` (`model.go:102`): Topic, MessageCount, Growth, ErrorPattern (first `error`-suffixed header or `-`).
- Active hints only when data present: ENTER opens the inspect view (a `viewport.Model`-based message list, reuse the log view pattern), R replays with a confirm prompt, D/A are deferred to v0.2 (no-op hints removed until implemented — the review flagged dead hints as a UX defect).

## Failure modes

- Original topic missing (e.g. `payments.dlq` exists, `payments` deleted) → replay lists the issue per message batch, does not produce (error + exit 2); list shows `original: missing`.
- Empty DLQ → clean "no messages" output, exit 0.
- Broker down → wrapped dial error, exit 2, no partial state.
- Replay crash mid-batch → at-least-once duplicates on the original topic; mitigated by `--skip-existing` (checks `x-streampulse-source-offset` header presence) and idempotency is documented as producer responsibility.
- Produce to a non-existent original topic → kafka auto-create must be disabled; error per batch with the topic name.

## Testing

- Discovery: suffix table tests (nested suffixes, no-suffix topics, internal topics `__*` excluded).
- Inspect/replay: against the docker compose cluster (topics `payments.dlq`/`orders.dlq` exist and receive messages every 20th produce — see `docker-compose.yaml:60-62`). Integration tests skipped without broker (existing `STREAMPULSE_TEST_BROKER` pattern).
- Replay correctness: produce 5 messages to a scratch DLQ topic, replay to a scratch original, assert key/value/headers preserved + `x-streampulse-replayed` header present; `--dry-run` produces nothing; `--filter` selects the right subset.
- `--skip-existing`: replayed-then-replayed run produces no duplicates.
- TUI: `ListDLQs` mapping unit test; DLQ tab renders rows from `DLQRow` data.
