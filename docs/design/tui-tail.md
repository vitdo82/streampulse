# Design: TUI topic tail (subscribe from the terminal)

**Status:** Design · **Depends on:** `dlq.md` (inspect/display pattern), existing TUI views · **Serves:** `internal/tui` Topics tab, new `internal/tail` package

## Goal

Let the user subscribe to a topic's messages from inside the TUI: press **Enter** on a topic row in the Topics tab to open a tail view that shows the last 50 messages and then live-follows new ones (like `tail -f`). Read-only, ephemeral, no cluster side effects.

## Scope decisions (confirmed)

| Decision | Choice |
|----------|--------|
| Open behavior | Snapshot last 50 messages, then live-follow new arrivals |
| Read mechanics | **No consumer group** — direct partition reads with in-memory offsets (nothing written to `__consumer_offsets`, nothing appears in the Consumers tab) |
| Partitions | All partitions interleaved by arrival order |
| Filtering | None in v1 (no key/value substring filter, no partition filter — see Follow-ups) |
| Persistence | None — messages are display-only, never written to the store |

## Package layout

```
internal/tail/
  tail.go       # Message, Snapshot, Follow (poll loop), display formatting
  tail_test.go  # unit tests + docker integration tests
internal/tui/
  model.go      # tail view state (topic, messages, pause flag, offsets)
  views_tail.go # open/close, key handling, render (new file, mirrors views_dlq.go)
```

## Core API (`internal/tail`)

```go
// Message is one consumed record.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

// Snapshot reads the last limit messages of a topic across all partitions,
// oldest-first within each partition, merged chronologically by timestamp
// (offset as tiebreaker). Limits the per-partition read to
// ceil(limit/partitions)+1 so the total stays near limit.
func Snapshot(ctx context.Context, brokers []string, topic string, limit int) ([]Message, error)

// Follow polls the topic for new messages past the given offsets and sends
// them on ch until ctx is canceled. Poll interval is a parameter (1s default).
// Offsets maps partition → next offset to read; nil starts from the current
// high-watermarks (only new arrivals).
func Follow(ctx context.Context, brokers []string, topic string, offsets map[int]int64,
	interval time.Duration, ch chan<- Message) error

// DisplayValue renders a payload for the terminal: text as-is (trimmed to
// maxBytes), binary as hex, truncated with a "+N more bytes" marker.
// Reuses the same convention as dlq.DisplayValue.
func DisplayValue(v []byte, maxBytes int) string
```

### Reading mechanics (no consumer group)

- **Snapshot:** per partition, `DialLeader` + `Seek(SeekAbsolute, hw-remaining)` + `ReadBatch` (the same approach `dlq.Inspect` uses — kafka-go v0.4.47 has no `ReadAt`/`ReadLastOffsets`), reading `ceil(limit/partitions)+1` messages, then merging chronologically.
- **Follow:** every `interval` (default 1s), re-read each partition from `offsets[p]`; a lightweight `kafka.Conn` per partition (`DialLeader`, `Seek` to the saved offset, `ReadBatch` with a short `MaxWait`/`MaxBytes`), updating `offsets[p]` after each successful read. The offsets map is the only state — no group, no commits.
- If a partition's leader changes between polls, re-dial handles it transparently.
- **Polling vs long-lived Reader:** polling with explicit offsets is chosen over a persistent `kafka.Reader` because (a) it gives pause/resume for free (just stop polling), (b) it survives broker restarts without rebalance machinery, and (c) it never creates a consumer group.

## TUI integration

### State (model.go)

```go
	tailTopic      string          // "" = tail view closed
	tailMessages   []tail.Message  // rolling buffer, capped at 200
	tailOffsets    map[int]int64   // next offset per partition
	tailPaused     bool
	tailView       *viewport.Model
	tailErr        string          // last follow error, shown in the view
```

### Data flow

```
Topics tab, Enter on row ─► openTailView(topic)
  ├─ tailView = viewport (full width/height minus chrome)
  ├─ cmd 1: tail.Snapshot(ctx, brokers, topic, 50)  → tailSnapshotMsg
  │         on success: buffer + render, seed tailOffsets
  └─ cmd 2: tail.Tick(ctx, topic, 1s)               → tailTickMsg (loops until ctx canceled)
                    │
                    └─ per tick (when !tailPaused): tail.Follow increment
                       new messages append to buffer (cap 200), GotoBottom if pinned
```

- `tailSnapshotMsg{msgs, err}` / `tailTickMsg{}` messages handled in `Update`; follow errors land in `tailErr` (displayed, follow continues).
- Follow uses `context.WithCancel` stored in the model; `closeTailView()` cancels it (no goroutine leaks — same discipline as the daemon loops).

### Keys (mirrors the DLQ view)

| Key | Action |
|-----|--------|
| `esc` / `q` | close the tail view (cancel follow) |
| `j` / `k` / `up` / `down` | scroll the viewport |
| `p` | pause / resume live follow |
| `g` | jump to bottom (re-pin auto-scroll) |

Auto-scroll: while pinned (default after open), new messages trigger `GotoBottom`; any manual `k`/`up` unpins; `g` re-pins.

### Message line format

```
[p 2|o 1243|12:34:56.789] key="order-42" value={"id":"ord_42","amount":137}
```

- `[p <part>|o <offset>|HH:MM:SS.mmm]` prefix (timestamp UTC), key rendered quoted (omitted when empty), value via `DisplayValue(..., 200)` — binary payloads as hex. Header count shown when non-empty: `(2 headers)`.

### Help text

Topics tab hint line gains: `ENTER: tail  │  p: pause  │  g: bottom`. Global help stays unchanged (tab-scoped key).

## Failure modes

- **Topic deleted mid-tail** → next poll errors; `tailErr` shows it, follow stops, view stays readable (Esc to close).
- **Broker down mid-tail** → poll errors logged to `tailErr`; retries each tick (no exit).
- **High-volume topic** → buffer cap 200 keeps memory bounded; the view scrolls to the newest when pinned.
- **Huge payloads** → `DisplayValue` truncation (200 bytes default).
- **Snapshot with 0 partitions / missing topic** → empty view + "no messages" state (not an error).
- **TUI quit while tailing** → ctx cancel via `closeTailView` in the quit path; no goroutine leaks (verified with `-race`).

## Testing

- **Unit (tail package):** offset math for per-partition remaining reads; chronological merge (timestamp tiebreak by offset); `DisplayValue` (text/binary/truncation) golden cases; `Follow` progress semantics with a fake reader interface (inject a `reader` func map so no broker needed).
- **Integration (docker broker, skip-without-broker pattern):** produce 60 messages to a scratch topic → `Snapshot(limit 50)` returns 50, newest last; produce 3 more → `Follow` with the returned offsets delivers exactly the 3 new messages; second `Follow` call with nil offsets delivers only future messages.
- **TUI:** Enter on a topics-table row opens the view (`tailTopic` set, cmd non-nil); `tailSnapshotMsg` populates the viewport; `p` toggles pause (no follow on tick while paused); `esc`/`q` close and cancel ctx; buffer cap enforced (simulate 250 messages → 200 retained); render contains topic name + message lines; no goroutine leaks (`-race` + goroutine-count test reusing the `TestTransportGoroutinesReleasedOnClose` pattern).

## Follow-ups (not in v1)

- Partition filter key (narrow the tail to one partition).
- Key/value substring filter.
- `--follow` mode for the `dlq inspect` CLI (shares `tail.Follow`).
- Copy message to clipboard / jump to offset in DLQ view.
