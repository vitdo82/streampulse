# StreamPulse Bug & Reliability Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the 4 critical bugs (nil store, activity-log wipe, data race, stale data), add timeouts and broker failover, and replace the hand-rolled raw TCP Kafka protocol with kafka-go's native `ListGroups`/`DescribeGroups` APIs.

**Architecture:** Three packages change. `storage`: `NewStore` returns errors instead of `(nil, nil)`; SQLite writes valid JSON tags and millisecond timestamps. `kafka`: delete ~220 lines of raw TCP protocol code; use `kafka.Client{Transport}` for consumer groups; add multi-broker failover in `dial`. `tui`: refresh goroutines build local log buffers (returned via `DataUpdated.Logs`), model state is only mutated in `Update` (race-free), 5s timeout contexts, in-flight guard against overlapping ticks, and snapshot semantics so stale data clears.

**Tech Stack:** Go 1.25, kafka-go v0.4.47 (verified: `Client.ListGroups` + `Client.DescribeGroups` exist), bubbletea v1.3.4, modernc.org/sqlite, testify.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `internal/storage/store.go` | Fix `NewStore` error handling |
| `internal/storage/sqlite.go` | JSON tags via `encoding/json`, `UnixMilli()` timestamps |
| `internal/storage/store_test.go` | **New** — `NewStore` + SQLite write tests |
| `internal/kafka/client.go` | Delete `listGroupsTCP`/`describeGroupsTCP`; kafka-go Client; `dial` failover; skip errored partitions |
| `internal/kafka/client_test.go` | Failover, no-broker, errored-partition tests; integration tests stay |
| `internal/tui/model.go` | Log pipeline, `loading` guard, timeout ctx, `applyData` snapshot, empty-state text, `lastUpdated` header, double-close fix |
| `internal/tui/model_test.go` | New tests; update empty-state assertions |
| `docs/2026-08-10-bugfix-reliability-plan.md` | **This plan** |

**Out of scope (deferred to separate plans):** roadmap features — `serve`/`check`/`dlq`/`analyze`/`alerts` implementations, storage `Query*`/`Rollup`, viper config wiring, search/DLQ keybindings, terminal-height table scrolling. Tests must pass with `go test -race -count=1 ./...` after every task.

---

### Task 1: `NewStore` returns an error for unimplemented backends

**Files:**
- Modify: `internal/storage/store.go:88-98`
- Create: `internal/storage/store_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/storage/store_test.go`:

```go
package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStoreUnsupportedBackends(t *testing.T) {
	for _, typ := range []string{"postgres", "clickhouse"} {
		store, err := NewStore(typ, "")
		require.Error(t, err, typ)
		assert.Nil(t, store, typ)
	}
}

func TestNewStoreDefaultsToSQLite(t *testing.T) {
	store, err := NewStore("", ":memory:")
	require.NoError(t, err)
	require.NotNil(t, store)
	defer store.Close()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/storage/ -run TestNewStore -v`
Expected: FAIL — `TestNewStoreUnsupportedBackends` gets `(nil, nil)`, `require.Error` fails.

- [ ] **Step 3: Fix `NewStore`**

In `internal/storage/store.go`, replace the `postgres`/`clickhouse` cases (currently `return nil, nil // TODO: v0.2` / `// TODO: v0.3`) with:

```go
	case "postgres":
		return nil, fmt.Errorf("storage type %q not implemented (planned for v0.2)", storeType)
	case "clickhouse":
		return nil, fmt.Errorf("storage type %q not implemented (planned for v0.3)", storeType)
```

`fmt` is already imported.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/storage/ -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add internal/storage/store.go internal/storage/store_test.go
git commit -m "fix: return error for unimplemented storage backends"
```

---

### Task 2: SQLite writes valid JSON tags and millisecond timestamps

**Files:**
- Modify: `internal/storage/sqlite.go:56-88`
- Modify: `internal/storage/store_test.go` (append test)

- [ ] **Step 1: Write the failing test**

Append to `internal/storage/store_test.go`:

```go
func TestSQLiteStoreWriteBatch(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	ts := time.Now().Truncate(time.Millisecond)
	err = s.WriteBatch(context.Background(), []Metric{
		{TS: ts, ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Value: 12.5},
		{TS: ts.Add(5 * time.Second), ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Tags: map[string]string{"a": "b"}, Value: 13.5},
	})
	require.NoError(t, err)

	var count int
	require.NoError(t, s.db.QueryRow(`SELECT COUNT(*) FROM raw_metrics`).Scan(&count))
	assert.Equal(t, 2, count)

	var storedTS int64
	require.NoError(t, s.db.QueryRow(`SELECT ts FROM raw_metrics WHERE value = 12.5`).Scan(&storedTS))
	assert.Equal(t, ts.UnixMilli(), storedTS)

	var tags string
	require.NoError(t, s.db.QueryRow(`SELECT tags FROM raw_metrics WHERE value = 13.5`).Scan(&tags))
	assert.JSONEq(t, `{"a":"b"}`, tags)
}
```

Add imports `context` and `time` to the test file.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/storage/ -run TestSQLiteStoreWriteBatch -v`
Expected: FAIL — `assert.JSONEq` fails (`tags` is `map[a:b]`, not JSON) and `storedTS` is `ts.Unix()` not `ts.UnixMilli()`.

- [ ] **Step 3: Fix `WriteBatch`**

In `internal/storage/sqlite.go`: add `"encoding/json"` to imports; replace lines 73-77:

```go
		tagsJSON := "{}"
		if len(m.Tags) > 0 {
			// Simplified: real implementation uses encoding/json
			tagsJSON = fmt.Sprintf("%v", m.Tags)
		}
```

with:

```go
		tagsJSON := "{}"
		if len(m.Tags) > 0 {
			b, err := json.Marshal(m.Tags)
			if err != nil {
				return fmt.Errorf("marshal tags for %s/%s: %w", m.EntityType, m.EntityName, err)
			}
			tagsJSON = string(b)
		}
```

and change the timestamp in the `INSERT` (line 80) from `m.TS.Unix()` to `m.TS.UnixMilli()`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/storage/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/sqlite.go internal/storage/store_test.go
git commit -m "fix: store tags as JSON and timestamps with millisecond precision"
```

---

### Task 3: Kafka `dial` failover + skip errored partitions

**Files:**
- Modify: `internal/kafka/client.go:382-392` (`dial`), `395-414` (`partitionsToTopics`)
- Modify: `internal/kafka/client_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/kafka/client_test.go`:

```go
func TestDialFailoverTriesAllBrokers(t *testing.T) {
	c := NewClient([]string{"127.0.0.1:1", "127.0.0.1:2"})
	err := c.Ping(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "127.0.0.1:1")
	assert.Contains(t, err.Error(), "127.0.0.1:2")
}

func TestListConsumerGroupsNoBrokers(t *testing.T) {
	c := NewClient(nil)
	_, err := c.ListConsumerGroups(context.Background())
	require.Error(t, err)
}

func TestPartitionsToTopicsSkipsErroredPartitions(t *testing.T) {
	partitions := []kafka.Partition{
		{Topic: "orders", ID: 0, Error: fmt.Errorf("leader not available")},
		{Topic: "orders", ID: 1},
		{Topic: "__consumer_offsets", ID: 0},
	}
	topics := partitionsToTopics(partitions)
	require.Len(t, topics, 1)
	assert.Equal(t, "orders", topics[0].Name)
	assert.Equal(t, 1, topics[0].Partitions)
}
```

Add `"fmt"` and `"github.com/stretchr/testify/require"` to imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/kafka/ -run 'TestDialFailover|TestListConsumerGroupsNoBrokers|TestPartitionsToTopicsSkips' -v`
Expected: FAIL — `dial` only tries `c.brokers[0]` (error mentions one broker); errored partition counted.

- [ ] **Step 3: Fix `dial` and `partitionsToTopics`**

In `internal/kafka/client.go`, add `"errors"` to imports. Replace `dial` (lines 382-392) with:

```go
// dial opens a connection to the first available broker.
func (c *Client) dial(ctx context.Context) (*kafka.Conn, error) {
	if len(c.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}

	dialer := &kafka.Dialer{Timeout: 5 * time.Second}
	var errs []error
	for _, b := range c.brokers {
		conn, err := dialer.DialContext(ctx, "tcp", b)
		if err == nil {
			return conn, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", b, err))
	}
	return nil, fmt.Errorf("dial all brokers: %w", errors.Join(errs...))
}
```

In `partitionsToTopics`, change the filter (line 398) to also skip errored partitions:

```go
		if isInternalTopic(p.Topic) || p.Error != nil {
			continue
		}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/kafka/ -v`
Expected: PASS (integration tests skip if no broker; `TestListTopicsIntegration` may take ~10s per broker timeout).

- [ ] **Step 5: Commit**

```bash
git add internal/kafka/client.go internal/kafka/client_test.go
git commit -m "fix: try all brokers on dial, skip errored partitions"
```

---

### Task 4: Replace raw TCP ListGroups/DescribeGroups with kafka-go Client

**Files:**
- Modify: `internal/kafka/client.go:118-357` (delete `ListConsumerGroups` body, `listGroupsTCP`, `describeGroupsTCP`)

- [ ] **Step 1: Write the failing test**

Append to `internal/kafka/client_test.go`:

```go
func TestListConsumerGroupsIntegration(t *testing.T) {
	broker := os.Getenv("STREAMPULSE_TEST_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}

	client := NewClient([]string{broker})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	groups, err := client.ListConsumerGroups(ctx)
	if err != nil {
		t.Skipf("Kafka not available at %s: %v", broker, err)
	}

	t.Logf("discovered groups: %+v", groups)
}
```

(Replaces the existing `TestListConsumerGroupsIntegration` — delete the old one. Same test body; the point is the new code path must compile and hit the real broker when available.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go vet ./internal/kafka/` — actually, verify the test compiles against the *new* implementation first. This step is ordering-sensitive; do the implementation first as the code is a full rewrite:

- [ ] **Step 3: Rewrite `ListConsumerGroups` and delete the raw TCP functions**

In `internal/kafka/client.go`:

1. Remove imports `"encoding/binary"` and `"io"`; add `"errors"` and `"net"` (already present) — final import list:

```go
import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)
```

2. Replace the entire body of `ListConsumerGroups` (lines 118-135) and delete `listGroupsTCP` (137-219) and `describeGroupsTCP` (221-357). New code:

```go
// ListConsumerGroups returns all consumer groups in the cluster.
// Uses kafka-go's Client (ListGroups + DescribeGroups), which supports
// SASL/TLS via Dialer configuration and multi-broker failover.
func (c *Client) ListConsumerGroups(ctx context.Context) ([]GroupInfo, error) {
	if len(c.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}

	dialer := &kafka.Dialer{Timeout: 5 * time.Second}
	client := &kafka.Client{Transport: &kafka.Transport{Dial: dialer.DialFunc}}

	var errs []error
	for _, b := range c.brokers {
		groups, err := c.groupsFromBroker(ctx, client, kafka.TCP(b))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b, err))
			continue
		}
		return groups, nil
	}
	return nil, fmt.Errorf("all brokers failed: %w", errors.Join(errs...))
}

// groupsFromBroker lists and describes consumer groups through one broker.
func (c *Client) groupsFromBroker(ctx context.Context, client *kafka.Client, addr net.Addr) ([]GroupInfo, error) {
	listResp, err := client.ListGroups(ctx, &kafka.ListGroupsRequest{Addr: addr})
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	if listResp.Error != nil {
		return nil, fmt.Errorf("list groups: %w", listResp.Error)
	}
	if len(listResp.Groups) == 0 {
		return nil, nil
	}

	ids := make([]string, 0, len(listResp.Groups))
	for _, g := range listResp.Groups {
		ids = append(ids, g.GroupID)
	}

	descResp, err := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{Addr: addr, GroupIDs: ids})
	if err != nil {
		return nil, fmt.Errorf("describe groups: %w", err)
	}

	groups := make([]GroupInfo, 0, len(descResp.Groups))
	for _, g := range descResp.Groups {
		if g.Error != nil {
			continue
		}
		groups = append(groups, GroupInfo{Name: g.GroupID, State: g.GroupState, Members: len(g.Members)})
	}
	return groups, nil
}
```

- [ ] **Step 4: Run tests to verify it passes**

Run: `go build ./... && go vet ./... && go test ./internal/kafka/ -v`
Expected: build/vet clean; unit tests PASS; integration tests skip without Kafka or PASS with `docker compose up -d`.

- [ ] **Step 5: Commit**

```bash
git add internal/kafka/client.go internal/kafka/client_test.go
git commit -m "feat: use kafka-go ListGroups/DescribeGroups, drop raw TCP protocol code"
```

---

### Task 5: TUI log pipeline — fix wipe bug and data race

**Files:**
- Modify: `internal/tui/model.go` (`loadData`, `fetchFromKafka`, `applyData`, add `loading`/`lastUpdated` fields)
- Modify: `internal/tui/model_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/tui/model_test.go`:

```go
func TestApplyDataPopulatesLogs(t *testing.T) {
	m := NewModelWithStore(nil)
	m.applyData(DataUpdated{Logs: []string{"[00:00:01] store connected"}})
	if len(m.logs) != 1 || m.logs[0] != "[00:00:01] store connected" {
		t.Errorf("logs not populated by applyData, got %v", m.logs)
	}
}

func TestFetchFromKafkaLogsErrors(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	data := m.fetchFromKafka()
	if len(data.Logs) == 0 {
		t.Fatal("expected error log from unreachable broker")
	}
	if !strings.Contains(data.Logs[0], "kafka error") {
		t.Errorf("expected kafka error log, got %q", data.Logs[0])
	}
}
```

Add `"github.com/pulsedev/streampulse/internal/kafka"` to the test imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/tui/ -run 'TestApplyDataPopulatesLogs|TestFetchFromKafkaLogsErrors' -v`
Expected: FAIL — `applyData` sets `m.logs = d.Logs` but `fetchFromKafka` returns empty `Logs` (the wipe bug).

- [ ] **Step 3: Fix the log pipeline**

In `internal/tui/model.go`:

1. Add fields to `Model` (after `logs []string`, line 130):

```go
	lastUpdated time.Time
	loading     bool
```

2. Replace `loadData` (lines 203-224) with a version that builds a local log buffer:

```go
func (m *Model) loadData() DataUpdated {
	if m.kafkaClient != nil {
		return m.fetchFromKafka()
	}

	if m.store == nil {
		return DataUpdated{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logs := make([]string, 0, 2)
	if err := m.store.Ping(ctx); err != nil {
		logs = append(logs, fmt.Sprintf("[%s] store offline: %v", time.Now().Format("15:04:05"), err))
	} else {
		logs = append(logs, fmt.Sprintf("[%s] store connected", time.Now().Format("15:04:05")))
	}
	return DataUpdated{Logs: logs}
}
```

3. Replace `fetchFromKafka` (lines 226-296) with a version that uses a timeout context and a local log buffer (no model mutation in the goroutine):

```go
func (m *Model) fetchFromKafka() DataUpdated {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := DataUpdated{}
	var logs []string
	logf := func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...)))
		if len(logs) > 50 {
			logs = logs[len(logs)-50:]
		}
	}

	cluster, err := m.kafkaClient.DescribeCluster(ctx)
	if err != nil {
		logf("kafka error: %v", err)
		data.Logs = logs
		return data
	}

	topics, topicErr := m.kafkaClient.ListTopics(ctx)
	groups, groupErr := m.kafkaClient.ListConsumerGroups(ctx)

	topicCount := len(topics)
	partCount := 0
	for _, t := range topics {
		partCount += t.Partitions
	}

	if topicErr != nil {
		logf("topics error: %v", topicErr)
	}
	if groupErr != nil {
		logf("groups error: %v", groupErr)
	}
	logf("scrape: %d brokers, %d topics (%d partitions), %d groups",
		cluster.BrokerCount, topicCount, partCount, len(groups))

	for _, b := range cluster.Brokers {
		status := statusOK + " UP"
		if b.ID == cluster.ControllerID {
			status = statusOK + " CONTROLLER"
		}
		data.Brokers = append(data.Brokers, BrokerRow{
			ID:     fmt.Sprintf("%s:%d", b.Host, b.Port),
			Status: status,
			CPU:    fmt.Sprintf("%d leaders", b.LeaderPartitions),
			Memory: fmt.Sprintf("%d replicas", b.ReplicaPartitions),
			Rate:   b.Rack,
		})
	}

	for _, t := range topics {
		data.Topics = append(data.Topics, TopicRow{
			Name:       t.Name,
			Partitions: t.Partitions,
			MsgRate:    "-",
			BytesRate:  "-",
			Retention:  "-",
		})
	}

	for _, g := range groups {
		data.ConsumerGroups = append(data.ConsumerGroups, ConsumerGroupRow{
			Group:   g.Name,
			Status:  statusOK + " " + g.State,
			Members: g.Members,
			Lag:     "-",
			Topic:   "-",
		})
	}

	data.Logs = logs
	return data
}
```

4. Replace `applyData` (lines 346-363) — snapshot semantics + `lastUpdated`:

```go
func (m *Model) applyData(d DataUpdated) {
	m.brokers = d.Brokers
	m.topics = d.Topics
	m.consumerGroups = d.ConsumerGroups
	m.alerts = d.Alerts
	m.dlqTopics = d.DLQTopics
	m.logs = d.Logs
	m.lastUpdated = time.Now()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/tui/ -race -v`
Expected: PASS — all tests including new ones.

- [ ] **Step 5: Commit**

```bash
git add internal/tui/model.go internal/tui/model_test.go
git commit -m "fix: activity log wiped every tick and data race on model logs"
```

---

### Task 6: TUI stale-data clearing + honest empty states + double close

**Files:**
- Modify: `internal/tui/model.go` (`rowsFrom*` text, quit branch, `runWithStore`)
- Modify: `internal/tui/model_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/tui/model_test.go`:

```go
func TestApplyDataClearsStaleData(t *testing.T) {
	m := NewModelWithStore(nil)
	m.applyData(DataUpdated{Brokers: []BrokerRow{{ID: "b1"}}, Topics: []TopicRow{{Name: "t1"}}})
	if len(m.topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(m.topics))
	}

	m.applyData(DataUpdated{})
	if len(m.topics) != 0 || len(m.brokers) != 0 {
		t.Errorf("empty snapshot should clear stale data, got topics=%d brokers=%d", len(m.topics), len(m.brokers))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/tui/ -run TestApplyDataClearsStaleData -v`
Expected: FAIL — the old `len(d.X) > 0` guards keep stale rows. (Task 5's `applyData` already removed the guards, so this test may pass if Task 5 landed — run it; if it passes, this step confirms the behavior and you may proceed.)

- [ ] **Step 3: Fix empty-state text, quit double-close**

1. In `rowsFromBrokers` (line 440), `rowsFromTopics` (451), `rowsFromConsumerGroups` (462): replace `"Waiting for daemon..."` with `"No data"`.

2. In `Update`, simplify the quit branch (lines 324-328) — remove the store close (double close with `runWithStore`'s defer):

```go
		case "ctrl+c", "q":
			return m, tea.Quit
```

3. Remove the defer from `runWithStore` (line 702) since the model still owns its store:

```go
	m, err := NewModel(storePath)
	if err != nil {
		return fmt.Errorf("create model: %w", err)
	}
	p := tea.NewProgram(m, tea.WithAltScreen())
```

- [ ] **Step 4: Update existing test and verify**

In `TestEmptyTablesShowWaitingState`, update the broker assertion to expect the new text:

```go
	if !strings.Contains(brokerView, "No data") {
		t.Errorf("empty brokers table should show no-data state, got: %s", brokerView)
	}
```

Run: `go test ./internal/tui/ -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/tui/model.go internal/tui/model_test.go
git commit -m "fix: clear stale data on empty snapshots, honest empty states, single store close"
```

---

### Task 7: TUI fetch timeout + in-flight guard against overlapping refreshes

**Files:**
- Modify: `internal/tui/model.go` (`Update` tick + `r` handler)
- Modify: `internal/tui/model_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/tui/model_test.go`:

```go
func TestTickGuardPreventsOverlappingRefreshes(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))

	tm, _ := m.Update(tickMsg(time.Now()))
	m = tm.(*Model)
	if !m.loading {
		t.Error("expected loading=true after tick dispatched refresh")
	}

	tm, _ = m.Update(DataUpdated{})
	m = tm.(*Model)
	if m.loading {
		t.Error("expected loading=false after DataUpdated arrived")
	}

	tm, _ = m.Update(tickMsg(time.Now()))
	m = tm.(*Model)
	if !m.loading {
		t.Error("expected loading=true again after DataUpdated")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/tui/ -run TestTickGuard -v`
Expected: FAIL — `m.loading` field doesn't exist yet (compile error) or stays false.

- [ ] **Step 3: Implement the guard**

In `internal/tui/model.go`, in `Update`:

Replace the `tickMsg` case (lines 313-315):

```go
	case tickMsg:
		if !m.loading {
			m.loading = true
			cmds = append(cmds, m.refreshCmd())
		}
		cmds = append(cmds, tickCmd())

	case DataUpdated:
		m.loading = false
		m.applyData(msg)
		m.buildTables()
```

Replace the `"r"` case (lines 337-340):

```go
		case "r":
			if !m.loading {
				m.loading = true
				cmds = append(cmds, m.refreshCmd())
			}
```

(The 5s timeout contexts were already added in Task 5's `fetchFromKafka`/`loadData`; combined with the guard, a slow broker can no longer pile up connections or freeze the UI.)

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/tui/ -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/tui/model.go internal/tui/model_test.go
git commit -m "fix: timeout Kafka fetches and prevent overlapping refresh goroutines"
```

---

### Task 8: Header shows real data timestamp

**Files:**
- Modify: `internal/tui/model.go` (`renderHeader`)
- Modify: `internal/tui/model_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/tui/model_test.go`:

```go
func TestHeaderShowsLastUpdateTime(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 120
	m.ready = true
	m.buildTables()

	m.applyData(DataUpdated{Logs: []string{"x"}})
	expected := m.lastUpdated.Format("15:04:05")
	if !strings.Contains(m.View(), expected) {
		t.Errorf("header should show data timestamp %q, view: %s", expected, m.View())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/tui/ -run TestHeaderShowsLastUpdateTime -v`
Expected: FAIL — header renders `time.Now()` which may differ by a second from `lastUpdated`.

- [ ] **Step 3: Fix `renderHeader`**

In `internal/tui/model.go` (lines 508-532), replace the status line construction:

```go
	updated := "—"
	if !m.lastUpdated.IsZero() {
		updated = m.lastUpdated.Format("15:04:05")
	}
	status := fmt.Sprintf("Brokers: %d  │  Updated: %s  │  Auto-refresh: 2s",
		brokerCount, updated)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/tui/ -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/tui/model.go internal/tui/model_test.go
git commit -m "fix: header shows last data update time instead of wall clock"
```

---

### Task 9: Full verification

- [ ] **Step 1: Run the full suite**

Run: `go build ./... && go vet ./... && go test -race -count=1 ./...`
Expected: all PASS.

- [ ] **Step 2: Lint (if golangci-lint installed)**

Run: `make lint`
Expected: clean. If golangci-lint is not installed, `go vet` above is the fallback.

- [ ] **Step 3: Manual smoke test (optional, requires Docker)**

```bash
docker compose up -d
make run ARGS="--brokers localhost:9093"
```

Expected: TUI shows brokers/topics/groups; Activity Log shows "scrape: 1 brokers, ..." lines that persist across refreshes; `q` quits cleanly.

- [ ] **Step 4: Commit this plan document**

```bash
git add docs/2026-08-10-bugfix-reliability-plan.md
git commit -m "docs: bugfix and reliability implementation plan"
```

---

## Self-review notes

- **Spec coverage:** Items 1-7 and 11/14/15 from the review map to Tasks 1-8. Items 8-10 (roadmap features), 12-13 (UX features), 16 (minor, folded into Task 3), 17 (folded into Task 2), 18 (tests added per task) — 16/17/18 are covered; 8-10/12-13 explicitly deferred.
- **Type consistency:** `DataUpdated.Logs`, `Model.loading`, `Model.lastUpdated`, `GroupInfo{Name, State, Members}` are consistent across tasks. `kafka.TCP(b)` returns `net.Addr` matching `kafka.ListGroupsRequest.Addr` / `DescribeGroupsRequest.Addr` (verified against kafka-go v0.4.47 source).
- **Task ordering:** Task 5 removes the `len(d.X) > 0` guards, so Task 6's stale-data test may already pass — the plan marks this so the executor doesn't get confused.
