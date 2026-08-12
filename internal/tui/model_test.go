package tui

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModelInitialization(t *testing.T) {
	m := NewModelWithStore(nil)

	if m.activeTab != 0 {
		t.Errorf("expected activeTab 0, got %d", m.activeTab)
	}

	if len(m.tabs) != 6 {
		t.Errorf("expected 6 tabs, got %d", len(m.tabs))
	}

	expectedTabs := []string{"Overview", "Topics", "Consumers", "Alerts", "DLQ", "Analytics"}
	for i, tab := range expectedTabs {
		if !strings.Contains(m.tabs[i], tab) {
			t.Errorf("tab %d expected to contain %q, got %q", i, tab, m.tabs[i])
		}
	}
}

func TestModelViewWithNoData(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 120
	m.height = 40
	m.ready = true
	m.buildTables()

	view := m.View()
	if !strings.Contains(view, "StreamPulse") {
		t.Error("view should contain title")
	}
	if !strings.Contains(view, "BROKERS") {
		t.Error("overview should contain brokers section")
	}
}

func TestTabSwitching(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.buildTables()

	m.activeTab = 1
	view := m.renderContent()
	if !strings.Contains(view, "TOPICS") {
		t.Error("topics tab should show topics table")
	}

	m.activeTab = 3
	view = m.renderContent()
	if !strings.Contains(view, "ALERTS") {
		t.Error("alerts tab should show active alerts")
	}

	m.activeTab = 4
	view = m.renderContent()
	if !strings.Contains(view, "DEAD LETTER QUEUES") {
		t.Error("dlq tab should show DLQ table")
	}
}

func TestEmptyTablesShowNoDataState(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.buildTables()

	// All tables should show an honest empty state (text may be truncated by column width)
	brokerView := m.brokersTable.View()
	if !strings.Contains(brokerView, "No data") {
		t.Errorf("empty brokers table should show no-data state, got: %s", brokerView)
	}

	alertView := m.alertsTable.View()
	if !strings.Contains(alertView, "No alerts firing") {
		t.Error("empty alerts table should show 'No alerts firing'")
	}
}

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

func TestApplyDataAccumulatesLogs(t *testing.T) {
	m := NewModelWithStore(nil)
	m.applyData(DataUpdated{Logs: []string{"[00:00:01] a"}})
	m.applyData(DataUpdated{Logs: []string{"[00:00:02] b"}})
	if len(m.logs) != 2 || m.logs[0] != "[00:00:01] a" || m.logs[1] != "[00:00:02] b" {
		t.Errorf("logs should accumulate, got %v", m.logs)
	}
}

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

func TestApplyDataPreservesTablesOnError(t *testing.T) {
	m := NewModelWithStore(nil)
	m.applyData(DataUpdated{Brokers: []BrokerRow{{ID: "b1"}}, Topics: []TopicRow{{Name: "t1"}}})
	m.applyData(DataUpdated{Failed: true, Logs: []string{"[00:00:02] kafka error: boom"}})
	if len(m.topics) != 1 || len(m.brokers) != 1 {
		t.Errorf("failed snapshot must not wipe tables, got topics=%d brokers=%d", len(m.topics), len(m.brokers))
	}
	if len(m.logs) != 1 || !strings.Contains(m.logs[0], "kafka error") {
		t.Errorf("logs must still apply on failed snapshot, got %v", m.logs)
	}
}

func TestHeaderShowsLastUpdateTime(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 120
	m.ready = true
	m.buildTables()

	known := time.Date(2026, 8, 10, 12, 34, 56, 0, time.Local)
	m.lastUpdated = known
	if !strings.Contains(m.View(), known.Format("15:04:05")) {
		t.Errorf("header should show data timestamp %q, view: %s", known.Format("15:04:05"), m.View())
	}
}

func TestHeaderShowsPlaceholderBeforeFirstUpdate(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 120
	m.ready = true
	m.buildTables()

	if !strings.Contains(m.View(), "Updated: —") {
		t.Error("header should show em-dash placeholder before first data arrival")
	}
}

func TestFailedFetchDoesNotStampLastUpdated(t *testing.T) {
	m := NewModelWithStore(nil)
	m.applyData(DataUpdated{Brokers: []BrokerRow{{ID: "b1"}}})
	before := m.lastUpdated
	m.applyData(DataUpdated{Failed: true, Logs: []string{"[00:00:02] kafka error"}})
	if !m.lastUpdated.Equal(before) {
		t.Errorf("failed fetch must not change lastUpdated: before=%v after=%v", before, m.lastUpdated)
	}
}

func TestTickGuardPreventsOverlappingRefreshes(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	m.Init()
	if !m.loading {
		t.Error("expected loading=true after Init dispatched first refresh")
	}

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

// ─── Store mode (daemon persistence) ────────────────────────────────────────

func TestLoadDataReadsPersistedMetrics(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	now := time.Now().Add(-30 * time.Second)
	err = store.WriteBatch(context.Background(), []storage.Metric{
		{TS: now, ClusterID: "local-dev", Metric: "kafka.broker.leader_partitions", EntityType: "broker", EntityName: "localhost:9093", Value: 12},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.broker.replica_partitions", EntityType: "broker", EntityName: "localhost:9093", Value: 20},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.topic.partition_count", EntityType: "topic", EntityName: "orders", Value: 6},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.topic.msg_rate", EntityType: "topic", EntityName: "orders", Value: 42.5},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.topic.bytes_rate", EntityType: "topic", EntityName: "orders", Value: 1000},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.group.lag", EntityType: "consumer_group", EntityName: "orders-processor", Value: 1500},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.group.member_count", EntityType: "consumer_group", EntityName: "orders-processor", Value: 3},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.group.state", EntityType: "consumer_group", EntityName: "orders-processor", Value: 1},
	})
	require.NoError(t, err)

	m := NewModelWithStore(store)
	data := m.loadData()

	require.Len(t, data.Brokers, 1)
	b := data.Brokers[0]
	assert.Equal(t, "localhost:9093", b.ID)
	assert.Contains(t, b.CPU, "12")
	assert.Contains(t, b.Memory, "20")

	require.Len(t, data.Topics, 1)
	tp := data.Topics[0]
	assert.Equal(t, "orders", tp.Name)
	assert.Equal(t, 6, tp.Partitions)
	assert.Equal(t, "42.5", tp.MsgRate)
	assert.Equal(t, "1000.0", tp.BytesRate)

	require.Len(t, data.ConsumerGroups, 1)
	g := data.ConsumerGroups[0]
	assert.Equal(t, "orders-processor", g.Group)
	assert.Equal(t, "1500.0", g.Lag)
	assert.Equal(t, 3, g.Members)
	assert.Contains(t, g.Status, "STABLE")

	require.NotEmpty(t, data.Logs)
	assert.False(t, data.Failed)
}

func TestLoadDataReadsPersistedAlertState(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	fired := time.Now().Add(-2 * time.Minute)
	require.NoError(t, store.SaveAlertState(context.Background(), storage.AlertStateRow{
		RuleName: "growth_rate >= 10.5", Status: "ok", LastValue: 2,
	}))
	require.NoError(t, store.SaveAlertState(context.Background(), storage.AlertStateRow{
		RuleName: "lag > 1000", Status: "firing", LastFired: fired, LastValue: 2500, NotifyCount: 3,
	}))

	m := NewModelWithStore(store)
	data := m.loadData()

	// QueryAlertState orders by rule name: growth_rate before lag.
	require.Len(t, data.Alerts, 2)
	g := data.Alerts[0]
	assert.Equal(t, "growth_rate >= 10.5", g.Name)
	assert.Equal(t, "-", g.Severity)
	assert.Equal(t, "2.0", g.Value)
	assert.Equal(t, "-", g.FiredAt)

	l := data.Alerts[1]
	assert.Equal(t, "lag > 1000", l.Name)
	assert.Equal(t, "-", l.Severity)
	assert.Equal(t, "2500.0", l.Value)
	// QueryAlertState normalizes last_fired to UTC.
	assert.Equal(t, fired.UTC().Format("15:04:05"), l.FiredAt)

	assert.False(t, data.Failed)
}

func TestLoadDataStoreOffline(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Close()) // closed store fails Ping

	m := NewModelWithStore(store)
	data := m.loadData()

	assert.True(t, data.Failed, "offline store must mark the snapshot failed")
	require.Len(t, data.Logs, 1)
	assert.Contains(t, data.Logs[0], "store offline")
}

func TestLoadDataEmptyStore(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	m := NewModelWithStore(store)
	data := m.loadData()

	assert.False(t, data.Failed)
	assert.Len(t, data.Brokers, 0)
	assert.Len(t, data.Topics, 0)
	assert.Len(t, data.ConsumerGroups, 0)
	require.NotEmpty(t, data.Logs)
	assert.Contains(t, data.Logs[0], "store connected")
}
