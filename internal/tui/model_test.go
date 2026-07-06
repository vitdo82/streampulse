package tui

import (
	"errors"
	"strings"
	"testing"

	"github.com/pulsedev/streampulse/internal/kafka"
)

func TestModelInitialization(t *testing.T) {
	m := NewModel(nil)

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

func TestModelView(t *testing.T) {
	m := NewModel(nil)
	m.width = 120
	m.height = 40
	m.ready = true

	view := m.View()
	if !strings.Contains(view, "StreamPulse") {
		t.Error("view should contain title")
	}
	if !strings.Contains(view, "BROKERS") {
		t.Error("overview should contain brokers section")
	}
	if !strings.Contains(view, "CONSUMER GROUPS") {
		t.Error("overview should contain consumer groups section")
	}
}

func TestTabSwitching(t *testing.T) {
	m := NewModel(nil)

	// Switch to Topics tab
	m.activeTab = 1
	view := m.renderContent()
	if !strings.Contains(view, "TOPICS") {
		t.Error("topics tab should show topics table")
	}

	// Switch to Alerts tab
	m.activeTab = 3
	view = m.renderContent()
	if !strings.Contains(view, "ACTIVE ALERTS") {
		t.Error("alerts tab should show active alerts")
	}

	// Switch to DLQ tab
	m.activeTab = 4
	view = m.renderContent()
	if !strings.Contains(view, "DEAD LETTER QUEUES") {
		t.Error("dlq tab should show DLQ table")
	}

	// Switch to Analytics tab
	m.activeTab = 5
	view = m.renderContent()
	if !strings.Contains(view, "TOPIC GROWTH") {
		t.Error("analytics tab should show growth data")
	}
}

func TestTopicsViewWithKafkaClient(t *testing.T) {
	m := NewModel(nil)
	m.kafkaClient = kafka.NewClient([]string{"localhost:9092"}) // non-nil signals Kafka mode
	m.topicsLoading = true
	m.width = 120
	m.height = 40
	m.ready = true

	view := m.renderTopicsView()
	if !strings.Contains(view, "Loading topics") {
		t.Error("topics view should show loading state")
	}

	// Simulate successful topic fetch
	m.Update(topicsMsg{topics: []kafka.TopicInfo{
		{Name: "orders", Partitions: 6},
		{Name: "payments", Partitions: 3},
	}})

	view = m.renderTopicsView()
	if !strings.Contains(view, "orders") {
		t.Error("topics view should contain fetched topic orders")
	}
	if !strings.Contains(view, "payments") {
		t.Error("topics view should contain fetched topic payments")
	}
}

func TestTopicsViewError(t *testing.T) {
	m := NewModel(nil)
	m.kafkaClient = kafka.NewClient([]string{"localhost:9092"})
	m.width = 120
	m.height = 40
	m.ready = true

	m.Update(topicsMsg{err: errors.New("connection refused")})

	view := m.renderTopicsView()
	if !strings.Contains(view, "connection refused") {
		t.Error("topics view should display fetch error")
	}
}

func TestOverviewWithKafkaClient(t *testing.T) {
	m := NewModel(nil)
	m.kafkaClient = kafka.NewClient([]string{"localhost:9092"})
	m.width = 120
	m.height = 40
	m.ready = true

	m.Update(topicsMsg{topics: []kafka.TopicInfo{
		{Name: "orders", Partitions: 6},
		{Name: "payments", Partitions: 3},
	}})
	m.Update(clusterInfoMsg{info: &kafka.ClusterInfo{
		BrokerCount: 1,
		Brokers:     []kafka.BrokerInfo{{Host: "localhost", Port: 9092, ID: 1}},
	}})

	view := m.renderOverview()
	if !strings.Contains(view, "1 connected") {
		t.Error("overview should show connected broker count")
	}
	if !strings.Contains(view, "2 topics") {
		t.Error("overview should show real topic count")
	}
	if !strings.Contains(view, "9 partitions") {
		t.Error("overview should show real partition count")
	}
}

func TestLogWithKafkaClient(t *testing.T) {
	m := NewModel(nil)
	m.kafkaClient = kafka.NewClient([]string{"localhost:9092"})
	m.width = 120
	m.height = 40

	m.Update(topicsMsg{topics: []kafka.TopicInfo{
		{Name: "orders", Partitions: 6},
	}})
	m.Update(clusterInfoMsg{info: &kafka.ClusterInfo{
		BrokerCount: 3,
		Brokers:     nil,
	}})
	m.Update(groupsMsg{groups: []kafka.GroupInfo{
		{Name: "orders-processor", State: "Stable", Members: 3},
	}})

	m.Update(tickMsg{})

	found := false
	for _, entry := range m.logs {
		if strings.Contains(entry, "3 brokers, 1 topics, 1 groups") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("log should show real counts when connected, got: %v", m.logs)
	}
}

func TestBrokersTableFromClusterInfo(t *testing.T) {
	m := NewModel(nil)
	m.kafkaClient = kafka.NewClient([]string{"localhost:9092"})
	m.width = 120
	m.height = 40

	m.Update(clusterInfoMsg{info: &kafka.ClusterInfo{
		BrokerCount:  2,
		ControllerID: 1,
		Brokers: []kafka.BrokerInfo{
			{Host: "broker-a", Port: 9092, ID: 1, Rack: "us-east-1a", LeaderPartitions: 14, ReplicaPartitions: 28},
			{Host: "broker-b", Port: 9092, ID: 2, LeaderPartitions: 6, ReplicaPartitions: 12},
		},
	}})

	view := m.brokersTable.View()
	if !strings.Contains(view, "broker-a:9092") {
		t.Error("brokers table should contain broker-a:9092")
	}
	if !strings.Contains(view, "broker-b:9092") {
		t.Error("brokers table should contain broker-b:9092")
	}
	if !strings.Contains(view, "CONTROLLER") {
		t.Error("brokers table should show CONTROLLER for controller broker")
	}
	if !strings.Contains(view, "us-east-1a") {
		t.Error("brokers table should contain rack info")
	}
	if !strings.Contains(view, "14") {
		t.Error("brokers table should contain leader partition count")
	}
}

func TestConsumerGroupsWhenConnected(t *testing.T) {
	m := NewModel(kafka.NewClient([]string{"localhost:9092"}))
	m.width = 120
	m.height = 40
	m.ready = true

	view := m.renderConsumersView()
	if !strings.Contains(view, "Loading groups") {
		t.Errorf("consumer groups should show loading state when connected, got: %s", view)
	}

	m.Update(groupsMsg{groups: []kafka.GroupInfo{
		{Name: "orders-processor", State: "Stable", Members: 3},
	}})

	view = m.renderConsumersView()
	if !strings.Contains(view, "orders-processor") {
		t.Error("consumer groups table should contain fetched group")
	}
	if !strings.Contains(view, "Stable") {
		t.Error("consumer groups table should contain group state")
	}
}
