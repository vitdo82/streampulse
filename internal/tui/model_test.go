package tui

import (
	"strings"
	"testing"
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

func TestEmptyTablesShowWaitingState(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.buildTables()

	// All tables should show waiting/empty state (text may be truncated by column width)
	brokerView := m.brokersTable.View()
	if !strings.Contains(brokerView, "Waiting for dae") && !strings.Contains(brokerView, "Waiting for daemon") {
		t.Errorf("empty brokers table should show waiting state, got: %s", brokerView)
	}

	alertView := m.alertsTable.View()
	if !strings.Contains(alertView, "No alerts firing") {
		t.Error("empty alerts table should show 'No alerts firing'")
	}
}
