package tui

import (
	"strings"
	"testing"
)

func TestModelInitialization(t *testing.T) {
	m := NewModel()

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
	m := NewModel()
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
	m := NewModel()

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
