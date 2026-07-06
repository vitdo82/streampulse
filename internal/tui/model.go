// Package tui provides the bubbletea terminal UI for StreamPulse.
// This is a k9s-style interactive dashboard for Kafka observability.
package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ─── Styles ────────────────────────────────────────────────────────────────

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#7C3AED")).
			Padding(0, 1)

	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#A78BFA")).
			Background(lipgloss.Color("#1F1A2E")).
			Padding(0, 1)

	statusOK = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#22C55E")).Render("●")

	statusWarn = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#EAB308")).Render("●")

	statusCrit = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#EF4444")).Render("●")

	tabStyle = lipgloss.NewStyle().
			Padding(0, 2).
			Foreground(lipgloss.Color("#6B7280"))

	activeTabStyle = lipgloss.NewStyle().
			Padding(0, 2).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(lipgloss.Color("#7C3AED")).
			Bold(true)

	helpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#6B7280"))
)

// ─── Model ─────────────────────────────────────────────────────────────────

type tickMsg time.Time

// Model is the bubbletea model for the StreamPulse TUI.
type Model struct {
	width  int
	height int

	activeTab int
	tabs      []string

	// Tables
	brokersTable  table.Model
	topicsTable   table.Model
	groupsTable   table.Model
	alertsTable   table.Model
	dlqTable      table.Model

	// Log viewport
	logs      []string
	logView   viewport.Model

	ready bool
}

// NewModel creates a new TUI model with mock data for demonstration.
func NewModel() *Model {
	m := &Model{
		tabs: []string{"📊 Overview", "📨 Topics", "👥 Consumers", "🔔 Alerts", "📂 DLQ", "📈 Analytics"},
	}

	// Brokers table
	m.brokersTable = newTable(
		[]table.Column{
			{Title: "BROKER", Width: 16},
			{Title: "STATUS", Width: 8},
			{Title: "CPU", Width: 8},
			{Title: "MEM", Width: 8},
			{Title: "MSGS/S", Width: 10},
			{Title: "BYTES/S", Width: 12},
		},
		[]table.Row{
			{"broker-1", statusOK + " OK", "34%", "62%", "42.1k", "12.4 MB/s"},
			{"broker-2", statusOK + " OK", "28%", "58%", "38.7k", "11.8 MB/s"},
			{"broker-3", statusWarn + " WARN", "72%", "91%", "28.4k", "8.2 MB/s"},
		},
	)

	// Topics table
	m.topicsTable = newTable(
		[]table.Column{
			{Title: "TOPIC", Width: 20},
			{Title: "PARTITIONS", Width: 12},
			{Title: "MSG/S", Width: 10},
			{Title: "BYTES/S", Width: 12},
			{Title: "RETENTION", Width: 12},
		},
		[]table.Row{
			{"orders", "12", "14.2k", "4.7 MB/s", "7d"},
			{"payments", "6", "8.4k", "2.1 MB/s", "30d"},
			{"inventory", "4", "3.2k", "0.8 MB/s", "7d"},
			{"audit", "8", "22.1k", "6.8 MB/s", "90d"},
			{"shipping", "3", "1.1k", "0.3 MB/s", "7d"},
		},
	)

	// Consumer groups table
	m.groupsTable = newTable(
		[]table.Column{
			{Title: "GROUP", Width: 22},
			{Title: "LAG", Width: 10},
			{Title: "STATUS", Width: 10},
			{Title: "MEMBERS", Width: 10},
			{Title: "TOPIC", Width: 14},
		},
		[]table.Row{
			{"payment-processor", "0", statusOK + " OK", "3", "payments"},
			{"inventory-indexer", "147", statusWarn + " WARN", "2", "inventory"},
			{"audit-consumer", "8.2k", statusCrit + " CRIT", "4", "audit"},
			{"orders-matcher", "0", statusOK + " OK", "2", "orders"},
			{"shipping-svc", "12", statusOK + " OK", "1", "shipping"},
		},
	)

	// Alerts table
	m.alertsTable = newTable(
		[]table.Column{
			{Title: "ALERT", Width: 30},
			{Title: "SEVERITY", Width: 10},
			{Title: "VALUE", Width: 12},
			{Title: "FIRED", Width: 10},
		},
		[]table.Row{
			{"under-replicated-partitions", "🔴 CRITICAL", "14 partitions", "3m ago"},
			{"consumer-slowing-down", "🟡 WARNING", "147 lag", "12m ago"},
		},
	)

	// DLQ table
	m.dlqTable = newTable(
		[]table.Column{
			{Title: "DLQ TOPIC", Width: 22},
			{Title: "MESSAGES", Width: 12},
			{Title: "GROWTH", Width: 10},
			{Title: "ERROR PATTERN", Width: 30},
		},
		[]table.Row{
			{"payments.dlq", "28,402", "+340/min", "DB Connection timeout (99%)"},
			{"orders.dlq", "1,247", "+12/min", "Deserialization error (89%)"},
			{"inventory.dlq", "892", "0/min", "NullPointerException (100%)"},
		},
	)

	m.logView = viewport.New(0, 0)

	return m
}

func newTable(cols []table.Column, rows []table.Row) table.Model {
	t := table.New(
		table.WithColumns(cols),
		table.WithRows(rows),
		table.WithFocused(true),
		table.WithHeight(len(rows)+1),
	)

	s := table.DefaultStyles()
	s.Header = headerStyle
	s.Selected = lipgloss.NewStyle().
		Background(lipgloss.Color("#4C1D95")).
		Foreground(lipgloss.Color("#FFFFFF"))
	t.SetStyles(s)

	return t
}

// ─── Init ──────────────────────────────────────────────────────────────────

func (m *Model) Init() tea.Cmd {
	return tea.Batch(tickCmd(), tea.EnterAltScreen)
}

func tickCmd() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// ─── Update ────────────────────────────────────────────────────────────────

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

		if !m.ready {
			m.ready = true
			m.logView = viewport.New(msg.Width, 6)
		}

	case tickMsg:
		m.logs = append(m.logs, fmt.Sprintf("[%s] scrape: 3 brokers, 47 topics, 26 partitions",
			time.Now().Format("15:04:05")))
		if len(m.logs) > 50 {
			m.logs = m.logs[len(m.logs)-50:]
		}
		m.logView.SetContent(strings.Join(m.logs, "\n"))
		m.logView.GotoBottom()
		cmds = append(cmds, tickCmd())

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "tab", "l", "right":
			m.activeTab = (m.activeTab + 1) % len(m.tabs)
		case "shift+tab", "h", "left":
			m.activeTab = (m.activeTab - 1 + len(m.tabs)) % len(m.tabs)
		case "1", "2", "3", "4", "5", "6":
			m.activeTab = int(msg.Runes[0] - '1')
		}
	}

	return m, tea.Batch(cmds...)
}

// ─── View ──────────────────────────────────────────────────────────────────

func (m *Model) View() string {
	if !m.ready {
		return "Loading..."
	}

	return lipgloss.JoinVertical(lipgloss.Left,
		m.renderHeader(),
		m.renderTabs(),
		m.renderContent(),
		m.renderHelp(),
	)
}

func (m *Model) renderHeader() string {
	left := titleStyle.Render("⚡ StreamPulse v0.1.0")
	right := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#6B7280")).
		Render(fmt.Sprintf("Cluster: prod-kafka  │  Brokers: 3  │  Topics: 47  │  Consumers: 12  │  %s",
			time.Now().Format("15:04:05")))

	bar := lipgloss.NewStyle().
		Background(lipgloss.Color("#1F1A2E")).
		Width(m.width).
		Padding(0, 2).
		Render(lipgloss.JoinHorizontal(lipgloss.Left, left, strings.Repeat(" ", m.width-lipgloss.Width(left)-lipgloss.Width(right)-4), right))

	return bar
}

func (m *Model) renderTabs() string {
	var tabs []string
	for i, t := range m.tabs {
		if i == m.activeTab {
			tabs = append(tabs, activeTabStyle.Render(t))
		} else {
			tabs = append(tabs, tabStyle.Render(t))
		}
	}
	return lipgloss.NewStyle().
		Background(lipgloss.Color("#181425")).
		Width(m.width).
		Render(lipgloss.JoinHorizontal(lipgloss.Left, tabs...))
}

func (m *Model) renderContent() string {
	switch m.activeTab {
	case 0:
		return m.renderOverview()
	case 1:
		return m.renderTopicsView()
	case 2:
		return m.renderConsumersView()
	case 3:
		return m.renderAlertsView()
	case 4:
		return m.renderDLQView()
	case 5:
		return m.renderAnalyticsView()
	default:
		return ""
	}
}

func (m *Model) renderOverview() string {
	// Summary cards
	cardStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#4C1D95")).
		Padding(1, 2).
		Width(28)

	cards := lipgloss.JoinHorizontal(lipgloss.Top,
		cardStyle.Render(
			lipgloss.JoinVertical(lipgloss.Left,
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("BROKERS"),
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#22C55E")).Render("3 healthy"),
				lipgloss.NewStyle().Foreground(lipgloss.Color("#EF4444")).Render("1 warning"),
			),
		),
		cardStyle.Render(
			lipgloss.JoinVertical(lipgloss.Left,
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("TOPICS"),
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFFFF")).Render("47 topics"),
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("312 partitions"),
			),
		),
		cardStyle.Render(
			lipgloss.JoinVertical(lipgloss.Left,
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("ALERTS"),
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#EF4444")).Render("1 critical"),
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#EAB308")).Render("1 warning"),
			),
		),
	)

	// Brokers table
	brokerSection := lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("BROKERS"),
		m.brokersTable.View(),
	)

	// Consumer groups table
	groupSection := lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("CONSUMER GROUPS"),
		m.groupsTable.View(),
	)

	// Logs
	logSection := lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("ACTIVITY LOG"),
		m.logView.View(),
	)

	return lipgloss.JoinVertical(lipgloss.Left,
		cards,
		"",
		brokerSection,
		"",
		groupSection,
		"",
		logSection,
	)
}

func (m *Model) renderTopicsView() string {
	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("TOPICS"),
		m.topicsTable.View(),
	)
}

func (m *Model) renderConsumersView() string {
	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("CONSUMER GROUPS"),
		m.groupsTable.View(),
	)
}

func (m *Model) renderAlertsView() string {
	firing := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#EF4444")).
		Padding(1, 2).
		Width(m.width - 4)

	alertDetail := firing.Render(
		lipgloss.JoinVertical(lipgloss.Left,
			lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#EF4444")).Render("🔴 CRITICAL: under-replicated-partitions"),
			lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("Fired: 3 min ago  │  notified: ✅ Slack #oncall  ✅ PagerDuty"),
			"",
			lipgloss.NewStyle().Foreground(lipgloss.Color("#D1D5DB")).Render("broker-3 down — 14 partitions under-replicated — 2 offline"),
			lipgloss.NewStyle().Foreground(lipgloss.Color("#9CA3AF")).Render("⚠️  Risk: RF=3, only 2 replicas in-sync. If another broker fails → data loss."),
		),
	)

	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("ACTIVE ALERTS"),
		m.alertsTable.View(),
		"",
		alertDetail,
	)
}

func (m *Model) renderDLQView() string {
	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("DEAD LETTER QUEUES"),
		m.dlqTable.View(),
		"",
		lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("  ENTER: inspect  │  R: replay  │  D: drain  │  A: archive"),
	)
}

func (m *Model) renderAnalyticsView() string {
	chartStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#4C1D95")).
		Padding(1, 2).
		Width(m.width - 4).
		Height(12)

	analyticsView := chartStyle.Render(
		lipgloss.JoinVertical(lipgloss.Left,
			lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Render("📈 TOPIC GROWTH: orders (last 7 days)"),
			"",
			lipgloss.NewStyle().Foreground(lipgloss.Color("#D1D5DB")).Render("  Messages/day:  2.1M → 12.4M  (+490%)"),
			lipgloss.NewStyle().Foreground(lipgloss.Color("#D1D5DB")).Render("  Bytes/day:     4.7 GB → 28.1 GB  (+498%)"),
			lipgloss.NewStyle().Foreground(lipgloss.Color("#D1D5DB")).Render("  Partitions:    6 → 12  (repartitioned Tue 3:14 AM)"),
			"",
			lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("  Sparkline:  ▁▂▃▄▅▆▇█▇▆▅▄▃▂▁  (placeholder — real charts in v0.1)"),
			"",
			lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#EAB308")).Render("  🔥 Hot partition: 3  (62% of traffic — key skew detected)"),
		),
	)

	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("ANALYTICS"),
		analyticsView,
	)
}

func (m *Model) renderHelp() string {
	return helpStyle.Render(
		lipgloss.NewStyle().
			Background(lipgloss.Color("#1F1A2E")).
			Width(m.width).
			Padding(0, 2).
			Render(
				"tab/l/r: switch view  │  1-6: jump to tab  │  /: search  │  q: quit  │  ?: help",
			),
	)
}

// Run starts the bubbletea program.
func Run() error {
	m := NewModel()
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}
