// Package tui provides the bubbletea terminal UI for StreamPulse.
// This is a k9s-style interactive dashboard for Kafka observability.
package tui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/pulsedev/streampulse/internal/kafka"
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

// topicsMsg carries the result of an asynchronous topic fetch.
type topicsMsg struct {
	topics []kafka.TopicInfo
	err    error
}

// clusterInfoMsg carries the result of an asynchronous DescribeCluster fetch.
type clusterInfoMsg struct {
	info *kafka.ClusterInfo
	err  error
}

// groupsMsg carries the result of an asynchronous consumer group fetch.
type groupsMsg struct {
	groups []kafka.GroupInfo
	err    error
}

// Model is the bubbletea model for the StreamPulse TUI.
type Model struct {
	width  int
	height int

	activeTab int
	tabs      []string

	kafkaClient *kafka.Client

	// Tables
	brokersTable table.Model
	topicsTable  table.Model
	groupsTable  table.Model
	alertsTable  table.Model
	dlqTable     table.Model

	// Topic fetch state
	topicsLoading  bool
	topicsErr      error
	topicCount     int
	partitionCount int

	// Cluster fetch state
	clusterLoading bool
	clusterErr     error
	brokerCount    int
	brokers        []kafka.BrokerInfo

	// Consumer group fetch state
	groupsLoading bool
	groupsErr     error
	groupCount    int

	// Log viewport
	logs    []string
	logView viewport.Model

	ready bool
}

// NewModel creates a new TUI model. When client is nil, mock data is used.
func NewModel(client *kafka.Client) *Model {
	m := &Model{
		tabs:        []string{"📊 Overview", "📨 Topics", "👥 Consumers", "🔔 Alerts", "📂 DLQ", "📈 Analytics"},
		kafkaClient: client,
	}

	// Brokers table
	if client != nil {
		m.clusterLoading = true
		m.brokersTable = newTable(
			[]table.Column{
				{Title: "BROKER", Width: 24},
				{Title: "ID", Width: 6},
				{Title: "STATUS", Width: 12},
				{Title: "LEADERS", Width: 8},
				{Title: "REPLICAS", Width: 10},
				{Title: "RACK", Width: 10},
			},
			[]table.Row{{"Loading brokers...", "-", "-", "-", "-", "-"}},
		)
	} else {
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
	}

	// Topics table
	if client != nil {
		m.topicsLoading = true
		m.topicsTable = newTable(
			[]table.Column{
				{Title: "TOPIC", Width: 20},
				{Title: "PARTITIONS", Width: 12},
				{Title: "MSG/S", Width: 10},
				{Title: "BYTES/S", Width: 12},
				{Title: "RETENTION", Width: 12},
			},
			[]table.Row{{"Loading topics...", "-", "-", "-", "-"}},
		)
	} else {
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
	}

	// Consumer groups table
	if client != nil {
		m.groupsLoading = true
		m.groupsTable = newTable(
			[]table.Column{
				{Title: "GROUP", Width: 22},
				{Title: "STATE", Width: 12},
				{Title: "LAG", Width: 10},
				{Title: "MEMBERS", Width: 10},
				{Title: "TOPIC", Width: 14},
			},
			[]table.Row{{"Loading groups...", "-", "-", "-", "-"}},
		)
	} else {
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
	}

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

// newTopicsTable builds the topics table from Kafka metadata.
func (m *Model) newTopicsTable(topics []kafka.TopicInfo) table.Model {
	cols := []table.Column{
		{Title: "TOPIC", Width: 20},
		{Title: "PARTITIONS", Width: 12},
		{Title: "MSG/S", Width: 10},
		{Title: "BYTES/S", Width: 12},
		{Title: "RETENTION", Width: 12},
	}

	if len(topics) == 0 {
		return newTable(cols, []table.Row{{"No topics found", "-", "-", "-", "-"}})
	}

	rows := make([]table.Row, len(topics))
	for i, t := range topics {
		rows[i] = table.Row{t.Name, strconv.Itoa(t.Partitions), "-", "-", "-"}
	}

	return newTable(cols, rows)
}

// newBrokersTable builds the brokers table from Kafka cluster metadata.
func (m *Model) newBrokersTable(info *kafka.ClusterInfo) table.Model {
	cols := []table.Column{
		{Title: "BROKER", Width: 24},
		{Title: "ID", Width: 6},
		{Title: "STATUS", Width: 12},
		{Title: "LEADERS", Width: 8},
		{Title: "REPLICAS", Width: 10},
		{Title: "RACK", Width: 10},
	}

	brokers := info.Brokers
	controllerID := info.ControllerID

	if len(brokers) == 0 {
		return newTable(cols, []table.Row{{"No brokers found", "-", "-", "-", "-", "-"}})
	}

	rows := make([]table.Row, len(brokers))
	for i, b := range brokers {
		addr := fmt.Sprintf("%s:%d", b.Host, b.Port)
		status := statusOK + " UP"
		if b.ID == controllerID {
			status = statusOK + " CONTROLLER"
		}
		rack := b.Rack
		if rack == "" {
			rack = "-"
		}
		rows[i] = table.Row{
			addr,
			strconv.Itoa(b.ID),
			status,
			strconv.Itoa(b.LeaderPartitions),
			strconv.Itoa(b.ReplicaPartitions),
			rack,
		}
	}

	return newTable(cols, rows)
}

// newGroupsTable builds the consumer groups table from Kafka group metadata.
func (m *Model) newGroupsTable(groups []kafka.GroupInfo) table.Model {
	cols := []table.Column{
		{Title: "GROUP", Width: 22},
		{Title: "STATE", Width: 12},
		{Title: "LAG", Width: 10},
		{Title: "MEMBERS", Width: 10},
		{Title: "TOPIC", Width: 14},
	}

	if len(groups) == 0 {
		return newTable(cols, []table.Row{{"No consumer groups found", "-", "-", "-", "-"}})
	}

	rows := make([]table.Row, len(groups))
	for i, g := range groups {
		stateDot := statusOK
		if strings.EqualFold(g.State, "Dead") || strings.EqualFold(g.State, "Empty") {
			stateDot = statusWarn
		}
		rows[i] = table.Row{
			g.Name,
			stateDot + " " + g.State,
			"-",
			strconv.Itoa(g.Members),
			"-",
		}
	}

	return newTable(cols, rows)
}

// ─── Init ──────────────────────────────────────────────────────────────────

func (m *Model) Init() tea.Cmd {
	cmds := []tea.Cmd{tickCmd(), tea.EnterAltScreen}
	if m.kafkaClient != nil {
		cmds = append(cmds,
			fetchTopicsCmd(m.kafkaClient),
			fetchClusterCmd(m.kafkaClient),
			fetchGroupsCmd(m.kafkaClient),
		)
	}
	return tea.Batch(cmds...)
}

func tickCmd() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func fetchTopicsCmd(client *kafka.Client) tea.Cmd {
	return func() tea.Msg {
		topics, err := client.ListTopics(context.Background())
		return topicsMsg{topics: topics, err: err}
	}
}

func fetchClusterCmd(client *kafka.Client) tea.Cmd {
	return func() tea.Msg {
		info, err := client.DescribeCluster(context.Background())
		return clusterInfoMsg{info: info, err: err}
	}
}

func fetchGroupsCmd(client *kafka.Client) tea.Cmd {
	return func() tea.Msg {
		groups, err := client.ListConsumerGroups(context.Background())
		return groupsMsg{groups: groups, err: err}
	}
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
		if m.kafkaClient != nil {
			m.logs = append(m.logs, fmt.Sprintf("[%s] scrape: %d brokers, %d topics, %d groups",
				time.Now().Format("15:04:05"), m.brokerCount, m.topicCount, m.groupCount))
		} else {
			m.logs = append(m.logs, fmt.Sprintf("[%s] scrape: 3 brokers, 47 topics, 26 partitions",
				time.Now().Format("15:04:05")))
		}
		if len(m.logs) > 50 {
			m.logs = m.logs[len(m.logs)-50:]
		}
		m.logView.SetContent(strings.Join(m.logs, "\n"))
		m.logView.GotoBottom()
		cmds = append(cmds, tickCmd())
		if m.kafkaClient != nil {
			cmds = append(cmds,
				fetchTopicsCmd(m.kafkaClient),
				fetchClusterCmd(m.kafkaClient),
				fetchGroupsCmd(m.kafkaClient),
			)
		}

	case topicsMsg:
		m.topicsLoading = false
		m.topicsErr = msg.err
		if msg.err == nil {
			m.topicsTable = m.newTopicsTable(msg.topics)
			m.topicCount = len(msg.topics)
			m.partitionCount = 0
			for _, t := range msg.topics {
				m.partitionCount += t.Partitions
			}
		}

	case clusterInfoMsg:
		m.clusterLoading = false
		m.clusterErr = msg.err
		if msg.err == nil && msg.info != nil {
			m.brokersTable = m.newBrokersTable(msg.info)
			m.brokerCount = msg.info.BrokerCount
			m.brokers = msg.info.Brokers
		}

	case groupsMsg:
		m.groupsLoading = false
		m.groupsErr = msg.err
		if msg.err == nil {
			m.groupsTable = m.newGroupsTable(msg.groups)
			m.groupCount = len(msg.groups)
		}

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

	var clusterInfo string
	if m.kafkaClient != nil {
		clusterInfo = fmt.Sprintf("Kafka connected  │  Brokers: %d  │  Topics: %d  │  Groups: %d  │  %s",
			m.brokerCount, m.topicCount, m.groupCount, time.Now().Format("15:04:05"))
	} else {
		clusterInfo = fmt.Sprintf("Cluster: prod-kafka  │  Brokers: 3  │  Topics: 47  │  Consumers: 12  │  %s",
			time.Now().Format("15:04:05"))
	}

	right := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#6B7280")).
		Render(clusterInfo)

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

	var brokerCard, topicsCard string
	if m.kafkaClient != nil {
		brokerStatus := fmt.Sprintf("%d connected", m.brokerCount)
		brokerCard = cardStyle.Render(
			lipgloss.JoinVertical(lipgloss.Left,
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("BROKERS"),
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#22C55E")).Render(brokerStatus),
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("real-time"),
			),
		)
		topicsCard = cardStyle.Render(
			lipgloss.JoinVertical(lipgloss.Left,
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("TOPICS"),
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFFFF")).Render(
					fmt.Sprintf("%d topics", m.topicCount)),
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render(
					fmt.Sprintf("%d partitions", m.partitionCount)),
			),
		)
	} else {
		brokerCard = cardStyle.Render(
			lipgloss.JoinVertical(lipgloss.Left,
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("BROKERS"),
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#22C55E")).Render("3 healthy"),
				lipgloss.NewStyle().Foreground(lipgloss.Color("#EF4444")).Render("1 warning"),
			),
		)
		topicsCard = cardStyle.Render(
			lipgloss.JoinVertical(lipgloss.Left,
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("TOPICS"),
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFFFF")).Render("47 topics"),
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("312 partitions"),
			),
		)
	}

	cards := lipgloss.JoinHorizontal(lipgloss.Top,
		brokerCard,
		topicsCard,
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
	var status string
	if m.topicsLoading {
		status = lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("  Loading topics...")
	} else if m.topicsErr != nil {
		status = lipgloss.NewStyle().Foreground(lipgloss.Color("#EF4444")).Render("  Error: " + m.topicsErr.Error())
	}

	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("TOPICS"),
		m.topicsTable.View(),
		status,
	)
}

func (m *Model) renderConsumersView() string {
	var status string
	if m.groupsLoading {
		status = lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("  Loading groups...")
	} else if m.groupsErr != nil {
		status = lipgloss.NewStyle().Foreground(lipgloss.Color("#EF4444")).Render("  Error: " + m.groupsErr.Error())
	}

	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("CONSUMER GROUPS"),
		m.groupsTable.View(),
		status,
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

// Run starts the bubbletea program. If client is non-nil, the TUI connects to Kafka.
func Run(client *kafka.Client) error {
	m := NewModel(client)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}
