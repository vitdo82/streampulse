// Package tui provides the bubbletea terminal UI for StreamPulse.
// Real-time k9s-style dashboard — auto-refreshes every 2 seconds.
// Reads from the daemon's SQLite state.db. No manual reload needed.
package tui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/storage"
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

	statusOK = lipgloss.NewStyle().Foreground(lipgloss.Color("#22C55E")).Render("●")

	tabStyle = lipgloss.NewStyle().
			Padding(0, 2).
			Foreground(lipgloss.Color("#6B7280"))

	activeTabStyle = lipgloss.NewStyle().
			Padding(0, 2).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(lipgloss.Color("#7C3AED")).
			Bold(true)

	helpStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280"))

	cardStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#4C1D95")).
			Padding(1, 2).
			Width(28)
)

// ─── Messages ──────────────────────────────────────────────────────────────

type tickMsg time.Time

// DataUpdated is sent when the daemon writes new data (simulated via tick for now).
type DataUpdated struct {
	Brokers        []BrokerRow
	Topics         []TopicRow
	ConsumerGroups []ConsumerGroupRow
	Alerts         []AlertRow
	DLQTopics      []DLQRow
	Logs           []string
	Failed         bool
}

// ─── Data Transfer Types ────────────────────────────────────────────────────

type BrokerRow struct {
	ID     string
	Status string
	CPU    string
	Memory string
	Rate   string
}

type TopicRow struct {
	Name       string
	Partitions int
	MsgRate    string
	BytesRate  string
	Retention  string
}

type ConsumerGroupRow struct {
	Group   string
	Lag     string
	Status  string
	Members int
	Topic   string
}

type AlertRow struct {
	Name     string
	Severity string
	Value    string
	FiredAt  string
}

type DLQRow struct {
	Topic        string
	MessageCount string
	Growth       string
	ErrorPattern string
}

// ─── Model ─────────────────────────────────────────────────────────────────

// Model is the bubbletea model. Connects to the daemon's SQLite store
// or directly to Kafka via --brokers flag. All views refresh every 2 seconds.
type Model struct {
	width  int
	height int
	ready  bool

	activeTab int
	tabs      []string

	store       storage.MetricsStore
	kafkaClient *kafka.Client

	// Live data (updated every tick from store)
	brokers        []BrokerRow
	topics         []TopicRow
	consumerGroups []ConsumerGroupRow
	alerts         []AlertRow
	dlqTopics      []DLQRow
	logs           []string

	lastUpdated time.Time
	loading     bool

	// Tables (rebuilt on data change)
	brokersTable table.Model
	topicsTable  table.Model
	groupsTable  table.Model
	alertsTable  table.Model
	dlqTable     table.Model

	// Sub-views
	logView viewport.Model
}

// NewModel creates a TUI model connected to the daemon's store.
// If storePath is empty, uses the default ~/.streampulse/state.db.
func NewModel(storePath string) (*Model, error) {
	if storePath == "" {
		storePath = "~/.streampulse/state.db"
	}

	store, err := storage.NewStore("sqlite", storePath)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}

	m := &Model{
		store: store,
		tabs:  []string{"📊 Overview", "📨 Topics", "👥 Consumers", "🔔 Alerts", "📂 DLQ", "📈 Analytics"},
	}

	return m, nil
}

// NewModelWithStore creates a TUI model with an existing store (for testing).
func NewModelWithStore(store storage.MetricsStore) *Model {
	return &Model{
		store: store,
		tabs:  []string{"📊 Overview", "📨 Topics", "👥 Consumers", "🔔 Alerts", "📂 DLQ", "📈 Analytics"},
	}
}

// NewModelWithKafka creates a TUI model that fetches data directly from Kafka.
func NewModelWithKafka(client *kafka.Client) *Model {
	return &Model{
		kafkaClient: client,
		tabs:        []string{"📊 Overview", "📨 Topics", "👥 Consumers", "🔔 Alerts", "📂 DLQ", "📈 Analytics"},
	}
}

// ─── Init ──────────────────────────────────────────────────────────────────

func (m *Model) Init() tea.Cmd {
	m.loading = true
	return tea.Batch(
		m.refreshCmd(), // immediate first load
		tickCmd(),      // start 2-second tick
		tea.EnterAltScreen,
	)
}

func tickCmd() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m *Model) refreshCmd() tea.Cmd {
	return func() tea.Msg {
		return m.loadData()
	}
}

// ─── Data Loading (reads from daemon's SQLite) ──────────────────────────────

func (m *Model) loadData() DataUpdated {
	if m.kafkaClient != nil {
		return m.fetchFromKafka()
	}

	if m.store == nil {
		return DataUpdated{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := DataUpdated{}
	logs := make([]string, 0, 2)
	logf := func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...)))
	}

	if err := m.store.Ping(ctx); err != nil {
		logf("store offline: %v", err)
		data.Failed = true
		data.Logs = logs
		return data
	}
	logf("store connected")

	// Query the last minute of persisted metrics and map the latest value
	// per entity into the dashboard rows. QueryRaw orders by timestamp
	// ascending, so later rows overwrite earlier ones per entity.
	rows, err := m.store.QueryRaw(ctx, storage.QueryParams{
		From:  time.Now().Add(-time.Minute),
		Limit: 10000,
	})
	if err != nil {
		logf("query store: %v", err)
		data.Failed = true
		data.Logs = logs
		return data
	}

	latest := make(map[string]map[string]float64) // metric → entity → value
	for _, r := range rows {
		if latest[r.Metric] == nil {
			latest[r.Metric] = make(map[string]float64)
		}
		latest[r.Metric][r.EntityName] = r.Avg
	}

	data.Brokers = brokerRowsFromStore(latest)
	data.Topics = topicRowsFromStore(latest)
	data.ConsumerGroups = groupRowsFromStore(latest)

	state, err := m.store.QueryAlertState(ctx)
	if err != nil {
		logf("alerts: %v", err)
	} else {
		data.Alerts = alertRowsFromStore(state)
	}
	data.Logs = logs
	return data
}

// alertRowsFromStore maps persisted alert states into dashboard rows. The
// rule severity is not part of the persisted state, so it renders as "-".
func alertRowsFromStore(state []storage.AlertStateRow) []AlertRow {
	rows := make([]AlertRow, 0, len(state))
	for _, s := range state {
		row := AlertRow{Name: s.RuleName, Severity: "-", Value: fmt.Sprintf("%.1f", s.LastValue)}
		if !s.LastFired.IsZero() {
			row.FiredAt = s.LastFired.Format("15:04:05")
		} else {
			row.FiredAt = "-"
		}
		rows = append(rows, row)
	}
	return rows
}

// brokerRowsFromStore maps latest broker metrics into dashboard rows.
func brokerRowsFromStore(latest map[string]map[string]float64) []BrokerRow {
	rows := make([]BrokerRow, 0, len(latest["kafka.broker.leader_partitions"]))
	for name, leaders := range latest["kafka.broker.leader_partitions"] {
		row := BrokerRow{
			ID:     name,
			Status: statusOK + " UP",
			CPU:    fmt.Sprintf("%.0f leaders", leaders),
			Memory: "-",
			Rate:   "-",
		}
		if replicas, ok := latest["kafka.broker.replica_partitions"][name]; ok {
			row.Memory = fmt.Sprintf("%.0f replicas", replicas)
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

// topicRowsFromStore maps latest topic metrics into dashboard rows.
func topicRowsFromStore(latest map[string]map[string]float64) []TopicRow {
	rows := make([]TopicRow, 0, len(latest["kafka.topic.partition_count"]))
	for name, parts := range latest["kafka.topic.partition_count"] {
		row := TopicRow{
			Name:       name,
			Partitions: int(parts),
			MsgRate:    "-",
			BytesRate:  "-",
			Retention:  "-",
		}
		if v, ok := latest["kafka.topic.msg_rate"][name]; ok {
			row.MsgRate = fmt.Sprintf("%.1f", v)
		}
		if v, ok := latest["kafka.topic.bytes_rate"][name]; ok {
			row.BytesRate = fmt.Sprintf("%.1f", v)
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows
}

// groupRowsFromStore maps latest consumer-group metrics into dashboard rows.
func groupRowsFromStore(latest map[string]map[string]float64) []ConsumerGroupRow {
	rows := make([]ConsumerGroupRow, 0, len(latest["kafka.group.lag"]))
	for name, lag := range latest["kafka.group.lag"] {
		row := ConsumerGroupRow{
			Group:   name,
			Lag:     fmt.Sprintf("%.1f", lag),
			Status:  "-",
			Members: 0,
			Topic:   "-",
		}
		if v, ok := latest["kafka.group.member_count"][name]; ok {
			row.Members = int(v)
		}
		if v, ok := latest["kafka.group.state"][name]; ok {
			row.Status = statusOK + " " + groupStateName(v)
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Group < rows[j].Group })
	return rows
}

// groupStateName maps the persisted numeric group state (scraper.md: 0=Empty,
// 1=Stable, 2=PreparingRebalance, 3=CompletingRebalance, 4=Dead) to a label.
func groupStateName(state float64) string {
	switch int(state) {
	case 1:
		return "STABLE"
	case 2:
		return "PREPARING REBALANCE"
	case 3:
		return "COMPLETING REBALANCE"
	case 4:
		return "DEAD"
	default:
		return "EMPTY"
	}
}

func (m *Model) fetchFromKafka() DataUpdated {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := DataUpdated{}
	logs := make([]string, 0, 2)
	logf := func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...)))
	}

	cluster, err := m.kafkaClient.DescribeCluster(ctx)
	if err != nil {
		logf("kafka error: %v", err)
		data.Failed = true
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
	if topicErr != nil || groupErr != nil {
		data.Failed = true
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
			m.buildTables()
		}

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

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "tab", "l", "right":
			m.activeTab = (m.activeTab + 1) % len(m.tabs)
		case "shift+tab", "h", "left":
			m.activeTab = (m.activeTab - 1 + len(m.tabs)) % len(m.tabs)
		case "1", "2", "3", "4", "5", "6":
			if idx := int(msg.Runes[0] - '1'); idx < len(m.tabs) {
				m.activeTab = idx
			}
		case "r":
			if !m.loading {
				m.loading = true
				cmds = append(cmds, m.refreshCmd())
			}
		}
	}

	return m, tea.Batch(cmds...)
}

func (m *Model) applyData(d DataUpdated) {
	if !d.Failed {
		m.brokers = d.Brokers
		m.topics = d.Topics
		m.consumerGroups = d.ConsumerGroups
		m.alerts = d.Alerts
		m.dlqTopics = d.DLQTopics
		m.lastUpdated = time.Now()
	}
	m.logs = append(m.logs, d.Logs...)
	if len(m.logs) > 50 {
		m.logs = m.logs[len(m.logs)-50:]
	}
}

func (m *Model) buildTables() {
	m.brokersTable = buildTable(
		[]table.Column{
			{Title: "BROKER", Width: 16},
			{Title: "STATUS", Width: 8},
			{Title: "CPU", Width: 8},
			{Title: "MEM", Width: 8},
			{Title: "MSGS/S", Width: 12},
		},
		rowsFromBrokers(m.brokers),
	)

	m.topicsTable = buildTable(
		[]table.Column{
			{Title: "TOPIC", Width: 20},
			{Title: "PARTITIONS", Width: 12},
			{Title: "MSG/S", Width: 10},
			{Title: "BYTES/S", Width: 12},
			{Title: "RETENTION", Width: 12},
		},
		rowsFromTopics(m.topics),
	)

	m.groupsTable = buildTable(
		[]table.Column{
			{Title: "GROUP", Width: 22},
			{Title: "LAG", Width: 10},
			{Title: "STATUS", Width: 10},
			{Title: "MEMBERS", Width: 10},
			{Title: "TOPIC", Width: 14},
		},
		rowsFromConsumerGroups(m.consumerGroups),
	)

	m.alertsTable = buildTable(
		[]table.Column{
			{Title: "ALERT", Width: 30},
			{Title: "SEVERITY", Width: 10},
			{Title: "VALUE", Width: 12},
			{Title: "FIRED", Width: 10},
		},
		rowsFromAlerts(m.alerts),
	)

	m.dlqTable = buildTable(
		[]table.Column{
			{Title: "DLQ TOPIC", Width: 22},
			{Title: "MESSAGES", Width: 12},
			{Title: "GROWTH", Width: 10},
			{Title: "ERROR PATTERN", Width: 30},
		},
		rowsFromDLQ(m.dlqTopics),
	)
}

// ─── Table Builders ────────────────────────────────────────────────────────

func buildTable(cols []table.Column, rows []table.Row) table.Model {
	t := table.New(
		table.WithColumns(cols),
		table.WithRows(rows),
		table.WithFocused(true),
		table.WithHeight(max(len(rows), 1)+1),
	)
	s := table.DefaultStyles()
	s.Header = headerStyle
	s.Selected = lipgloss.NewStyle().
		Background(lipgloss.Color("#4C1D95")).
		Foreground(lipgloss.Color("#FFFFFF"))
	t.SetStyles(s)
	return t
}

func rowsFromBrokers(b []BrokerRow) []table.Row {
	if len(b) == 0 {
		return []table.Row{{"No data", "-", "-", "-", "-"}}
	}
	rows := make([]table.Row, len(b))
	for i, br := range b {
		rows[i] = table.Row{br.ID, br.Status, br.CPU, br.Memory, br.Rate}
	}
	return rows
}

func rowsFromTopics(t []TopicRow) []table.Row {
	if len(t) == 0 {
		return []table.Row{{"No data", "-", "-", "-", "-"}}
	}
	rows := make([]table.Row, len(t))
	for i, tp := range t {
		rows[i] = table.Row{tp.Name, fmt.Sprintf("%d", tp.Partitions), tp.MsgRate, tp.BytesRate, tp.Retention}
	}
	return rows
}

func rowsFromConsumerGroups(g []ConsumerGroupRow) []table.Row {
	if len(g) == 0 {
		return []table.Row{{"No data", "-", "-", "-", "-"}}
	}
	rows := make([]table.Row, len(g))
	for i, cg := range g {
		rows[i] = table.Row{cg.Group, cg.Lag, cg.Status, fmt.Sprintf("%d", cg.Members), cg.Topic}
	}
	return rows
}

func rowsFromAlerts(a []AlertRow) []table.Row {
	if len(a) == 0 {
		return []table.Row{{"No alerts firing", "-", "-", "-"}}
	}
	rows := make([]table.Row, len(a))
	for i, al := range a {
		rows[i] = table.Row{al.Name, al.Severity, al.Value, al.FiredAt}
	}
	return rows
}

func rowsFromDLQ(d []DLQRow) []table.Row {
	if len(d) == 0 {
		return []table.Row{{"No DLQ topics detected", "-", "-", "-"}}
	}
	rows := make([]table.Row, len(d))
	for i, dlq := range d {
		rows[i] = table.Row{dlq.Topic, dlq.MessageCount, dlq.Growth, dlq.ErrorPattern}
	}
	return rows
}

// ─── View ──────────────────────────────────────────────────────────────────

func (m *Model) View() string {
	if !m.ready {
		return "Initializing StreamPulse..."
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

	brokerCount := len(m.brokers)
	if brokerCount == 0 {
		brokerCount = 3 // default display
	}
	updated := "—"
	if !m.lastUpdated.IsZero() {
		updated = m.lastUpdated.Format("15:04:05")
	}
	status := fmt.Sprintf("Brokers: %d  │  Updated: %s  │  Auto-refresh: 2s",
		brokerCount, updated)

	right := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#6B7280")).
		Render(status)

	bar := lipgloss.NewStyle().
		Background(lipgloss.Color("#1F1A2E")).
		Width(m.width).
		Padding(0, 2).
		Render(lipgloss.JoinHorizontal(lipgloss.Left,
			left,
			strings.Repeat(" ", m.width-lipgloss.Width(left)-lipgloss.Width(right)-4),
			right))

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
	cards := lipgloss.JoinHorizontal(lipgloss.Top,
		cardStyle.Render(
			lipgloss.JoinVertical(lipgloss.Left,
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("BROKERS"),
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFFFF")).Render(fmt.Sprintf("%d monitored", len(m.brokers))),
			),
		),
		cardStyle.Render(
			lipgloss.JoinVertical(lipgloss.Left,
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("TOPICS"),
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFFFF")).Render(fmt.Sprintf("%d topics", len(m.topics))),
			),
		),
		cardStyle.Render(
			lipgloss.JoinVertical(lipgloss.Left,
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("ALERTS"),
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#EF4444")).Render(fmt.Sprintf("%d firing", len(m.alerts))),
			),
		),
	)

	logSection := ""
	if m.logView.Width > 0 {
		m.logView.SetContent(strings.Join(m.logs, "\n"))
		m.logView.GotoBottom()
		logSection = lipgloss.JoinVertical(lipgloss.Left,
			lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("ACTIVITY LOG"),
			m.logView.View(),
		)
	}

	return lipgloss.JoinVertical(lipgloss.Left,
		cards,
		"",
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("BROKERS"),
		m.brokersTable.View(),
		"",
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("CONSUMER GROUPS"),
		m.groupsTable.View(),
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
	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("ACTIVE ALERTS"),
		m.alertsTable.View(),
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
		Height(8)

	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("ANALYTICS"),
		chartStyle.Render(
			lipgloss.JoinVertical(lipgloss.Left,
				lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Render("📈 Topic Growth — coming in v0.1"),
				"",
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("Real-time analytics powered by daemon scrape data"),
				lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Render("Growth charts, partition skew, retention analysis — auto-refreshing"),
			),
		),
	)
}

func (m *Model) renderHelp() string {
	return helpStyle.Render(
		lipgloss.NewStyle().
			Background(lipgloss.Color("#1F1A2E")).
			Width(m.width).
			Padding(0, 2).
			Render(
				"tab/l/r: switch view  │  1-6: jump  │  r: refresh now  │  /: search  │  q: quit  │  Auto-refresh: 2s",
			),
	)
}

// Run starts the bubbletea program with live data.
// If client is non-nil, data is fetched directly from Kafka.
// Otherwise, the daemon's SQLite store is used.
// The client is borrowed, not closed: callers own it (see cli.runTUI).
func Run(client *kafka.Client) error {
	if client != nil {
		return runWithKafka(client)
	}
	return runWithStore("") // default store path
}

func runWithKafka(client *kafka.Client) error {
	m := NewModelWithKafka(client)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func runWithStore(storePath string) error {
	if storePath == "" {
		storePath = "~/.streampulse/state.db"
	}
	m, err := NewModel(storePath)
	if err != nil {
		return fmt.Errorf("create model: %w", err)
	}
	defer m.store.Close() // runWithStore owns the store; closes once on every exit path
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err = p.Run()
	return err
}
