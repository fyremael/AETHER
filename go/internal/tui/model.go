package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/fyremael/aether/go/internal/client"
)

type DataSource interface {
	Health(context.Context) (*client.HealthResponse, error)
	Status(context.Context) (*client.ServiceStatusResponse, error)
	History(context.Context) (*client.HistoryResponse, error)
	AuditLog(context.Context) (*client.AuditLogResponse, error)
	CoordinationPilotReport(context.Context, *client.PolicyContext) (*client.CoordinationPilotReport, error)
	CoordinationDeltaReport(context.Context, client.CoordinationCut, client.CoordinationCut, *client.PolicyContext) (*client.CoordinationDeltaReport, error)
	ExplainTupleWithPolicy(context.Context, uint64, *client.PolicyContext) (*client.ExplainTupleResponse, error)
}

type Tab int

const (
	OverviewTab Tab = iota
	CoordinationTab
	DeltaTab
	AuditTab
	HistoryTab
	ExplainTab
)

func (t Tab) Title() string {
	switch t {
	case OverviewTab:
		return "Overview"
	case CoordinationTab:
		return "Coordination"
	case DeltaTab:
		return "Delta"
	case AuditTab:
		return "Audit"
	case HistoryTab:
		return "History"
	case ExplainTab:
		return "Explain"
	default:
		return "Unknown"
	}
}

type CoordinationSection int

const (
	CurrentAuthorizedSection CoordinationSection = iota
	ClaimableSection
	LiveHeartbeatsSection
	AcceptedOutcomesSection
	RejectedOutcomesSection
)

func (s CoordinationSection) Title() string {
	switch s {
	case CurrentAuthorizedSection:
		return "Current Authorized"
	case ClaimableSection:
		return "Claimable"
	case LiveHeartbeatsSection:
		return "Live Heartbeats"
	case AcceptedOutcomesSection:
		return "Accepted Outcomes"
	case RejectedOutcomesSection:
		return "Rejected Outcomes"
	default:
		return "Unknown"
	}
}

type DeltaPreset int

const (
	AuthorizedHandoffPreset DeltaPreset = iota
	PreHeartbeatToCurrentPreset
)

func (p DeltaPreset) Title() string {
	switch p {
	case PreHeartbeatToCurrentPreset:
		return "AsOf(e5) -> Current"
	default:
		return "AsOf(e9) -> Current"
	}
}

func (p DeltaPreset) Cuts() (client.CoordinationCut, client.CoordinationCut) {
	switch p {
	case PreHeartbeatToCurrentPreset:
		return client.AsOfCut(5), client.CurrentCut()
	default:
		return client.AsOfCut(9), client.CurrentCut()
	}
}

type keyMap struct {
	nextTab     key.Binding
	prevTab     key.Binding
	tabOne      key.Binding
	tabTwo      key.Binding
	tabThree    key.Binding
	tabFour     key.Binding
	tabFive     key.Binding
	tabSix      key.Binding
	up          key.Binding
	down        key.Binding
	sectionPrev key.Binding
	sectionNext key.Binding
	open        key.Binding
	refresh     key.Binding
	help        key.Binding
	quit        key.Binding
}

func newKeyMap() keyMap {
	return keyMap{
		nextTab:     key.NewBinding(key.WithKeys("tab"), key.WithHelp("tab", "next tab")),
		prevTab:     key.NewBinding(key.WithKeys("shift+tab"), key.WithHelp("shift+tab", "prev tab")),
		tabOne:      key.NewBinding(key.WithKeys("1"), key.WithHelp("1-5", "jump tab")),
		tabTwo:      key.NewBinding(key.WithKeys("2"), key.WithHelp("", "")),
		tabThree:    key.NewBinding(key.WithKeys("3"), key.WithHelp("", "")),
		tabFour:     key.NewBinding(key.WithKeys("4"), key.WithHelp("", "")),
		tabFive:     key.NewBinding(key.WithKeys("5"), key.WithHelp("", "")),
		tabSix:      key.NewBinding(key.WithKeys("6"), key.WithHelp("", "")),
		up:          key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("↑/k", "move")),
		down:        key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("↓/j", "move")),
		sectionPrev: key.NewBinding(key.WithKeys("left", "h"), key.WithHelp("←/h", "prev section")),
		sectionNext: key.NewBinding(key.WithKeys("right", "l"), key.WithHelp("→/l", "next section")),
		open:        key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "open / explain")),
		refresh:     key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "refresh")),
		help:        key.NewBinding(key.WithKeys("?"), key.WithHelp("?", "toggle help")),
		quit:        key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q", "quit")),
	}
}

func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.nextTab, k.up, k.open, k.refresh, k.help, k.quit}
}

func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.nextTab, k.prevTab, k.tabOne, k.tabSix, k.up, k.down, k.open},
		{k.sectionPrev, k.sectionNext, k.refresh, k.help, k.quit},
	}
}

type liveDataMsg struct {
	health  *client.HealthResponse
	status  *client.ServiceStatusResponse
	history *client.HistoryResponse
	audit   *client.AuditLogResponse
	report  *client.CoordinationPilotReport
	delta   *client.CoordinationDeltaReport
	err     error
}

type historyDataMsg struct {
	history *client.HistoryResponse
	err     error
}

type explainDataMsg struct {
	tupleID uint64
	explain *client.ExplainTupleResponse
	err     error
}

type tickMsg struct{}

type Model struct {
	dataSource         DataSource
	baseURL            string
	policy             *client.PolicyContext
	refreshInterval    time.Duration
	activeTab          Tab
	section            CoordinationSection
	deltaPreset        DeltaPreset
	coordSelection     map[CoordinationSection]int
	auditSelection     int
	historySelection   int
	auditDetailOpen    bool
	historyDetailOpen  bool
	selectedTupleID    *uint64
	health             *client.HealthResponse
	status             *client.ServiceStatusResponse
	liveHistory        *client.HistoryResponse
	history            *client.HistoryResponse
	historyLoaded      bool
	audit              *client.AuditLogResponse
	report             *client.CoordinationPilotReport
	delta              *client.CoordinationDeltaReport
	explain            *client.ExplainTupleResponse
	lastLiveRefresh    time.Time
	lastHistoryRefresh time.Time
	lastExplainRefresh time.Time
	staleMessage       string
	showHelp           bool
	width              int
	height             int
	keys               keyMap
	helpModel          help.Model
}

func NewModel(
	dataSource DataSource,
	baseURL string,
	policy *client.PolicyContext,
	refreshInterval time.Duration,
) Model {
	if refreshInterval <= 0 {
		refreshInterval = 2 * time.Second
	}

	helpModel := help.New()
	helpModel.ShowAll = false

	return Model{
		dataSource:      dataSource,
		baseURL:         baseURL,
		policy:          policy,
		refreshInterval: refreshInterval,
		activeTab:       OverviewTab,
		section:         CurrentAuthorizedSection,
		deltaPreset:     AuthorizedHandoffPreset,
		coordSelection: map[CoordinationSection]int{
			CurrentAuthorizedSection: 0,
			ClaimableSection:         0,
			LiveHeartbeatsSection:    0,
			AcceptedOutcomesSection:  0,
			RejectedOutcomesSection:  0,
		},
		keys:      newKeyMap(),
		helpModel: helpModel,
	}
}

func Run(
	dataSource DataSource,
	baseURL string,
	policy *client.PolicyContext,
	refreshInterval time.Duration,
) error {
	model := NewModel(dataSource, baseURL, policy, refreshInterval)
	_, err := tea.NewProgram(model, tea.WithAltScreen()).Run()
	return err
}

func (m Model) Init() tea.Cmd {
	return m.loadLiveDataCmd()
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tickMsg:
		if m.isLiveTab() {
			return m, m.loadLiveDataCmd()
		}
		return m, m.tickCmd()
	case liveDataMsg:
		if msg.err != nil {
			m.staleMessage = fmt.Sprintf("Live refresh failed: %v", msg.err)
			return m, m.tickCmd()
		}
		m.health = msg.health
		m.status = msg.status
		m.liveHistory = msg.history
		if !m.historyLoaded {
			m.history = msg.history
		}
		m.audit = msg.audit
		m.report = msg.report
		m.delta = msg.delta
		m.lastLiveRefresh = time.Now()
		m.staleMessage = ""
		m.clampSelections()
		return m, m.tickCmd()
	case historyDataMsg:
		if msg.err != nil {
			m.staleMessage = fmt.Sprintf("History refresh failed: %v", msg.err)
			return m, nil
		}
		m.history = msg.history
		m.historyLoaded = true
		m.lastHistoryRefresh = time.Now()
		m.staleMessage = ""
		m.clampSelections()
		return m, nil
	case explainDataMsg:
		if msg.err != nil {
			m.staleMessage = fmt.Sprintf("Explain refresh failed: %v", msg.err)
			return m, nil
		}
		if m.selectedTupleID != nil && *m.selectedTupleID == msg.tupleID {
			m.explain = msg.explain
			m.lastExplainRefresh = time.Now()
			m.staleMessage = ""
		}
		return m, nil
	case tea.KeyMsg:
		switch {
		case key.Matches(msg, m.keys.quit):
			return m, tea.Quit
		case key.Matches(msg, m.keys.help):
			m.showHelp = !m.showHelp
			m.helpModel.ShowAll = m.showHelp
			return m, nil
		case key.Matches(msg, m.keys.nextTab):
			m.setActiveTab((int(m.activeTab) + 1) % 6)
			return m, m.tabEntryCmd()
		case key.Matches(msg, m.keys.prevTab):
			m.setActiveTab((int(m.activeTab) + 5) % 6)
			return m, m.tabEntryCmd()
		case key.Matches(msg, m.keys.tabOne):
			m.setActiveTab(0)
			return m, m.tabEntryCmd()
		case key.Matches(msg, m.keys.tabTwo):
			m.setActiveTab(1)
			return m, m.tabEntryCmd()
		case key.Matches(msg, m.keys.tabThree):
			m.setActiveTab(2)
			return m, m.tabEntryCmd()
		case key.Matches(msg, m.keys.tabFour):
			m.setActiveTab(3)
			return m, m.tabEntryCmd()
		case key.Matches(msg, m.keys.tabFive):
			m.setActiveTab(4)
			return m, m.tabEntryCmd()
		case key.Matches(msg, m.keys.tabSix):
			m.setActiveTab(5)
			return m, m.tabEntryCmd()
		case key.Matches(msg, m.keys.sectionPrev):
			if m.activeTab == CoordinationTab {
				m.section = CoordinationSection((int(m.section) + 4) % 5)
				return m, nil
			}
			if m.activeTab == DeltaTab {
				m.deltaPreset = DeltaPreset((int(m.deltaPreset) + 1) % 2)
				return m, m.loadLiveDataCmd()
			}
			return m, nil
		case key.Matches(msg, m.keys.sectionNext):
			if m.activeTab == CoordinationTab {
				m.section = CoordinationSection((int(m.section) + 1) % 5)
				return m, nil
			}
			if m.activeTab == DeltaTab {
				m.deltaPreset = DeltaPreset((int(m.deltaPreset) + 1) % 2)
				return m, m.loadLiveDataCmd()
			}
			return m, nil
		case key.Matches(msg, m.keys.up):
			m.moveSelection(-1)
			return m, nil
		case key.Matches(msg, m.keys.down):
			m.moveSelection(1)
			return m, nil
		case key.Matches(msg, m.keys.open):
			return m, m.openSelectionCmd()
		case key.Matches(msg, m.keys.refresh):
			return m, m.refreshCurrentTabCmd()
		}
	}
	return m, nil
}

func (m Model) View() string {
	var output strings.Builder
	output.WriteString(m.renderHeader())
	output.WriteString("\n")
	output.WriteString(m.renderTabs())
	output.WriteString("\n\n")
	output.WriteString(m.renderBody())
	output.WriteString("\n\n")
	output.WriteString(m.helpModel.View(m.keys))
	return output.String()
}

func (m *Model) setActiveTab(index int) {
	m.activeTab = Tab(index)
	if m.activeTab != AuditTab {
		m.auditDetailOpen = false
	}
	if m.activeTab != HistoryTab {
		m.historyDetailOpen = false
	}
}

func (m Model) tabEntryCmd() tea.Cmd {
	switch m.activeTab {
	case HistoryTab:
		return m.loadHistoryCmd()
	case ExplainTab:
		if m.selectedTupleID != nil {
			return m.loadExplainCmd(*m.selectedTupleID)
		}
	}
	return nil
}

func (m Model) refreshCurrentTabCmd() tea.Cmd {
	switch m.activeTab {
	case OverviewTab, CoordinationTab, DeltaTab, AuditTab:
		return m.loadLiveDataCmd()
	case HistoryTab:
		return m.loadHistoryCmd()
	case ExplainTab:
		if m.selectedTupleID != nil {
			return m.loadExplainCmd(*m.selectedTupleID)
		}
	}
	return nil
}

func (m *Model) openSelectionCmd() tea.Cmd {
	switch m.activeTab {
	case CoordinationTab:
		row := m.selectedCoordinationRow()
		if row == nil || row.TupleID == nil {
			return nil
		}
		tupleID := *row.TupleID
		m.selectedTupleID = &tupleID
		m.activeTab = ExplainTab
		return m.loadExplainCmd(tupleID)
	case AuditTab:
		m.auditDetailOpen = !m.auditDetailOpen
	case HistoryTab:
		m.historyDetailOpen = !m.historyDetailOpen
	}
	return nil
}

func (m *Model) moveSelection(delta int) {
	switch m.activeTab {
	case CoordinationTab:
		rows := m.currentSectionRows()
		if len(rows) == 0 {
			m.coordSelection[m.section] = 0
			return
		}
		next := m.coordSelection[m.section] + delta
		if next < 0 {
			next = 0
		}
		if next >= len(rows) {
			next = len(rows) - 1
		}
		m.coordSelection[m.section] = next
	case AuditTab:
		entries := m.auditEntriesNewestFirst()
		if len(entries) == 0 {
			m.auditSelection = 0
			return
		}
		next := m.auditSelection + delta
		if next < 0 {
			next = 0
		}
		if next >= len(entries) {
			next = len(entries) - 1
		}
		m.auditSelection = next
	case HistoryTab:
		datoms := m.historyDatomsNewestFirst()
		if len(datoms) == 0 {
			m.historySelection = 0
			return
		}
		next := m.historySelection + delta
		if next < 0 {
			next = 0
		}
		if next >= len(datoms) {
			next = len(datoms) - 1
		}
		m.historySelection = next
	}
}

func (m *Model) clampSelections() {
	for section := CurrentAuthorizedSection; section <= RejectedOutcomesSection; section++ {
		rows := m.rowsForSection(section)
		if len(rows) == 0 {
			m.coordSelection[section] = 0
			continue
		}
		if m.coordSelection[section] >= len(rows) {
			m.coordSelection[section] = len(rows) - 1
		}
	}

	if entries := m.auditEntriesNewestFirst(); len(entries) == 0 {
		m.auditSelection = 0
	} else if m.auditSelection >= len(entries) {
		m.auditSelection = len(entries) - 1
	}

	if datoms := m.historyDatomsNewestFirst(); len(datoms) == 0 {
		m.historySelection = 0
	} else if m.historySelection >= len(datoms) {
		m.historySelection = len(datoms) - 1
	}
}

func (m Model) isLiveTab() bool {
	return m.activeTab == OverviewTab || m.activeTab == CoordinationTab || m.activeTab == DeltaTab || m.activeTab == AuditTab
}

func (m Model) loadLiveDataCmd() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		health, err := m.dataSource.Health(ctx)
		if err != nil {
			return liveDataMsg{err: err}
		}
		status, err := m.dataSource.Status(ctx)
		if err != nil {
			return liveDataMsg{err: err}
		}
		history, err := m.dataSource.History(ctx)
		if err != nil {
			return liveDataMsg{err: err}
		}
		audit, err := m.dataSource.AuditLog(ctx)
		if err != nil {
			return liveDataMsg{err: err}
		}
		report, err := m.dataSource.CoordinationPilotReport(ctx, m.policy)
		if err != nil {
			return liveDataMsg{err: err}
		}
		left, right := m.deltaPreset.Cuts()
		delta, err := m.dataSource.CoordinationDeltaReport(ctx, left, right, m.policy)
		if err != nil {
			return liveDataMsg{err: err}
		}

		return liveDataMsg{
			health:  health,
			status:  status,
			history: history,
			audit:   audit,
			report:  report,
			delta:   delta,
		}
	}
}

func (m Model) loadHistoryCmd() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		history, err := m.dataSource.History(ctx)
		return historyDataMsg{history: history, err: err}
	}
}

func (m Model) loadExplainCmd(tupleID uint64) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		explain, err := m.dataSource.ExplainTupleWithPolicy(ctx, tupleID, m.policy)
		return explainDataMsg{
			tupleID: tupleID,
			explain: explain,
			err:     err,
		}
	}
}

func (m Model) tickCmd() tea.Cmd {
	return tea.Tick(m.refreshInterval, func(time.Time) tea.Msg {
		return tickMsg{}
	})
}

func (m Model) currentSectionRows() []client.ReportRow {
	return m.rowsForSection(m.section)
}

func (m Model) rowsForSection(section CoordinationSection) []client.ReportRow {
	if m.report == nil {
		return nil
	}
	switch section {
	case CurrentAuthorizedSection:
		return m.report.CurrentAuthorized
	case ClaimableSection:
		return m.report.Claimable
	case LiveHeartbeatsSection:
		return m.report.LiveHeartbeats
	case AcceptedOutcomesSection:
		return m.report.AcceptedOutcomes
	case RejectedOutcomesSection:
		return m.report.RejectedOutcomes
	default:
		return nil
	}
}

func (m Model) selectedCoordinationRow() *client.ReportRow {
	rows := m.currentSectionRows()
	if len(rows) == 0 {
		return nil
	}
	index := m.coordSelection[m.section]
	if index < 0 || index >= len(rows) {
		return nil
	}
	return &rows[index]
}

func (m Model) auditEntriesNewestFirst() []client.AuditEntry {
	if m.audit == nil || len(m.audit.Entries) == 0 {
		return nil
	}
	entries := make([]client.AuditEntry, 0, len(m.audit.Entries))
	for index := len(m.audit.Entries) - 1; index >= 0; index-- {
		entries = append(entries, m.audit.Entries[index])
	}
	return entries
}

func (m Model) historyDatomsNewestFirst() []client.Datom {
	var source *client.HistoryResponse
	if m.historyLoaded && m.history != nil {
		source = m.history
	} else {
		source = m.liveHistory
	}
	if source == nil || len(source.Datoms) == 0 {
		return nil
	}
	datoms := make([]client.Datom, 0, len(source.Datoms))
	for index := len(source.Datoms) - 1; index >= 0; index-- {
		datoms = append(datoms, source.Datoms[index])
	}
	return datoms
}

func (m Model) renderHeader() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("230"))
	statusStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("109"))
	warningStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("204"))

	var lines []string
	lines = append(lines, titleStyle.Render("AETHER Operator Cockpit"))
	lines = append(lines, statusStyle.Render(fmt.Sprintf("Base URL: %s", m.baseURL)))
	if m.report != nil {
		lines = append(lines, statusStyle.Render("Effective policy: "+client.FormatPolicyContext(m.report.PolicyContext)))
	} else {
		lines = append(lines, statusStyle.Render("Requested policy: "+client.FormatPolicyContext(m.policy)))
	}
	if m.staleMessage != "" {
		lines = append(lines, warningStyle.Render("Stale snapshot: "+m.staleMessage))
	}
	return strings.Join(lines, "\n")
}

func (m Model) renderTabs() string {
	active := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("230")).Background(lipgloss.Color("62")).Padding(0, 1)
	inactive := lipgloss.NewStyle().Foreground(lipgloss.Color("250")).Padding(0, 1)
	tabs := []string{
		OverviewTab.Title(),
		CoordinationTab.Title(),
		DeltaTab.Title(),
		AuditTab.Title(),
		HistoryTab.Title(),
		ExplainTab.Title(),
	}
	rendered := make([]string, 0, len(tabs))
	for index, label := range tabs {
		if Tab(index) == m.activeTab {
			rendered = append(rendered, active.Render(label))
		} else {
			rendered = append(rendered, inactive.Render(label))
		}
	}
	return strings.Join(rendered, " ")
}

func (m Model) renderBody() string {
	switch m.activeTab {
	case OverviewTab:
		return m.renderOverview()
	case CoordinationTab:
		return m.renderCoordination()
	case DeltaTab:
		return m.renderDelta()
	case AuditTab:
		return m.renderAudit()
	case HistoryTab:
		return m.renderHistory()
	case ExplainTab:
		return m.renderExplain()
	default:
		return ""
	}
}

func (m Model) renderOverview() string {
	muted := lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	highlight := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("159"))

	status := "unknown"
	if m.health != nil {
		status = m.health.Status
	}
	historyCount := 0
	var latestElement string
	if m.liveHistory != nil {
		historyCount = len(m.liveHistory.Datoms)
		if historyCount > 0 {
			latestElement = fmt.Sprintf("e%d", m.liveHistory.Datoms[historyCount-1].Element)
		}
	}
	if latestElement == "" {
		latestElement = "-"
	}

	reportCount := func(rows []client.ReportRow) int {
		return len(rows)
	}

	var lines []string
	lines = append(lines, highlight.Render("Live service summary"))
	lines = append(lines, fmt.Sprintf("Health: %s", status))
	if m.status != nil {
		lines = append(lines, fmt.Sprintf("Mode: %s", m.status.ServiceMode))
		lines = append(lines, fmt.Sprintf("Build/config/schema: %s / %s / %s", stringOrDefault(m.status.BuildVersion, "-"), stringOrDefault(m.status.ConfigVersion, "-"), stringOrDefault(m.status.SchemaVersion, "-")))
		if m.status.BindAddr != nil {
			lines = append(lines, fmt.Sprintf("Bind: %s", *m.status.BindAddr))
		}
		if m.status.Storage.DatabasePath != nil {
			lines = append(lines, fmt.Sprintf("Database: %s", *m.status.Storage.DatabasePath))
		}
	}
	lines = append(lines, fmt.Sprintf("Journal entries: %d", historyCount))
	lines = append(lines, fmt.Sprintf("Latest element: %s", latestElement))
	if m.report != nil {
		lines = append(lines, fmt.Sprintf("Current authorized: %d", reportCount(m.report.CurrentAuthorized)))
		lines = append(lines, fmt.Sprintf("Claimable: %d", reportCount(m.report.Claimable)))
		lines = append(lines, fmt.Sprintf("Live heartbeats: %d", reportCount(m.report.LiveHeartbeats)))
		lines = append(lines, fmt.Sprintf("Accepted outcomes: %d", reportCount(m.report.AcceptedOutcomes)))
		lines = append(lines, fmt.Sprintf("Rejected outcomes: %d", reportCount(m.report.RejectedOutcomes)))
	} else {
		lines = append(lines, muted.Render("Waiting for first live report..."))
	}
	if m.status != nil {
		lines = append(lines, "")
		lines = append(lines, highlight.Render("Operator identity surface"))
		lines = append(lines, fmt.Sprintf("Configured principals: %d", len(m.status.Principals)))
		revoked := 0
		for _, principal := range m.status.Principals {
			if principal.Revoked {
				revoked++
			}
		}
		lines = append(lines, fmt.Sprintf("Revoked principals: %d", revoked))
		if len(m.status.Replicas) > 0 {
			lines = append(lines, "")
			lines = append(lines, highlight.Render("Replica summary"))
			for _, replica := range m.status.Replicas {
				applied := "-"
				if replica.AppliedElement != nil {
					applied = fmt.Sprintf("e%d", *replica.AppliedElement)
				}
				lines = append(lines, fmt.Sprintf(
					"%s/%d | %s | epoch=%d | applied=%s | lag=%d | healthy=%t",
					replica.Partition,
					replica.ReplicaID,
					replica.Role,
					replica.LeaderEpoch,
					applied,
					replica.ReplicationLag,
					replica.Healthy,
				))
			}
		}
	}

	if !m.lastLiveRefresh.IsZero() {
		lines = append(lines, muted.Render("Last refresh: "+m.lastLiveRefresh.Format(time.RFC3339)))
	}
	return strings.Join(lines, "\n")
}

func (m Model) renderDelta() string {
	muted := lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	active := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("230")).Background(lipgloss.Color("63")).Padding(0, 1)
	inactive := lipgloss.NewStyle().Foreground(lipgloss.Color("248")).Padding(0, 1)
	if m.delta == nil {
		return muted.Render("No coordination delta loaded yet.")
	}

	presets := []DeltaPreset{AuthorizedHandoffPreset, PreHeartbeatToCurrentPreset}
	var tabs []string
	for _, preset := range presets {
		if preset == m.deltaPreset {
			tabs = append(tabs, active.Render(preset.Title()))
		} else {
			tabs = append(tabs, inactive.Render(preset.Title()))
		}
	}

	lines := []string{
		strings.Join(tabs, " "),
		"",
		fmt.Sprintf("Left history: %d", m.delta.LeftHistoryLen),
		fmt.Sprintf("Right history: %d", m.delta.RightHistoryLen),
		"",
		renderSectionDelta("Authorization", m.delta.CurrentAuthorized),
		"",
		renderSectionDelta("Claimable", m.delta.Claimable),
		"",
		renderSectionDelta("Heartbeats", m.delta.LiveHeartbeats),
		"",
		renderSectionDelta("Accepted outcomes", m.delta.AcceptedOutcomes),
		"",
		renderSectionDelta("Rejected outcomes", m.delta.RejectedOutcomes),
	}
	return strings.Join(lines, "\n")
}

func (m Model) renderCoordination() string {
	active := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("230")).Background(lipgloss.Color("63")).Padding(0, 1)
	inactive := lipgloss.NewStyle().Foreground(lipgloss.Color("248")).Padding(0, 1)
	selected := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("229"))
	muted := lipgloss.NewStyle().Foreground(lipgloss.Color("245"))

	sections := []CoordinationSection{
		CurrentAuthorizedSection,
		ClaimableSection,
		LiveHeartbeatsSection,
		AcceptedOutcomesSection,
		RejectedOutcomesSection,
	}
	sectionTabs := make([]string, 0, len(sections))
	for _, section := range sections {
		if section == m.section {
			sectionTabs = append(sectionTabs, active.Render(section.Title()))
		} else {
			sectionTabs = append(sectionTabs, inactive.Render(section.Title()))
		}
	}

	rows := m.currentSectionRows()
	var body []string
	body = append(body, strings.Join(sectionTabs, " "))
	body = append(body, "")
	if len(rows) == 0 {
		body = append(body, muted.Render("No rows in this section."))
		return strings.Join(body, "\n")
	}

	index := m.coordSelection[m.section]
	for rowIndex, row := range rows {
		prefix := "  "
		line := fmt.Sprintf("%s%s", prefix, client.FormatValues(row.Values))
		if row.TupleID != nil {
			line = fmt.Sprintf("%s | t%d", line, *row.TupleID)
		} else {
			line = fmt.Sprintf("%s | -", line)
		}
		if rowIndex == index {
			line = selected.Render("> " + line)
		}
		body = append(body, line)
	}

	if selectedRow := m.selectedCoordinationRow(); selectedRow != nil && selectedRow.TupleID != nil {
		body = append(body, "")
		body = append(body, muted.Render(fmt.Sprintf("Press enter to explain t%d", *selectedRow.TupleID)))
	}
	return strings.Join(body, "\n")
}

func (m Model) renderAudit() string {
	selected := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("229"))
	muted := lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	entries := m.auditEntriesNewestFirst()
	if len(entries) == 0 {
		return muted.Render("No audit entries loaded yet.")
	}

	var lines []string
	for index, entry := range entries {
		line := fmt.Sprintf(
			"  %-19s %-16s %-6s %-34s %-8s %-10s",
			formatTimestamp(entry.TimestampMS),
			trimTo(entry.Principal, 16),
			entry.Method,
			trimTo(entry.Path, 34),
			fmt.Sprintf("%d", entry.Status),
			trimTo(policyDecision(entry.Context), 10),
		)
		if index == m.auditSelection {
			line = selected.Render("> " + line)
		}
		lines = append(lines, line)
	}

	if !m.auditDetailOpen {
		lines = append(lines, "")
		lines = append(lines, muted.Render("Press enter to open the selected audit detail."))
		return strings.Join(lines, "\n")
	}

	entry := entries[m.auditSelection]
	lines = append(lines, "")
	lines = append(lines, muted.Render("Selected audit detail"))
	lines = append(lines, fmt.Sprintf("Temporal view: %s", stringOrDash(entry.Context.TemporalView)))
	lines = append(lines, fmt.Sprintf("Query goal: %s", stringOrDash(entry.Context.QueryGoal)))
	lines = append(lines, fmt.Sprintf("Tuple: %s", optionalUint(entry.Context.TupleID, "t%d")))
	lines = append(lines, fmt.Sprintf("Requested element: %s", optionalUint(entry.Context.RequestedElement, "e%d")))
	lines = append(lines, fmt.Sprintf("Counts: datoms=%s rows=%s trace=%s", optionalInt(entry.Context.DatomCount), optionalInt(entry.Context.RowCount), optionalInt(entry.Context.TraceTupleCount)))
	lines = append(lines, fmt.Sprintf("Effective policy: %s", formatEffectiveAuditPolicy(entry.Context)))
	return strings.Join(lines, "\n")
}

func (m Model) renderHistory() string {
	selected := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("229"))
	muted := lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	datoms := m.historyDatomsNewestFirst()
	if len(datoms) == 0 {
		return muted.Render("No history loaded yet.")
	}

	var lines []string
	for index, datom := range datoms {
		line := fmt.Sprintf(
			"  e%-6d entity(%-4d) attr(%-4d) %-11s %s",
			datom.Element,
			datom.Entity,
			datom.Attribute,
			datom.Op,
			trimTo(datom.Value.Display(), 64),
		)
		if index == m.historySelection {
			line = selected.Render("> " + line)
		}
		lines = append(lines, line)
	}

	if !m.historyDetailOpen {
		lines = append(lines, "")
		lines = append(lines, muted.Render("Press enter to open the selected history detail."))
		return strings.Join(lines, "\n")
	}

	datom := datoms[m.historySelection]
	lines = append(lines, "")
	lines = append(lines, muted.Render("Selected datom detail"))
	lines = append(lines, fmt.Sprintf("Policy: %s", client.FormatPolicyEnvelope(datom.Policy)))
	lines = append(lines, fmt.Sprintf("Parent datoms: %s", formatElementIDs(datom.Provenance.ParentDatomIDs)))
	lines = append(lines, fmt.Sprintf("Source ref: %s", formatSourceRef(datom.Provenance.SourceRef)))
	lines = append(lines, fmt.Sprintf("Author/tool/session: %s / %s / %s", stringOrDefault(datom.Provenance.AuthorPrincipal, "-"), stringOrDefault(datom.Provenance.ToolID, "-"), stringOrDefault(datom.Provenance.SessionID, "-")))
	return strings.Join(lines, "\n")
}

func (m Model) renderExplain() string {
	muted := lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	if m.selectedTupleID == nil || m.explain == nil {
		return muted.Render("No tuple trace selected yet. Choose a coordination row with a tuple id and press enter.")
	}

	var lines []string
	lines = append(lines, fmt.Sprintf("Root tuple: t%d", m.explain.Trace.Root))
	lines = append(lines, fmt.Sprintf("Trace tuples: %d", len(m.explain.Trace.Tuples)))
	if !m.lastExplainRefresh.IsZero() {
		lines = append(lines, muted.Render("Last refresh: "+m.lastExplainRefresh.Format(time.RFC3339)))
	}
	lines = append(lines, "")
	for _, tuple := range m.explain.Trace.Tuples {
		lines = append(lines, fmt.Sprintf(
			"t%d | %s | iteration=%d | sources=%s | parents=%s",
			tuple.Tuple.ID,
			client.FormatValues(tuple.Tuple.Values),
			tuple.Metadata.Iteration,
			formatElementIDs(tuple.Metadata.SourceDatomIDs),
			formatTupleIDs(tuple.Metadata.ParentTupleIDs),
		))
		if len(tuple.Metadata.ImportedCuts) > 0 {
			lines = append(lines, fmt.Sprintf("  imported cuts=%s", formatImportedCuts(tuple.Metadata.ImportedCuts)))
		}
	}
	return strings.Join(lines, "\n")
}

func formatTimestamp(timestampMS uint64) string {
	if timestampMS == 0 {
		return "-"
	}
	return time.UnixMilli(int64(timestampMS)).Format("2006-01-02 15:04:05")
}

func trimTo(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	if limit <= 1 {
		return value[:limit]
	}
	return value[:limit-1] + "…"
}

func stringOrDash(value *string) string {
	if value == nil || *value == "" {
		return "-"
	}
	return *value
}

func stringOrDefault(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func optionalUint(value *uint64, pattern string) string {
	if value == nil {
		return "-"
	}
	return fmt.Sprintf(pattern, *value)
}

func optionalInt(value *int) string {
	if value == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *value)
}

func policyDecision(context client.AuditContext) string {
	if context.PolicyDecision == nil || *context.PolicyDecision == "" {
		return "-"
	}
	return *context.PolicyDecision
}

func formatEffectiveAuditPolicy(context client.AuditContext) string {
	policy := &client.PolicyContext{
		Capabilities: context.EffectiveCapabilities,
		Visibilities: context.EffectiveVisibilities,
	}
	return client.FormatPolicyContext(policy)
}

func formatElementIDs(ids []client.ElementID) string {
	if len(ids) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("e%d", id))
	}
	return strings.Join(parts, ", ")
}

func formatTupleIDs(ids []client.TupleID) string {
	if len(ids) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("t%d", id))
	}
	return strings.Join(parts, ", ")
}

func formatSourceRef(source client.SourceRef) string {
	if source.URI == "" && source.Digest == nil {
		return "-"
	}
	if source.Digest == nil || *source.Digest == "" {
		return source.URI
	}
	return fmt.Sprintf("%s (%s)", source.URI, *source.Digest)
}

func renderSectionDelta(title string, section client.ReportSectionDelta) string {
	lines := []string{title}
	lines = append(lines, fmt.Sprintf("  added=%d removed=%d changed=%d", len(section.Added), len(section.Removed), len(section.Changed)))
	for _, row := range section.Added {
		lines = append(lines, fmt.Sprintf("  + %s", client.FormatValues(row.Row.Values)))
	}
	for _, row := range section.Removed {
		lines = append(lines, fmt.Sprintf("  - %s", client.FormatValues(row.Row.Values)))
	}
	for _, row := range section.Changed {
		lines = append(lines, fmt.Sprintf("  ~ %s -> %s", client.FormatValues(row.Before.Values), client.FormatValues(row.After.Values)))
	}
	return strings.Join(lines, "\n")
}

func formatImportedCuts(cuts []client.ImportedCut) string {
	if len(cuts) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(cuts))
	for _, cut := range cuts {
		if cut.AsOf == nil {
			parts = append(parts, cut.Partition+"@current")
		} else {
			parts = append(parts, fmt.Sprintf("%s@e%d", cut.Partition, *cut.AsOf))
		}
	}
	return strings.Join(parts, ", ")
}
