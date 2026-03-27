package tui

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/fyremael/aether/go/internal/client"
)

func TestStartupLoadsOverviewData(t *testing.T) {
	t.Helper()

	dataSource := newFakeDataSource()
	model := NewModel(dataSource, "http://127.0.0.1:3000", policy(), 2*time.Second)

	msg := execCmd(t, model.Init())
	updated, _ := model.Update(msg)
	loaded := updated.(Model)

	if loaded.health == nil || loaded.health.Status != "ok" {
		t.Fatalf("expected health to load, got %#v", loaded.health)
	}
	if loaded.status == nil || loaded.status.ServiceMode != "single_node" {
		t.Fatalf("expected service status to load, got %#v", loaded.status)
	}
	if loaded.report == nil || loaded.report.HistoryLen != 2 {
		t.Fatalf("expected report to load, got %#v", loaded.report)
	}
	if loaded.delta == nil || loaded.delta.RightHistoryLen != 2 {
		t.Fatalf("expected delta report to load, got %#v", loaded.delta)
	}
	if loaded.lastLiveRefresh.IsZero() {
		t.Fatalf("expected last live refresh to be set")
	}
}

func TestTabSwitchingPreservesSelectionState(t *testing.T) {
	t.Helper()

	dataSource := newFakeDataSource()
	dataSource.reportResponses = []*client.CoordinationPilotReport{baseReport(2)}
	model := loadModel(t, dataSource)
	model.activeTab = CoordinationTab
	model.moveSelection(1)

	updated, _ := model.Update(keyRunes("4"))
	model = updated.(Model)
	updated, _ = model.Update(keyRunes("2"))
	model = updated.(Model)

	if model.activeTab != CoordinationTab {
		t.Fatalf("expected coordination tab, got %v", model.activeTab)
	}
	if got := model.coordSelection[CurrentAuthorizedSection]; got != 1 {
		t.Fatalf("expected coordination selection to persist, got %d", got)
	}
}

func TestSelectingCoordinationRowLoadsExplainAndSwitchesTab(t *testing.T) {
	t.Helper()

	model := loadModel(t, newFakeDataSource())
	model.activeTab = CoordinationTab

	updated, cmd := model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = updated.(Model)
	if model.activeTab != ExplainTab {
		t.Fatalf("expected explain tab after enter, got %v", model.activeTab)
	}
	if model.selectedTupleID == nil || *model.selectedTupleID != 7 {
		t.Fatalf("expected selected tuple 7, got %#v", model.selectedTupleID)
	}

	msg := execCmd(t, cmd)
	updated, _ = model.Update(msg)
	model = updated.(Model)

	if model.explain == nil || model.explain.Trace.Root != 7 {
		t.Fatalf("expected explain trace for tuple 7, got %#v", model.explain)
	}
}

func TestRefreshTickerUpdatesLiveData(t *testing.T) {
	t.Helper()

	dataSource := newFakeDataSource()
	dataSource.reportResponses = []*client.CoordinationPilotReport{
		baseReport(1),
		baseReport(2),
	}
	model := loadModel(t, dataSource)

	updated, cmd := model.Update(tickMsg{})
	model = updated.(Model)
	msg := execCmd(t, cmd)
	updated, _ = model.Update(msg)
	model = updated.(Model)

	if model.report == nil || len(model.report.CurrentAuthorized) != 2 {
		t.Fatalf("expected refreshed report with two rows, got %#v", model.report)
	}
}

func TestRefreshFailureRetainsLastGoodSnapshotAndShowsBanner(t *testing.T) {
	t.Helper()

	dataSource := newFakeDataSource()
	dataSource.healthErrors = []error{nil, errors.New("network down")}
	model := loadModel(t, dataSource)

	updated, cmd := model.Update(tickMsg{})
	model = updated.(Model)
	msg := execCmd(t, cmd)
	updated, _ = model.Update(msg)
	model = updated.(Model)

	if model.report == nil || len(model.report.CurrentAuthorized) != 1 {
		t.Fatalf("expected last good report to remain, got %#v", model.report)
	}
	if !strings.Contains(model.staleMessage, "network down") {
		t.Fatalf("expected stale banner to mention error, got %q", model.staleMessage)
	}
}

func TestExplainViewHasEmptyStateBeforeSelection(t *testing.T) {
	t.Helper()

	model := loadModel(t, newFakeDataSource())
	model.activeTab = ExplainTab
	model.selectedTupleID = nil
	model.explain = nil

	view := model.renderExplain()
	if !strings.Contains(view, "No tuple trace selected yet") {
		t.Fatalf("expected empty explain state, got %q", view)
	}
}

func loadModel(t *testing.T, dataSource *fakeDataSource) Model {
	t.Helper()

	model := NewModel(dataSource, "http://127.0.0.1:3000", policy(), 2*time.Second)
	msg := execCmd(t, model.Init())
	updated, _ := model.Update(msg)
	return updated.(Model)
}

func execCmd(t *testing.T, cmd tea.Cmd) tea.Msg {
	t.Helper()
	if cmd == nil {
		t.Fatalf("expected command")
	}
	return cmd()
}

func keyRunes(value string) tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(value)}
}

func policy() *client.PolicyContext {
	return &client.PolicyContext{Capabilities: []string{"executor"}}
}

type fakeDataSource struct {
	healthResponses  []*client.HealthResponse
	healthErrors     []error
	statusResponses  []*client.ServiceStatusResponse
	statusErrors     []error
	historyResponses []*client.HistoryResponse
	historyErrors    []error
	auditResponses   []*client.AuditLogResponse
	auditErrors      []error
	reportResponses  []*client.CoordinationPilotReport
	reportErrors     []error
	deltaResponses   []*client.CoordinationDeltaReport
	deltaErrors      []error
	explainResponses []*client.ExplainTupleResponse
	explainErrors    []error
	healthCalls      int
	statusCalls      int
	historyCalls     int
	auditCalls       int
	reportCalls      int
	deltaCalls       int
	explainCalls     int
}

func newFakeDataSource() *fakeDataSource {
	return &fakeDataSource{
		healthResponses:  []*client.HealthResponse{{Status: "ok"}},
		statusResponses:  []*client.ServiceStatusResponse{baseStatus()},
		historyResponses: []*client.HistoryResponse{baseHistory()},
		auditResponses:   []*client.AuditLogResponse{baseAudit()},
		reportResponses:  []*client.CoordinationPilotReport{baseReport(1)},
		deltaResponses:   []*client.CoordinationDeltaReport{baseDelta()},
		explainResponses: []*client.ExplainTupleResponse{baseExplain()},
	}
}

func (f *fakeDataSource) Health(context.Context) (*client.HealthResponse, error) {
	index := f.healthCalls
	f.healthCalls++
	if err := pickErr(f.healthErrors, index); err != nil {
		return nil, err
	}
	return pickHealth(f.healthResponses, index), nil
}

func (f *fakeDataSource) Status(context.Context) (*client.ServiceStatusResponse, error) {
	index := f.statusCalls
	f.statusCalls++
	if err := pickErr(f.statusErrors, index); err != nil {
		return nil, err
	}
	return pickStatus(f.statusResponses, index), nil
}

func (f *fakeDataSource) History(context.Context) (*client.HistoryResponse, error) {
	index := f.historyCalls
	f.historyCalls++
	if err := pickErr(f.historyErrors, index); err != nil {
		return nil, err
	}
	return pickHistory(f.historyResponses, index), nil
}

func (f *fakeDataSource) AuditLog(context.Context) (*client.AuditLogResponse, error) {
	index := f.auditCalls
	f.auditCalls++
	if err := pickErr(f.auditErrors, index); err != nil {
		return nil, err
	}
	return pickAudit(f.auditResponses, index), nil
}

func (f *fakeDataSource) CoordinationPilotReport(context.Context, *client.PolicyContext) (*client.CoordinationPilotReport, error) {
	index := f.reportCalls
	f.reportCalls++
	if err := pickErr(f.reportErrors, index); err != nil {
		return nil, err
	}
	return pickReport(f.reportResponses, index), nil
}

func (f *fakeDataSource) CoordinationDeltaReport(context.Context, client.CoordinationCut, client.CoordinationCut, *client.PolicyContext) (*client.CoordinationDeltaReport, error) {
	index := f.deltaCalls
	f.deltaCalls++
	if err := pickErr(f.deltaErrors, index); err != nil {
		return nil, err
	}
	return pickDelta(f.deltaResponses, index), nil
}

func (f *fakeDataSource) ExplainTupleWithPolicy(context.Context, uint64, *client.PolicyContext) (*client.ExplainTupleResponse, error) {
	index := f.explainCalls
	f.explainCalls++
	if err := pickErr(f.explainErrors, index); err != nil {
		return nil, err
	}
	return pickExplain(f.explainResponses, index), nil
}

func pickErr(values []error, index int) error {
	if len(values) == 0 {
		return nil
	}
	if index >= len(values) {
		return values[len(values)-1]
	}
	return values[index]
}

func pickHealth(values []*client.HealthResponse, index int) *client.HealthResponse {
	if index >= len(values) {
		return values[len(values)-1]
	}
	return values[index]
}

func pickHistory(values []*client.HistoryResponse, index int) *client.HistoryResponse {
	if index >= len(values) {
		return values[len(values)-1]
	}
	return values[index]
}

func pickStatus(values []*client.ServiceStatusResponse, index int) *client.ServiceStatusResponse {
	if index >= len(values) {
		return values[len(values)-1]
	}
	return values[index]
}

func pickAudit(values []*client.AuditLogResponse, index int) *client.AuditLogResponse {
	if index >= len(values) {
		return values[len(values)-1]
	}
	return values[index]
}

func pickReport(values []*client.CoordinationPilotReport, index int) *client.CoordinationPilotReport {
	if index >= len(values) {
		return values[len(values)-1]
	}
	return values[index]
}

func pickDelta(values []*client.CoordinationDeltaReport, index int) *client.CoordinationDeltaReport {
	if index >= len(values) {
		return values[len(values)-1]
	}
	return values[index]
}

func pickExplain(values []*client.ExplainTupleResponse, index int) *client.ExplainTupleResponse {
	if index >= len(values) {
		return values[len(values)-1]
	}
	return values[index]
}

func baseHistory() *client.HistoryResponse {
	return &client.HistoryResponse{
		Datoms: []client.Datom{
			{
				Entity:    1,
				Attribute: 1,
				Value:     client.Value{Kind: "String", String: "claimed"},
				Op:        "Claim",
				Element:   1,
			},
			{
				Entity:    1,
				Attribute: 1,
				Value:     client.Value{Kind: "String", String: "leased"},
				Op:        "LeaseOpen",
				Element:   2,
			},
		},
	}
}

func baseStatus() *client.ServiceStatusResponse {
	return &client.ServiceStatusResponse{
		Status:        "ok",
		BuildVersion:  "0.1.0",
		ConfigVersion: "pilot-v1",
		SchemaVersion: "v1",
		ServiceMode:   "single_node",
		Principals: []client.PrincipalStatusSummary{
			{
				Principal:   "pilot-operator",
				PrincipalID: "principal:pilot-operator",
				TokenID:     "token:pilot-operator",
				Scopes:      []string{"append", "query", "explain", "ops"},
			},
		},
	}
}

func baseAudit() *client.AuditLogResponse {
	return &client.AuditLogResponse{
		Entries: []client.AuditEntry{
			{
				TimestampMS: 1000,
				Principal:   "pilot-operator",
				Method:      "POST",
				Path:        "/v1/reports/pilot/coordination",
				Status:      200,
				Scope:       "query",
				Outcome:     "ok",
				Context: client.AuditContext{
					TemporalView:          ptrString("coordination_pilot_report"),
					PolicyDecision:        ptrString("token_default"),
					EffectiveCapabilities: []string{"executor"},
				},
			},
		},
	}
}

func baseReport(currentCount int) *client.CoordinationPilotReport {
	authorized := make([]client.ReportRow, 0, currentCount)
	for index := 0; index < currentCount; index++ {
		tupleID := uint64(7 + index)
		authorized = append(authorized, client.ReportRow{
			TupleID: &tupleID,
			Values: []client.Value{
				{Kind: "Entity", Entity: 1},
				{Kind: "String", String: "worker-b"},
				{Kind: "U64", U64: uint64(index + 1)},
			},
		})
	}
	return &client.CoordinationPilotReport{
		GeneratedAtMS:     1000,
		PolicyContext:     policy(),
		HistoryLen:        2,
		CurrentAuthorized: authorized,
		Claimable: []client.ReportRow{
			{
				Values: []client.Value{
					{Kind: "Entity", Entity: 1},
					{Kind: "String", String: "worker-b"},
				},
			},
		},
		LiveHeartbeats: []client.ReportRow{
			{
				TupleID: ptrUint64(11),
				Values: []client.Value{
					{Kind: "Entity", Entity: 1},
					{Kind: "String", String: "worker-b"},
					{Kind: "U64", U64: 2},
					{Kind: "U64", U64: 200},
				},
			},
		},
		AcceptedOutcomes: []client.ReportRow{
			{
				TupleID: ptrUint64(12),
				Values: []client.Value{
					{Kind: "Entity", Entity: 1},
					{Kind: "String", String: "worker-b"},
				},
			},
		},
		RejectedOutcomes: []client.ReportRow{
			{
				TupleID: ptrUint64(13),
				Values: []client.Value{
					{Kind: "Entity", Entity: 1},
					{Kind: "String", String: "worker-a"},
				},
			},
		},
		Trace: &client.TraceSummary{
			Root:       7,
			TupleCount: 1,
			Tuples: []client.TraceTupleSummary{
				{
					TupleID:        7,
					Values:         []client.Value{{Kind: "Entity", Entity: 1}, {Kind: "String", String: "worker-b"}},
					Iteration:      1,
					SourceDatomIDs: []client.ElementID{1, 2},
					ParentTupleIDs: []client.TupleID{3},
				},
			},
		},
	}
}

func baseDelta() *client.CoordinationDeltaReport {
	return &client.CoordinationDeltaReport{
		GeneratedAtMS:    1000,
		Left:             client.AsOfCut(9),
		Right:            client.CurrentCut(),
		LeftHistoryLen:   2,
		RightHistoryLen:  2,
		CurrentAuthorized: client.ReportSectionDelta{
			Changed: []client.ReportRowChange{
				{
					Before: client.ReportRow{
						TupleID: ptrUint64(7),
						Values: []client.Value{
							{Kind: "Entity", Entity: 1},
							{Kind: "String", String: "worker-a"},
						},
					},
					After: client.ReportRow{
						TupleID: ptrUint64(8),
						Values: []client.Value{
							{Kind: "Entity", Entity: 1},
							{Kind: "String", String: "worker-b"},
						},
					},
				},
			},
		},
	}
}

func baseExplain() *client.ExplainTupleResponse {
	return &client.ExplainTupleResponse{
		Trace: client.DerivationTrace{
			Root: 7,
			Tuples: []client.DerivedTuple{
				{
					Tuple: client.Tuple{
						ID:        7,
						Predicate: 42,
						Values: []client.Value{
							{Kind: "Entity", Entity: 1},
							{Kind: "String", String: "worker-b"},
						},
					},
					Metadata: client.DerivedTupleMetadata{
						Iteration:      1,
						SourceDatomIDs: []client.ElementID{1, 2},
						ParentTupleIDs: []client.TupleID{3},
					},
				},
			},
		},
	}
}

func ptrString(value string) *string {
	return &value
}

func ptrUint64(value uint64) *uint64 {
	return &value
}
