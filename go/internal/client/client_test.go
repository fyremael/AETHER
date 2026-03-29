package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthSendsBearerToken(t *testing.T) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer pilot-token" {
			t.Fatalf("expected bearer token, got %q", got)
		}
		if r.URL.Path != "/health" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(HealthResponse{Status: "ok"})
	}))
	defer server.Close()

	api := New(server.URL, "pilot-token")
	response, err := api.Health(context.Background())
	if err != nil {
		t.Fatalf("health failed: %v", err)
	}
	if response.Status != "ok" {
		t.Fatalf("unexpected status %q", response.Status)
	}
}

func TestStructuredErrorsPreserveMessageAndPayload(t *testing.T) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":  "policy denied for explain",
			"reason": "visibility mismatch",
		})
	}))
	defer server.Close()

	api := New(server.URL, "pilot-token")
	_, err := api.Health(context.Background())
	if err == nil {
		t.Fatalf("expected health request to fail")
	}

	apiErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected *Error, got %T", err)
	}
	if apiErr.StatusCode != http.StatusForbidden {
		t.Fatalf("unexpected status code %d", apiErr.StatusCode)
	}
	if apiErr.Message != "policy denied for explain" {
		t.Fatalf("unexpected message %q", apiErr.Message)
	}
	payload, ok := apiErr.Payload.(map[string]any)
	if !ok {
		t.Fatalf("expected structured payload, got %#v", apiErr.Payload)
	}
	if payload["reason"] != "visibility mismatch" {
		t.Fatalf("unexpected structured payload %#v", payload)
	}
}

func TestRunDocumentCarriesPolicyContext(t *testing.T) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/documents/run" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}

		var request RunDocumentRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request.PolicyContext == nil || len(request.PolicyContext.Capabilities) != 1 || request.PolicyContext.Capabilities[0] != "executor" {
			t.Fatalf("policy context missing from request: %#v", request.PolicyContext)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"query": map[string]any{
				"rows": []map[string]any{
					{"values": []map[string]any{{"Entity": float64(1)}}},
				},
			},
		})
	}))
	defer server.Close()

	api := New(server.URL, "")
	response, err := api.RunDocument(context.Background(), RunDocumentRequest{
		DSL: "query current_cut { current goal ready(x) keep x }",
		PolicyContext: &PolicyContext{
			Capabilities: []string{"executor"},
		},
	})
	if err != nil {
		t.Fatalf("run document failed: %v", err)
	}
	if _, ok := response["query"]; !ok {
		t.Fatalf("expected query in response: %#v", response)
	}
}

func TestCoordinationPilotReportCarriesPolicyContextAndDecodes(t *testing.T) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/reports/pilot/coordination" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		var request CoordinationPilotReportRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request.PolicyContext == nil || len(request.PolicyContext.Visibilities) != 1 || request.PolicyContext.Visibilities[0] != "ops" {
			t.Fatalf("policy context missing from report request: %#v", request.PolicyContext)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"generated_at_ms": 1234,
			"policy_context": map[string]any{
				"capabilities": []string{"executor"},
				"visibilities": []string{"ops"},
			},
			"history_len":              25,
			"pre_heartbeat_authorized": []any{},
			"as_of_authorized":         []any{},
			"live_heartbeats":          []any{},
			"current_authorized": []map[string]any{
				{
					"tuple_id": 7,
					"values": []map[string]any{
						{"Entity": 1},
						{"String": "worker-b"},
						{"U64": 2},
					},
				},
			},
			"claimable":         []any{},
			"accepted_outcomes": []any{},
			"rejected_outcomes": []any{},
			"trace": map[string]any{
				"root":        7,
				"tuple_count": 1,
				"tuples": []map[string]any{
					{
						"tuple_id": 7,
						"values": []map[string]any{
							{"Entity": 1},
							{"String": "worker-b"},
						},
						"iteration":        1,
						"source_datom_ids": []uint64{9, 10},
						"parent_tuple_ids": []uint64{4},
					},
				},
			},
		})
	}))
	defer server.Close()

	api := New(server.URL, "")
	report, err := api.CoordinationPilotReport(context.Background(), &PolicyContext{
		Capabilities: []string{"executor"},
		Visibilities: []string{"ops"},
	})
	if err != nil {
		t.Fatalf("coordination report failed: %v", err)
	}
	if report.HistoryLen != 25 {
		t.Fatalf("unexpected history length %d", report.HistoryLen)
	}
	if len(report.CurrentAuthorized) != 1 {
		t.Fatalf("expected one current row, got %d", len(report.CurrentAuthorized))
	}
	if report.CurrentAuthorized[0].TupleID == nil || *report.CurrentAuthorized[0].TupleID != 7 {
		t.Fatalf("unexpected tuple id %#v", report.CurrentAuthorized[0].TupleID)
	}
	if got := report.CurrentAuthorized[0].Values[0].Display(); got != "entity(1)" {
		t.Fatalf("unexpected decoded entity value %q", got)
	}
	if report.Trace == nil || report.Trace.Root != 7 || report.Trace.TupleCount != 1 {
		t.Fatalf("unexpected trace %#v", report.Trace)
	}
}

func TestHistoryAuditAndExplainDecodeTypedModels(t *testing.T) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/history":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"datoms": []map[string]any{
					{
						"entity":    1,
						"attribute": 2,
						"value":     map[string]any{"String": "ready"},
						"op":        "Assert",
						"element":   9,
						"replica":   1,
						"causal_context": map[string]any{
							"frontier": []uint64{8},
						},
						"provenance": map[string]any{
							"author_principal": "pilot-operator",
							"agent_id":         "agent-1",
							"tool_id":          "tool-1",
							"session_id":       "session-1",
							"source_ref": map[string]any{
								"uri":    "file://ready",
								"digest": "sha256:abc",
							},
							"parent_datom_ids": []uint64{7},
							"confidence":       1.0,
							"trust_domain":     "pilot",
							"schema_version":   "v1",
						},
						"policy": map[string]any{
							"capabilities": []string{"executor"},
						},
					},
				},
			})
		case "/v1/audit":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []map[string]any{
					{
						"timestamp_ms": 321,
						"principal":    "pilot-operator",
						"method":       "POST",
						"path":         "/v1/reports/pilot/coordination",
						"status":       200,
						"scope":        "query",
						"outcome":      "ok",
						"context": map[string]any{
							"temporal_view":          "coordination_pilot_report",
							"row_count":              3,
							"effective_capabilities": []string{"executor"},
							"policy_decision":        "token_default",
						},
					},
				},
			})
		case "/v1/explain/tuple":
			var request ExplainTupleRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode explain request: %v", err)
			}
			if request.PolicyContext == nil || len(request.PolicyContext.Capabilities) != 1 {
				t.Fatalf("expected explain policy context, got %#v", request.PolicyContext)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"trace": map[string]any{
					"root": 7,
					"tuples": []map[string]any{
						{
							"tuple": map[string]any{
								"id":        7,
								"predicate": 42,
								"values": []map[string]any{
									{"Entity": 1},
									{"String": "worker-b"},
								},
							},
							"metadata": map[string]any{
								"rule_id":          5,
								"predicate_id":     42,
								"stratum":          0,
								"scc_id":           0,
								"iteration":        1,
								"parent_tuple_ids": []uint64{3},
								"source_datom_ids": []uint64{9},
								"imported_cuts":    []any{},
							},
							"policy": map[string]any{
								"capabilities": []string{"executor"},
							},
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	api := New(server.URL, "")

	history, err := api.History(context.Background())
	if err != nil {
		t.Fatalf("history failed: %v", err)
	}
	if len(history.Datoms) != 1 || history.Datoms[0].Element != 9 {
		t.Fatalf("unexpected history response %#v", history)
	}
	if history.Datoms[0].Value.Display() != `"ready"` {
		t.Fatalf("unexpected history value %q", history.Datoms[0].Value.Display())
	}

	audit, err := api.AuditLog(context.Background())
	if err != nil {
		t.Fatalf("audit failed: %v", err)
	}
	if len(audit.Entries) != 1 || audit.Entries[0].Context.PolicyDecision == nil || *audit.Entries[0].Context.PolicyDecision != "token_default" {
		t.Fatalf("unexpected audit response %#v", audit)
	}

	explain, err := api.ExplainTupleWithPolicy(context.Background(), 7, &PolicyContext{
		Capabilities: []string{"executor"},
	})
	if err != nil {
		t.Fatalf("explain failed: %v", err)
	}
	if explain.Trace.Root != 7 || len(explain.Trace.Tuples) != 1 {
		t.Fatalf("unexpected explain response %#v", explain)
	}
	if explain.Trace.Tuples[0].Policy == nil || len(explain.Trace.Tuples[0].Policy.Capabilities) != 1 {
		t.Fatalf("expected policy on trace tuple: %#v", explain.Trace.Tuples[0])
	}
}
