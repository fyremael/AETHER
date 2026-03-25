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
