package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

type Error struct {
	StatusCode int
	Message    string
	Payload    any
}

func (e *Error) Error() string {
	return fmt.Sprintf("AETHER API error (%d): %s", e.StatusCode, e.Message)
}

func New(baseURL string, token string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	var response HealthResponse
	if err := c.doJSON(ctx, http.MethodGet, "/health", nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) History(ctx context.Context) (*HistoryResponse, error) {
	var response HistoryResponse
	if err := c.doJSON(ctx, http.MethodGet, "/v1/history", nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) AuditLog(ctx context.Context) (*AuditLogResponse, error) {
	var response AuditLogResponse
	if err := c.doJSON(ctx, http.MethodGet, "/v1/audit", nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) CoordinationPilotReport(
	ctx context.Context,
	policy *PolicyContext,
) (*CoordinationPilotReport, error) {
	var response CoordinationPilotReport
	if err := c.doJSON(ctx, http.MethodPost, "/v1/reports/pilot/coordination", CoordinationPilotReportRequest{
		PolicyContext: policy,
	}, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) RunDocument(ctx context.Context, request RunDocumentRequest) (map[string]any, error) {
	var response map[string]any
	if err := c.doJSON(ctx, http.MethodPost, "/v1/documents/run", request, &response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Client) ExplainTuple(ctx context.Context, tupleID uint64) (*ExplainTupleResponse, error) {
	return c.ExplainTupleWithPolicy(ctx, tupleID, nil)
}

func (c *Client) ExplainTupleWithPolicy(
	ctx context.Context,
	tupleID uint64,
	policy *PolicyContext,
) (*ExplainTupleResponse, error) {
	var response ExplainTupleResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/explain/tuple", ExplainTupleRequest{
		TupleID:       tupleID,
		PolicyContext: policy,
	}, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) doJSON(ctx context.Context, method string, path string, payload any, out any) error {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}

	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	rawBody, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}

	if response.StatusCode >= 400 {
		var payloadBody any
		if len(rawBody) > 0 {
			_ = json.Unmarshal(rawBody, &payloadBody)
		}
		message := strings.TrimSpace(string(rawBody))
		if structured, ok := payloadBody.(map[string]any); ok {
			if value, ok := structured["error"].(string); ok && value != "" {
				message = value
			}
		}
		if message == "" {
			message = response.Status
		}
		return &Error{
			StatusCode: response.StatusCode,
			Message:    message,
			Payload:    payloadBody,
		}
	}

	if out == nil || len(rawBody) == 0 {
		return nil
	}
	return json.Unmarshal(rawBody, out)
}
