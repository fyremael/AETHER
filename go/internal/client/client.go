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
	namespace  string
	httpClient *http.Client
}

type Error struct {
	StatusCode int
	Message    string
	Code       string
	RequestID  string
	Details    map[string]any
	Payload    any
}

func (e *Error) Error() string {
	context := ""
	if e.Code != "" {
		context += " code=" + e.Code
	}
	if e.RequestID != "" {
		context += " request_id=" + e.RequestID
	}
	return fmt.Sprintf("AETHER API error (%d%s): %s", e.StatusCode, context, e.Message)
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

func (c *Client) WithNamespace(namespace string) *Client {
	next := *c
	next.namespace = namespace
	return &next
}

func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	var response HealthResponse
	if err := c.doJSON(ctx, http.MethodGet, "/health", nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) Status(ctx context.Context) (*ServiceStatusResponse, error) {
	var response ServiceStatusResponse
	if err := c.doJSON(ctx, http.MethodGet, "/v1/status", nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) RequireCapabilities(ctx context.Context, required ...string) error {
	status, err := c.Status(ctx)
	if err != nil {
		return err
	}
	missing := make([]string, 0)
	for _, capability := range required {
		if !status.Supports(capability) {
			missing = append(missing, capability)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return &Error{
		StatusCode: http.StatusUpgradeRequired,
		Message:    "server is missing required capabilities: " + strings.Join(missing, ", "),
		Code:       "capability_required",
		Details: map[string]any{
			"available": status.Capabilities,
			"missing":   missing,
		},
	}
}

func (c *Client) ReloadAuth(ctx context.Context) (*AuthReloadResponse, error) {
	var response AuthReloadResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/admin/auth/reload", struct{}{}, &response); err != nil {
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

func (c *Client) HistoryPage(ctx context.Context, offset int, limit int) (*PagedHistoryResponse, error) {
	var response PagedHistoryResponse
	path := fmt.Sprintf("/v1/history/page?offset=%d&limit=%d", offset, limit)
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) Append(ctx context.Context, request AppendAdmissionRequest) (*AppendReceipt, error) {
	var response AppendReceipt
	if err := c.doJSON(ctx, http.MethodPost, "/v1/append", request, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) AppendDryRun(ctx context.Context, request AppendAdmissionRequest) (*AppendDryRunResponse, error) {
	var response AppendDryRunResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/append/dry-run", request, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) AppendReceipts(ctx context.Context) ([]AppendReceipt, error) {
	var response []AppendReceipt
	if err := c.doJSON(ctx, http.MethodGet, "/v1/append/receipts", nil, &response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Client) SchemaCatalog(ctx context.Context) (*SchemaCatalogResponse, error) {
	var response SchemaCatalogResponse
	if err := c.doJSON(ctx, http.MethodGet, "/v1/schema", nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) ActiveSchemaRef(ctx context.Context) (*SchemaRef, error) {
	catalog, err := c.SchemaCatalog(ctx)
	if err != nil {
		return nil, err
	}
	if catalog.Active == nil {
		return nil, &Error{
			StatusCode: http.StatusPreconditionFailed,
			Message:    "namespace has no active schema",
			Code:       "active_schema_required",
			Details:    map[string]any{},
		}
	}
	active := catalog.Active.SchemaRef
	return &active, nil
}

func (c *Client) RegisterSchema(ctx context.Context, request RegisterSchemaRequest) (*NamespaceSchemaRevision, error) {
	var response NamespaceSchemaRevision
	if err := c.doJSON(ctx, http.MethodPost, "/v1/schema/register", request, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) ActivateSchema(ctx context.Context, request ActivateSchemaRequest) (*NamespaceSchemaRevision, error) {
	var response NamespaceSchemaRevision
	if err := c.doJSON(ctx, http.MethodPost, "/v1/schema/activate", request, &response); err != nil {
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

func (c *Client) CoordinationDeltaReport(
	ctx context.Context,
	left CoordinationCut,
	right CoordinationCut,
	policy *PolicyContext,
) (*CoordinationDeltaReport, error) {
	var response CoordinationDeltaReport
	if err := c.doJSON(ctx, http.MethodPost, "/v1/reports/pilot/coordination-delta", CoordinationDeltaReportRequest{
		Left:          left,
		Right:         right,
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

func (c *Client) RunDocumentPage(ctx context.Context, request RunDocumentRequest, offset int, limit int) (map[string]any, error) {
	var response map[string]any
	path := fmt.Sprintf("/v1/documents/run/page?offset=%d&limit=%d", offset, limit)
	if err := c.doJSON(ctx, http.MethodPost, path, request, &response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Client) ResolveTraceHandle(ctx context.Context, handle string) (*ResolveTraceHandleResponse, error) {
	return c.ResolveTraceHandleWithPolicy(ctx, handle, nil, false)
}

func (c *Client) ResolveTraceHandlePage(ctx context.Context, handle string, offset int, limit int) (map[string]any, error) {
	var response map[string]any
	path := fmt.Sprintf("/v1/explanations/resolve/page?offset=%d&limit=%d", offset, limit)
	request := ResolveTraceHandleRequest{Handle: handle, VerifyReplay: true}
	if err := c.doJSON(ctx, http.MethodPost, path, request, &response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Client) ResolveTraceHandleWithPolicy(
	ctx context.Context,
	handle string,
	policy *PolicyContext,
	verifyReplay bool,
) (*ResolveTraceHandleResponse, error) {
	var response ResolveTraceHandleResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/explanations/resolve", ResolveTraceHandleRequest{
		Handle:        handle,
		PolicyContext: policy,
		VerifyReplay:  verifyReplay,
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
	if c.namespace != "" {
		request.Header.Set("X-Aether-Namespace", c.namespace)
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
		code := ""
		requestID := response.Header.Get("X-Aether-Request-Id")
		var details map[string]any
		if structured, ok := payloadBody.(map[string]any); ok {
			if value, ok := structured["error"].(string); ok && value != "" {
				message = value
			}
			if value, ok := structured["code"].(string); ok {
				code = value
			}
			if value, ok := structured["request_id"].(string); ok && value != "" {
				requestID = value
			}
			if value, ok := structured["details"].(map[string]any); ok {
				details = value
			}
		}
		if message == "" {
			message = response.Status
		}
		return &Error{
			StatusCode: response.StatusCode,
			Message:    message,
			Code:       code,
			RequestID:  requestID,
			Details:    details,
			Payload:    payloadBody,
		}
	}

	if out == nil || len(rawBody) == 0 {
		return nil
	}
	return json.Unmarshal(rawBody, out)
}
