package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type ElementID = uint64
type TupleID = uint64

type PolicyContext struct {
	Capabilities []string `json:"capabilities,omitempty"`
	Visibilities []string `json:"visibilities,omitempty"`
}

func (p *PolicyContext) IsEmpty() bool {
	return p == nil || (len(p.Capabilities) == 0 && len(p.Visibilities) == 0)
}

type PolicyEnvelope struct {
	Capabilities []string `json:"capabilities,omitempty"`
	Visibilities []string `json:"visibilities,omitempty"`
}

type SourceRef struct {
	URI    string  `json:"uri"`
	Digest *string `json:"digest,omitempty"`
}

type DatomProvenance struct {
	AuthorPrincipal string      `json:"author_principal"`
	AgentID         string      `json:"agent_id"`
	ToolID          string      `json:"tool_id"`
	SessionID       string      `json:"session_id"`
	SourceRef       SourceRef   `json:"source_ref"`
	ParentDatomIDs  []ElementID `json:"parent_datom_ids"`
	Confidence      float32     `json:"confidence"`
	TrustDomain     string      `json:"trust_domain"`
	SchemaVersion   string      `json:"schema_version"`
}

type CausalContext struct {
	Frontier []ElementID `json:"frontier"`
}

type Value struct {
	Kind   string
	Bool   bool
	I64    int64
	U64    uint64
	F64    float64
	String string
	Bytes  []byte
	Entity uint64
	List   []Value
}

func (v *Value) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil
	}

	var unit string
	if err := json.Unmarshal(trimmed, &unit); err == nil {
		if unit == "Null" {
			*v = Value{Kind: "Null"}
			return nil
		}
	}

	var tagged map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &tagged); err != nil {
		return fmt.Errorf("decode tagged value: %w", err)
	}
	if len(tagged) != 1 {
		return fmt.Errorf("expected single tagged value, got %d keys", len(tagged))
	}

	for kind, payload := range tagged {
		switch kind {
		case "Null":
			*v = Value{Kind: "Null"}
			return nil
		case "Bool":
			var value bool
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			*v = Value{Kind: kind, Bool: value}
			return nil
		case "I64":
			var value int64
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			*v = Value{Kind: kind, I64: value}
			return nil
		case "U64":
			var value uint64
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			*v = Value{Kind: kind, U64: value}
			return nil
		case "F64":
			var value float64
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			*v = Value{Kind: kind, F64: value}
			return nil
		case "String":
			var value string
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			*v = Value{Kind: kind, String: value}
			return nil
		case "Bytes":
			var value []byte
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			*v = Value{Kind: kind, Bytes: value}
			return nil
		case "Entity":
			var value uint64
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			*v = Value{Kind: kind, Entity: value}
			return nil
		case "List":
			var value []Value
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			*v = Value{Kind: kind, List: value}
			return nil
		default:
			return fmt.Errorf("unsupported value kind %q", kind)
		}
	}

	return nil
}

func (v Value) Display() string {
	switch v.Kind {
	case "", "Null":
		return "null"
	case "Bool":
		if v.Bool {
			return "true"
		}
		return "false"
	case "I64":
		return fmt.Sprintf("%d", v.I64)
	case "U64":
		return fmt.Sprintf("%d", v.U64)
	case "F64":
		return fmt.Sprintf("%g", v.F64)
	case "String":
		return fmt.Sprintf("%q", v.String)
	case "Bytes":
		return fmt.Sprintf("%v", v.Bytes)
	case "Entity":
		return fmt.Sprintf("entity(%d)", v.Entity)
	case "List":
		parts := make([]string, 0, len(v.List))
		for _, item := range v.List {
			parts = append(parts, item.Display())
		}
		return "[" + strings.Join(parts, ", ") + "]"
	default:
		return "<unknown>"
	}
}

type Datom struct {
	Entity        uint64          `json:"entity"`
	Attribute     uint64          `json:"attribute"`
	Value         Value           `json:"value"`
	Op            string          `json:"op"`
	Element       uint64          `json:"element"`
	Replica       uint64          `json:"replica"`
	CausalContext CausalContext   `json:"causal_context"`
	Provenance    DatomProvenance `json:"provenance"`
	Policy        *PolicyEnvelope `json:"policy,omitempty"`
}

type HistoryResponse struct {
	Datoms []Datom `json:"datoms"`
}

type RunDocumentRequest struct {
	DSL           string         `json:"dsl"`
	PolicyContext *PolicyContext `json:"policy_context,omitempty"`
}

type ExplainTupleRequest struct {
	TupleID       uint64         `json:"tuple_id"`
	PolicyContext *PolicyContext `json:"policy_context,omitempty"`
}

type HealthResponse struct {
	Status string `json:"status"`
}

type AuditContext struct {
	TemporalView          *string  `json:"temporal_view"`
	QueryGoal             *string  `json:"query_goal"`
	TupleID               *uint64  `json:"tuple_id"`
	RequestedElement      *uint64  `json:"requested_element"`
	DatomCount            *int     `json:"datom_count"`
	EntityCount           *int     `json:"entity_count"`
	RowCount              *int     `json:"row_count"`
	DerivedTupleCount     *int     `json:"derived_tuple_count"`
	TraceTupleCount       *int     `json:"trace_tuple_count"`
	LastElement           *uint64  `json:"last_element"`
	RequestedCapabilities []string `json:"requested_capabilities"`
	RequestedVisibilities []string `json:"requested_visibilities"`
	GrantedCapabilities   []string `json:"granted_capabilities"`
	GrantedVisibilities   []string `json:"granted_visibilities"`
	EffectiveCapabilities []string `json:"effective_capabilities"`
	EffectiveVisibilities []string `json:"effective_visibilities"`
	PolicyDecision        *string  `json:"policy_decision"`
}

type AuditEntry struct {
	TimestampMS uint64       `json:"timestamp_ms"`
	Principal   string       `json:"principal"`
	Method      string       `json:"method"`
	Path        string       `json:"path"`
	Status      uint16       `json:"status"`
	Scope       string       `json:"scope"`
	Outcome     string       `json:"outcome"`
	Detail      *string      `json:"detail"`
	Context     AuditContext `json:"context"`
}

type AuditLogResponse struct {
	Entries []AuditEntry `json:"entries"`
}

type CoordinationPilotReportRequest struct {
	PolicyContext *PolicyContext `json:"policy_context,omitempty"`
}

type ReportRow struct {
	TupleID *uint64 `json:"tuple_id,omitempty"`
	Values  []Value `json:"values"`
}

type TraceTupleSummary struct {
	TupleID        uint64      `json:"tuple_id"`
	Values         []Value     `json:"values"`
	Iteration      int         `json:"iteration"`
	SourceDatomIDs []ElementID `json:"source_datom_ids"`
	ParentTupleIDs []TupleID   `json:"parent_tuple_ids"`
}

type TraceSummary struct {
	Root       uint64              `json:"root"`
	TupleCount int                 `json:"tuple_count"`
	Tuples     []TraceTupleSummary `json:"tuples"`
}

type CoordinationPilotReport struct {
	GeneratedAtMS          uint64         `json:"generated_at_ms"`
	PolicyContext          *PolicyContext `json:"policy_context,omitempty"`
	HistoryLen             int            `json:"history_len"`
	PreHeartbeatAuthorized []ReportRow    `json:"pre_heartbeat_authorized"`
	AsOfAuthorized         []ReportRow    `json:"as_of_authorized"`
	LiveHeartbeats         []ReportRow    `json:"live_heartbeats"`
	CurrentAuthorized      []ReportRow    `json:"current_authorized"`
	Claimable              []ReportRow    `json:"claimable"`
	AcceptedOutcomes       []ReportRow    `json:"accepted_outcomes"`
	RejectedOutcomes       []ReportRow    `json:"rejected_outcomes"`
	Trace                  *TraceSummary  `json:"trace,omitempty"`
}

type Tuple struct {
	ID        uint64  `json:"id"`
	Predicate uint64  `json:"predicate"`
	Values    []Value `json:"values"`
}

type ImportedCut struct {
	Partition string  `json:"partition"`
	AsOf      *uint64 `json:"as_of,omitempty"`
}

type DerivedTupleMetadata struct {
	RuleID         uint64        `json:"rule_id"`
	PredicateID    uint64        `json:"predicate_id"`
	Stratum        int           `json:"stratum"`
	SccID          int           `json:"scc_id"`
	Iteration      int           `json:"iteration"`
	ParentTupleIDs []TupleID     `json:"parent_tuple_ids"`
	SourceDatomIDs []ElementID   `json:"source_datom_ids"`
	ImportedCuts   []ImportedCut `json:"imported_cuts"`
}

type DerivedTuple struct {
	Tuple    Tuple                `json:"tuple"`
	Metadata DerivedTupleMetadata `json:"metadata"`
	Policy   *PolicyEnvelope      `json:"policy,omitempty"`
}

type DerivationTrace struct {
	Root   uint64         `json:"root"`
	Tuples []DerivedTuple `json:"tuples"`
}

type ExplainTupleResponse struct {
	Trace DerivationTrace `json:"trace"`
}

func FormatPolicyContext(policy *PolicyContext) string {
	if policy == nil || policy.IsEmpty() {
		return "public"
	}
	capabilities := "-"
	if len(policy.Capabilities) > 0 {
		capabilities = strings.Join(policy.Capabilities, ", ")
	}
	visibilities := "-"
	if len(policy.Visibilities) > 0 {
		visibilities = strings.Join(policy.Visibilities, ", ")
	}
	return fmt.Sprintf("capabilities=[%s] visibilities=[%s]", capabilities, visibilities)
}

func FormatPolicyEnvelope(policy *PolicyEnvelope) string {
	if policy == nil {
		return "public"
	}
	return FormatPolicyContext(&PolicyContext{
		Capabilities: policy.Capabilities,
		Visibilities: policy.Visibilities,
	})
}

func FormatValues(values []Value) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, value.Display())
	}
	return strings.Join(parts, ", ")
}
