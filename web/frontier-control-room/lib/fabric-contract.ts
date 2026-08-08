export type SourceState = "current" | "degraded" | "unconfigured";

export type SourceObservation = {
  id: "service" | "journal" | "release";
  kind: "aether-http" | "aether-journal" | "github-actions";
  state: SourceState;
  observed_at: string;
  latency_ms: number | null;
  detail: string;
};

export type FabricDatom = {
  element: number;
  entity: string;
  attribute: string;
  value: string;
  op: string;
  replica: number | null;
  causal_frontier: number[];
  provenance: {
    agent_id: string | null;
    tool_id: string | null;
    trust_domain: string | null;
    confidence: number | null;
    source_digest: string | null;
  };
};

export type FabricAuditEvent = {
  timestamp_ms: number;
  method: string;
  path: string;
  status: number;
  scope: string;
  outcome: string;
  temporal_view: string | null;
  selected_cut: string | null;
  query_goal: string | null;
  last_element: number | null;
  policy_decision: string | null;
};

export type ReleaseRun = {
  lane: "ci" | "supply_chain" | "pages" | "capacity" | "release_readiness";
  name: string;
  run_id: number;
  run_attempt: number;
  status: string;
  conclusion: string | null;
  tooling_sha: string;
  event: string;
  updated_at: string;
  url: string;
};

export type FabricEnvelope = {
  contract_version: "aether.orbital.fabric.v1";
  generated_at: string;
  mode: "live" | "hybrid" | "disconnected";
  read_only: true;
  sources: SourceObservation[];
  service: null | {
    status: string;
    build_version: string;
    config_version: string;
    schema_version: string;
    service_mode: string;
    storage_backend: string;
    sidecar_mode: string;
    active_namespace_count: number;
    capabilities: string[];
    replicas: Array<{
      partition: string;
      replica_id: number;
      role: string;
      leader_epoch: number;
      applied_element: number | null;
      replication_lag: number;
      healthy: boolean;
    }>;
    resource_controls: {
      operation_timeout_ms: number | null;
      max_page_size: number | null;
      global_worker_limit: number | null;
      per_namespace_queue_limit: number | null;
    };
  };
  journal: {
    total: number;
    latest_element: number | null;
    datoms: FabricDatom[];
    audit_events: FabricAuditEvent[];
  };
  release: {
    repository: string | null;
    product_sha: string | null;
    tooling_sha: string | null;
    identity_binding: "verified" | "unverified";
    promotion_authorized: boolean;
    gates: ReleaseRun[];
  };
  warnings: string[];
};
