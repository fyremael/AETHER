use aether_ast::PolicyContext;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceMode {
    #[default]
    SingleNode,
    Partitioned,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceStatusStorage {
    #[serde(default = "default_storage_backend")]
    pub backend: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_root: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres_schema: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub postgres_url_configured: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres_tls_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres_ca_certificate_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres_client_certificate_configured: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres_system_roots_enabled: Option<bool>,
    #[serde(default = "default_sidecar_mode")]
    pub sidecar_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecar_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_log_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_root: Option<PathBuf>,
}

impl Default for ServiceStatusStorage {
    fn default() -> Self {
        Self {
            backend: default_storage_backend(),
            database_path: None,
            data_root: None,
            postgres_schema: None,
            postgres_url_configured: false,
            postgres_tls_mode: None,
            postgres_ca_certificate_count: None,
            postgres_client_certificate_configured: None,
            postgres_system_roots_enabled: None,
            sidecar_mode: default_sidecar_mode(),
            sidecar_path: None,
            audit_log_path: None,
            partition_root: None,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrincipalStatusSummary {
    pub principal: String,
    pub principal_id: String,
    pub token_id: String,
    pub scopes: Vec<String>,
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
    pub source: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub revoked: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicaStatusSummary {
    pub partition: String,
    pub replica_id: u64,
    #[serde(default)]
    pub leader_replica: u64,
    pub role: String,
    pub leader_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_element: Option<u64>,
    pub replication_lag: u64,
    pub healthy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceStatusResponse {
    pub status: String,
    pub build_version: String,
    pub config_version: String,
    pub schema_version: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind_addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_namespace: Option<String>,
    pub service_mode: ServiceMode,
    #[serde(default)]
    pub transport: ServiceTransportStatus,
    pub storage: ServiceStatusStorage,
    #[serde(default)]
    pub active_namespace_count: usize,
    #[serde(default)]
    pub namespaces: Vec<NamespaceStatusSummary>,
    pub principals: Vec<PrincipalStatusSummary>,
    pub replicas: Vec<ReplicaStatusSummary>,
    #[serde(default)]
    pub resource_controls: ServiceResourceControlStatus,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceResourceControlStatus {
    pub max_request_body_bytes: usize,
    pub max_document_bytes: usize,
    pub max_document_rules: usize,
    pub max_runtime_iterations: usize,
    pub max_derived_tuples: usize,
    pub operation_timeout_ms: u64,
    pub max_page_size: usize,
    pub requests_per_minute: usize,
    pub global_worker_limit: usize,
    pub per_namespace_concurrency_limit: usize,
    pub per_namespace_queue_limit: usize,
    pub audit_queue_limit: usize,
    pub execution_retention: usize,
    pub cancellation_semantics: String,
}

impl Default for ServiceResourceControlStatus {
    fn default() -> Self {
        Self {
            max_request_body_bytes: 1_048_576,
            max_document_bytes: 262_144,
            max_document_rules: 512,
            max_runtime_iterations: 4_096,
            max_derived_tuples: 1_000_000,
            operation_timeout_ms: 30_000,
            max_page_size: 500,
            requests_per_minute: 600,
            global_worker_limit: 8,
            per_namespace_concurrency_limit: 1,
            per_namespace_queue_limit: 64,
            audit_queue_limit: 1_024,
            execution_retention: crate::execution::DEFAULT_EXECUTION_RETENTION,
            cancellation_semantics: "cancel_before_start_complete_after_start".into(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct NamespaceStatusSummary {
    pub namespace: String,
    pub principals: Vec<String>,
}

impl ServiceStatusResponse {
    pub fn single_node(
        build_version: impl Into<String>,
        config_version: impl Into<String>,
        schema_version: impl Into<String>,
    ) -> Self {
        Self {
            status: "ok".into(),
            build_version: build_version.into(),
            config_version: config_version.into(),
            schema_version: schema_version.into(),
            capabilities: capability_flags(),
            bind_addr: None,
            effective_namespace: None,
            service_mode: ServiceMode::SingleNode,
            transport: ServiceTransportStatus::default(),
            storage: ServiceStatusStorage::default(),
            active_namespace_count: 1,
            namespaces: Vec::new(),
            principals: Vec::new(),
            replicas: Vec::new(),
            resource_controls: ServiceResourceControlStatus::default(),
        }
    }

    pub fn supports(&self, capability: &str) -> bool {
        self.capabilities
            .iter()
            .any(|candidate| candidate == capability)
    }

    pub fn supports_required_client_contract(&self) -> bool {
        required_client_capabilities()
            .iter()
            .all(|capability| self.supports(capability))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceTransportStatus {
    pub http_mode: String,
    pub listener_loopback: bool,
    pub listener_tls: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_https_origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted_ingress: Option<String>,
}

impl Default for ServiceTransportStatus {
    fn default() -> Self {
        Self {
            http_mode: "loopback_plaintext".into(),
            listener_loopback: true,
            listener_tls: false,
            external_https_origin: None,
            trusted_ingress: None,
        }
    }
}

pub fn capability_flags() -> Vec<String> {
    vec![
        "trace_handles_v1".into(),
        "namespace_schema_ref_v1".into(),
        "append_receipts_v1".into(),
        "structured_errors_v1".into(),
        "capability_negotiation_v1".into(),
        "resource_limits_v1".into(),
        "pagination_v1".into(),
    ]
}

pub fn required_client_capabilities() -> Vec<&'static str> {
    vec![
        "trace_handles_v1",
        "namespace_schema_ref_v1",
        "append_receipts_v1",
        "structured_errors_v1",
        "resource_limits_v1",
        "pagination_v1",
    ]
}

fn default_storage_backend() -> String {
    "sqlite".into()
}

fn default_sidecar_mode() -> String {
    "sqlite_local".into()
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthReloadResponse {
    pub reloaded_at_ms: u64,
    pub principal_count: usize,
    pub revoked_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_status_negotiates_the_required_client_contract() {
        let status = ServiceStatusResponse::single_node("build", "config", "schema");
        assert!(status.supports("trace_handles_v1"));
        assert!(status.supports_required_client_contract());

        let mut old_status = status;
        old_status
            .capabilities
            .retain(|capability| capability != "structured_errors_v1");
        assert!(!old_status.supports_required_client_contract());
    }
}
