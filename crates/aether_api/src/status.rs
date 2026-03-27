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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceStatusStorage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecar_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_log_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_root: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrincipalStatusSummary {
    pub principal: String,
    pub principal_id: String,
    pub token_id: String,
    pub scopes: Vec<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind_addr: Option<String>,
    pub service_mode: ServiceMode,
    pub storage: ServiceStatusStorage,
    pub principals: Vec<PrincipalStatusSummary>,
    pub replicas: Vec<ReplicaStatusSummary>,
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
            bind_addr: None,
            service_mode: ServiceMode::SingleNode,
            storage: ServiceStatusStorage::default(),
            principals: Vec::new(),
            replicas: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthReloadResponse {
    pub reloaded_at_ms: u64,
    pub principal_count: usize,
    pub revoked_count: usize,
}
