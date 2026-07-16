use aether_service_core::*;

#[doc(hidden)]
pub mod evaluation {
    pub use aether_service_core::evaluation::*;
}
pub mod execution {
    pub use aether_service_core::execution::*;
}

use crate::{
    evaluation::{FederationIdentityMaterial, ScopedEvaluationBuilder},
    execution::{
        persist_execution, resolve_trace, ExecutionStore, InMemoryExecutionStore,
        SqliteExecutionStore,
    },
};
use aether_ast::{
    merge_partition_cuts, Atom, Datom, ExplainSpec, ExplainTarget, ExtensionalFact, FactProvenance,
    FederatedCut, PartitionCut, PartitionId, PolicyContext, PolicyScope, PredicateRef, QueryResult,
    QueryRow, ReplicaId, SourceRef, TemporalView, TupleId, Value,
};
use aether_explain::{Explainer, InMemoryExplainer};
use aether_resolver::ResolvedState;
use aether_runtime::{execute_scoped_query, DerivedSet, EvaluationBundle};
use aether_schema::{PredicateSignature, Schema, ValueType};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::BTreeSet,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Default)]
pub struct PartitionedInMemoryKernelService {
    partitions: IndexMap<PartitionId, InMemoryKernelService>,
    execution_store: InMemoryExecutionStore,
}

#[derive(Debug)]
pub struct SqlitePartitionedKernelService {
    root: PathBuf,
    partitions: RefCell<IndexMap<PartitionId, crate::SqliteKernelService>>,
    execution_store: SqliteExecutionStore,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaderEpoch(pub u64);

impl LeaderEpoch {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaRole {
    #[default]
    Leader,
    Follower,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicaConfig {
    pub replica_id: ReplicaId,
    pub database_path: PathBuf,
    pub role: ReplicaRole,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorityPartitionConfig {
    pub partition: PartitionId,
    pub replicas: Vec<ReplicaConfig>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicaStatus {
    pub partition: PartitionId,
    pub replica_id: ReplicaId,
    pub role: ReplicaRole,
    pub leader_epoch: LeaderEpoch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_element: Option<aether_ast::ElementId>,
    pub replication_lag: u64,
    pub healthy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionStatus {
    pub partition: PartitionId,
    pub leader_epoch: LeaderEpoch,
    #[serde(default)]
    pub leader_replica: ReplicaId,
    pub replicas: Vec<ReplicaStatus>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionStatusResponse {
    pub partitions: Vec<PartitionStatus>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PromoteReplicaRequest {
    pub partition: PartitionId,
    pub replica_id: ReplicaId,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PromoteReplicaResponse {
    pub partition: PartitionId,
    pub replica_id: ReplicaId,
    pub leader_epoch: LeaderEpoch,
}

#[derive(Debug)]
pub struct ReplicatedAuthorityPartitionService {
    root: PathBuf,
    partitions: IndexMap<PartitionId, Arc<Mutex<ReplicatedPartition>>>,
    execution_store: Mutex<SqliteExecutionStore>,
}

#[derive(Debug)]
struct ReplicatedPartition {
    metadata_path: PathBuf,
    metadata: ReplicatedPartitionMetadata,
    replicas: IndexMap<ReplicaId, crate::SqliteKernelService>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct ReplicatedPartitionMetadata {
    partition: PartitionId,
    leader_epoch: LeaderEpoch,
    replicas: Vec<ReplicaConfig>,
}

impl PartitionedInMemoryKernelService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append_partition(
        &mut self,
        request: PartitionAppendRequest,
    ) -> Result<PartitionAppendResponse, ApiError> {
        let PartitionAppendRequest {
            partition,
            schema_ref,
            expected_cut,
            idempotency_key,
            datoms,
            principal,
            ..
        } = request;
        let receipt = self
            .partitions
            .entry(partition.clone())
            .or_default()
            .admit_append(crate::AppendAdmissionRequest {
                schema_ref,
                expected_cut,
                idempotency_key,
                datoms,
                principal,
            })?;
        Ok(PartitionAppendResponse {
            partition,
            leader_epoch: None,
            appended: receipt.appended,
            receipt: Some(receipt),
        })
    }

    pub fn partition_history(
        &self,
        request: PartitionHistoryRequest,
    ) -> Result<PartitionHistoryResponse, ApiError> {
        partition_history_for(self.partition_service(&request.cut.partition)?, request)
    }

    pub fn partition_state(
        &self,
        request: PartitionStateRequest,
    ) -> Result<PartitionStateResponse, ApiError> {
        partition_state_for(self.partition_service(&request.cut.partition)?, request)
    }

    pub fn federated_history(
        &self,
        request: FederatedHistoryRequest,
    ) -> Result<FederatedHistoryResponse, ApiError> {
        let cut = validate_federated_cut(request.cut)?;
        let mut partitions = Vec::with_capacity(cut.cuts.len());
        for partition_cut in cut.cuts {
            partitions.push(self.partition_history(PartitionHistoryRequest {
                cut: partition_cut,
                policy_context: request.policy_context.clone(),
            })?);
        }
        Ok(FederatedHistoryResponse { partitions })
    }

    pub fn import_partition_facts(
        &mut self,
        request: ImportedFactQueryRequest,
        policy_context: Option<PolicyContext>,
    ) -> Result<ImportedFactQueryResponse, ApiError> {
        import_partition_facts_from_service(
            self.partition_service_mut(&request.cut.partition)?,
            request,
            policy_context,
        )
    }

    pub fn federated_run_document(
        &mut self,
        request: FederatedRunDocumentRequest,
    ) -> Result<FederatedRunDocumentResponse, ApiError> {
        let imports = request
            .imports
            .iter()
            .cloned()
            .map(|import| self.import_partition_facts(import, request.policy_context.clone()))
            .collect::<Result<Vec<_>, ApiError>>()?;
        execute_federated_document_request(request, imports, &mut self.execution_store)
    }

    pub fn build_federated_explain_report(
        &mut self,
        request: FederatedRunDocumentRequest,
    ) -> Result<FederatedExplainReport, ApiError> {
        let policy_context = request.policy_context.clone();
        let response = self.federated_run_document(request)?;
        Ok(build_federated_explain_report_from_response(
            response,
            policy_context,
        ))
    }

    pub fn resolve_trace_handle(
        &mut self,
        request: ResolveTraceHandleRequest,
    ) -> Result<ResolveTraceHandleResponse, ApiError> {
        Ok(resolve_trace(
            &mut self.execution_store,
            &NamespaceId::default(),
            request,
        )?)
    }

    fn partition_service(
        &self,
        partition: &PartitionId,
    ) -> Result<&InMemoryKernelService, ApiError> {
        self.partitions
            .get(partition)
            .ok_or_else(|| ApiError::Validation(format!("unknown partition {}", partition)))
    }

    fn partition_service_mut(
        &mut self,
        partition: &PartitionId,
    ) -> Result<&mut InMemoryKernelService, ApiError> {
        self.partitions
            .get_mut(partition)
            .ok_or_else(|| ApiError::Validation(format!("unknown partition {}", partition)))
    }
}

impl SqlitePartitionedKernelService {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ApiError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|error| {
            ApiError::Validation(format!(
                "failed to create partition root {}: {}",
                root.display(),
                error
            ))
        })?;
        let execution_store = SqliteExecutionStore::open(root.join("federated.executions.sqlite"))
            .map_err(crate::execution::ExecutionError::from)?;
        Ok(Self {
            root,
            partitions: RefCell::new(IndexMap::new()),
            execution_store,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn append_partition(
        &mut self,
        request: PartitionAppendRequest,
    ) -> Result<PartitionAppendResponse, ApiError> {
        let PartitionAppendRequest {
            partition,
            schema_ref,
            expected_cut,
            idempotency_key,
            datoms,
            principal,
            ..
        } = request;
        self.ensure_partition_open(&partition, true)?;
        let mut partitions = self.partitions.borrow_mut();
        let receipt = partitions
            .get_mut(&partition)
            .expect("partition should be open")
            .admit_append(crate::AppendAdmissionRequest {
                schema_ref,
                expected_cut,
                idempotency_key,
                datoms,
                principal,
            })?;
        Ok(PartitionAppendResponse {
            partition,
            leader_epoch: None,
            appended: receipt.appended,
            receipt: Some(receipt),
        })
    }

    pub fn partition_history(
        &self,
        request: PartitionHistoryRequest,
    ) -> Result<PartitionHistoryResponse, ApiError> {
        self.ensure_partition_open(&request.cut.partition, false)?;
        let mut partitions = self.partitions.borrow_mut();
        let service = partitions
            .get_mut(&request.cut.partition)
            .expect("partition should be open");
        partition_history_for(service, request)
    }

    pub fn partition_state(
        &self,
        request: PartitionStateRequest,
    ) -> Result<PartitionStateResponse, ApiError> {
        self.ensure_partition_open(&request.cut.partition, false)?;
        let mut partitions = self.partitions.borrow_mut();
        let service = partitions
            .get_mut(&request.cut.partition)
            .expect("partition should be open");
        partition_state_for(service, request)
    }

    pub fn federated_history(
        &self,
        request: FederatedHistoryRequest,
    ) -> Result<FederatedHistoryResponse, ApiError> {
        let cut = validate_federated_cut(request.cut)?;
        let mut partitions = Vec::with_capacity(cut.cuts.len());
        for partition_cut in cut.cuts {
            partitions.push(self.partition_history(PartitionHistoryRequest {
                cut: partition_cut,
                policy_context: request.policy_context.clone(),
            })?);
        }
        Ok(FederatedHistoryResponse { partitions })
    }

    pub fn import_partition_facts(
        &mut self,
        request: ImportedFactQueryRequest,
        policy_context: Option<PolicyContext>,
    ) -> Result<ImportedFactQueryResponse, ApiError> {
        self.ensure_partition_open(&request.cut.partition, false)?;
        let mut partitions = self.partitions.borrow_mut();
        let service = partitions
            .get_mut(&request.cut.partition)
            .expect("partition should be open");
        import_partition_facts_from_service(service, request, policy_context)
    }

    pub fn federated_run_document(
        &mut self,
        request: FederatedRunDocumentRequest,
    ) -> Result<FederatedRunDocumentResponse, ApiError> {
        let imports = request
            .imports
            .iter()
            .cloned()
            .map(|import| self.import_partition_facts(import, request.policy_context.clone()))
            .collect::<Result<Vec<_>, ApiError>>()?;
        execute_federated_document_request(request, imports, &mut self.execution_store)
    }

    pub fn build_federated_explain_report(
        &mut self,
        request: FederatedRunDocumentRequest,
    ) -> Result<FederatedExplainReport, ApiError> {
        let policy_context = request.policy_context.clone();
        let response = self.federated_run_document(request)?;
        Ok(build_federated_explain_report_from_response(
            response,
            policy_context,
        ))
    }

    pub fn resolve_trace_handle(
        &mut self,
        request: ResolveTraceHandleRequest,
    ) -> Result<ResolveTraceHandleResponse, ApiError> {
        Ok(resolve_trace(
            &mut self.execution_store,
            &NamespaceId::default(),
            request,
        )?)
    }

    fn ensure_partition_open(
        &self,
        partition: &PartitionId,
        create_if_missing: bool,
    ) -> Result<(), ApiError> {
        if self.partitions.borrow().contains_key(partition) {
            return Ok(());
        }

        let path = self.partition_path(partition);
        if !create_if_missing && !path.exists() {
            return Err(ApiError::Validation(format!(
                "unknown partition {}",
                partition
            )));
        }

        let service = crate::SqliteKernelService::open(&path)?;
        self.partitions
            .borrow_mut()
            .insert(partition.clone(), service);
        Ok(())
    }

    fn partition_path(&self, partition: &PartitionId) -> PathBuf {
        self.root.join(format!(
            "partition-{}.sqlite",
            encode_partition_id(partition)
        ))
    }
}

impl ReplicatedAuthorityPartitionService {
    pub fn open(
        root: impl AsRef<Path>,
        configs: Vec<AuthorityPartitionConfig>,
    ) -> Result<Self, ApiError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|error| {
            ApiError::Validation(format!(
                "failed to create replication root {}: {}",
                root.display(),
                error
            ))
        })?;
        let mut partitions = IndexMap::new();
        for config in configs {
            let partition = config.partition.clone();
            let runtime = ReplicatedPartition::open(&root, config)?;
            partitions.insert(partition, Arc::new(Mutex::new(runtime)));
        }
        let execution_store = SqliteExecutionStore::open(root.join("federated.executions.sqlite"))
            .map_err(crate::execution::ExecutionError::from)?;
        Ok(Self {
            root,
            partitions,
            execution_store: Mutex::new(execution_store),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn append_partition(
        &self,
        request: PartitionAppendRequest,
    ) -> Result<PartitionAppendResponse, ApiError> {
        let mut partition = self.partition(&request.partition)?;
        partition.append(request)
    }

    pub fn partition_history(
        &self,
        request: PartitionHistoryRequest,
    ) -> Result<PartitionHistoryResponse, ApiError> {
        let mut partition = self.partition(&request.cut.partition)?;
        partition_history_for(partition.leader_service_mut()?, request)
    }

    pub fn partition_state(
        &self,
        request: PartitionStateRequest,
    ) -> Result<PartitionStateResponse, ApiError> {
        let mut partition = self.partition(&request.cut.partition)?;
        partition_state_for(partition.leader_service_mut()?, request)
    }

    pub fn federated_history(
        &self,
        request: FederatedHistoryRequest,
    ) -> Result<FederatedHistoryResponse, ApiError> {
        let cut = validate_federated_cut(request.cut)?;
        let mut partitions = Vec::with_capacity(cut.cuts.len());
        for partition_cut in cut.cuts {
            partitions.push(self.partition_history(PartitionHistoryRequest {
                cut: partition_cut,
                policy_context: request.policy_context.clone(),
            })?);
        }
        Ok(FederatedHistoryResponse { partitions })
    }

    pub fn import_partition_facts(
        &self,
        request: ImportedFactQueryRequest,
        policy_context: Option<PolicyContext>,
    ) -> Result<ImportedFactQueryResponse, ApiError> {
        let mut partition = self.partition(&request.cut.partition)?;
        let epoch = partition.metadata.leader_epoch.clone();
        let mut response = import_partition_facts_from_service(
            partition.leader_service_mut()?,
            request,
            policy_context,
        )?;
        response.leader_epoch = Some(epoch);
        Ok(response)
    }

    pub fn federated_run_document(
        &self,
        request: FederatedRunDocumentRequest,
    ) -> Result<FederatedRunDocumentResponse, ApiError> {
        let imports = request
            .imports
            .iter()
            .cloned()
            .map(|import| self.import_partition_facts(import, request.policy_context.clone()))
            .collect::<Result<Vec<_>, ApiError>>()?;
        let mut execution_store = self.execution_store.lock().map_err(|_| {
            ApiError::Validation("partition execution store lock is poisoned".into())
        })?;
        let response = execute_federated_document_request(request, imports, &mut *execution_store)?;
        Ok(response)
    }

    pub fn build_federated_explain_report(
        &self,
        request: FederatedRunDocumentRequest,
    ) -> Result<FederatedExplainReport, ApiError> {
        let policy_context = request.policy_context.clone();
        let response = self.federated_run_document(request)?;
        Ok(build_federated_explain_report_from_response(
            response,
            policy_context,
        ))
    }

    pub fn resolve_trace_handle(
        &self,
        request: ResolveTraceHandleRequest,
    ) -> Result<ResolveTraceHandleResponse, ApiError> {
        let mut execution_store = self.execution_store.lock().map_err(|_| {
            ApiError::Validation("partition execution store lock is poisoned".into())
        })?;
        Ok(resolve_trace(
            &mut *execution_store,
            &NamespaceId::default(),
            request,
        )?)
    }

    pub fn partition_status(&self) -> Result<PartitionStatusResponse, ApiError> {
        let mut statuses = Vec::new();
        for partition in self.partitions.values() {
            statuses.push(
                partition
                    .lock()
                    .map_err(|_| ApiError::Validation("partition service lock is poisoned".into()))?
                    .status()?,
            );
        }
        Ok(PartitionStatusResponse {
            partitions: statuses,
        })
    }

    pub fn promote_replica(
        &self,
        request: PromoteReplicaRequest,
    ) -> Result<PromoteReplicaResponse, ApiError> {
        let mut partition = self.partition(&request.partition)?;
        let leader_epoch = partition.promote(request.replica_id)?;
        Ok(PromoteReplicaResponse {
            partition: request.partition,
            replica_id: request.replica_id,
            leader_epoch,
        })
    }

    fn partition(
        &self,
        partition: &PartitionId,
    ) -> Result<MutexGuard<'_, ReplicatedPartition>, ApiError> {
        self.partitions
            .get(partition)
            .ok_or_else(|| ApiError::Validation(format!("unknown partition {partition}")))?
            .lock()
            .map_err(|_| ApiError::Validation(format!("partition {partition} lock is poisoned")))
    }
}

impl ReplicatedPartition {
    fn open(root: &Path, config: AuthorityPartitionConfig) -> Result<Self, ApiError> {
        if config.replicas.is_empty() {
            return Err(ApiError::Validation(format!(
                "replicated partition {} must declare at least one replica",
                config.partition
            )));
        }
        let leader_count = config
            .replicas
            .iter()
            .filter(|replica| replica.role == ReplicaRole::Leader)
            .count();
        if leader_count != 1 {
            return Err(ApiError::Validation(format!(
                "replicated partition {} must declare exactly one leader replica",
                config.partition
            )));
        }
        let metadata_path = root.join(format!(
            "replication-{}.json",
            encode_partition_id(&config.partition)
        ));
        let metadata = if metadata_path.exists() {
            let contents = fs::read_to_string(metadata_path.clone()).map_err(|error| {
                ApiError::Validation(format!(
                    "failed to read replication metadata {}: {}",
                    metadata_path.display(),
                    error
                ))
            })?;
            serde_json::from_str::<ReplicatedPartitionMetadata>(&contents).map_err(|error| {
                ApiError::Validation(format!(
                    "failed to parse replication metadata {}: {}",
                    metadata_path.display(),
                    error
                ))
            })?
        } else {
            let metadata = ReplicatedPartitionMetadata {
                partition: config.partition.clone(),
                leader_epoch: LeaderEpoch::new(1),
                replicas: config.replicas,
            };
            write_partition_metadata(&metadata_path, &metadata)?;
            metadata
        };

        let mut seen = BTreeSet::new();
        let leader_count = metadata
            .replicas
            .iter()
            .filter(|replica| replica.role == ReplicaRole::Leader)
            .count();
        if leader_count != 1 {
            return Err(ApiError::Validation(format!(
                "replicated partition {} metadata must contain exactly one leader",
                metadata.partition
            )));
        }

        let mut replicas = IndexMap::new();
        for replica in &metadata.replicas {
            if !seen.insert(replica.replica_id) {
                return Err(ApiError::Validation(format!(
                    "replicated partition {} has duplicate replica {}",
                    metadata.partition, replica.replica_id.0
                )));
            }
            let database_path = if replica.database_path.is_absolute() {
                replica.database_path.clone()
            } else {
                root.join(&replica.database_path)
            };
            replicas.insert(
                replica.replica_id,
                crate::SqliteKernelService::open(database_path)?,
            );
        }

        let mut partition = Self {
            metadata_path,
            metadata,
            replicas,
        };
        partition.replicate_followers()?;
        Ok(partition)
    }

    fn append(
        &mut self,
        request: PartitionAppendRequest,
    ) -> Result<PartitionAppendResponse, ApiError> {
        if let Some(leader_epoch) = request.leader_epoch.as_ref() {
            if *leader_epoch != self.metadata.leader_epoch {
                return Err(ApiError::Validation(format!(
                    "stale leader epoch {} for partition {}; current epoch is {}",
                    leader_epoch.0, request.partition, self.metadata.leader_epoch.0
                )));
            }
        }
        let leader_id = self.leader_id()?;
        self.append_via_replica(leader_id, request)
    }

    fn append_via_replica(
        &mut self,
        replica_id: ReplicaId,
        request: PartitionAppendRequest,
    ) -> Result<PartitionAppendResponse, ApiError> {
        let config = self.replica_config(replica_id)?;
        if config.role != ReplicaRole::Leader {
            return Err(ApiError::Validation(format!(
                "replica {} for partition {} is read-only follower",
                replica_id.0, request.partition
            )));
        }
        let datoms = request.datoms;
        let (receipt, revision) = {
            let service = self
                .replicas
                .get_mut(&replica_id)
                .ok_or_else(|| ApiError::Validation(format!("unknown replica {}", replica_id.0)))?;
            let receipt = service.admit_append(crate::AppendAdmissionRequest {
                schema_ref: request.schema_ref,
                expected_cut: request.expected_cut,
                idempotency_key: request.idempotency_key,
                datoms: datoms.clone(),
                principal: request.principal,
            })?;
            let revision = service
                .schema_catalog()?
                .revisions
                .into_iter()
                .find(|revision| revision.schema_ref == receipt.schema_ref)
                .ok_or_else(|| {
                    ApiError::Validation(
                        "leader receipt schema was absent from the schema catalog".into(),
                    )
                })?;
            (receipt, revision)
        };
        self.replicate_followers_admitted(&datoms, &revision, &receipt)?;
        Ok(PartitionAppendResponse {
            partition: request.partition,
            leader_epoch: Some(self.metadata.leader_epoch.clone()),
            appended: receipt.appended,
            receipt: Some(receipt),
        })
    }

    fn replicate_followers_admitted(
        &mut self,
        datoms: &[Datom],
        revision: &crate::NamespaceSchemaRevision,
        receipt: &crate::AppendReceipt,
    ) -> Result<(), ApiError> {
        let leader_id = self.leader_id()?;
        let follower_ids = self
            .metadata
            .replicas
            .iter()
            .filter(|config| config.replica_id != leader_id)
            .map(|config| config.replica_id)
            .collect::<Vec<_>>();
        for follower_id in follower_ids {
            let follower = self.replicas.get_mut(&follower_id).ok_or_else(|| {
                ApiError::Validation(format!("unknown follower replica {}", follower_id.0))
            })?;
            follower.replicate_admitted_append(revision, receipt, datoms.to_vec())?;
        }
        Ok(())
    }

    fn leader_service_mut(&mut self) -> Result<&mut crate::SqliteKernelService, ApiError> {
        let leader_id = self.leader_id()?;
        self.replicas
            .get_mut(&leader_id)
            .ok_or_else(|| ApiError::Validation(format!("unknown leader replica {}", leader_id.0)))
    }

    fn leader_id(&self) -> Result<ReplicaId, ApiError> {
        self.metadata
            .replicas
            .iter()
            .find(|replica| replica.role == ReplicaRole::Leader)
            .map(|replica| replica.replica_id)
            .ok_or_else(|| {
                ApiError::Validation(format!(
                    "partition {} has no leader replica",
                    self.metadata.partition
                ))
            })
    }

    fn promote(&mut self, replica_id: ReplicaId) -> Result<LeaderEpoch, ApiError> {
        if !self.replicas.contains_key(&replica_id) {
            return Err(ApiError::Validation(format!(
                "unknown replica {} for partition {}",
                replica_id.0, self.metadata.partition
            )));
        }
        self.replicate_followers()?;

        for config in &mut self.metadata.replicas {
            config.role = if config.replica_id == replica_id {
                ReplicaRole::Leader
            } else {
                ReplicaRole::Follower
            };
        }
        self.metadata.leader_epoch.0 += 1;
        write_partition_metadata(&self.metadata_path, &self.metadata)?;
        self.replicate_followers()?;
        Ok(self.metadata.leader_epoch.clone())
    }

    fn replicate_followers(&mut self) -> Result<(), ApiError> {
        let leader_id = self.leader_id()?;
        let (leader_history, leader_receipts, schema_catalog) = {
            let leader = self
                .replicas
                .get_mut(&leader_id)
                .ok_or_else(|| ApiError::Validation(format!("unknown leader {}", leader_id.0)))?;
            (
                leader.authority_history()?,
                leader.authority_append_receipts()?,
                leader.authority_schema_catalog()?,
            )
        };
        for replica in &self.metadata.replicas {
            if replica.replica_id == leader_id {
                continue;
            }
            let follower = self.replicas.get_mut(&replica.replica_id).ok_or_else(|| {
                ApiError::Validation(format!("unknown replica {}", replica.replica_id.0))
            })?;
            let follower_history = follower.authority_history()?;
            validate_replica_prefix(&follower_history, &leader_history).map_err(|detail| {
                ApiError::Validation(format!(
                    "replica {} for partition {} diverged from leader prefix: {}",
                    replica.replica_id.0, self.metadata.partition, detail
                ))
            })?;
            let mut applied = follower_history.len() as u64;
            for receipt in &leader_receipts {
                if receipt.committed_cut.entry_count <= applied {
                    continue;
                }
                if receipt.prior_cut.entry_count != applied {
                    return Err(ApiError::Validation(format!(
                        "replica {} for partition {} is not aligned to a leader receipt boundary",
                        replica.replica_id.0, self.metadata.partition
                    )));
                }
                let revision = schema_catalog
                    .revisions
                    .iter()
                    .find(|revision| revision.schema_ref == receipt.schema_ref)
                    .ok_or_else(|| {
                        ApiError::Validation(
                            "leader receipt schema was absent from the schema catalog".into(),
                        )
                    })?;
                let start = receipt.prior_cut.entry_count as usize;
                let end = receipt.committed_cut.entry_count as usize;
                follower.replicate_admitted_append(
                    revision,
                    receipt,
                    leader_history[start..end].to_vec(),
                )?;
                applied = receipt.committed_cut.entry_count;
            }
            if applied != leader_history.len() as u64 {
                return Err(ApiError::Validation(format!(
                    "replica {} for partition {} cannot catch up because leader history lacks admission receipts",
                    replica.replica_id.0, self.metadata.partition
                )));
            }
        }
        Ok(())
    }

    fn status(&self) -> Result<PartitionStatus, ApiError> {
        let leader_id = self.leader_id()?;
        let leader_history = self
            .replicas
            .get(&leader_id)
            .ok_or_else(|| ApiError::Validation(format!("unknown leader {}", leader_id.0)))?
            .authority_history()?;
        let leader_last = leader_history.last().map(|datom| datom.element);

        let mut replicas = Vec::new();
        for config in &self.metadata.replicas {
            let service = self.replicas.get(&config.replica_id).ok_or_else(|| {
                ApiError::Validation(format!("unknown replica {}", config.replica_id.0))
            })?;
            let history = service.authority_history()?;
            let last = history.last().map(|datom| datom.element);
            let mismatch = validate_replica_prefix(&history, &leader_history).err();
            let replication_lag = leader_last
                .zip(last)
                .map(|(leader, replica)| leader.0.saturating_sub(replica.0))
                .unwrap_or_else(|| leader_last.map(|leader| leader.0).unwrap_or(0));
            let detail = replica_status_detail(mismatch, replication_lag);
            replicas.push(ReplicaStatus {
                partition: self.metadata.partition.clone(),
                replica_id: config.replica_id,
                role: config.role,
                leader_epoch: self.metadata.leader_epoch.clone(),
                applied_element: last,
                replication_lag,
                healthy: detail.is_none(),
                detail,
            });
        }

        Ok(PartitionStatus {
            partition: self.metadata.partition.clone(),
            leader_epoch: self.metadata.leader_epoch.clone(),
            leader_replica: leader_id,
            replicas,
        })
    }

    fn replica_config(&self, replica_id: ReplicaId) -> Result<&ReplicaConfig, ApiError> {
        self.metadata
            .replicas
            .iter()
            .find(|replica| replica.replica_id == replica_id)
            .ok_or_else(|| {
                ApiError::Validation(format!(
                    "unknown replica {} for partition {}",
                    replica_id.0, self.metadata.partition
                ))
            })
    }
}

fn write_partition_metadata(
    path: &Path,
    metadata: &ReplicatedPartitionMetadata,
) -> Result<(), ApiError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            ApiError::Validation(format!(
                "failed to create replication metadata directory {}: {}",
                parent.display(),
                error
            ))
        })?;
    }
    let encoded = serde_json::to_string_pretty(metadata).map_err(|error| {
        ApiError::Validation(format!(
            "failed to encode replication metadata {}: {}",
            path.display(),
            error
        ))
    })?;
    fs::write(path, encoded).map_err(|error| {
        ApiError::Validation(format!(
            "failed to write replication metadata {}: {}",
            path.display(),
            error
        ))
    })
}

fn validate_replica_prefix(
    follower_history: &[Datom],
    leader_history: &[Datom],
) -> Result<(), String> {
    if follower_history.len() > leader_history.len() {
        return Err("follower history exceeds leader history".into());
    }
    for (index, (follower, leader)) in follower_history
        .iter()
        .zip(leader_history.iter())
        .enumerate()
    {
        if follower != leader {
            return Err(format!("entry {} does not match leader", index));
        }
    }
    Ok(())
}

fn replica_status_detail(mismatch: Option<String>, replication_lag: u64) -> Option<String> {
    match (mismatch, replication_lag) {
        (Some(detail), _) => Some(format!("diverged from leader prefix: {detail}")),
        (None, 0) => None,
        (None, lag) => Some(format!("behind leader by element delta {lag}")),
    }
}

fn validate_federated_cut(cut: FederatedCut) -> Result<FederatedCut, ApiError> {
    let normalized = cut.normalized();
    for pair in normalized.cuts.windows(2) {
        if pair[0].partition == pair[1].partition {
            return Err(ApiError::Validation(format!(
                "federated cut contains duplicate partition {}",
                pair[0].partition
            )));
        }
    }
    Ok(normalized)
}

fn partition_history_for(
    service: &dyn KernelService,
    request: PartitionHistoryRequest,
) -> Result<PartitionHistoryResponse, ApiError> {
    let datoms = match request.cut.as_of {
        Some(element) => {
            let visible_history = service
                .history(HistoryRequest {
                    policy_context: request.policy_context.clone(),
                })?
                .datoms;
            let end = visible_history
                .iter()
                .position(|datom| datom.element == element)
                .ok_or_else(|| {
                    ApiError::Validation(format!(
                        "unknown element {} for partition {}",
                        element, request.cut.partition
                    ))
                })?;
            visible_history[..=end].to_vec()
        }
        None => {
            service
                .history(HistoryRequest {
                    policy_context: request.policy_context.clone(),
                })?
                .datoms
        }
    };

    Ok(PartitionHistoryResponse {
        cut: request.cut,
        datoms,
    })
}

fn partition_state_for(
    service: &dyn KernelService,
    request: PartitionStateRequest,
) -> Result<PartitionStateResponse, ApiError> {
    let state = match request.cut.as_of {
        Some(element) => {
            let visible_history = service
                .history(HistoryRequest {
                    policy_context: request.policy_context.clone(),
                })?
                .datoms;
            if !visible_history.iter().any(|datom| datom.element == element) {
                return Err(ApiError::Validation(format!(
                    "unknown element {} for partition {}",
                    element, request.cut.partition
                )));
            }
            service
                .as_of(crate::AsOfRequest {
                    schema: request.schema,
                    datoms: Vec::new(),
                    at: element,
                    policy_context: request.policy_context,
                })?
                .state
        }
        None => {
            service
                .current_state(CurrentStateRequest {
                    schema: request.schema,
                    datoms: Vec::new(),
                    policy_context: request.policy_context,
                })?
                .state
        }
    };

    Ok(PartitionStateResponse {
        cut: request.cut,
        state,
    })
}

fn import_partition_facts_from_service(
    service: &mut dyn KernelService,
    request: ImportedFactQueryRequest,
    policy_context: Option<PolicyContext>,
) -> Result<ImportedFactQueryResponse, ApiError> {
    let parsed = service.parse_document(ParseDocumentRequest {
        dsl: request.dsl.clone(),
    })?;
    let query_spec = select_query_spec(&parsed, request.query_name.as_deref())?;
    ensure_importable_query_shape(query_spec, request.query_name.as_deref())?;
    let response = service.run_document(RunDocumentRequest {
        dsl: request.dsl.clone(),
        policy_context,
    })?;
    let source_execution = response.execution.as_ref().ok_or_else(|| {
        ApiError::Validation("imported fact query did not produce an execution receipt".into())
    })?;
    let result = select_query_result(&response, request.query_name.as_deref())?;
    let tuple_index = response
        .derived
        .tuples
        .iter()
        .map(|tuple| (tuple.tuple.id, tuple))
        .collect::<IndexMap<_, _>>();

    let facts = result
        .rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            build_imported_fact(&request, index, row, &tuple_index, &response.derived)
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    Ok(ImportedFactQueryResponse {
        cut: request.cut,
        leader_epoch: None,
        visible_prefix_digest: source_execution
            .manifest
            .journal_cut
            .visible_prefix_digest
            .clone(),
        imported_execution_id: source_execution.manifest.execution_id.clone(),
        predicate: request.predicate,
        query_name: request.query_name,
        row_count: result.rows.len(),
        facts,
    })
}

fn execute_federated_document_request(
    request: FederatedRunDocumentRequest,
    imports: Vec<ImportedFactQueryResponse>,
    execution_store: &mut dyn ExecutionStore,
) -> Result<FederatedRunDocumentResponse, ApiError> {
    let cut = federated_cut_from_imports(&imports)?;

    let local = InMemoryKernelService::new();
    let parsed = local.parse_document(ParseDocumentRequest {
        dsl: request.dsl.clone(),
    })?;
    ensure_federated_document_uses_current_views(&parsed)?;

    let mut schema = parsed.schema.clone();
    let mut program = parsed.program.clone();
    let predicate_lookup = parsed
        .program
        .predicates
        .iter()
        .map(|predicate| (predicate.name.clone(), predicate.clone()))
        .collect::<IndexMap<_, _>>();
    for import in &imports {
        for fact in &import.facts {
            let mut fact = fact.clone();
            let predicate = predicate_lookup
                .get(&fact.predicate.name)
                .ok_or_else(|| {
                    ApiError::Validation(format!(
                        "federated document does not declare imported predicate {}",
                        fact.predicate.name
                    ))
                })?
                .clone();
            if predicate.arity != fact.values.len() {
                return Err(ApiError::Validation(format!(
                    "federated document predicate {} expects arity {}, but imported fact supplied {} value(s)",
                    predicate.name,
                    predicate.arity,
                    fact.values.len()
                )));
            }
            fact.predicate = predicate;
            program.facts.push(fact);
        }
    }
    ensure_schema_covers_fact_predicates(&mut schema, &program.facts)?;

    let scope = PolicyScope::from_optional(request.policy_context.clone());
    let history = Vec::<Datom>::new();
    let mut federation_sources = imports
        .iter()
        .map(|import| FederatedExecutionSource {
            partition: import.cut.partition.to_string(),
            as_of: import.cut.as_of,
            leader_epoch: import.leader_epoch.as_ref().map(|epoch| epoch.0),
            visible_prefix_digest: import.visible_prefix_digest.clone(),
            imported_execution_id: import.imported_execution_id.clone(),
        })
        .collect::<Vec<_>>();
    federation_sources.sort_by(|left, right| left.partition.cmp(&right.partition));
    let federation = FederationManifest {
        sources: federation_sources,
    };
    let federation_identity = FederationIdentityMaterial {
        leader_epochs: federation
            .sources
            .iter()
            .filter_map(|source| {
                source
                    .leader_epoch
                    .map(|epoch| (source.partition.clone(), epoch))
            })
            .collect(),
        visible_prefix_digests: federation
            .sources
            .iter()
            .map(|source| {
                (
                    source.partition.clone(),
                    source.visible_prefix_digest.0.clone(),
                )
            })
            .collect(),
        imported_execution_ids: federation
            .sources
            .iter()
            .map(|source| {
                (
                    source.partition.clone(),
                    source.imported_execution_id.0.clone(),
                )
            })
            .collect(),
    };
    let builder = ScopedEvaluationBuilder::new_in_namespace(
        NamespaceId::default().as_str(),
        &schema,
        &history,
        &program,
        scope.clone(),
    )?
    .with_federation_identity(federation_identity);
    let (evaluation_key, evaluation) = builder.evaluate_with_key(TemporalView::Current)?;
    let execution_id = ExecutionId(evaluation_key.to_hex());

    let query = match &parsed.query {
        Some(query) => Some(execute_scoped_query(&evaluation, &query.query)?),
        None => None,
    };
    let queries = parsed
        .queries
        .iter()
        .map(|named_query| {
            Ok(NamedQueryResult {
                name: named_query.name.clone(),
                spec: named_query.spec.clone(),
                result: execute_scoped_query(&evaluation, &named_query.spec.query)?,
                execution_id: Some(execution_id.clone()),
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let explains = parsed
        .explains
        .iter()
        .map(|named_explain| {
            Ok(NamedExplainResult {
                name: named_explain.name.clone(),
                spec: named_explain.spec.clone(),
                result: execute_federated_explain_spec(&evaluation, &named_explain.spec)?,
                execution_id: Some(execution_id.clone()),
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    let execution = persist_execution(
        execution_store,
        &NamespaceId::default(),
        &evaluation_key,
        &schema,
        history.clone(),
        builder.program().compiled(),
        &scope,
        TemporalView::Current,
        evaluation.derived(),
        Some(federation),
    )?;

    Ok(FederatedRunDocumentResponse {
        cut,
        imports,
        run: RunDocumentResponse {
            state: ResolvedState::default(),
            program: evaluation.program().compiled().clone(),
            derived: evaluation.derived().clone(),
            query,
            queries,
            explains,
            execution: Some(execution.clone()),
            executions: vec![execution],
        },
    })
}

fn build_federated_explain_report_from_response(
    response: FederatedRunDocumentResponse,
    policy_context: Option<PolicyContext>,
) -> FederatedExplainReport {
    let execution = response.run.execution.as_ref();
    let primary_query = response
        .run
        .query
        .as_ref()
        .map(|result| report_rows(result, execution))
        .unwrap_or_default();
    let named_queries = response
        .run
        .queries
        .iter()
        .map(|query| FederatedNamedQuerySummary {
            name: query.name.clone(),
            rows: report_rows(&query.result, execution),
        })
        .collect::<Vec<_>>();
    let traces = response
        .run
        .explains
        .iter()
        .filter_map(|explain| match &explain.result {
            ExplainArtifact::Tuple(trace) => Some(build_trace_summary(explain.name.clone(), trace)),
            ExplainArtifact::Plan(_) => None,
        })
        .collect::<Vec<_>>();

    FederatedExplainReport {
        generated_at_ms: now_millis(),
        cut: response.cut,
        policy_context,
        imports: response
            .imports
            .iter()
            .map(|import| FederatedImportedSourceSummary {
                cut: import.cut.clone(),
                predicate: import.predicate.clone(),
                query_name: import.query_name.clone(),
                fact_count: import.facts.len(),
            })
            .collect(),
        primary_query,
        named_queries,
        traces,
    }
}

fn encode_partition_id(partition: &PartitionId) -> String {
    let mut encoded = String::with_capacity(partition.as_str().len() * 2);
    for byte in partition.as_str().as_bytes() {
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

fn select_query_result<'a>(
    response: &'a RunDocumentResponse,
    query_name: Option<&str>,
) -> Result<&'a QueryResult, ApiError> {
    match query_name {
        Some(name) => response
            .queries
            .iter()
            .find(|query| query.name.as_deref() == Some(name))
            .map(|query| &query.result)
            .ok_or_else(|| ApiError::Validation(format!("unknown named query {}", name))),
        None => response
            .query
            .as_ref()
            .ok_or_else(|| ApiError::Validation("document did not produce a primary query".into())),
    }
}

fn select_query_spec<'a>(
    response: &'a crate::ParseDocumentResponse,
    query_name: Option<&str>,
) -> Result<&'a aether_ast::QuerySpec, ApiError> {
    match query_name {
        Some(name) => response
            .queries
            .iter()
            .find(|query| query.name.as_deref() == Some(name))
            .map(|query| &query.spec)
            .ok_or_else(|| ApiError::Validation(format!("unknown named query {}", name))),
        None => response
            .query
            .as_ref()
            .ok_or_else(|| ApiError::Validation("document did not produce a primary query".into())),
    }
}

fn ensure_importable_query_shape(
    spec: &aether_ast::QuerySpec,
    query_name: Option<&str>,
) -> Result<(), ApiError> {
    if spec.query.goals.len() == 1 {
        Ok(())
    } else {
        let label = query_name.unwrap_or("<primary>");
        Err(ApiError::Validation(format!(
            "imported fact query {label} must have exactly one goal so imported provenance maps to a single semantic row"
        )))
    }
}

fn build_imported_fact(
    request: &ImportedFactQueryRequest,
    index: usize,
    row: &QueryRow,
    tuples: &IndexMap<TupleId, &aether_ast::DerivedTuple>,
    derived: &DerivedSet,
) -> Result<ExtensionalFact, ApiError> {
    if row.values.len() != request.predicate.arity {
        return Err(ApiError::Validation(format!(
            "imported fact row {} from {} produced {} value(s), but predicate {} expects arity {}",
            index,
            request.cut,
            row.values.len(),
            request.predicate.name,
            request.predicate.arity
        )));
    }
    let tuple_id = row.tuple_id.ok_or_else(|| {
        ApiError::Validation(format!(
            "imported fact row {} from {} was not backed by a derived tuple; import a tuple-producing query instead",
            index, request.cut
        ))
    })?;
    let tuple = tuples.get(&tuple_id).copied().ok_or_else(|| {
        ApiError::Validation(format!(
            "imported fact row {} from {} referenced missing tuple t{}",
            index, request.cut, tuple_id.0
        ))
    })?;
    let imported_cuts = merge_partition_cuts(
        std::iter::once(&request.cut).chain(tuple.metadata.imported_cuts.iter()),
    );
    let policy = tuple.policy.clone();

    if !derived
        .tuples
        .iter()
        .any(|candidate| candidate.tuple.id == tuple_id)
    {
        return Err(ApiError::Validation(format!(
            "imported fact row {} from {} referenced tuple t{} outside the derived set",
            index, request.cut, tuple_id.0
        )));
    }

    Ok(ExtensionalFact {
        predicate: request.predicate.clone(),
        values: row.values.clone(),
        policy,
        provenance: Some(FactProvenance {
            source_datom_ids: tuple.metadata.source_datom_ids.clone(),
            imported_cuts,
            sidecar_origin: None,
            source_ref: Some(SourceRef {
                uri: format!(
                    "aether://partition/{}/tuple/t{}",
                    request.cut.partition, tuple_id.0
                ),
                digest: None,
            }),
        }),
    })
}

fn federated_cut_from_imports(
    imports: &[ImportedFactQueryResponse],
) -> Result<FederatedCut, ApiError> {
    let mut by_partition = IndexMap::<PartitionId, PartitionCut>::new();
    for import in imports {
        match by_partition.get(&import.cut.partition) {
            Some(existing) if existing != &import.cut => {
                return Err(ApiError::Validation(format!(
                    "federated imports contain conflicting cuts for partition {}",
                    import.cut.partition
                )));
            }
            Some(_) => {}
            None => {
                by_partition.insert(import.cut.partition.clone(), import.cut.clone());
            }
        }
    }
    validate_federated_cut(FederatedCut {
        cuts: by_partition.into_values().collect(),
    })
}

fn ensure_federated_document_uses_current_views(
    parsed: &crate::ParseDocumentResponse,
) -> Result<(), ApiError> {
    if let Some(query) = &parsed.query {
        ensure_current_view("primary query", &query.view)?;
    }
    for named_query in &parsed.queries {
        ensure_current_view(
            named_query
                .name
                .as_deref()
                .map(|name| format!("query {}", name))
                .unwrap_or_else(|| "query".into())
                .as_str(),
            &named_query.spec.view,
        )?;
    }
    for named_explain in &parsed.explains {
        ensure_current_view(
            named_explain
                .name
                .as_deref()
                .map(|name| format!("explain {}", name))
                .unwrap_or_else(|| "explain".into())
                .as_str(),
            &named_explain.spec.view,
        )?;
    }
    Ok(())
}

fn ensure_current_view(label: &str, view: &TemporalView) -> Result<(), ApiError> {
    match view {
        TemporalView::Current => Ok(()),
        TemporalView::AsOf(element) => Err(ApiError::Validation(format!(
            "{label} cannot use AsOf(e{}); federated time must be expressed through explicit partition cuts",
            element.0
        ))),
    }
}

fn ensure_schema_covers_fact_predicates(
    schema: &mut Schema,
    facts: &[ExtensionalFact],
) -> Result<(), ApiError> {
    let mut signatures = IndexMap::<aether_ast::PredicateId, PredicateSignature>::new();
    for fact in facts {
        let fields = fact.values.iter().map(value_type_for).collect::<Vec<_>>();
        match signatures.get(&fact.predicate.id) {
            Some(existing) if existing.fields != fields => {
                return Err(ApiError::Validation(format!(
                    "imported predicate {} has inconsistent field types across federated facts",
                    fact.predicate.name
                )));
            }
            Some(_) => {}
            None => {
                signatures.insert(
                    fact.predicate.id,
                    PredicateSignature {
                        id: fact.predicate.id,
                        name: fact.predicate.name.clone(),
                        fields,
                    },
                );
            }
        }
    }

    for signature in signatures.into_values() {
        if schema.predicate(&signature.id).is_none() {
            schema
                .register_predicate(signature)
                .map_err(|error| ApiError::Validation(error.to_string()))?;
        }
    }
    Ok(())
}

fn value_type_for(value: &Value) -> ValueType {
    match value {
        Value::Null => ValueType::String,
        Value::Bool(_) => ValueType::Bool,
        Value::I64(_) => ValueType::I64,
        Value::U64(_) => ValueType::U64,
        Value::F64(_) => ValueType::F64,
        Value::String(_) => ValueType::String,
        Value::Bytes(_) => ValueType::Bytes,
        Value::Entity(_) => ValueType::Entity,
        Value::List(values) => ValueType::List(Box::new(
            values
                .first()
                .map(value_type_for)
                .unwrap_or(ValueType::String),
        )),
    }
}

fn execute_federated_explain_spec(
    evaluation: &EvaluationBundle,
    spec: &ExplainSpec,
) -> Result<ExplainArtifact, ApiError> {
    match &spec.target {
        ExplainTarget::Plan => Ok(ExplainArtifact::Plan(
            InMemoryExplainer::default()
                .explain_plan(&evaluation.program().compiled().phase_graph)?,
        )),
        ExplainTarget::Tuple(atom) => {
            let tuple_id =
                find_matching_derived_tuple(evaluation.derived(), atom).ok_or_else(|| {
                    ApiError::Validation(format!(
                        "no derived tuple matched explain target {}",
                        atom.predicate.name
                    ))
                })?;
            let trace = InMemoryExplainer::from_derived_set(evaluation.derived())
                .explain_tuple(&tuple_id)?;
            if !trace
                .tuples
                .iter()
                .all(|tuple| evaluation.scope().allows(tuple.policy.as_ref()))
            {
                return Err(ApiError::Validation(
                    "federated trace contains data outside the evaluation policy scope".into(),
                ));
            }
            Ok(ExplainArtifact::Tuple(trace))
        }
    }
}

fn find_matching_derived_tuple(
    derived: &aether_runtime::DerivedSet,
    atom: &Atom,
) -> Option<TupleId> {
    derived.tuples.iter().find_map(|tuple| {
        if tuple.tuple.predicate != atom.predicate.id
            || tuple.tuple.values.len() != atom.terms.len()
        {
            return None;
        }
        let matches = atom
            .terms
            .iter()
            .zip(&tuple.tuple.values)
            .all(|(term, value)| matches_term(term, value));
        matches.then_some(tuple.tuple.id)
    })
}

fn matches_term(term: &aether_ast::Term, value: &Value) -> bool {
    match term {
        aether_ast::Term::Value(expected) => expected == value,
        aether_ast::Term::Variable(_) => true,
        aether_ast::Term::Aggregate(_) => false,
    }
}

fn report_rows(
    result: &QueryResult,
    execution: Option<&crate::ExecutionReceipt>,
) -> Vec<FederatedReportRow> {
    result
        .rows
        .iter()
        .map(|row| FederatedReportRow {
            tuple_id: row.tuple_id,
            execution_id: row
                .tuple_id
                .and_then(|_| execution.map(|receipt| receipt.manifest.execution_id.clone())),
            trace_handle: row.tuple_id.and_then(|tuple_id| {
                execution.and_then(|receipt| {
                    receipt
                        .trace_handles
                        .iter()
                        .find(|binding| binding.local_tuple_id == tuple_id)
                        .map(|binding| binding.handle.clone())
                })
            }),
            values: row.values.clone(),
        })
        .collect()
}

fn build_trace_summary(
    name: Option<String>,
    trace: &aether_ast::DerivationTrace,
) -> FederatedTraceSummary {
    let imported_cuts = merge_partition_cuts(
        trace
            .tuples
            .iter()
            .flat_map(|tuple| tuple.metadata.imported_cuts.iter()),
    );
    FederatedTraceSummary {
        name,
        root: trace.root,
        tuple_count: trace.tuples.len(),
        imported_cuts,
        tuples: trace
            .tuples
            .iter()
            .map(|tuple| FederatedTraceTupleSummary {
                tuple_id: tuple.tuple.id,
                values: tuple.tuple.values.clone(),
                iteration: tuple.metadata.iteration,
                source_datom_ids: tuple.metadata.source_datom_ids.clone(),
                imported_cuts: tuple.metadata.imported_cuts.clone(),
                parent_tuple_ids: tuple.metadata.parent_tuple_ids.clone(),
            })
            .collect(),
    }
}

pub fn render_federated_explain_report_markdown(report: &FederatedExplainReport) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Federated Explain Report\n\n");
    markdown.push_str(&format!(
        "- Generated at (ms): `{}`\n- Federated cut: `{}`\n- Effective policy: `{}`\n\n",
        report.generated_at_ms,
        format_federated_cut(&report.cut),
        format_policy_context(report.policy_context.as_ref())
    ));

    markdown.push_str("## Imported Sources\n\n");
    if report.imports.is_empty() {
        markdown.push_str("_None._\n\n");
    } else {
        for import in &report.imports {
            markdown.push_str(&format!(
                "- `{}` -> `{}` | query `{}` | facts `{}`\n",
                import.cut,
                import.predicate.name,
                import.query_name.as_deref().unwrap_or("<primary>"),
                import.fact_count
            ));
        }
        markdown.push('\n');
    }

    markdown.push_str("## Primary Query\n\n");
    if report.primary_query.is_empty() {
        markdown.push_str("_No rows._\n\n");
    } else {
        for row in &report.primary_query {
            markdown.push_str(&format!(
                "- `{}`{}\n",
                format_values(&row.values),
                row.tuple_id
                    .map(|tuple_id| format!(" | tuple `t{}`", tuple_id.0))
                    .unwrap_or_default()
            ));
        }
        markdown.push('\n');
    }

    if !report.named_queries.is_empty() {
        markdown.push_str("## Named Queries\n\n");
        for query in &report.named_queries {
            markdown.push_str(&format!(
                "### {}\n\n",
                query.name.as_deref().unwrap_or("<unnamed>")
            ));
            if query.rows.is_empty() {
                markdown.push_str("_No rows._\n\n");
            } else {
                for row in &query.rows {
                    markdown.push_str(&format!(
                        "- `{}`{}\n",
                        format_values(&row.values),
                        row.tuple_id
                            .map(|tuple_id| format!(" | tuple `t{}`", tuple_id.0))
                            .unwrap_or_default()
                    ));
                }
                markdown.push('\n');
            }
        }
    }

    if !report.traces.is_empty() {
        markdown.push_str("## Federated Traces\n\n");
        for trace in &report.traces {
            markdown.push_str(&format!(
                "### {}\n\n- Root: `t{}`\n- Tuple count: `{}`\n- Imported cuts: `{}`\n\n",
                trace.name.as_deref().unwrap_or("<unnamed trace>"),
                trace.root.0,
                trace.tuple_count,
                format_partition_cuts(&trace.imported_cuts)
            ));
            for tuple in &trace.tuples {
                markdown.push_str(&format!(
                    "- `t{}` via iteration `{}` -> `{}` | sources `{}` | imported `{}` | parents `{}`\n",
                    tuple.tuple_id.0,
                    tuple.iteration,
                    format_values(&tuple.values),
                    format_element_ids(&tuple.source_datom_ids),
                    format_partition_cuts(&tuple.imported_cuts),
                    format_tuple_ids(&tuple.parent_tuple_ids)
                ));
            }
            markdown.push('\n');
        }
    }

    markdown
}

fn format_federated_cut(cut: &FederatedCut) -> String {
    if cut.cuts.is_empty() {
        "<none>".into()
    } else {
        cut.cuts
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_partition_cuts(cuts: &[PartitionCut]) -> String {
    if cuts.is_empty() {
        "<none>".into()
    } else {
        cuts.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_values(values: &[Value]) -> String {
    values
        .iter()
        .map(format_value)
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(value) => value.to_string(),
        Value::I64(value) => value.to_string(),
        Value::U64(value) => value.to_string(),
        Value::F64(value) => format!("{value:.4}"),
        Value::String(value) => value.clone(),
        Value::Bytes(value) => format!("<{} bytes>", value.len()),
        Value::Entity(value) => format!("entity({})", value.0),
        Value::List(values) => format!("[{}]", format_values(values)),
    }
}

fn format_element_ids(elements: &[aether_ast::ElementId]) -> String {
    if elements.is_empty() {
        "<none>".into()
    } else {
        elements
            .iter()
            .map(|element| format!("e{}", element.0))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_tuple_ids(tuple_ids: &[TupleId]) -> String {
    if tuple_ids.is_empty() {
        "<none>".into()
    } else {
        tuple_ids
            .iter()
            .map(|tuple_id| format!("t{}", tuple_id.0))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_policy_context(policy_context: Option<&PolicyContext>) -> String {
    match policy_context {
        None => "public".into(),
        Some(policy) if policy.is_empty() => "public".into(),
        Some(policy) => {
            let capabilities = if policy.capabilities.is_empty() {
                "capabilities=<none>".into()
            } else {
                format!("capabilities={}", policy.capabilities.join(","))
            };
            let visibilities = if policy.visibilities.is_empty() {
                "visibilities=<none>".into()
            } else {
                format!("visibilities={}", policy.visibilities.join(","))
            };
            format!("{capabilities}; {visibilities}")
        }
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionAppendRequest {
    pub partition: PartitionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_epoch: Option<LeaderEpoch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<crate::SchemaRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_cut: Option<aether_storage::JournalCutRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub datoms: Vec<Datom>,
    #[serde(skip)]
    pub principal: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionAppendResponse {
    pub partition: PartitionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_epoch: Option<LeaderEpoch>,
    pub appended: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<crate::AppendReceipt>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionHistoryRequest {
    pub cut: PartitionCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionHistoryResponse {
    pub cut: PartitionCut,
    pub datoms: Vec<Datom>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionStateRequest {
    pub cut: PartitionCut,
    pub schema: Schema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionStateResponse {
    pub cut: PartitionCut,
    pub state: ResolvedState,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedHistoryRequest {
    pub cut: FederatedCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedHistoryResponse {
    pub partitions: Vec<PartitionHistoryResponse>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ImportedFactQueryRequest {
    pub cut: PartitionCut,
    pub dsl: String,
    pub predicate: PredicateRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_name: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ImportedFactQueryResponse {
    pub cut: PartitionCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_epoch: Option<LeaderEpoch>,
    pub visible_prefix_digest: ContentDigest,
    pub imported_execution_id: ExecutionId,
    pub predicate: PredicateRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_name: Option<String>,
    pub row_count: usize,
    pub facts: Vec<ExtensionalFact>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedRunDocumentRequest {
    pub dsl: String,
    pub imports: Vec<ImportedFactQueryRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedRunDocumentResponse {
    pub cut: FederatedCut,
    pub imports: Vec<ImportedFactQueryResponse>,
    pub run: RunDocumentResponse,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedExplainReport {
    pub generated_at_ms: u64,
    pub cut: FederatedCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
    pub imports: Vec<FederatedImportedSourceSummary>,
    pub primary_query: Vec<FederatedReportRow>,
    pub named_queries: Vec<FederatedNamedQuerySummary>,
    pub traces: Vec<FederatedTraceSummary>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedImportedSourceSummary {
    pub cut: PartitionCut,
    pub predicate: PredicateRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_name: Option<String>,
    pub fact_count: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedReportRow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tuple_id: Option<TupleId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_handle: Option<crate::TraceHandle>,
    pub values: Vec<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedNamedQuerySummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub rows: Vec<FederatedReportRow>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedTraceSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub root: TupleId,
    pub tuple_count: usize,
    pub imported_cuts: Vec<PartitionCut>,
    pub tuples: Vec<FederatedTraceTupleSummary>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedTraceTupleSummary {
    pub tuple_id: TupleId,
    pub values: Vec<Value>,
    pub iteration: usize,
    pub source_datom_ids: Vec<aether_ast::ElementId>,
    pub imported_cuts: Vec<PartitionCut>,
    pub parent_tuple_ids: Vec<TupleId>,
}

#[cfg(test)]
mod tests {
    use super::{
        render_federated_explain_report_markdown, AuthorityPartitionConfig,
        FederatedHistoryRequest, FederatedRunDocumentRequest, ImportedFactQueryRequest,
        LeaderEpoch, PartitionAppendRequest, PartitionHistoryRequest, PartitionStateRequest,
        PartitionedInMemoryKernelService, PromoteReplicaRequest, ReplicaConfig, ReplicaRole,
        ReplicatedAuthorityPartitionService, SqlitePartitionedKernelService,
    };
    use aether_ast::{
        AttributeId, Datom, DatomProvenance, ElementId, EntityId, FederatedCut, OperationKind,
        PartitionCut, PartitionId, PolicyContext, PolicyEnvelope, PredicateId, PredicateRef,
        ReplicaId, Value,
    };
    use aether_resolver::ResolvedValue;
    use aether_schema::{AttributeClass, AttributeSchema, Schema, ValueType};
    use std::{
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        sync::{mpsc, Arc},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn blocked_partition_does_not_delay_another_partition() {
        let temp = TestPartitionDir::new("partition-lock-isolation");
        let service = Arc::new(
            ReplicatedAuthorityPartitionService::open(
                temp.path(),
                vec![
                    AuthorityPartitionConfig {
                        partition: PartitionId::new("blocked"),
                        replicas: vec![ReplicaConfig {
                            replica_id: ReplicaId::new(1),
                            database_path: PathBuf::from("blocked.sqlite"),
                            role: ReplicaRole::Leader,
                        }],
                    },
                    AuthorityPartitionConfig {
                        partition: PartitionId::new("free"),
                        replicas: vec![ReplicaConfig {
                            replica_id: ReplicaId::new(1),
                            database_path: PathBuf::from("free.sqlite"),
                            role: ReplicaRole::Leader,
                        }],
                    },
                ],
            )
            .expect("open partition service"),
        );
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("free"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "free-value", 1, None)],
            })
            .expect("seed free partition");

        let blocked = Arc::clone(
            service
                .partitions
                .get(&PartitionId::new("blocked"))
                .expect("blocked partition"),
        );
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let blocked_thread = thread::spawn(move || {
            let _guard = blocked.lock().expect("blocked partition lock");
            started_tx.send(()).expect("signal blocked lock");
            release_rx.recv().expect("release blocked lock");
        });
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("blocked partition lock acquired");

        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let free_service = Arc::clone(&service);
        let free_thread = thread::spawn(move || {
            result_tx
                .send(free_service.partition_history(PartitionHistoryRequest {
                    cut: PartitionCut::as_of(PartitionId::new("free"), ElementId::new(1)),
                    policy_context: None,
                }))
                .expect("send free partition result");
        });
        let history = result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("free partition must not wait")
            .expect("free partition history");
        assert_eq!(history.datoms.len(), 1);
        free_thread.join().expect("free partition thread");
        release_tx.send(()).expect("release blocked partition");
        blocked_thread.join().expect("blocked partition thread");
    }

    #[test]
    fn partitioned_service_keeps_local_truth_exact_per_partition() {
        let mut service = PartitionedInMemoryKernelService::new();
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-a"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![
                    sample_datom(1, 1, "tenant-a-open", 1, None),
                    sample_datom(1, 1, "tenant-a-running", 2, None),
                ],
            })
            .expect("append tenant-a");
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-b"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "tenant-b-ready", 1, None)],
            })
            .expect("append tenant-b");

        let tenant_a_current = service
            .partition_state(PartitionStateRequest {
                cut: PartitionCut::current("tenant-a"),
                schema: schema(),
                policy_context: None,
            })
            .expect("tenant-a current state");
        let tenant_a_as_of = service
            .partition_state(PartitionStateRequest {
                cut: PartitionCut::as_of("tenant-a", ElementId::new(1)),
                schema: schema(),
                policy_context: None,
            })
            .expect("tenant-a as-of state");
        let tenant_b_history = service
            .partition_history(PartitionHistoryRequest {
                cut: PartitionCut::current("tenant-b"),
                policy_context: None,
            })
            .expect("tenant-b history");

        assert_eq!(
            tenant_a_current
                .state
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&AttributeId::new(1))),
            Some(&ResolvedValue::Scalar(Some(Value::String(
                "tenant-a-running".into()
            ))))
        );
        assert_eq!(
            tenant_a_as_of
                .state
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&AttributeId::new(1))),
            Some(&ResolvedValue::Scalar(Some(Value::String(
                "tenant-a-open".into()
            ))))
        );
        assert_eq!(tenant_b_history.datoms.len(), 1);
        assert_eq!(
            tenant_b_history.datoms[0].value,
            Value::String("tenant-b-ready".into())
        );
    }

    #[test]
    fn federated_history_requires_explicit_unique_partition_cuts() {
        let mut service = PartitionedInMemoryKernelService::new();
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-a"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "alpha", 1, None)],
            })
            .expect("append tenant-a");
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-b"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(
                    1,
                    1,
                    "beta",
                    1,
                    Some(PolicyEnvelope {
                        capabilities: vec!["ops".into()],
                        visibilities: Vec::new(),
                    }),
                )],
            })
            .expect("append tenant-b");

        let federated = service
            .federated_history(FederatedHistoryRequest {
                cut: FederatedCut {
                    cuts: vec![
                        PartitionCut::current("tenant-b"),
                        PartitionCut::current("tenant-a"),
                    ],
                },
                policy_context: None,
            })
            .expect("federated history");
        assert_eq!(
            federated
                .partitions
                .iter()
                .map(|partition| partition.cut.to_string())
                .collect::<Vec<_>>(),
            vec![
                "tenant-a@current".to_string(),
                "tenant-b@current".to_string()
            ]
        );
        assert_eq!(federated.partitions[0].datoms.len(), 1);
        assert!(federated.partitions[1].datoms.is_empty());

        let duplicate_partition = service.federated_history(FederatedHistoryRequest {
            cut: FederatedCut {
                cuts: vec![
                    PartitionCut::current("tenant-a"),
                    PartitionCut::as_of("tenant-a", ElementId::new(1)),
                ],
            },
            policy_context: None,
        });
        assert!(matches!(
            duplicate_partition,
            Err(crate::ApiError::Validation(message))
                if message == "federated cut contains duplicate partition tenant-a"
        ));
    }

    #[test]
    fn unknown_partition_is_rejected_cleanly() {
        let service = PartitionedInMemoryKernelService::new();
        let error = service.partition_history(PartitionHistoryRequest {
            cut: PartitionCut::current("missing"),
            policy_context: None,
        });
        assert!(matches!(
            error,
            Err(crate::ApiError::Validation(message))
                if message == "unknown partition missing"
        ));
    }

    #[test]
    fn imported_facts_and_federated_explain_reports_preserve_partition_cuts() {
        let mut service = PartitionedInMemoryKernelService::new();
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("readiness"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "ready", 1, None)],
            })
            .expect("append readiness datoms");
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("authority"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "worker-a", 3, None)],
            })
            .expect("append authority datoms");

        let response = service
            .federated_run_document(FederatedRunDocumentRequest {
                dsl: federated_assignment_document(),
                imports: vec![
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("readiness", ElementId::new(1)),
                        dsl: readiness_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(11),
                            name: "imported_ready_task".into(),
                            arity: 1,
                        },
                        query_name: Some("ready_now".into()),
                    },
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("authority", ElementId::new(3)),
                        dsl: authority_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(12),
                            name: "imported_authorized_worker".into(),
                            arity: 2,
                        },
                        query_name: Some("authorized_now".into()),
                    },
                ],
                policy_context: None,
            })
            .expect("run federated document");

        assert_eq!(
            response
                .cut
                .cuts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["authority@e3".to_string(), "readiness@e1".to_string()]
        );
        let receipt = response.run.execution.as_ref().expect("execution receipt");
        let federation = receipt
            .manifest
            .federation
            .as_ref()
            .expect("federation identity");
        assert_eq!(
            federation
                .sources
                .iter()
                .map(|source| source.partition.as_str())
                .collect::<Vec<_>>(),
            vec!["authority", "readiness"]
        );
        assert!(federation.sources.iter().all(|source| {
            !source.visible_prefix_digest.0.is_empty() && !source.imported_execution_id.0.is_empty()
        }));
        assert_eq!(response.imports.len(), 2);
        assert_eq!(
            response
                .run
                .query
                .as_ref()
                .expect("primary query result")
                .rows[0]
                .values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into())
            ]
        );

        let actionable_tuple_id = response
            .run
            .query
            .as_ref()
            .expect("primary query result")
            .rows[0]
            .tuple_id
            .expect("actionable tuple id");
        let handle = receipt
            .trace_handles
            .iter()
            .find(|binding| binding.local_tuple_id == actionable_tuple_id)
            .expect("actionable trace handle")
            .handle
            .clone();
        let resolved = service
            .resolve_trace_handle(crate::ResolveTraceHandleRequest {
                handle,
                policy_context: None,
                verify_replay: true,
            })
            .expect("resolve federated trace");
        assert_eq!(resolved.record.local_tuple_id, actionable_tuple_id);
        assert!(resolved.replay_verified);
        let actionable_tuple = response
            .run
            .derived
            .tuples
            .iter()
            .find(|tuple| tuple.tuple.id == actionable_tuple_id)
            .expect("actionable tuple");
        assert_eq!(
            actionable_tuple
                .metadata
                .imported_cuts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["authority@e3".to_string(), "readiness@e1".to_string()]
        );

        let trace = response
            .run
            .explains
            .iter()
            .find(|explain| explain.name.as_deref() == Some("actionable_trace"))
            .expect("actionable trace");
        let crate::ExplainArtifact::Tuple(trace) = &trace.result else {
            panic!("expected tuple trace");
        };
        assert_eq!(
            trace.tuples[0]
                .metadata
                .imported_cuts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["authority@e3".to_string(), "readiness@e1".to_string()]
        );

        let report = service
            .build_federated_explain_report(FederatedRunDocumentRequest {
                dsl: federated_assignment_document(),
                imports: vec![
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("readiness", ElementId::new(1)),
                        dsl: readiness_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(11),
                            name: "imported_ready_task".into(),
                            arity: 1,
                        },
                        query_name: Some("ready_now".into()),
                    },
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("authority", ElementId::new(3)),
                        dsl: authority_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(12),
                            name: "imported_authorized_worker".into(),
                            arity: 2,
                        },
                        query_name: Some("authorized_now".into()),
                    },
                ],
                policy_context: None,
            })
            .expect("build federated report");
        assert_eq!(report.traces.len(), 1);
        assert!(report.primary_query[0].trace_handle.is_some());
        let markdown = render_federated_explain_report_markdown(&report);
        assert!(markdown.contains("authority@e3, readiness@e1"));
        assert!(markdown.contains("imported_authorized_worker"));
        assert!(markdown.contains("imported_ready_task"));
    }

    #[test]
    fn imported_fact_queries_must_be_single_goal() {
        let mut service = PartitionedInMemoryKernelService::new();
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("joined"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![
                    sample_datom(1, 1, "ready", 1, None),
                    Datom {
                        entity: EntityId::new(1),
                        attribute: AttributeId::new(2),
                        value: Value::String("worker-a".into()),
                        op: OperationKind::Assert,
                        element: ElementId::new(2),
                        replica: ReplicaId::new(1),
                        causal_context: Default::default(),
                        provenance: DatomProvenance::default(),
                        policy: None,
                    },
                ],
            })
            .expect("append joined datoms");

        let error = service
            .import_partition_facts(
                ImportedFactQueryRequest {
                    cut: PartitionCut::current("joined"),
                    dsl: joined_import_document(),
                    predicate: PredicateRef {
                        id: PredicateId::new(21),
                        name: "imported_assignment".into(),
                        arity: 2,
                    },
                    query_name: Some("joined_now".into()),
                },
                None,
            )
            .expect_err("joined import should be rejected");

        match error {
            crate::ApiError::Validation(message) => assert!(
                message
                    .contains("must have exactly one goal so imported provenance maps to a single semantic row"),
                "{message}"
            ),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn hidden_partition_cut_is_rejected_under_policy() {
        let mut service = PartitionedInMemoryKernelService::new();
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("secure"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![
                    sample_datom(1, 1, "ready", 1, None),
                    sample_datom(
                        1,
                        1,
                        "running",
                        2,
                        Some(PolicyEnvelope {
                            capabilities: vec!["ops".into()],
                            visibilities: Vec::new(),
                        }),
                    ),
                ],
            })
            .expect("append secure datoms");

        let history = service.partition_history(PartitionHistoryRequest {
            cut: PartitionCut::as_of("secure", ElementId::new(2)),
            policy_context: None,
        });
        assert!(matches!(
            history,
            Err(crate::ApiError::Validation(message))
                if message == "unknown element 2 for partition secure"
        ));

        let state = service.partition_state(PartitionStateRequest {
            cut: PartitionCut::as_of("secure", ElementId::new(2)),
            schema: schema(),
            policy_context: None,
        });
        assert!(matches!(
            state,
            Err(crate::ApiError::Validation(message))
                if message == "unknown element 2 for partition secure"
        ));

        let visible = service
            .partition_state(PartitionStateRequest {
                cut: PartitionCut::as_of("secure", ElementId::new(2)),
                schema: schema(),
                policy_context: Some(PolicyContext {
                    capabilities: vec!["ops".into()],
                    visibilities: Vec::new(),
                }),
            })
            .expect("authorized cut should resolve");
        assert_eq!(
            visible.cut,
            PartitionCut::as_of("secure", ElementId::new(2))
        );
    }

    #[test]
    fn sqlite_partitioned_service_replays_federated_imports_after_restart() {
        let temp = TestPartitionDir::new("partitioned-sqlite");
        {
            let mut service =
                SqlitePartitionedKernelService::open(temp.path()).expect("open sqlite partitions");
            service
                .append_partition(PartitionAppendRequest {
                    partition: PartitionId::new("readiness"),
                    leader_epoch: None,
                    schema_ref: None,
                    expected_cut: None,
                    idempotency_key: None,
                    principal: None,
                    datoms: vec![sample_datom(1, 1, "ready", 1, None)],
                })
                .expect("append readiness datoms");
            service
                .append_partition(PartitionAppendRequest {
                    partition: PartitionId::new("authority"),
                    leader_epoch: None,
                    schema_ref: None,
                    expected_cut: None,
                    idempotency_key: None,
                    principal: None,
                    datoms: vec![sample_datom(1, 1, "worker-a", 3, None)],
                })
                .expect("append authority datoms");
        }

        let mut service =
            SqlitePartitionedKernelService::open(temp.path()).expect("reopen sqlite partitions");
        let response = service
            .federated_run_document(FederatedRunDocumentRequest {
                dsl: federated_assignment_document(),
                imports: vec![
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("readiness", ElementId::new(1)),
                        dsl: readiness_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(11),
                            name: "imported_ready_task".into(),
                            arity: 1,
                        },
                        query_name: Some("ready_now".into()),
                    },
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("authority", ElementId::new(3)),
                        dsl: authority_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(12),
                            name: "imported_authorized_worker".into(),
                            arity: 2,
                        },
                        query_name: Some("authorized_now".into()),
                    },
                ],
                policy_context: None,
            })
            .expect("run federated document after restart");

        assert_eq!(
            response.run.query.as_ref().expect("query result").rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into())
            ]
        );
        let report = service
            .build_federated_explain_report(FederatedRunDocumentRequest {
                dsl: federated_assignment_document(),
                imports: vec![
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("readiness", ElementId::new(1)),
                        dsl: readiness_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(11),
                            name: "imported_ready_task".into(),
                            arity: 1,
                        },
                        query_name: Some("ready_now".into()),
                    },
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("authority", ElementId::new(3)),
                        dsl: authority_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(12),
                            name: "imported_authorized_worker".into(),
                            arity: 2,
                        },
                        query_name: Some("authorized_now".into()),
                    },
                ],
                policy_context: None,
            })
            .expect("build report after restart");
        assert!(render_federated_explain_report_markdown(&report)
            .contains("authority@e3, readiness@e1"));
    }

    #[test]
    fn replicated_partition_service_replays_followers_and_fences_stale_epochs() {
        let temp = TestPartitionDir::new("replicated");
        let service = ReplicatedAuthorityPartitionService::open(
            temp.path(),
            vec![AuthorityPartitionConfig {
                partition: PartitionId::new("authority"),
                replicas: vec![
                    ReplicaConfig {
                        replica_id: ReplicaId::new(1),
                        database_path: PathBuf::from("authority-leader.sqlite"),
                        role: ReplicaRole::Leader,
                    },
                    ReplicaConfig {
                        replica_id: ReplicaId::new(2),
                        database_path: PathBuf::from("authority-follower.sqlite"),
                        role: ReplicaRole::Follower,
                    },
                ],
            }],
        )
        .expect("open replicated partitions");

        let appended = service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("authority"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "worker-a", 1, None)],
            })
            .expect("append through leader");
        assert_eq!(appended.leader_epoch.expect("leader epoch").0, 1);
        let leader = crate::SqliteKernelService::open(temp.path().join("authority-leader.sqlite"))
            .expect("reopen leader authority");
        let follower =
            crate::SqliteKernelService::open(temp.path().join("authority-follower.sqlite"))
                .expect("reopen follower authority");
        let leader_receipts = leader.authority_append_receipts().expect("leader receipts");
        let follower_receipts = follower
            .authority_append_receipts()
            .expect("follower receipts");
        assert_eq!(leader_receipts, follower_receipts);
        assert_eq!(
            leader_receipts[0].batch_id,
            appended.receipt.expect("partition append receipt").batch_id
        );
        assert_eq!(
            leader.authority_schema_catalog().expect("leader schemas"),
            follower
                .authority_schema_catalog()
                .expect("follower schemas")
        );

        let status = service.partition_status().expect("partition status");
        let authority = &status.partitions[0];
        assert_eq!(authority.leader_replica, ReplicaId::new(1));
        assert_eq!(authority.replicas.len(), 2);
        assert!(authority.replicas.iter().all(|replica| replica.healthy));
        assert!(authority
            .replicas
            .iter()
            .all(|replica| replica.applied_element == Some(ElementId::new(1))));

        let promoted = service
            .promote_replica(PromoteReplicaRequest {
                partition: PartitionId::new("authority"),
                replica_id: ReplicaId::new(2),
            })
            .expect("promote follower");
        assert_eq!(promoted.leader_epoch.0, 2);

        let stale = service.append_partition(PartitionAppendRequest {
            partition: PartitionId::new("authority"),
            leader_epoch: Some(LeaderEpoch::new(1)),
            schema_ref: None,
            expected_cut: None,
            idempotency_key: None,
            principal: None,
            datoms: vec![sample_datom(1, 1, "worker-b", 2, None)],
        });
        assert!(matches!(
            stale,
            Err(crate::ApiError::Validation(message))
                if message.contains("stale leader epoch 1 for partition authority; current epoch is 2")
        ));

        let appended = service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("authority"),
                leader_epoch: Some(LeaderEpoch::new(2)),
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "worker-b", 2, None)],
            })
            .expect("append after promotion");
        assert_eq!(appended.leader_epoch.expect("leader epoch").0, 2);

        let history = service
            .partition_history(PartitionHistoryRequest {
                cut: PartitionCut::current("authority"),
                policy_context: None,
            })
            .expect("current history");
        assert_eq!(history.datoms.len(), 2);

        let status = service
            .partition_status()
            .expect("partition status after promotion");
        let authority = &status.partitions[0];
        assert_eq!(authority.leader_replica, ReplicaId::new(2));
        assert!(authority.replicas.iter().any(|replica| {
            replica.replica_id == ReplicaId::new(2)
                && replica.role == ReplicaRole::Leader
                && replica.leader_epoch.0 == 2
        }));
        assert!(authority
            .replicas
            .iter()
            .all(|replica| replica.applied_element == Some(ElementId::new(2))));
    }

    #[test]
    fn replicated_partition_service_restarts_promotes_and_preserves_exact_cuts() {
        let temp = TestPartitionDir::new("replicated-restart");
        let configs = replicated_authority_config();
        {
            let service = ReplicatedAuthorityPartitionService::open(temp.path(), configs.clone())
                .expect("open replicated partitions");
            service
                .append_partition(PartitionAppendRequest {
                    partition: PartitionId::new("authority"),
                    leader_epoch: None,
                    schema_ref: None,
                    expected_cut: None,
                    idempotency_key: None,
                    principal: None,
                    datoms: vec![sample_datom(1, 1, "worker-a", 1, None)],
                })
                .expect("append before restart");
            assert_partition_value(
                &service,
                PartitionCut::as_of("authority", ElementId::new(1)),
                "worker-a",
            );
        }

        let service = ReplicatedAuthorityPartitionService::open(temp.path(), configs.clone())
            .expect("reopen replicated partitions");
        let status = service.partition_status().expect("status after reopen");
        let authority = &status.partitions[0];
        assert_eq!(authority.leader_epoch, LeaderEpoch::new(1));
        assert_eq!(authority.leader_replica, ReplicaId::new(1));
        assert!(authority.replicas.iter().all(|replica| {
            replica.healthy
                && replica.replication_lag == 0
                && replica.applied_element == Some(ElementId::new(1))
        }));
        assert_partition_value(&service, PartitionCut::current("authority"), "worker-a");

        let promoted = service
            .promote_replica(PromoteReplicaRequest {
                partition: PartitionId::new("authority"),
                replica_id: ReplicaId::new(2),
            })
            .expect("promote follower after restart");
        assert_eq!(promoted.leader_epoch, LeaderEpoch::new(2));

        let stale = service.append_partition(PartitionAppendRequest {
            partition: PartitionId::new("authority"),
            leader_epoch: Some(LeaderEpoch::new(1)),
            schema_ref: None,
            expected_cut: None,
            idempotency_key: None,
            principal: None,
            datoms: vec![sample_datom(1, 1, "stale-worker", 2, None)],
        });
        assert!(matches!(
            stale,
            Err(crate::ApiError::Validation(message))
                if message.contains("stale leader epoch 1 for partition authority; current epoch is 2")
        ));

        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("authority"),
                leader_epoch: Some(LeaderEpoch::new(2)),
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "worker-b", 2, None)],
            })
            .expect("append under promoted epoch");
        assert_partition_value(&service, PartitionCut::current("authority"), "worker-b");
        assert_partition_value(
            &service,
            PartitionCut::as_of("authority", ElementId::new(1)),
            "worker-a",
        );

        drop(service);
        let service = ReplicatedAuthorityPartitionService::open(temp.path(), configs)
            .expect("reopen after promotion");
        let status = service
            .partition_status()
            .expect("status after promoted reopen");
        let authority = &status.partitions[0];
        assert_eq!(authority.leader_epoch, LeaderEpoch::new(2));
        assert_eq!(authority.leader_replica, ReplicaId::new(2));
        assert!(authority.replicas.iter().all(|replica| replica.healthy));
        let history = service
            .partition_history(PartitionHistoryRequest {
                cut: PartitionCut::current("authority"),
                policy_context: None,
            })
            .expect("current history after promoted reopen");
        assert_eq!(history.datoms.len(), 2);
    }

    #[test]
    fn replicated_partition_status_reports_lag_and_rejects_divergence() {
        let temp = TestPartitionDir::new("replicated-divergence");
        let configs = replicated_authority_config();
        let service = ReplicatedAuthorityPartitionService::open(temp.path(), configs.clone())
            .expect("open replicated partitions");
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("authority"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "worker-a", 1, None)],
            })
            .expect("append through leader");

        {
            let mut partition = service
                .partitions
                .get(&PartitionId::new("authority"))
                .expect("authority partition")
                .lock()
                .expect("partition lock");
            let leader = partition
                .replicas
                .get_mut(&ReplicaId::new(1))
                .expect("leader replica");
            crate::KernelService::append(
                leader,
                crate::AppendRequest {
                    datoms: vec![sample_datom(1, 1, "worker-b", 2, None)],
                },
            )
            .expect("leader-only append");
        }

        let status = service
            .partition_status()
            .expect("status with lagging follower");
        let authority = &status.partitions[0];
        let follower = authority
            .replicas
            .iter()
            .find(|replica| replica.replica_id == ReplicaId::new(2))
            .expect("follower status");
        assert!(!follower.healthy);
        assert_eq!(follower.replication_lag, 1);
        assert_eq!(
            follower.detail.as_deref(),
            Some("behind leader by element delta 1")
        );

        {
            let mut partition = service
                .partitions
                .get(&PartitionId::new("authority"))
                .expect("authority partition")
                .lock()
                .expect("partition lock");
            let follower = partition
                .replicas
                .get_mut(&ReplicaId::new(2))
                .expect("follower replica");
            crate::KernelService::append(
                follower,
                crate::AppendRequest {
                    datoms: vec![sample_datom(1, 1, "divergent-worker", 2, None)],
                },
            )
            .expect("divergent follower append");
        }

        let status = service
            .partition_status()
            .expect("status with divergent follower");
        let authority = &status.partitions[0];
        let follower = authority
            .replicas
            .iter()
            .find(|replica| replica.replica_id == ReplicaId::new(2))
            .expect("follower status");
        assert!(!follower.healthy);
        assert_eq!(follower.replication_lag, 0);
        assert_eq!(
            follower.detail.as_deref(),
            Some("diverged from leader prefix: entry 1 does not match leader")
        );

        let promoted = service.promote_replica(PromoteReplicaRequest {
            partition: PartitionId::new("authority"),
            replica_id: ReplicaId::new(2),
        });
        assert!(matches!(
            promoted,
            Err(crate::ApiError::Validation(message))
                if message.contains("replica 2 for partition authority diverged from leader prefix: entry 1 does not match leader")
        ));

        drop(service);
        let reopened = ReplicatedAuthorityPartitionService::open(temp.path(), configs);
        assert!(matches!(
            reopened,
            Err(crate::ApiError::Validation(message))
                if message.contains("replica 2 for partition authority diverged from leader prefix: entry 1 does not match leader")
        ));
    }

    #[test]
    fn replicated_partition_service_reuses_identity_but_reissues_trace_handles() {
        let temp = TestPartitionDir::new("replicated-cache");
        let service = ReplicatedAuthorityPartitionService::open(
            temp.path(),
            vec![
                AuthorityPartitionConfig {
                    partition: PartitionId::new("readiness"),
                    replicas: vec![ReplicaConfig {
                        replica_id: ReplicaId::new(1),
                        database_path: PathBuf::from("readiness.sqlite"),
                        role: ReplicaRole::Leader,
                    }],
                },
                AuthorityPartitionConfig {
                    partition: PartitionId::new("authority"),
                    replicas: vec![ReplicaConfig {
                        replica_id: ReplicaId::new(1),
                        database_path: PathBuf::from("authority.sqlite"),
                        role: ReplicaRole::Leader,
                    }],
                },
            ],
        )
        .expect("open replicated partitions");
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("readiness"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "ready", 1, None)],
            })
            .expect("append readiness");
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("authority"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "worker-a", 2, None)],
            })
            .expect("append authority");

        let request = FederatedRunDocumentRequest {
            dsl: federated_assignment_query_document(),
            imports: vec![
                ImportedFactQueryRequest {
                    cut: PartitionCut::as_of("readiness", ElementId::new(1)),
                    dsl: readiness_document(),
                    predicate: PredicateRef {
                        id: PredicateId::new(11),
                        name: "imported_ready_task".into(),
                        arity: 1,
                    },
                    query_name: Some("ready_now".into()),
                },
                ImportedFactQueryRequest {
                    cut: PartitionCut::as_of("authority", ElementId::new(2)),
                    dsl: authority_document(),
                    predicate: PredicateRef {
                        id: PredicateId::new(12),
                        name: "imported_authorized_worker".into(),
                        arity: 2,
                    },
                    query_name: Some("authorized_now".into()),
                },
            ],
            policy_context: None,
        };

        let first = service
            .federated_run_document(request.clone())
            .expect("first federated run");
        let second = service
            .federated_run_document(request.clone())
            .expect("second federated run");
        assert_eq!(first.cut, second.cut);
        assert_eq!(first.run.state, second.run.state);
        assert_eq!(first.run.derived, second.run.derived);
        let first_receipt = first.run.execution.as_ref().expect("first receipt");
        let second_receipt = second.run.execution.as_ref().expect("second receipt");
        assert_eq!(
            first_receipt.manifest.execution_id,
            second_receipt.manifest.execution_id
        );
        assert_ne!(
            first_receipt.trace_handles[0].handle,
            second_receipt.trace_handles[0].handle
        );

        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("authority"),
                leader_epoch: None,
                schema_ref: None,
                expected_cut: None,
                idempotency_key: None,
                principal: None,
                datoms: vec![sample_datom(1, 1, "worker-b", 3, None)],
            })
            .expect("append updated authority");

        let updated = service
            .federated_run_document(FederatedRunDocumentRequest {
                dsl: federated_assignment_query_document(),
                imports: vec![
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("readiness", ElementId::new(1)),
                        dsl: readiness_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(11),
                            name: "imported_ready_task".into(),
                            arity: 1,
                        },
                        query_name: Some("ready_now".into()),
                    },
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("authority", ElementId::new(3)),
                        dsl: authority_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(12),
                            name: "imported_authorized_worker".into(),
                            arity: 2,
                        },
                        query_name: Some("authorized_now".into()),
                    },
                ],
                policy_context: None,
            })
            .expect("updated federated run");
        assert_eq!(
            updated.run.query.as_ref().expect("query result").rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into())
            ]
        );
    }

    fn replicated_authority_config() -> Vec<AuthorityPartitionConfig> {
        vec![AuthorityPartitionConfig {
            partition: PartitionId::new("authority"),
            replicas: vec![
                ReplicaConfig {
                    replica_id: ReplicaId::new(1),
                    database_path: PathBuf::from("authority-leader.sqlite"),
                    role: ReplicaRole::Leader,
                },
                ReplicaConfig {
                    replica_id: ReplicaId::new(2),
                    database_path: PathBuf::from("authority-follower.sqlite"),
                    role: ReplicaRole::Follower,
                },
            ],
        }]
    }

    fn assert_partition_value(
        service: &ReplicatedAuthorityPartitionService,
        cut: PartitionCut,
        expected: &str,
    ) {
        let state = service
            .partition_state(PartitionStateRequest {
                cut,
                schema: schema(),
                policy_context: None,
            })
            .expect("partition state");
        assert_eq!(
            state
                .state
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&AttributeId::new(1))),
            Some(&ResolvedValue::Scalar(Some(Value::String(expected.into()))))
        );
    }

    fn schema() -> Schema {
        let mut schema = Schema::new("partitioned-v1");
        schema
            .register_attribute(AttributeSchema {
                id: AttributeId::new(1),
                name: "task.status".into(),
                class: AttributeClass::ScalarLww,
                value_type: ValueType::String,
            })
            .expect("register status attribute");
        schema
    }

    fn readiness_document() -> String {
        r#"
schema {
  attr task.status: ScalarLWW<String>
}

predicates {
  task_status(Entity, String)
  ready_task(Entity)
}

rules {
  ready_task(t) <- task_status(t, "ready")
}

materialize {
  ready_task
}

query ready_now {
  current
  goal ready_task(t)
  keep t
}
"#
        .into()
    }

    fn authority_document() -> String {
        r#"
schema {
  attr task.owner: ScalarLWW<String>
}

predicates {
  task_owner(Entity, String)
  authorized_worker(Entity, String)
}

rules {
  authorized_worker(t, worker) <- task_owner(t, worker)
}

materialize {
  authorized_worker
}

query authorized_now {
  current
  goal authorized_worker(t, worker)
  keep t, worker
}
"#
        .into()
    }

    fn federated_assignment_document() -> String {
        r#"
schema {
}

predicates {
  imported_ready_task(Entity)
  imported_authorized_worker(Entity, String)
  actionable_assignment(Entity, String)
}

rules {
  actionable_assignment(t, worker) <- imported_ready_task(t), imported_authorized_worker(t, worker)
}

materialize {
  actionable_assignment
}

query actionable_now {
  current
  goal actionable_assignment(t, worker)
  keep t, worker
}

explain actionable_trace {
  tuple actionable_assignment(entity(1), "worker-a")
}
"#
        .into()
    }

    fn federated_assignment_query_document() -> String {
        r#"
schema {
}

predicates {
  imported_ready_task(Entity)
  imported_authorized_worker(Entity, String)
  actionable_assignment(Entity, String)
}

rules {
  actionable_assignment(t, worker) <- imported_ready_task(t), imported_authorized_worker(t, worker)
}

materialize {
  actionable_assignment
}

query actionable_now {
  current
  goal actionable_assignment(t, worker)
  keep t, worker
}
"#
        .into()
    }

    fn joined_import_document() -> String {
        r#"
schema {
  attr task.status: ScalarLWW<String>
  attr task.owner: ScalarLWW<String>
}

predicates {
  task_status(Entity, String)
  task_owner(Entity, String)
}

rules {
}

query joined_now {
  current
  goal task_status(t, "ready")
  goal task_owner(t, worker)
  keep t, worker
}
"#
        .into()
    }

    fn sample_datom(
        entity: u64,
        attribute: u64,
        value: &str,
        element: u64,
        policy: Option<PolicyEnvelope>,
    ) -> Datom {
        Datom {
            entity: EntityId::new(entity),
            attribute: AttributeId::new(attribute),
            value: Value::String(value.into()),
            op: OperationKind::Assert,
            element: ElementId::new(element),
            replica: ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: DatomProvenance::default(),
            policy,
        }
    }

    struct TestPartitionDir {
        path: PathBuf,
    }

    impl TestPartitionDir {
        fn new(name: &str) -> Self {
            let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos();
            let mut path = std::env::temp_dir();
            path.push(format!("aether-partitions-{name}-{nanos}-{unique}"));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestPartitionDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
