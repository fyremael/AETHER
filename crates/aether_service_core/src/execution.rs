use crate::{evaluation::EvaluationKey, NamespaceId, ENGINE_SEMANTICS_VERSION};
use aether_ast::{
    Datom, DerivationTrace, ElementId, PolicyContext, PolicyScope, TemporalView, TupleId,
};
use aether_explain::{Explainer, InMemoryExplainer};
use aether_plan::CompiledProgram;
use aether_resolver::{MaterializedResolver, Resolver};
use aether_runtime::{DerivedSet, RuleRuntime, SemiNaiveRuntime};
use aether_schema::Schema;
use rand::{rngs::OsRng, RngCore};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

pub const DEFAULT_EXECUTION_RETENTION: usize = 1_024;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentDigest(pub String);

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExecutionId(pub String);

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SchemaRef {
    pub version: String,
    pub digest: ContentDigest,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct JournalCut {
    pub visible_last_element: Option<ElementId>,
    pub visible_entry_count: u64,
    pub visible_prefix_digest: ContentDigest,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct FederatedExecutionSource {
    pub partition: String,
    pub as_of: Option<ElementId>,
    pub leader_epoch: Option<u64>,
    pub visible_prefix_digest: ContentDigest,
    pub imported_execution_id: ExecutionId,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct FederationManifest {
    pub sources: Vec<FederatedExecutionSource>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionManifest {
    pub execution_id: ExecutionId,
    pub namespace: NamespaceId,
    pub journal_cut: JournalCut,
    pub schema_ref: SchemaRef,
    pub document_digest: ContentDigest,
    pub compiled_program_digest: ContentDigest,
    pub effective_policy_digest: ContentDigest,
    pub engine_semantics_version: String,
    pub requested_view: TemporalView,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation: Option<FederationManifest>,
    pub created_at_ms: u64,
    pub expires_at_ms: Option<u64>,
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct TraceHandle(String);

impl TraceHandle {
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        Self(hex_encode(&bytes))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for TraceHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("TraceHandle")
            .field(&"<opaque>")
            .finish()
    }
}

impl fmt::Display for TraceHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for TraceHandle {
    type Err = ExecutionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            Ok(Self(value.to_ascii_lowercase()))
        } else {
            Err(ExecutionError::MalformedTraceHandle)
        }
    }
}

impl<'de> Deserialize<'de> for TraceHandle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(D::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceHandleBinding {
    pub local_tuple_id: TupleId,
    pub handle: TraceHandle,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub manifest: ExecutionManifest,
    pub trace_handles: Vec<TraceHandleBinding>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TraceRecord {
    pub handle: TraceHandle,
    pub execution_id: ExecutionId,
    pub local_tuple_id: TupleId,
    pub tuple_digest: ContentDigest,
    pub trace_digest: ContentDigest,
    pub trace: DerivationTrace,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResolveTraceHandleRequest {
    pub handle: TraceHandle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
    #[serde(default)]
    pub verify_replay: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolveTraceHandleResponse {
    pub record: TraceRecord,
    pub digests_verified: bool,
    pub replay_verified: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredExecutionInputs {
    schema: Schema,
    visible_history: Vec<Datom>,
    compiled_program: CompiledProgram,
    effective_policy: PolicyContext,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredExecution {
    manifest: ExecutionManifest,
    manifest_digest: ContentDigest,
    inputs: StoredExecutionInputs,
}

pub trait ExecutionStore: fmt::Debug + Send {
    fn put_execution(&mut self, execution: StoredExecution) -> Result<(), ExecutionStoreError>;
    fn execution(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Option<StoredExecution>, ExecutionStoreError>;
    fn put_trace(&mut self, trace: TraceRecord) -> Result<(), ExecutionStoreError>;
    fn trace(&self, handle: &TraceHandle) -> Result<TraceRecord, ExecutionStoreError>;
    fn expire_execution(&mut self, execution_id: &ExecutionId) -> Result<(), ExecutionStoreError>;
}

#[derive(Debug)]
pub struct InMemoryExecutionStore {
    executions: BTreeMap<ExecutionId, StoredExecution>,
    traces: BTreeMap<TraceHandle, TraceRecord>,
    expired_handles: BTreeSet<TraceHandle>,
    max_executions: usize,
}

impl Default for InMemoryExecutionStore {
    fn default() -> Self {
        Self {
            executions: BTreeMap::new(),
            traces: BTreeMap::new(),
            expired_handles: BTreeSet::new(),
            max_executions: DEFAULT_EXECUTION_RETENTION,
        }
    }
}

impl ExecutionStore for InMemoryExecutionStore {
    fn put_execution(&mut self, execution: StoredExecution) -> Result<(), ExecutionStoreError> {
        self.executions
            .entry(execution.manifest.execution_id.clone())
            .or_insert(execution);
        while self.executions.len() > self.max_executions {
            let oldest = self
                .executions
                .values()
                .min_by_key(|execution| {
                    (
                        execution.manifest.created_at_ms,
                        execution.manifest.execution_id.0.clone(),
                    )
                })
                .map(|execution| execution.manifest.execution_id.clone())
                .ok_or_else(|| ExecutionStoreError::Corrupted("missing oldest execution".into()))?;
            self.expire_execution(&oldest)?;
        }
        Ok(())
    }

    fn execution(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Option<StoredExecution>, ExecutionStoreError> {
        Ok(self.executions.get(execution_id).cloned())
    }

    fn put_trace(&mut self, trace: TraceRecord) -> Result<(), ExecutionStoreError> {
        if self.traces.contains_key(&trace.handle) || self.expired_handles.contains(&trace.handle) {
            return Err(ExecutionStoreError::DuplicateTraceHandle);
        }
        self.traces.insert(trace.handle.clone(), trace);
        Ok(())
    }

    fn trace(&self, handle: &TraceHandle) -> Result<TraceRecord, ExecutionStoreError> {
        if let Some(trace) = self.traces.get(handle) {
            return Ok(trace.clone());
        }
        if self.expired_handles.contains(handle) {
            Err(ExecutionStoreError::ExpiredTraceHandle)
        } else {
            Err(ExecutionStoreError::UnknownTraceHandle)
        }
    }

    fn expire_execution(&mut self, execution_id: &ExecutionId) -> Result<(), ExecutionStoreError> {
        self.executions.remove(execution_id);
        let handles = self
            .traces
            .values()
            .filter(|trace| &trace.execution_id == execution_id)
            .map(|trace| trace.handle.clone())
            .collect::<Vec<_>>();
        for handle in handles {
            self.traces.remove(&handle);
            self.expired_handles.insert(handle);
        }
        Ok(())
    }
}

pub struct SqliteExecutionStore {
    connection: Connection,
    path: PathBuf,
    max_executions: usize,
}

impl fmt::Debug for SqliteExecutionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteExecutionStore")
            .field("path", &self.path)
            .field("max_executions", &self.max_executions)
            .finish()
    }
}

impl SqliteExecutionStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExecutionStoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS execution_records (
                execution_id TEXT PRIMARY KEY,
                created_at_ms INTEGER NOT NULL,
                record_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trace_records (
                handle TEXT PRIMARY KEY,
                execution_id TEXT NOT NULL,
                record_json TEXT NOT NULL,
                FOREIGN KEY(execution_id) REFERENCES execution_records(execution_id)
            );
            CREATE INDEX IF NOT EXISTS trace_records_by_execution
                ON trace_records(execution_id);
            CREATE TABLE IF NOT EXISTS expired_trace_handles (
                handle TEXT PRIMARY KEY,
                expired_at_ms INTEGER NOT NULL
            );
            ",
        )?;
        Ok(Self {
            connection,
            path,
            max_executions: DEFAULT_EXECUTION_RETENTION,
        })
    }

    fn prune(&mut self) -> Result<(), ExecutionStoreError> {
        loop {
            let count: i64 =
                self.connection
                    .query_row("SELECT COUNT(*) FROM execution_records", [], |row| {
                        row.get(0)
                    })?;
            if usize::try_from(count).unwrap_or(usize::MAX) <= self.max_executions {
                return Ok(());
            }
            let oldest = self
                .connection
                .query_row(
                    "SELECT execution_id FROM execution_records
                     ORDER BY created_at_ms ASC, execution_id ASC LIMIT 1",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            let Some(oldest) = oldest else {
                return Err(ExecutionStoreError::Corrupted(
                    "execution count was nonzero without an oldest row".into(),
                ));
            };
            self.expire_execution(&ExecutionId(oldest))?;
        }
    }
}

impl ExecutionStore for SqliteExecutionStore {
    fn put_execution(&mut self, execution: StoredExecution) -> Result<(), ExecutionStoreError> {
        self.connection.execute(
            "INSERT OR IGNORE INTO execution_records (execution_id, created_at_ms, record_json)
             VALUES (?1, ?2, ?3)",
            params![
                &execution.manifest.execution_id.0,
                i64::try_from(execution.manifest.created_at_ms).unwrap_or(i64::MAX),
                serde_json::to_string(&execution)?,
            ],
        )?;
        self.prune()
    }

    fn execution(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Option<StoredExecution>, ExecutionStoreError> {
        let json = self
            .connection
            .query_row(
                "SELECT record_json FROM execution_records WHERE execution_id = ?1",
                params![&execution_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        json.map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(ExecutionStoreError::from)
    }

    fn put_trace(&mut self, trace: TraceRecord) -> Result<(), ExecutionStoreError> {
        let inserted = self.connection.execute(
            "INSERT OR IGNORE INTO trace_records (handle, execution_id, record_json)
             VALUES (?1, ?2, ?3)",
            params![
                trace.handle.as_str(),
                &trace.execution_id.0,
                serde_json::to_string(&trace)?,
            ],
        )?;
        if inserted == 1 {
            Ok(())
        } else {
            Err(ExecutionStoreError::DuplicateTraceHandle)
        }
    }

    fn trace(&self, handle: &TraceHandle) -> Result<TraceRecord, ExecutionStoreError> {
        let json = self
            .connection
            .query_row(
                "SELECT record_json FROM trace_records WHERE handle = ?1",
                params![handle.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(json) = json {
            return serde_json::from_str(&json).map_err(ExecutionStoreError::from);
        }
        let expired = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM expired_trace_handles WHERE handle = ?1)",
            params![handle.as_str()],
            |row| row.get::<_, bool>(0),
        )?;
        if expired {
            Err(ExecutionStoreError::ExpiredTraceHandle)
        } else {
            Err(ExecutionStoreError::UnknownTraceHandle)
        }
    }

    fn expire_execution(&mut self, execution_id: &ExecutionId) -> Result<(), ExecutionStoreError> {
        let transaction = self.connection.transaction()?;
        let mut statement = transaction
            .prepare("SELECT handle FROM trace_records WHERE execution_id = ?1 ORDER BY handle")?;
        let handles = statement
            .query_map(params![&execution_id.0], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        let expired_at_ms = i64::try_from(now_ms()).unwrap_or(i64::MAX);
        for handle in &handles {
            transaction.execute(
                "INSERT OR IGNORE INTO expired_trace_handles (handle, expired_at_ms)
                 VALUES (?1, ?2)",
                params![handle, expired_at_ms],
            )?;
        }
        transaction.execute(
            "DELETE FROM trace_records WHERE execution_id = ?1",
            params![&execution_id.0],
        )?;
        transaction.execute(
            "DELETE FROM execution_records WHERE execution_id = ?1",
            params![&execution_id.0],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

pub fn execution_catalog_path_for_journal(path: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(format!("{}.executions.sqlite", path.as_ref().display()))
}

#[allow(clippy::too_many_arguments)]
pub fn persist_execution(
    store: &mut dyn ExecutionStore,
    namespace: &NamespaceId,
    key: &EvaluationKey,
    schema: &Schema,
    visible_history: Vec<Datom>,
    compiled_program: &CompiledProgram,
    effective_scope: &PolicyScope,
    requested_view: TemporalView,
    derived: &DerivedSet,
    federation: Option<FederationManifest>,
) -> Result<ExecutionReceipt, ExecutionError> {
    let execution_id = ExecutionId(key.to_hex());
    let candidate_manifest = ExecutionManifest {
        execution_id: execution_id.clone(),
        namespace: namespace.clone(),
        journal_cut: JournalCut {
            visible_last_element: visible_history.last().map(|datom| datom.element),
            visible_entry_count: visible_history.len() as u64,
            visible_prefix_digest: digest_json(&visible_history)?,
        },
        schema_ref: SchemaRef {
            version: schema.version.clone(),
            digest: digest_json(schema)?,
        },
        document_digest: digest_json(&("aether-scoped-document-v1", compiled_program))?,
        compiled_program_digest: digest_json(compiled_program)?,
        effective_policy_digest: digest_json(effective_scope.context())?,
        engine_semantics_version: ENGINE_SEMANTICS_VERSION.into(),
        requested_view,
        federation,
        created_at_ms: now_ms(),
        expires_at_ms: None,
    };
    let inputs = StoredExecutionInputs {
        schema: schema.clone(),
        visible_history,
        compiled_program: compiled_program.clone(),
        effective_policy: effective_scope.context().clone(),
    };
    let manifest = match store.execution(&execution_id)? {
        Some(existing) => {
            if digest_json(&existing.manifest)? != existing.manifest_digest
                || existing.inputs != inputs
                || existing.manifest.engine_semantics_version != ENGINE_SEMANTICS_VERSION
            {
                return Err(ExecutionError::CorruptedExecutionManifest);
            }
            existing.manifest
        }
        None => {
            let manifest_digest = digest_json(&candidate_manifest)?;
            store.put_execution(StoredExecution {
                manifest: candidate_manifest.clone(),
                manifest_digest,
                inputs,
            })?;
            candidate_manifest
        }
    };

    let explainer = InMemoryExplainer::from_derived_set(derived);
    let mut trace_handles = Vec::with_capacity(derived.tuples.len());
    for tuple in &derived.tuples {
        let trace = explainer.explain_tuple(&tuple.tuple.id)?;
        let tuple_digest = digest_json(tuple)?;
        let trace_digest = digest_json(&trace)?;
        let record = loop {
            let handle = TraceHandle::generate();
            let record = TraceRecord {
                handle: handle.clone(),
                execution_id: execution_id.clone(),
                local_tuple_id: tuple.tuple.id,
                tuple_digest: tuple_digest.clone(),
                trace_digest: trace_digest.clone(),
                trace: trace.clone(),
            };
            match store.put_trace(record.clone()) {
                Ok(()) => break record,
                Err(ExecutionStoreError::DuplicateTraceHandle) => continue,
                Err(error) => return Err(error.into()),
            }
        };
        trace_handles.push(TraceHandleBinding {
            local_tuple_id: tuple.tuple.id,
            handle: record.handle,
        });
    }

    Ok(ExecutionReceipt {
        manifest,
        trace_handles,
    })
}

pub fn resolve_trace(
    store: &mut dyn ExecutionStore,
    namespace: &NamespaceId,
    request: ResolveTraceHandleRequest,
) -> Result<ResolveTraceHandleResponse, ExecutionError> {
    let record = store.trace(&request.handle)?;
    let execution = store
        .execution(&record.execution_id)?
        .ok_or(ExecutionError::CorruptedExecutionManifest)?;
    if digest_json(&execution.manifest)? != execution.manifest_digest
        || execution.manifest.execution_id != record.execution_id
    {
        return Err(ExecutionError::CorruptedExecutionManifest);
    }
    if &execution.manifest.namespace != namespace {
        return Err(ExecutionError::UnknownTraceHandle);
    }
    if execution.manifest.engine_semantics_version != ENGINE_SEMANTICS_VERSION {
        return Err(ExecutionError::IncompatibleEngineSemantics);
    }

    let requested_scope = PolicyScope::from_optional(request.policy_context);
    if !execution
        .inputs
        .effective_policy
        .subset_of(requested_scope.context())
    {
        return Err(ExecutionError::InsufficientPolicy);
    }

    verify_record_digests(&record)?;
    let replay_verified = if request.verify_replay {
        verify_replay(&execution, &record)?;
        true
    } else {
        false
    };
    Ok(ResolveTraceHandleResponse {
        record,
        digests_verified: true,
        replay_verified,
    })
}

fn verify_record_digests(record: &TraceRecord) -> Result<(), ExecutionError> {
    if digest_json(&record.trace)? != record.trace_digest {
        return Err(ExecutionError::CorruptedTraceRecord);
    }
    let tuple = record
        .trace
        .tuples
        .iter()
        .find(|tuple| tuple.tuple.id == record.local_tuple_id)
        .ok_or(ExecutionError::CorruptedTraceRecord)?;
    if digest_json(tuple)? != record.tuple_digest {
        return Err(ExecutionError::CorruptedTraceRecord);
    }
    Ok(())
}

fn verify_replay(execution: &StoredExecution, record: &TraceRecord) -> Result<(), ExecutionError> {
    let state = MaterializedResolver
        .current(&execution.inputs.schema, &execution.inputs.visible_history)?;
    let derived = SemiNaiveRuntime.evaluate(&state, &execution.inputs.compiled_program)?;
    let replayed = InMemoryExplainer::from_derived_set(&derived)
        .explain_tuple(&record.local_tuple_id)
        .map_err(|_| ExecutionError::ReplayMismatch)?;
    if digest_json(&replayed)? != record.trace_digest {
        return Err(ExecutionError::ReplayMismatch);
    }
    Ok(())
}

fn digest_json<T: Serialize + ?Sized>(value: &T) -> Result<ContentDigest, ExecutionError> {
    Ok(digest_bytes(&serde_json::to_vec(value)?))
}

fn digest_bytes(bytes: &[u8]) -> ContentDigest {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    ContentDigest(hex_encode(&hasher.finalize()))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Debug, Error)]
pub enum ExecutionStoreError {
    #[error("trace handle is unknown")]
    UnknownTraceHandle,
    #[error("trace handle has expired")]
    ExpiredTraceHandle,
    #[error("trace handle collision")]
    DuplicateTraceHandle,
    #[error("execution store is corrupted: {0}")]
    Corrupted(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("trace handle is malformed")]
    MalformedTraceHandle,
    #[error("trace handle is unknown")]
    UnknownTraceHandle,
    #[error("trace handle has expired")]
    ExpiredTraceHandle,
    #[error("current authorization does not permit the original execution policy")]
    InsufficientPolicy,
    #[error("execution manifest is corrupted")]
    CorruptedExecutionManifest,
    #[error("trace record is corrupted")]
    CorruptedTraceRecord,
    #[error("stored execution uses incompatible engine semantics")]
    IncompatibleEngineSemantics,
    #[error("verified replay did not reproduce the stored trace")]
    ReplayMismatch,
    #[error(transparent)]
    Resolve(#[from] aether_resolver::ResolveError),
    #[error(transparent)]
    Runtime(#[from] aether_runtime::RuntimeError),
    #[error(transparent)]
    Explain(#[from] aether_explain::ExplainError),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error("execution store failure: {0}")]
    Store(String),
}

impl From<ExecutionStoreError> for ExecutionError {
    fn from(error: ExecutionStoreError) -> Self {
        match error {
            ExecutionStoreError::UnknownTraceHandle => Self::UnknownTraceHandle,
            ExecutionStoreError::ExpiredTraceHandle => Self::ExpiredTraceHandle,
            other => Self::Store(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

    fn stored_execution(id: &str, engine: &str) -> StoredExecution {
        let manifest = ExecutionManifest {
            execution_id: ExecutionId(id.into()),
            namespace: NamespaceId::default(),
            journal_cut: JournalCut::default(),
            schema_ref: SchemaRef::default(),
            document_digest: ContentDigest::default(),
            compiled_program_digest: ContentDigest::default(),
            effective_policy_digest: ContentDigest::default(),
            engine_semantics_version: engine.into(),
            requested_view: TemporalView::Current,
            federation: None,
            created_at_ms: 1,
            expires_at_ms: None,
        };
        StoredExecution {
            manifest_digest: digest_json(&manifest).expect("digest manifest"),
            manifest,
            inputs: StoredExecutionInputs {
                schema: Schema::new("test-v1"),
                visible_history: Vec::new(),
                compiled_program: CompiledProgram::default(),
                effective_policy: PolicyContext::default(),
            },
        }
    }

    fn trace_record(execution_id: &str) -> TraceRecord {
        TraceRecord {
            handle: TraceHandle::generate(),
            execution_id: ExecutionId(execution_id.into()),
            local_tuple_id: TupleId::new(1),
            tuple_digest: ContentDigest("invalid".into()),
            trace_digest: ContentDigest("invalid".into()),
            trace: DerivationTrace::default(),
        }
    }

    #[test]
    fn expired_handles_are_tombstoned_and_never_become_unknown() {
        let mut store = InMemoryExecutionStore::default();
        let execution = stored_execution("execution-a", ENGINE_SEMANTICS_VERSION);
        let record = trace_record("execution-a");
        let handle = record.handle.clone();
        store.put_execution(execution).expect("put execution");
        store.put_trace(record).expect("put trace");
        store
            .expire_execution(&ExecutionId("execution-a".into()))
            .expect("expire execution");

        assert!(matches!(
            store.trace(&handle),
            Err(ExecutionStoreError::ExpiredTraceHandle)
        ));
        assert!(matches!(
            store.put_trace(TraceRecord {
                handle,
                ..trace_record("execution-b")
            }),
            Err(ExecutionStoreError::DuplicateTraceHandle)
        ));
    }

    #[test]
    fn retention_quota_evicts_oldest_execution_and_tombstones_its_handles() {
        let mut memory = InMemoryExecutionStore {
            max_executions: 2,
            ..Default::default()
        };
        let memory_trace = trace_record("execution-a");
        let memory_handle = memory_trace.handle.clone();
        memory
            .put_execution(stored_execution("execution-a", ENGINE_SEMANTICS_VERSION))
            .expect("put first memory execution");
        memory.put_trace(memory_trace).expect("put memory trace");
        for id in ["execution-b", "execution-c"] {
            memory
                .put_execution(stored_execution(id, ENGINE_SEMANTICS_VERSION))
                .expect("put memory execution");
        }
        assert!(matches!(
            memory.trace(&memory_handle),
            Err(ExecutionStoreError::ExpiredTraceHandle)
        ));

        let nonce = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("aether-retention-{nonce}.sqlite"));
        let mut sqlite = SqliteExecutionStore::open(&path).expect("open sqlite store");
        sqlite.max_executions = 2;
        let sqlite_trace = trace_record("execution-a");
        let sqlite_handle = sqlite_trace.handle.clone();
        sqlite
            .put_execution(stored_execution("execution-a", ENGINE_SEMANTICS_VERSION))
            .expect("put first sqlite execution");
        sqlite.put_trace(sqlite_trace).expect("put sqlite trace");
        for id in ["execution-b", "execution-c"] {
            sqlite
                .put_execution(stored_execution(id, ENGINE_SEMANTICS_VERSION))
                .expect("put sqlite execution");
        }
        assert!(matches!(
            sqlite.trace(&sqlite_handle),
            Err(ExecutionStoreError::ExpiredTraceHandle)
        ));
        drop(sqlite);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn missing_manifest_corrupted_trace_and_incompatible_engine_fail_closed() {
        let namespace = NamespaceId::default();

        let mut missing = InMemoryExecutionStore::default();
        let missing_record = trace_record("missing");
        let missing_handle = missing_record.handle.clone();
        missing.put_trace(missing_record).expect("put orphan trace");
        assert!(matches!(
            resolve_trace(
                &mut missing,
                &namespace,
                ResolveTraceHandleRequest {
                    handle: missing_handle,
                    policy_context: None,
                    verify_replay: false,
                }
            ),
            Err(ExecutionError::CorruptedExecutionManifest)
        ));

        let mut corrupted_manifest = InMemoryExecutionStore::default();
        let mut altered = stored_execution("altered", ENGINE_SEMANTICS_VERSION);
        altered.manifest.schema_ref.version = "tampered".into();
        corrupted_manifest
            .put_execution(altered)
            .expect("put altered manifest");
        let altered_record = trace_record("altered");
        let altered_handle = altered_record.handle.clone();
        corrupted_manifest
            .put_trace(altered_record)
            .expect("put altered trace");
        assert!(matches!(
            resolve_trace(
                &mut corrupted_manifest,
                &namespace,
                ResolveTraceHandleRequest {
                    handle: altered_handle,
                    policy_context: None,
                    verify_replay: false,
                }
            ),
            Err(ExecutionError::CorruptedExecutionManifest)
        ));

        let mut incompatible = InMemoryExecutionStore::default();
        incompatible
            .put_execution(stored_execution("old", "old-engine"))
            .expect("put old execution");
        let old_record = trace_record("old");
        let old_handle = old_record.handle.clone();
        incompatible.put_trace(old_record).expect("put old trace");
        assert!(matches!(
            resolve_trace(
                &mut incompatible,
                &namespace,
                ResolveTraceHandleRequest {
                    handle: old_handle,
                    policy_context: None,
                    verify_replay: false,
                }
            ),
            Err(ExecutionError::IncompatibleEngineSemantics)
        ));

        let mut corrupted = InMemoryExecutionStore::default();
        corrupted
            .put_execution(stored_execution("corrupt", ENGINE_SEMANTICS_VERSION))
            .expect("put execution");
        let corrupt_record = trace_record("corrupt");
        let corrupt_handle = corrupt_record.handle.clone();
        corrupted
            .put_trace(corrupt_record)
            .expect("put corrupt trace");
        assert!(matches!(
            resolve_trace(
                &mut corrupted,
                &namespace,
                ResolveTraceHandleRequest {
                    handle: corrupt_handle,
                    policy_context: None,
                    verify_replay: false,
                }
            ),
            Err(ExecutionError::CorruptedTraceRecord)
        ));
    }

    #[test]
    fn local_metadata_store_survives_postgres_mode_backup_and_restore() {
        let unique = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "aether-postgres-execution-metadata-{}-{unique}",
            now_ms()
        ));
        let live = root.join("sidecars.sqlite.executions.sqlite");
        let backup = root.join("backup.executions.sqlite");
        std::fs::create_dir_all(&root).expect("create test directory");

        let handle = {
            let mut store = SqliteExecutionStore::open(&live).expect("open live store");
            store
                .put_execution(stored_execution("durable", ENGINE_SEMANTICS_VERSION))
                .expect("put execution");
            let record = trace_record("durable");
            let handle = record.handle.clone();
            store.put_trace(record).expect("put trace");
            handle
        };
        std::fs::copy(&live, &backup).expect("copy execution backup");
        std::fs::remove_file(&live).expect("remove live execution store");
        std::fs::copy(&backup, &live).expect("restore execution store");

        let restored = SqliteExecutionStore::open(&live).expect("reopen restored store");
        assert_eq!(
            restored.trace(&handle).expect("read restored trace").handle,
            handle
        );
        drop(restored);
        let _ = std::fs::remove_dir_all(root);
    }
}
