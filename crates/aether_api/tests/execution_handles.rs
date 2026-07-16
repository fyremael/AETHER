use aether_api::{
    execution::{execution_catalog_path_for_journal, ExecutionError},
    AppendRequest, InMemoryKernelService, KernelService, NamespaceId, ResolveTraceHandleRequest,
    RunDocumentRequest, SqliteKernelService, TraceHandle,
};
use aether_ast::{
    AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, PolicyContext,
    ReplicaId, Value,
};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn tuple_ids_from_different_runs_resolve_through_distinct_correct_handles() {
    let mut service = InMemoryKernelService::new();
    let first = service
        .run_document(RunDocumentRequest {
            dsl: fact_document(1, false),
            policy_context: None,
        })
        .expect("run first document");
    let second = service
        .run_document(RunDocumentRequest {
            dsl: fact_document(2, false),
            policy_context: None,
        })
        .expect("run second document");

    let first_binding = first
        .execution
        .as_ref()
        .expect("first execution receipt")
        .trace_handles
        .first()
        .expect("first trace handle");
    let second_binding = second
        .execution
        .as_ref()
        .expect("second execution receipt")
        .trace_handles
        .first()
        .expect("second trace handle");
    assert_eq!(first_binding.local_tuple_id, second_binding.local_tuple_id);
    assert_ne!(first_binding.handle, second_binding.handle);
    assert_ne!(
        first.execution.as_ref().unwrap().manifest.execution_id,
        second.execution.as_ref().unwrap().manifest.execution_id
    );

    let first_trace = resolve(&mut service, first_binding.handle.clone(), None, true);
    let second_trace = resolve(&mut service, second_binding.handle.clone(), None, true);
    assert!(first_trace.record.trace.tuples.iter().any(|tuple| {
        tuple.tuple.id == first_binding.local_tuple_id
            && tuple.tuple.values == vec![Value::Entity(EntityId::new(1))]
    }));
    assert!(second_trace.record.trace.tuples.iter().any(|tuple| {
        tuple.tuple.id == second_binding.local_tuple_id
            && tuple.tuple.values == vec![Value::Entity(EntityId::new(2))]
    }));
    assert!(first_trace.digests_verified && first_trace.replay_verified);
    assert!(second_trace.digests_verified && second_trace.replay_verified);
}

#[test]
fn equivalent_in_memory_executions_reuse_one_persisted_opaque_handle() {
    let mut service = InMemoryKernelService::new();
    let first = service
        .run_document(RunDocumentRequest {
            dsl: fact_document(7, false),
            policy_context: None,
        })
        .expect("run first equivalent document");
    let second = service
        .run_document(RunDocumentRequest {
            dsl: fact_document(7, false),
            policy_context: None,
        })
        .expect("run second equivalent document");
    let first = first.execution.expect("first receipt");
    let second = second.execution.expect("second receipt");

    assert_eq!(first.manifest.execution_id, second.manifest.execution_id);
    assert_eq!(
        first.trace_handles[0].handle,
        second.trace_handles[0].handle
    );
    assert_eq!(first.trace_handles[0].handle.as_str().len(), 64);
    assert!(first.trace_handles[0]
        .handle
        .as_str()
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit()));
    assert!(!first.trace_handles[0].handle.as_str().contains("default"));
}

#[test]
fn pre_append_handle_remains_bound_to_the_pre_append_execution() {
    let mut service = InMemoryKernelService::new();
    service
        .append(AppendRequest {
            datoms: vec![status_datom(1, "ready")],
        })
        .expect("append initial state");
    let before = service
        .run_document(RunDocumentRequest {
            dsl: journal_document(),
            policy_context: None,
        })
        .expect("run before append");
    let binding = before.execution.unwrap().trace_handles[0].clone();

    service
        .append(AppendRequest {
            datoms: vec![status_datom(2, "blocked")],
        })
        .expect("append later state");
    let after = service
        .run_document(RunDocumentRequest {
            dsl: journal_document(),
            policy_context: None,
        })
        .expect("run after append");
    assert!(after.derived.tuples.is_empty());

    let resolved = resolve(&mut service, binding.handle, None, true);
    assert!(resolved
        .record
        .trace
        .tuples
        .iter()
        .any(|tuple| { tuple.tuple.values == vec![Value::Entity(EntityId::new(1))] }));
}

#[test]
fn sqlite_execution_handles_survive_service_restart() {
    let temp = TestDbPath::new("execution-restart");
    let handle = {
        let mut service = SqliteKernelService::open(temp.path()).expect("open sqlite service");
        let response = service
            .run_document(RunDocumentRequest {
                dsl: fact_document(11, false),
                policy_context: None,
            })
            .expect("run durable execution");
        response.execution.unwrap().trace_handles[0].handle.clone()
    };

    let mut reopened = SqliteKernelService::open(temp.path()).expect("reopen sqlite service");
    let resolved = resolve(&mut reopened, handle, None, true);
    assert!(resolved
        .record
        .trace
        .tuples
        .iter()
        .any(|tuple| { tuple.tuple.values == vec![Value::Entity(EntityId::new(11))] }));
}

#[test]
fn equivalent_sqlite_executions_reuse_one_trace_row_across_restart() {
    let temp = TestDbPath::new("execution-reuse");
    let first_handle = {
        let mut service = SqliteKernelService::open(temp.path()).expect("open sqlite service");
        let mut handles = Vec::new();
        for _ in 0..128 {
            handles.push(
                service
                    .run_document(RunDocumentRequest {
                        dsl: fact_document(17, false),
                        policy_context: None,
                    })
                    .expect("run equivalent durable execution")
                    .execution
                    .expect("execution receipt")
                    .trace_handles[0]
                    .handle
                    .clone(),
            );
        }
        assert!(handles.windows(2).all(|pair| pair[0] == pair[1]));
        handles[0].clone()
    };

    let catalog = execution_catalog_path_for_journal(temp.path());
    let connection = rusqlite::Connection::open(&catalog).expect("open execution catalog");
    let executions: i64 = connection
        .query_row("SELECT COUNT(*) FROM execution_records", [], |row| {
            row.get(0)
        })
        .expect("count executions");
    let traces: i64 = connection
        .query_row("SELECT COUNT(*) FROM trace_records", [], |row| row.get(0))
        .expect("count traces");
    assert_eq!(executions, 1);
    assert_eq!(traces, 1);
    drop(connection);

    let mut reopened = SqliteKernelService::open(temp.path()).expect("reopen sqlite service");
    let reopened_handle = reopened
        .run_document(RunDocumentRequest {
            dsl: fact_document(17, false),
            policy_context: None,
        })
        .expect("rerun equivalent execution after restart")
        .execution
        .expect("execution receipt")
        .trace_handles[0]
        .handle
        .clone();
    assert_eq!(reopened_handle, first_handle);
}

#[test]
fn handle_resolution_enforces_original_policy_and_namespace() {
    let alpha = NamespaceId::new("alpha").expect("alpha namespace");
    let beta = NamespaceId::new("beta").expect("beta namespace");
    let mut service = InMemoryKernelService::new().with_namespace(alpha);
    let response = service
        .run_document(RunDocumentRequest {
            dsl: fact_document(13, true),
            policy_context: Some(restricted_context()),
        })
        .expect("run protected execution");
    let handle = response.execution.unwrap().trace_handles[0].handle.clone();

    let denied = service.resolve_trace_handle(ResolveTraceHandleRequest {
        handle: handle.clone(),
        policy_context: None,
        verify_replay: false,
    });
    assert!(matches!(
        denied,
        Err(aether_api::ApiError::Execution(
            ExecutionError::InsufficientPolicy
        ))
    ));
    resolve(
        &mut service,
        handle.clone(),
        Some(restricted_context()),
        false,
    );

    let mut service = service.with_namespace(beta);
    let cross_namespace = service.resolve_trace_handle(ResolveTraceHandleRequest {
        handle,
        policy_context: Some(restricted_context()),
        verify_replay: false,
    });
    assert!(matches!(
        cross_namespace,
        Err(aether_api::ApiError::Execution(
            ExecutionError::UnknownTraceHandle
        ))
    ));
}

#[test]
fn malformed_handle_is_rejected_before_store_lookup() {
    let malformed = "not-a-handle".parse::<TraceHandle>();
    assert!(matches!(
        malformed,
        Err(ExecutionError::MalformedTraceHandle)
    ));
}

fn resolve(
    service: &mut impl KernelService,
    handle: TraceHandle,
    policy_context: Option<PolicyContext>,
    verify_replay: bool,
) -> aether_api::ResolveTraceHandleResponse {
    service
        .resolve_trace_handle(ResolveTraceHandleRequest {
            handle,
            policy_context,
            verify_replay,
        })
        .expect("resolve trace handle")
}

fn restricted_context() -> PolicyContext {
    PolicyContext {
        capabilities: vec!["restricted".into()],
        visibilities: Vec::new(),
    }
}

fn fact_document(entity: u64, restricted: bool) -> String {
    let policy = if restricted {
        " @capability(\"restricted\")"
    } else {
        ""
    };
    format!(
        r#"
schema trace_v1 {{
}}

predicates {{
  source(Entity)
  derived(Entity)
}}

facts {{
  source(entity({entity})){policy}
}}

rules {{
  derived(x) <- source(x)
}}

materialize {{
  derived
}}

query {{
  current
  goal derived(x)
  keep x
}}
"#
    )
}

fn journal_document() -> String {
    r#"
schema trace_journal_v1 {
  attr task.status: ScalarLWW<String>
}

predicates {
  task_status(Entity, String)
  ready(Entity)
}

rules {
  ready(task) <- task_status(task, "ready")
}

materialize {
  ready
}

query {
  current
  goal ready(task)
  keep task
}
"#
    .into()
}

fn status_datom(element: u64, status: &str) -> Datom {
    Datom {
        entity: EntityId::new(1),
        attribute: AttributeId::new(1),
        value: Value::String(status.into()),
        op: OperationKind::Assert,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

struct TestDbPath {
    path: PathBuf,
}

impl TestDbPath {
    fn new(name: &str) -> Self {
        let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        Self {
            path: std::env::temp_dir().join(format!("aether-{name}-{nanos}-{unique}.sqlite")),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDbPath {
    fn drop(&mut self) {
        for path in [
            self.path.clone(),
            PathBuf::from(format!("{}.sidecars.sqlite", self.path.display())),
            PathBuf::from(format!("{}.executions.sqlite", self.path.display())),
        ] {
            let _ = std::fs::remove_file(&path);
            for suffix in ["-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
            }
        }
    }
}
