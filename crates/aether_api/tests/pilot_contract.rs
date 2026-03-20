use aether_api::{
    AppendRequest, ExplainTupleRequest, InMemoryKernelService, KernelService, RunDocumentRequest,
    SqliteKernelService,
};
use aether_ast::{AttributeId, Datom, DatomProvenance, ElementId, EntityId, Value};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

#[test]
fn coordination_pilot_contract_survives_sqlite_restart() {
    let temp = TestDbPath::new("coordination-pilot");
    let before_restart = {
        let mut service = SqliteKernelService::open(temp.path()).expect("open sqlite service");
        service
            .append(AppendRequest {
                datoms: coordination_history(),
            })
            .expect("append coordination history");
        capture_contract(&mut service)
    };

    let after_restart = {
        let mut service = SqliteKernelService::open(temp.path()).expect("reopen sqlite service");
        capture_contract(&mut service)
    };

    assert_eq!(before_restart, after_restart);
    assert_eq!(after_restart.history_len, 7);
    assert_eq!(
        after_restart.as_of_authorized,
        vec![vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-a".into()),
            Value::U64(1),
        ]]
    );
    assert_eq!(
        after_restart.current_authorized,
        vec![vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-b".into()),
            Value::U64(2),
        ]]
    );
    assert_eq!(
        after_restart.claimable,
        vec![
            vec![
                Value::Entity(EntityId::new(3)),
                Value::String("worker-a".into()),
            ],
            vec![
                Value::Entity(EntityId::new(3)),
                Value::String("worker-b".into()),
            ],
        ]
    );
    assert_eq!(
        after_restart.stale,
        vec![
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into()),
                Value::U64(1),
            ],
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into()),
                Value::U64(2),
            ],
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into()),
                Value::U64(1),
            ],
        ]
    );
    assert!(after_restart.trace_tuple_count > 0);
}

#[test]
fn in_memory_and_sqlite_services_agree_on_pilot_contract() {
    let mut in_memory = InMemoryKernelService::new();
    in_memory
        .append(AppendRequest {
            datoms: coordination_history(),
        })
        .expect("append in-memory history");

    let temp = TestDbPath::new("contract-equivalence");
    let mut sqlite = SqliteKernelService::open(temp.path()).expect("open sqlite service");
    sqlite
        .append(AppendRequest {
            datoms: coordination_history(),
        })
        .expect("append sqlite history");

    assert_eq!(
        capture_contract(&mut in_memory),
        capture_contract(&mut sqlite)
    );
}

#[derive(Debug, PartialEq)]
struct PilotContractSnapshot {
    history_len: usize,
    as_of_authorized: Vec<Vec<Value>>,
    current_authorized: Vec<Vec<Value>>,
    claimable: Vec<Vec<Value>>,
    stale: Vec<Vec<Value>>,
    trace_tuple_count: usize,
}

fn capture_contract(service: &mut impl KernelService) -> PilotContractSnapshot {
    let history_len = service
        .history(Default::default())
        .expect("history response")
        .datoms
        .len();

    let as_of_authorized = service
        .run_document(RunDocumentRequest {
            dsl: coordination_dsl(
                "as_of e5",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
        })
        .expect("run as-of authorization")
        .query
        .expect("as-of query result")
        .rows;

    let current_authorized_response = service
        .run_document(RunDocumentRequest {
            dsl: coordination_dsl(
                "current",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
        })
        .expect("run current authorization");
    let current_authorized = current_authorized_response
        .query
        .expect("current query result")
        .rows;
    let trace_tuple_count = service
        .explain_tuple(ExplainTupleRequest {
            tuple_id: current_authorized[0].tuple_id.expect("authorized tuple id"),
        })
        .expect("explain current authorization")
        .trace
        .tuples
        .len();

    let claimable = service
        .run_document(RunDocumentRequest {
            dsl: coordination_dsl(
                "current",
                "goal worker_can_claim(t, worker)\n  keep t, worker",
            ),
        })
        .expect("run claimability")
        .query
        .expect("claimability query result")
        .rows;

    let stale = service
        .run_document(RunDocumentRequest {
            dsl: coordination_dsl(
                "current",
                "goal execution_rejected_stale(t, worker, epoch)\n  keep t, worker, epoch",
            ),
        })
        .expect("run stale rejection")
        .query
        .expect("stale query result")
        .rows;

    PilotContractSnapshot {
        history_len,
        as_of_authorized: row_values(as_of_authorized),
        current_authorized: row_values(current_authorized),
        claimable: row_values(claimable),
        stale: row_values(stale),
        trace_tuple_count,
    }
}

fn row_values(rows: Vec<aether_ast::QueryRow>) -> Vec<Vec<Value>> {
    rows.into_iter().map(|row| row.values).collect()
}

fn coordination_dsl(view: &str, query_body: &str) -> String {
    format!(
        r#"
schema v1 {{
  attr task.depends_on: RefSet<Entity>
  attr task.status: ScalarLWW<String>
  attr task.claimed_by: ScalarLWW<String>
  attr task.lease_epoch: ScalarLWW<U64>
  attr task.lease_state: ScalarLWW<String>
}}

predicates {{
  task(Entity)
  worker(String)
  worker_capability(String, String)
  execution_attempt(Entity, String, U64)
  task_depends_on(Entity, Entity)
  task_status(Entity, String)
  task_claimed_by(Entity, String)
  task_lease_epoch(Entity, U64)
  task_lease_state(Entity, String)
  task_complete(Entity)
  dependency_blocked(Entity)
  lease_active(Entity, String, U64)
  active_claim(Entity)
  task_ready(Entity)
  worker_can_claim(Entity, String)
  execution_authorized(Entity, String, U64)
  execution_rejected_stale(Entity, String, U64)
}}

facts {{
  task(entity(1))
  task(entity(2))
  task(entity(3))
  worker("worker-a")
  worker("worker-b")
  worker_capability("worker-a", "executor")
  worker_capability("worker-b", "executor")
  execution_attempt(entity(1), "worker-a", 1)
  execution_attempt(entity(1), "worker-b", 1)
  execution_attempt(entity(1), "worker-a", 2)
  execution_attempt(entity(1), "worker-b", 2) @capability("executor") @visibility("ops")
}}

rules {{
  task_complete(t) <- task_status(t, "done")
  dependency_blocked(t) <- task_depends_on(t, dep), not task_complete(dep)
  lease_active(t, w, epoch) <- task_claimed_by(t, w), task_lease_epoch(t, epoch), task_lease_state(t, "active")
  active_claim(t) <- lease_active(t, w, epoch)
  task_ready(t) <- task(t), not task_complete(t), not dependency_blocked(t), not active_claim(t)
  worker_can_claim(t, w) <- task_ready(t), worker(w), worker_capability(w, "executor")
  execution_authorized(t, w, epoch) <- execution_attempt(t, w, epoch), lease_active(t, w, epoch)
  execution_rejected_stale(t, worker, epoch) <- execution_attempt(t, worker, epoch), not lease_active(t, worker, epoch)
}}

materialize {{
  task_ready
  worker_can_claim
  execution_authorized
  execution_rejected_stale
}}

query {{
  {view}
  {query_body}
}}
"#
    )
}

fn coordination_history() -> Vec<Datom> {
    vec![
        dependency_datom(1, 2, 1),
        datom(2, 2, Value::String("done".into()), 2),
        datom(1, 3, Value::String("worker-a".into()), 3),
        datom(1, 4, Value::U64(1), 4),
        datom(1, 5, Value::String("active".into()), 5),
        datom(1, 3, Value::String("worker-b".into()), 6),
        datom(1, 4, Value::U64(2), 7),
    ]
}

fn dependency_datom(entity: u64, value: u64, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(1),
        value: Value::Entity(EntityId::new(value)),
        op: aether_ast::OperationKind::Add,
        element: ElementId::new(element),
        replica: aether_ast::ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn datom(entity: u64, attribute: u64, value: Value, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(attribute),
        value,
        op: aether_ast::OperationKind::Assert,
        element: ElementId::new(element),
        replica: aether_ast::ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

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
        let mut path = std::env::temp_dir();
        path.push(format!("aether-api-{name}-{nanos}-{unique}.sqlite"));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDbPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);

        let wal = PathBuf::from(format!("{}-wal", self.path.display()));
        let shm = PathBuf::from(format!("{}-shm", self.path.display()));
        let _ = std::fs::remove_file(wal);
        let _ = std::fs::remove_file(shm);
    }
}
