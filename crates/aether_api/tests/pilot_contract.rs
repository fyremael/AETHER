use aether_api::{
    coordination_pilot_dsl, coordination_pilot_seed_history, AppendRequest, ExplainTupleRequest,
    InMemoryKernelService, KernelService, RunDocumentRequest, SqliteKernelService,
    COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT, COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
};
use aether_ast::{EntityId, Value};
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
                datoms: coordination_pilot_seed_history(),
            })
            .expect("append coordination history");
        capture_contract(&mut service)
    };

    let after_restart = {
        let mut service = SqliteKernelService::open(temp.path()).expect("reopen sqlite service");
        capture_contract(&mut service)
    };

    assert_eq!(before_restart, after_restart);
    assert_eq!(after_restart.history_len, 25);
    assert!(after_restart.pre_heartbeat_authorized.is_empty());
    assert_eq!(
        after_restart.as_of_authorized,
        vec![vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-a".into()),
            Value::U64(1),
        ]]
    );
    assert_eq!(
        after_restart.live_heartbeats,
        vec![vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-b".into()),
            Value::U64(2),
            Value::U64(200),
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
        after_restart.accepted_outcomes,
        vec![vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-b".into()),
            Value::U64(2),
            Value::String("completed".into()),
            Value::String("current-worker-b".into()),
        ]]
    );
    assert_eq!(
        after_restart.rejected_outcomes,
        vec![vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-a".into()),
            Value::U64(1),
            Value::String("completed".into()),
            Value::String("stale-worker-a".into()),
        ],]
    );
    assert!(after_restart.trace_tuple_count > 0);
}

#[test]
fn in_memory_and_sqlite_services_agree_on_pilot_contract() {
    let mut in_memory = InMemoryKernelService::new();
    in_memory
        .append(AppendRequest {
            datoms: coordination_pilot_seed_history(),
        })
        .expect("append in-memory history");

    let temp = TestDbPath::new("contract-equivalence");
    let mut sqlite = SqliteKernelService::open(temp.path()).expect("open sqlite service");
    sqlite
        .append(AppendRequest {
            datoms: coordination_pilot_seed_history(),
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
    pre_heartbeat_authorized: Vec<Vec<Value>>,
    as_of_authorized: Vec<Vec<Value>>,
    live_heartbeats: Vec<Vec<Value>>,
    current_authorized: Vec<Vec<Value>>,
    claimable: Vec<Vec<Value>>,
    accepted_outcomes: Vec<Vec<Value>>,
    rejected_outcomes: Vec<Vec<Value>>,
    trace_tuple_count: usize,
}

fn capture_contract(service: &mut impl KernelService) -> PilotContractSnapshot {
    let history_len = service
        .history(Default::default())
        .expect("history response")
        .datoms
        .len();

    let pre_heartbeat_authorized = service
        .run_document(RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                &format!("as_of e{}", COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT),
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
            policy_context: None,
        })
        .expect("run pre-heartbeat authorization")
        .query
        .expect("pre-heartbeat query result")
        .rows;

    let as_of_authorized = service
        .run_document(RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
            policy_context: None,
        })
        .expect("run as-of authorization")
        .query
        .expect("as-of query result")
        .rows;

    let current_authorized_response = service
        .run_document(RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
            policy_context: None,
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
    let live_heartbeats = service
        .run_document(RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal live_authority(t, worker, epoch, beat)\n  keep t, worker, epoch, beat",
            ),
            policy_context: None,
        })
        .expect("run live heartbeat query")
        .query
        .expect("live heartbeat query result")
        .rows;

    let claimable = service
        .run_document(RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal worker_can_claim(t, worker)\n  keep t, worker",
            ),
            policy_context: None,
        })
        .expect("run claimability")
        .query
        .expect("claimability query result")
        .rows;

    let accepted_outcomes = service
        .run_document(RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal execution_outcome_accepted(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
            ),
            policy_context: None,
        })
        .expect("run accepted outcomes")
        .query
        .expect("accepted outcome query result")
        .rows;
    let rejected_outcomes = service
        .run_document(RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal execution_outcome_rejected_stale(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
            ),
            policy_context: None,
        })
        .expect("run rejected outcomes")
        .query
        .expect("rejected outcome query result")
        .rows;

    PilotContractSnapshot {
        history_len,
        pre_heartbeat_authorized: row_values(pre_heartbeat_authorized),
        as_of_authorized: row_values(as_of_authorized),
        live_heartbeats: row_values(live_heartbeats),
        current_authorized: row_values(current_authorized),
        claimable: row_values(claimable),
        accepted_outcomes: row_values(accepted_outcomes),
        rejected_outcomes: row_values(rejected_outcomes),
        trace_tuple_count,
    }
}

fn row_values(rows: Vec<aether_ast::QueryRow>) -> Vec<Vec<Value>> {
    rows.into_iter().map(|row| row.values).collect()
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
