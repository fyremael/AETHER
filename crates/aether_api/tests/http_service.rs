use aether_api::{
    http_router, http_router_with_options, AppendRequest, AuditLogResponse, AuthScope,
    ExplainTupleRequest, HealthResponse, HistoryResponse, HttpAuthConfig, HttpKernelOptions,
    InMemoryKernelService, KernelService, ParseDocumentRequest, ParseDocumentResponse,
    RunDocumentRequest, RunDocumentResponse, SqliteKernelService,
};
use aether_ast::{AttributeId, Datom, DatomProvenance, ElementId, EntityId, Value};
use reqwest::Client;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

#[tokio::test]
async fn http_service_exposes_health_and_history() {
    let (base_url, server) = spawn_server(InMemoryKernelService::new()).await;
    let client = Client::new();

    let health = client
        .get(format!("{base_url}/health"))
        .send()
        .await
        .expect("health request");
    assert!(health.status().is_success());
    assert_eq!(
        health
            .json::<HealthResponse>()
            .await
            .expect("health response"),
        HealthResponse {
            status: "ok".into()
        }
    );

    let append = client
        .post(format!("{base_url}/v1/append"))
        .json(&AppendRequest {
            datoms: coordination_history(),
        })
        .send()
        .await
        .expect("append request");
    assert!(append.status().is_success());

    let history = client
        .get(format!("{base_url}/v1/history"))
        .send()
        .await
        .expect("history request");
    assert!(history.status().is_success());
    assert_eq!(
        history
            .json::<HistoryResponse>()
            .await
            .expect("history response")
            .datoms
            .len(),
        7
    );

    server.abort();
}

#[tokio::test]
async fn http_service_runs_documents_and_explains_tuples() {
    let (base_url, server) = spawn_server(InMemoryKernelService::new()).await;
    let client = Client::new();

    let append = client
        .post(format!("{base_url}/v1/append"))
        .json(&AppendRequest {
            datoms: coordination_history(),
        })
        .send()
        .await
        .expect("append request");
    assert!(append.status().is_success());

    let parsed = client
        .post(format!("{base_url}/v1/documents/parse"))
        .json(&ParseDocumentRequest {
            dsl: coordination_dsl(
                "as_of e5",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
        })
        .send()
        .await
        .expect("parse request");
    assert!(parsed.status().is_success());
    let parsed = parsed
        .json::<ParseDocumentResponse>()
        .await
        .expect("parse response");
    assert_eq!(parsed.program.facts.len(), 11);

    let as_of_authorized = run_document(
        &client,
        &base_url,
        coordination_dsl(
            "as_of e5",
            "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
        ),
    )
    .await;
    assert_eq!(as_of_authorized.state.as_of, Some(ElementId::new(5)));
    assert_eq!(
        as_of_authorized
            .query
            .expect("as_of query result")
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        vec![vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-a".into()),
            Value::U64(1),
        ]]
    );

    let current_authorized = run_document(
        &client,
        &base_url,
        coordination_dsl(
            "current",
            "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
        ),
    )
    .await;
    let current_rows = current_authorized
        .query
        .clone()
        .expect("current query result")
        .rows;
    assert_eq!(
        current_rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-b".into()),
            Value::U64(2),
        ]]
    );

    let tuple_id = current_rows[0].tuple_id.expect("tuple id");
    let explain = client
        .post(format!("{base_url}/v1/explain/tuple"))
        .json(&ExplainTupleRequest { tuple_id })
        .send()
        .await
        .expect("explain request");
    assert!(explain.status().is_success());
    let trace = explain
        .json::<aether_api::ExplainTupleResponse>()
        .await
        .expect("explain response")
        .trace;
    assert!(!trace.tuples.is_empty());

    let stale = run_document(
        &client,
        &base_url,
        coordination_dsl(
            "current",
            "goal execution_rejected_stale(t, worker, epoch)\n  keep t, worker, epoch",
        ),
    )
    .await;
    assert_eq!(
        stale
            .query
            .expect("stale query result")
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
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

    server.abort();
}

#[tokio::test]
async fn http_service_preserves_coordination_history_across_sqlite_restart() {
    let temp = TestDbPath::new("http-pilot");
    {
        let (base_url, server) = spawn_server(
            SqliteKernelService::open(temp.path()).expect("open sqlite kernel service"),
        )
        .await;
        let client = Client::new();

        let append = client
            .post(format!("{base_url}/v1/append"))
            .json(&AppendRequest {
                datoms: coordination_history(),
            })
            .send()
            .await
            .expect("append request");
        assert!(append.status().is_success());

        let current = run_document(
            &client,
            &base_url,
            coordination_dsl(
                "current",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
        )
        .await;
        assert_eq!(
            current
                .query
                .expect("current query result")
                .rows
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>(),
            vec![vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into()),
                Value::U64(2),
            ]]
        );

        stop_server(server).await;
    }

    let (base_url, server) =
        spawn_server(SqliteKernelService::open(temp.path()).expect("reopen sqlite kernel service"))
            .await;
    let client = Client::new();

    let history = client
        .get(format!("{base_url}/v1/history"))
        .send()
        .await
        .expect("history request");
    assert!(history.status().is_success());
    assert_eq!(
        history
            .json::<HistoryResponse>()
            .await
            .expect("history response")
            .datoms
            .len(),
        7
    );

    let as_of = run_document(
        &client,
        &base_url,
        coordination_dsl(
            "as_of e5",
            "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
        ),
    )
    .await;
    assert_eq!(
        as_of
            .query
            .expect("as_of query result")
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        vec![vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-a".into()),
            Value::U64(1),
        ]]
    );

    stop_server(server).await;
}

#[tokio::test]
async fn authenticated_http_service_enforces_scopes_and_records_audit_entries() {
    let audit = TestAuditPath::new("auth-audit");
    let options = HttpKernelOptions::new()
        .with_auth(pilot_auth())
        .with_audit_log_path(audit.path().to_path_buf());
    let (base_url, server) = spawn_server_with_options(InMemoryKernelService::new(), options).await;
    let client = Client::new();

    let unauthorized = client
        .get(format!("{base_url}/v1/history"))
        .send()
        .await
        .expect("unauthorized history request");
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);

    let forbidden = client
        .post(format!("{base_url}/v1/append"))
        .bearer_auth("pilot-query-token")
        .json(&AppendRequest {
            datoms: coordination_history(),
        })
        .send()
        .await
        .expect("forbidden append request");
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);

    let append = client
        .post(format!("{base_url}/v1/append"))
        .bearer_auth("pilot-operator-token")
        .json(&AppendRequest {
            datoms: coordination_history(),
        })
        .send()
        .await
        .expect("authorized append request");
    assert!(append.status().is_success());

    let current = client
        .post(format!("{base_url}/v1/documents/run"))
        .bearer_auth("pilot-operator-token")
        .json(&RunDocumentRequest {
            dsl: coordination_dsl(
                "current",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
        })
        .send()
        .await
        .expect("authorized run request");
    assert!(current.status().is_success());
    let current_rows = current
        .json::<RunDocumentResponse>()
        .await
        .expect("current response")
        .query
        .expect("current query result")
        .rows;

    let explain = client
        .post(format!("{base_url}/v1/explain/tuple"))
        .bearer_auth("pilot-operator-token")
        .json(&ExplainTupleRequest {
            tuple_id: current_rows[0].tuple_id.expect("tuple id"),
        })
        .send()
        .await
        .expect("authorized explain request");
    assert!(explain.status().is_success());

    let audit_response = client
        .get(format!("{base_url}/v1/audit"))
        .bearer_auth("pilot-operator-token")
        .send()
        .await
        .expect("audit request");
    assert!(audit_response.status().is_success());
    let audit_entries = audit_response
        .json::<AuditLogResponse>()
        .await
        .expect("audit response")
        .entries;
    assert!(audit_entries.iter().any(|entry| {
        entry.principal == "anonymous"
            && entry.path == "/v1/history"
            && entry.status == reqwest::StatusCode::UNAUTHORIZED.as_u16()
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.principal == "query-client"
            && entry.path == "/v1/append"
            && entry.status == reqwest::StatusCode::FORBIDDEN.as_u16()
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.principal == "pilot-operator"
            && entry.path == "/v1/append"
            && entry.status == reqwest::StatusCode::OK.as_u16()
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.principal == "pilot-operator"
            && entry.path == "/v1/documents/run"
            && entry.status == reqwest::StatusCode::OK.as_u16()
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.principal == "pilot-operator"
            && entry.path == "/v1/explain/tuple"
            && entry.status == reqwest::StatusCode::OK.as_u16()
    }));

    let audit_contents =
        std::fs::read_to_string(audit.path()).expect("read persisted audit log contents");
    assert!(audit_contents.contains("\"path\":\"/v1/append\""));
    assert!(audit_contents.contains("\"path\":\"/v1/documents/run\""));

    stop_server(server).await;
}

async fn run_document(client: &Client, base_url: &str, dsl: String) -> RunDocumentResponse {
    let response = client
        .post(format!("{base_url}/v1/documents/run"))
        .json(&RunDocumentRequest { dsl })
        .send()
        .await
        .expect("run request");
    assert!(response.status().is_success());
    response
        .json::<RunDocumentResponse>()
        .await
        .expect("run response")
}

async fn spawn_server(
    service: impl KernelService + Send + 'static,
) -> (String, tokio::task::JoinHandle<()>) {
    spawn_server_with_options(service, HttpKernelOptions::default()).await
}

async fn spawn_server_with_options(
    service: impl KernelService + Send + 'static,
    options: HttpKernelOptions,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let router = if options == HttpKernelOptions::default() {
            http_router(service)
        } else {
            http_router_with_options(service, options)
        };
        axum::serve(listener, router)
            .await
            .expect("serve http kernel");
    });

    (format!("http://{address}"), server)
}

async fn stop_server(server: tokio::task::JoinHandle<()>) {
    server.abort();
    let _ = server.await;
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
        path.push(format!("aether-http-{name}-{nanos}-{unique}.sqlite"));
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

struct TestAuditPath {
    path: PathBuf,
}

impl TestAuditPath {
    fn new(name: &str) -> Self {
        let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("aether-audit-{name}-{nanos}-{unique}.jsonl"));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestAuditPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn pilot_auth() -> HttpAuthConfig {
    HttpAuthConfig::new()
        .with_token(
            "pilot-operator-token",
            "pilot-operator",
            [
                AuthScope::Append,
                AuthScope::Query,
                AuthScope::Explain,
                AuthScope::Ops,
            ],
        )
        .with_token("pilot-query-token", "query-client", [AuthScope::Query])
}
