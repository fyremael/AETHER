use aether_api::{
    build_coordination_pilot_report, coordination_pilot_dsl, coordination_pilot_seed_history,
    http_router_with_options, AppendRequest, AuditEntry, AuditLogResponse, AuthScope,
    ExplainTupleRequest, HistoryResponse, HttpAuthConfig, HttpKernelOptions, KernelService,
    RunDocumentRequest, RunDocumentResponse, SqliteKernelService,
    COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT, COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
};
use aether_ast::{ElementId, EntityId, PolicyContext, TupleId, Value};
use reqwest::Client;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

#[tokio::test]
#[ignore = "launch validation soak workload"]
async fn soak_authenticated_pilot_service_survives_restarts() {
    let temp = TestDbPath::new("pilot-soak");
    let audit = TestAuditPath::new("pilot-soak");
    let options = HttpKernelOptions::new()
        .with_auth(pilot_auth())
        .with_audit_log_path(audit.path().to_path_buf());

    let cycles = 12;
    for cycle in 0..cycles {
        let (base_url, server) = spawn_server_with_options(
            SqliteKernelService::open(temp.path()).expect("open sqlite kernel service"),
            options.clone(),
        )
        .await;
        let client = Client::new();

        let history = client
            .get(format!("{base_url}/v1/history"))
            .bearer_auth("pilot-operator-token")
            .send()
            .await
            .expect("history request");
        assert!(history.status().is_success());
        let history = history
            .json::<HistoryResponse>()
            .await
            .expect("history response");

        if history.datoms.is_empty() {
            let append = client
                .post(format!("{base_url}/v1/append"))
                .bearer_auth("pilot-operator-token")
                .json(&AppendRequest {
                    datoms: coordination_pilot_seed_history(),
                })
                .send()
                .await
                .expect("append request");
            assert!(append.status().is_success());
        }

        let current = run_document_authorized(
            &client,
            &base_url,
            "pilot-operator-token",
            coordination_pilot_dsl(
                "current",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
        )
        .await;
        let current_rows = current.query.expect("current query result").rows;
        assert_eq!(
            current_rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into()),
                Value::U64(2),
            ]
        );

        let explain = client
            .post(format!("{base_url}/v1/explain/tuple"))
            .bearer_auth("pilot-operator-token")
            .json(&ExplainTupleRequest {
                tuple_id: current_rows[0].tuple_id.expect("tuple id"),
            })
            .send()
            .await
            .expect("explain request");
        assert!(explain.status().is_success());
        let explain = explain
            .json::<aether_api::ExplainTupleResponse>()
            .await
            .expect("explain response");
        assert!(!explain.trace.tuples.is_empty());

        if cycle % 2 == 0 {
            let as_of = run_document_authorized(
                &client,
                &base_url,
                "pilot-operator-token",
                coordination_pilot_dsl(
                    &format!("as_of e{}", COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT),
                    "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
                ),
            )
            .await;
            assert_eq!(
                as_of.state.as_of,
                Some(ElementId::new(COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT))
            );
            assert!(as_of.query.expect("as_of query result").rows.is_empty());

            let authorized_as_of = run_document_authorized(
                &client,
                &base_url,
                "pilot-operator-token",
                coordination_pilot_dsl(
                    &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
                    "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
                ),
            )
            .await;
            assert_eq!(
                authorized_as_of
                    .query
                    .expect("authorized as_of query result")
                    .rows[0]
                    .values,
                vec![
                    Value::Entity(EntityId::new(1)),
                    Value::String("worker-a".into()),
                    Value::U64(1),
                ]
            );
        }

        let audit_response = client
            .get(format!("{base_url}/v1/audit"))
            .bearer_auth("pilot-operator-token")
            .send()
            .await
            .expect("audit request");
        assert!(audit_response.status().is_success());

        stop_server(server).await;
    }

    let persisted = read_audit_entries(audit.path());
    let run_entries = persisted
        .iter()
        .filter(|entry| entry.path == "/v1/documents/run")
        .collect::<Vec<_>>();
    let explain_entries = persisted
        .iter()
        .filter(|entry| entry.path == "/v1/explain/tuple")
        .collect::<Vec<_>>();
    let append_entries = persisted
        .iter()
        .filter(|entry| entry.path == "/v1/append" && entry.outcome == "ok")
        .collect::<Vec<_>>();

    assert_eq!(append_entries.len(), 1);
    assert_eq!(explain_entries.len(), cycles);
    assert!(run_entries.len() >= cycles + cycles / 2);
    assert!(run_entries
        .iter()
        .all(|entry| entry.context.row_count == Some(1)));
    assert!(run_entries.iter().any(|entry| {
        entry.context.temporal_view.as_deref() == Some("current")
            && entry.context.query_goal.as_deref() == Some("execution_authorized(t, worker, epoch)")
    }));
    assert!(run_entries.iter().any(|entry| {
        entry.context.temporal_view.as_deref() == Some("as_of(e5)")
            && entry.context.requested_element == Some(5)
    }));
    assert!(explain_entries
        .iter()
        .all(|entry| entry.context.tuple_id.is_some()));
    assert!(explain_entries
        .iter()
        .all(|entry| { entry.context.trace_tuple_count.unwrap_or_default() > 0 }));

    let mut service = SqliteKernelService::open(temp.path()).expect("reopen sqlite kernel service");
    let report = build_coordination_pilot_report(&mut service).expect("build final pilot report");
    assert_eq!(report.history_len, 25);
    assert_eq!(
        report.current_authorized[0].values,
        vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-b".into()),
            Value::U64(2),
        ]
    );
}

#[tokio::test]
#[ignore = "launch validation misuse workload"]
async fn misuse_paths_are_rejected_cleanly_and_audited() {
    let temp = TestDbPath::new("pilot-misuse");
    let audit = TestAuditPath::new("pilot-misuse");
    let options = HttpKernelOptions::new()
        .with_auth(pilot_auth())
        .with_audit_log_path(audit.path().to_path_buf());
    let (base_url, server) = spawn_server_with_options(
        SqliteKernelService::open(temp.path()).expect("open sqlite kernel service"),
        options,
    )
    .await;
    let client = Client::new();

    let append = client
        .post(format!("{base_url}/v1/append"))
        .bearer_auth("pilot-operator-token")
        .json(&AppendRequest {
            datoms: coordination_pilot_seed_history(),
        })
        .send()
        .await
        .expect("append request");
    assert!(append.status().is_success());

    let duplicate = client
        .post(format!("{base_url}/v1/append"))
        .bearer_auth("pilot-operator-token")
        .json(&AppendRequest {
            datoms: coordination_pilot_seed_history(),
        })
        .send()
        .await
        .expect("duplicate append request");
    assert_eq!(duplicate.status(), reqwest::StatusCode::CONFLICT);

    let unauthorized = client
        .post(format!("{base_url}/v1/documents/run"))
        .json(&RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
            policy_context: None,
        })
        .send()
        .await
        .expect("unauthorized run request");
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);

    let forbidden = client
        .post(format!("{base_url}/v1/explain/tuple"))
        .bearer_auth("pilot-query-token")
        .json(&ExplainTupleRequest {
            tuple_id: TupleId::new(1),
        })
        .send()
        .await
        .expect("forbidden explain request");
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);

    let current = run_document_authorized(
        &client,
        &base_url,
        "pilot-operator-token",
        coordination_pilot_dsl(
            "current",
            "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
        ),
    )
    .await;
    let valid_tuple_id = current.query.expect("query result").rows[0]
        .tuple_id
        .expect("tuple id");

    let bad_tuple = client
        .post(format!("{base_url}/v1/explain/tuple"))
        .bearer_auth("pilot-operator-token")
        .json(&ExplainTupleRequest {
            tuple_id: TupleId::new(valid_tuple_id.0 + 10_000),
        })
        .send()
        .await
        .expect("bad tuple explain request");
    assert_eq!(bad_tuple.status(), reqwest::StatusCode::BAD_REQUEST);

    let history = client
        .get(format!("{base_url}/v1/history"))
        .bearer_auth("pilot-operator-token")
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
        25
    );

    let audit_entries = client
        .get(format!("{base_url}/v1/audit"))
        .bearer_auth("pilot-operator-token")
        .send()
        .await
        .expect("audit request")
        .json::<AuditLogResponse>()
        .await
        .expect("audit response")
        .entries;

    assert!(audit_entries.iter().any(|entry| {
        entry.path == "/v1/append"
            && entry.status == reqwest::StatusCode::CONFLICT.as_u16()
            && entry.context.datom_count == Some(25)
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.path == "/v1/documents/run"
            && entry.status == reqwest::StatusCode::UNAUTHORIZED.as_u16()
            && entry.context.temporal_view.as_deref() == Some("current")
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.path == "/v1/explain/tuple"
            && entry.status == reqwest::StatusCode::FORBIDDEN.as_u16()
            && entry.context.tuple_id == Some(1)
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.path == "/v1/explain/tuple"
            && entry.status == reqwest::StatusCode::BAD_REQUEST.as_u16()
            && entry.context.tuple_id == Some(valid_tuple_id.0 + 10_000)
    }));

    stop_server(server).await;

    let persisted = read_audit_entries(audit.path());
    assert!(persisted.iter().any(|entry| {
        entry.path == "/v1/append"
            && entry.status == reqwest::StatusCode::CONFLICT.as_u16()
            && entry.outcome == "error"
    }));
    assert!(persisted.iter().any(|entry| {
        entry.path == "/v1/documents/run"
            && entry.status == reqwest::StatusCode::UNAUTHORIZED.as_u16()
            && entry.outcome == "unauthorized"
    }));
    assert!(persisted.iter().any(|entry| {
        entry.path == "/v1/explain/tuple"
            && entry.status == reqwest::StatusCode::FORBIDDEN.as_u16()
            && entry.outcome == "forbidden"
    }));
}

async fn run_document_authorized(
    client: &Client,
    base_url: &str,
    token: &str,
    dsl: String,
) -> RunDocumentResponse {
    let response = client
        .post(format!("{base_url}/v1/documents/run"))
        .bearer_auth(token)
        .json(&RunDocumentRequest {
            dsl,
            policy_context: None,
        })
        .send()
        .await
        .expect("authorized run request");
    assert!(response.status().is_success());
    response
        .json::<RunDocumentResponse>()
        .await
        .expect("authorized run response")
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
        let router = http_router_with_options(service, options);
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

fn pilot_auth() -> HttpAuthConfig {
    HttpAuthConfig::new()
        .with_token_context(
            "pilot-operator-token",
            "pilot-operator",
            [
                AuthScope::Append,
                AuthScope::Query,
                AuthScope::Explain,
                AuthScope::Ops,
            ],
            PolicyContext {
                capabilities: vec!["executor".into()],
                visibilities: Vec::new(),
            },
        )
        .with_token("pilot-query-token", "query-client", [AuthScope::Query])
}

fn read_audit_entries(path: &Path) -> Vec<AuditEntry> {
    std::fs::read_to_string(path)
        .expect("read audit log")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("parse audit entry"))
        .collect()
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
        path.push(format!("aether-pilot-soak-{name}-{nanos}-{unique}.sqlite"));
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
        path.push(format!("aether-pilot-audit-{name}-{nanos}-{unique}.jsonl"));
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
