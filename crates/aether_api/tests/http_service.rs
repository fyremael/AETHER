use aether_api::{
    http_router, AppendRequest, ExplainTupleRequest, HealthResponse, HistoryResponse,
    InMemoryKernelService, ParseDocumentRequest, ParseDocumentResponse, RunDocumentRequest,
    RunDocumentResponse,
};
use aether_ast::{AttributeId, Datom, DatomProvenance, ElementId, EntityId, Value};
use reqwest::Client;

#[tokio::test]
async fn http_service_exposes_health_and_history() {
    let (base_url, server) = spawn_server().await;
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
    let (base_url, server) = spawn_server().await;
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

async fn spawn_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        axum::serve(listener, http_router(InMemoryKernelService::new()))
            .await
            .expect("serve http kernel");
    });

    (format!("http://{address}"), server)
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
