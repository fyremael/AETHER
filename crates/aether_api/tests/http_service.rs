use aether_api::{
    coordination_pilot_dsl, coordination_pilot_seed_history, http_router, http_router_with_options,
    AppendRequest, AuditEntry, AuditLogResponse, AuthScope, ExplainTupleRequest,
    GetArtifactReferenceRequest, HealthResponse, HistoryResponse, HttpAuthConfig,
    HttpKernelOptions, InMemoryKernelService, KernelService, ParseDocumentRequest,
    ParseDocumentResponse, RegisterArtifactReferenceRequest, RegisterVectorRecordRequest,
    RunDocumentRequest, RunDocumentResponse, SearchVectorsRequest, SearchVectorsResponse,
    SqliteKernelService, VectorFactProjection, VectorMetric,
    COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT, COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
};
use aether_ast::{
    AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, PolicyContext,
    PolicyEnvelope, PredicateId, PredicateRef, ReplicaId, Value,
};
use reqwest::Client;
use std::collections::BTreeMap;
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
            datoms: coordination_pilot_seed_history(),
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
        25
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
            datoms: coordination_pilot_seed_history(),
        })
        .send()
        .await
        .expect("append request");
    assert!(append.status().is_success());

    let parsed = client
        .post(format!("{base_url}/v1/documents/parse"))
        .json(&ParseDocumentRequest {
            dsl: coordination_pilot_dsl(
                &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
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
    assert_eq!(parsed.program.facts.len(), 7);

    let pre_heartbeat_authorized = run_document(
        &client,
        &base_url,
        coordination_pilot_dsl(
            &format!("as_of e{}", COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT),
            "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
        ),
    )
    .await;
    assert_eq!(
        pre_heartbeat_authorized.state.as_of,
        Some(ElementId::new(COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT))
    );
    assert_eq!(
        pre_heartbeat_authorized
            .query
            .expect("pre-heartbeat query result")
            .rows
            .len(),
        0
    );

    let as_of_authorized = run_document(
        &client,
        &base_url,
        coordination_pilot_dsl(
            &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
            "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
        ),
    )
    .await;
    assert_eq!(
        as_of_authorized.state.as_of,
        Some(ElementId::new(COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT))
    );
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
        coordination_pilot_dsl(
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
        coordination_pilot_dsl(
            "current",
            "goal execution_outcome_rejected_stale(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
        ),
    )
    .await;
    let accepted = run_document(
        &client,
        &base_url,
        coordination_pilot_dsl(
            "current",
            "goal execution_outcome_accepted(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
        ),
    )
    .await;
    assert_eq!(
        accepted
            .query
            .expect("accepted query result")
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        vec![vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-b".into()),
            Value::U64(2),
            Value::String("completed".into()),
            Value::String("current-worker-b".into()),
        ]]
    );
    assert_eq!(
        stale
            .query
            .expect("stale query result")
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        vec![vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-a".into()),
            Value::U64(1),
            Value::String("completed".into()),
            Value::String("stale-worker-a".into()),
        ],]
    );

    server.abort();
}

#[tokio::test]
async fn http_service_applies_policy_context_to_document_runs() {
    let (base_url, server) = spawn_server(InMemoryKernelService::new()).await;
    let client = Client::new();

    let append = client
        .post(format!("{base_url}/v1/append"))
        .json(&AppendRequest {
            datoms: vec![
                policy_status_datom(1, "ready", 1, None),
                policy_status_datom(
                    3,
                    "ready",
                    2,
                    Some(PolicyEnvelope {
                        capability: Some("executor".into()),
                        visibility: None,
                    }),
                ),
            ],
        })
        .send()
        .await
        .expect("append policy datoms");
    assert!(append.status().is_success());

    let dsl = policy_document_dsl();
    let default_response = client
        .post(format!("{base_url}/v1/documents/run"))
        .json(&RunDocumentRequest {
            dsl: dsl.clone(),
            policy_context: None,
        })
        .send()
        .await
        .expect("default run request");
    assert!(default_response.status().is_success());
    let default_rows = default_response
        .json::<RunDocumentResponse>()
        .await
        .expect("default response")
        .query
        .expect("default query result")
        .rows;
    assert_eq!(
        default_rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![vec![Value::Entity(EntityId::new(1))]]
    );

    let executor_response = client
        .post(format!("{base_url}/v1/documents/run"))
        .json(&RunDocumentRequest {
            dsl,
            policy_context: Some(PolicyContext {
                capabilities: vec!["executor".into()],
                visibilities: Vec::new(),
            }),
        })
        .send()
        .await
        .expect("executor run request");
    assert!(executor_response.status().is_success());
    let executor_rows = executor_response
        .json::<RunDocumentResponse>()
        .await
        .expect("executor response")
        .query
        .expect("executor query result")
        .rows;
    assert_eq!(
        executor_rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Entity(EntityId::new(1))],
            vec![Value::Entity(EntityId::new(2))],
            vec![Value::Entity(EntityId::new(3))],
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
                datoms: coordination_pilot_seed_history(),
            })
            .send()
            .await
            .expect("append request");
        assert!(append.status().is_success());

        let current = run_document(
            &client,
            &base_url,
            coordination_pilot_dsl(
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
        25
    );

    let as_of = run_document(
        &client,
        &base_url,
        coordination_pilot_dsl(
            &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
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
            datoms: coordination_pilot_seed_history(),
        })
        .send()
        .await
        .expect("forbidden append request");
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);

    let append = client
        .post(format!("{base_url}/v1/append"))
        .bearer_auth("pilot-operator-token")
        .json(&AppendRequest {
            datoms: coordination_pilot_seed_history(),
        })
        .send()
        .await
        .expect("authorized append request");
    assert!(append.status().is_success());

    let current = client
        .post(format!("{base_url}/v1/documents/run"))
        .bearer_auth("pilot-operator-token")
        .json(&RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
            policy_context: None,
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
            && entry.context.temporal_view.as_deref() == Some("history")
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.principal == "query-client"
            && entry.path == "/v1/append"
            && entry.status == reqwest::StatusCode::FORBIDDEN.as_u16()
            && entry.context.datom_count == Some(25)
            && entry.context.last_element == Some(25)
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.principal == "pilot-operator"
            && entry.path == "/v1/append"
            && entry.status == reqwest::StatusCode::OK.as_u16()
            && entry.context.datom_count == Some(25)
            && entry.context.last_element == Some(25)
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.principal == "pilot-operator"
            && entry.path == "/v1/documents/run"
            && entry.status == reqwest::StatusCode::OK.as_u16()
            && entry.context.temporal_view.as_deref() == Some("current")
            && entry.context.query_goal.as_deref() == Some("execution_authorized(t, worker, epoch)")
            && entry.context.row_count == Some(1)
            && entry.context.derived_tuple_count.is_some()
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.principal == "pilot-operator"
            && entry.path == "/v1/explain/tuple"
            && entry.status == reqwest::StatusCode::OK.as_u16()
            && entry.context.tuple_id == Some(current_rows[0].tuple_id.expect("tuple id").0)
            && entry.context.trace_tuple_count.is_some()
    }));

    let audit_contents =
        std::fs::read_to_string(audit.path()).expect("read persisted audit log contents");
    assert!(audit_contents.contains("\"path\":\"/v1/append\""));
    assert!(audit_contents.contains("\"path\":\"/v1/documents/run\""));
    assert!(audit_contents.contains("\"temporal_view\":\"current\""));
    assert!(audit_contents.contains("\"query_goal\":\"execution_authorized(t, worker, epoch)\""));

    stop_server(server).await;
}

#[tokio::test]
async fn authenticated_http_service_audits_query_goal_for_find_alias() {
    let audit = TestAuditPath::new("find-audit");
    let options = HttpKernelOptions::new()
        .with_auth(pilot_auth())
        .with_audit_log_path(audit.path().to_path_buf());
    let (base_url, server) = spawn_server_with_options(InMemoryKernelService::new(), options).await;
    let client = Client::new();

    let append = client
        .post(format!("{base_url}/v1/append"))
        .bearer_auth("pilot-operator-token")
        .json(&AppendRequest {
            datoms: coordination_pilot_seed_history(),
        })
        .send()
        .await
        .expect("authorized append request");
    assert!(append.status().is_success());

    let run = client
        .post(format!("{base_url}/v1/documents/run"))
        .bearer_auth("pilot-operator-token")
        .json(&RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "find execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
            policy_context: None,
        })
        .send()
        .await
        .expect("authorized run request");
    assert!(run.status().is_success());

    let persisted = read_audit_entries(audit.path());
    assert!(persisted.iter().any(|entry| {
        entry.path == "/v1/documents/run"
            && entry.context.temporal_view.as_deref() == Some("current")
            && entry.context.query_goal.as_deref() == Some("execution_authorized(t, worker, epoch)")
            && entry.context.row_count == Some(1)
    }));

    stop_server(server).await;
}

#[tokio::test]
async fn authenticated_http_service_persists_semantic_audit_context_across_restarts() {
    let temp = TestDbPath::new("http-audit-restart");
    let audit = TestAuditPath::new("audit-restart");
    let options = HttpKernelOptions::new()
        .with_auth(pilot_auth())
        .with_audit_log_path(audit.path().to_path_buf());

    {
        let (base_url, server) = spawn_server_with_options(
            SqliteKernelService::open(temp.path()).expect("open sqlite kernel service"),
            options.clone(),
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

        for _ in 0..3 {
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
            let tuple_id = current.query.expect("current query result").rows[0]
                .tuple_id
                .expect("tuple id");

            let explain = client
                .post(format!("{base_url}/v1/explain/tuple"))
                .bearer_auth("pilot-operator-token")
                .json(&ExplainTupleRequest { tuple_id })
                .send()
                .await
                .expect("explain request");
            assert!(explain.status().is_success());
        }

        stop_server(server).await;
    }

    {
        let (base_url, server) = spawn_server_with_options(
            SqliteKernelService::open(temp.path()).expect("reopen sqlite kernel service"),
            options,
        )
        .await;
        let client = Client::new();

        let as_of = client
            .post(format!("{base_url}/v1/documents/run"))
            .bearer_auth("pilot-operator-token")
            .json(&RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
                    "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
                ),
                policy_context: None,
            })
            .send()
            .await
            .expect("as_of request");
        assert!(as_of.status().is_success());

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

    assert!(run_entries.len() >= 4);
    assert!(explain_entries.len() >= 3);
    assert!(run_entries.iter().any(|entry| {
        entry.context.temporal_view.as_deref() == Some("current")
            && entry.context.query_goal.as_deref() == Some("execution_authorized(t, worker, epoch)")
            && entry.context.row_count == Some(1)
    }));
    assert!(run_entries.iter().any(|entry| {
        entry.context.temporal_view.as_deref() == Some("as_of(e9)")
            && entry.context.requested_element == Some(COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT)
            && entry.context.row_count == Some(1)
    }));
    assert!(explain_entries
        .iter()
        .all(|entry| entry.context.tuple_id.is_some()));
    assert!(explain_entries.iter().all(|entry| entry
        .context
        .trace_tuple_count
        .unwrap_or_default()
        > 0));
}

#[tokio::test]
async fn http_service_registers_and_searches_sidecar_records() {
    let (base_url, server) = spawn_server(InMemoryKernelService::new()).await;
    let client = Client::new();

    let append = client
        .post(format!("{base_url}/v1/append"))
        .json(&AppendRequest {
            datoms: vec![anchor_datom(1)],
        })
        .send()
        .await
        .expect("append artifact anchor request");
    assert!(append.status().is_success());

    let artifact = client
        .post(format!("{base_url}/v1/sidecars/artifacts/register"))
        .json(&RegisterArtifactReferenceRequest {
            reference: aether_api::ArtifactReference {
                sidecar_id: "semantic-memory".into(),
                artifact_id: "doc-1".into(),
                entity: EntityId::new(31),
                uri: "s3://aether/docs/doc-1.md".into(),
                media_type: "text/markdown".into(),
                byte_length: 256,
                digest: Some("sha256:doc-1".into()),
                metadata: BTreeMap::from([("kind".into(), Value::String("runbook".into()))]),
                provenance: DatomProvenance::default(),
                policy: None,
                registered_at: ElementId::new(1),
            },
        })
        .send()
        .await
        .expect("register artifact request");
    assert!(artifact.status().is_success());

    let append = client
        .post(format!("{base_url}/v1/append"))
        .json(&AppendRequest {
            datoms: vec![anchor_datom(2)],
        })
        .send()
        .await
        .expect("append vector anchor request");
    assert!(append.status().is_success());

    let vector = client
        .post(format!("{base_url}/v1/sidecars/vectors/register"))
        .json(&RegisterVectorRecordRequest {
            record: aether_api::VectorRecordMetadata {
                sidecar_id: "semantic-memory".into(),
                vector_id: "vec-1".into(),
                entity: EntityId::new(31),
                source_artifact_id: Some("doc-1".into()),
                embedding_ref: "s3://aether/vectors/vec-1.bin".into(),
                dimensions: 3,
                metric: VectorMetric::Cosine,
                metadata: BTreeMap::new(),
                provenance: DatomProvenance::default(),
                policy: None,
                registered_at: ElementId::new(2),
            },
            embedding: vec![0.9, 0.1, 0.0],
        })
        .send()
        .await
        .expect("register vector request");
    assert!(vector.status().is_success());

    let fetched = client
        .post(format!("{base_url}/v1/sidecars/artifacts/get"))
        .json(&GetArtifactReferenceRequest {
            sidecar_id: "semantic-memory".into(),
            artifact_id: "doc-1".into(),
            policy_context: None,
        })
        .send()
        .await
        .expect("get artifact request");
    assert!(fetched.status().is_success());

    let search = client
        .post(format!("{base_url}/v1/sidecars/vectors/search"))
        .json(&SearchVectorsRequest {
            sidecar_id: "semantic-memory".into(),
            query_embedding: vec![1.0, 0.0, 0.0],
            top_k: 1,
            metric: VectorMetric::Cosine,
            as_of: Some(ElementId::new(2)),
            projection: Some(VectorFactProjection {
                predicate: PredicateRef {
                    id: PredicateId::new(81),
                    name: "semantic_neighbor".into(),
                    arity: 3,
                },
                query_entity: EntityId::new(999),
            }),
            policy_context: None,
        })
        .send()
        .await
        .expect("search vectors request");
    assert!(search.status().is_success());
    let search = search
        .json::<SearchVectorsResponse>()
        .await
        .expect("search vectors response");
    assert_eq!(search.matches.len(), 1);
    assert_eq!(search.facts.len(), 1);
    assert_eq!(
        search.facts[0]
            .provenance
            .as_ref()
            .expect("fact provenance")
            .source_datom_ids,
        vec![ElementId::new(2), ElementId::new(1)]
    );

    server.abort();
}

fn anchor_datom(element: u64) -> Datom {
    Datom {
        entity: EntityId::new(1),
        attribute: AttributeId::new(1),
        value: Value::String(format!("sidecar-anchor-{element}")),
        op: OperationKind::Annotate,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn policy_status_datom(
    entity: u64,
    status: &str,
    element: u64,
    policy: Option<PolicyEnvelope>,
) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(1),
        value: Value::String(status.into()),
        op: OperationKind::Assert,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy,
    }
}

fn policy_document_dsl() -> String {
    r#"
schema {
  attr task.status: ScalarLWW<String>
}

predicates {
  task_status(Entity, String)
  protected_fact(Entity)
  visible_task(Entity)
}

rules {
  visible_task(t) <- task_status(t, "ready")
  visible_task(t) <- protected_fact(t)
}

materialize {
  visible_task
}

facts {
  protected_fact(entity(1))
  protected_fact(entity(2)) @capability("executor")
}

query current_cut {
  current
  goal visible_task(t)
  keep t
}
"#
    .into()
}

async fn run_document(client: &Client, base_url: &str, dsl: String) -> RunDocumentResponse {
    let response = client
        .post(format!("{base_url}/v1/documents/run"))
        .json(&RunDocumentRequest {
            dsl,
            policy_context: None,
        })
        .send()
        .await
        .expect("run request");
    assert!(response.status().is_success());
    response
        .json::<RunDocumentResponse>()
        .await
        .expect("run response")
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

fn read_audit_entries(path: &Path) -> Vec<AuditEntry> {
    std::fs::read_to_string(path)
        .expect("read audit log")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("parse audit entry"))
        .collect()
}
