use aether_api::{
    coordination_pilot_dsl, coordination_pilot_seed_history, http_router, http_router_with_options,
    http_router_with_partitioned_options, AppendRequest, AuditEntry, AuditLogResponse, AuthScope,
    AuthorityPartitionConfig, CoordinationCut, CoordinationDeltaReport,
    CoordinationDeltaReportRequest, CoordinationPilotReport, CoordinationPilotReportRequest,
    ExplainTupleRequest, FederatedExplainReport, FederatedRunDocumentRequest,
    GetArtifactReferenceRequest, HealthResponse, HistoryResponse, HttpAuthConfig,
    HttpKernelOptions, ImportedFactQueryRequest, InMemoryKernelService, KernelService,
    ParseDocumentRequest, ParseDocumentResponse, PartitionAppendRequest, PartitionStatusResponse,
    PilotAuthConfig, PilotServiceConfig, PilotTokenConfig, PromoteReplicaRequest,
    RegisterArtifactReferenceRequest, RegisterVectorRecordRequest, ReplicaConfig, ReplicaRole,
    ReplicatedAuthorityPartitionService, RunDocumentRequest, RunDocumentResponse,
    SearchVectorsRequest, SearchVectorsResponse, ServiceMode, ServiceStatusResponse,
    SqliteKernelService, VectorFactProjection, VectorMetric,
    COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT, COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
};
use aether_ast::{
    AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, PartitionCut,
    PartitionId, PolicyContext, PolicyEnvelope, PredicateId, PredicateRef, ReplicaId, Value,
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
        .json(&ExplainTupleRequest {
            tuple_id,
            policy_context: None,
        })
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
                        capabilities: vec!["executor".into()],
                        visibilities: Vec::new(),
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
            policy_context: None,
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
async fn authenticated_http_service_exposes_policy_aware_coordination_reports() {
    let audit = TestAuditPath::new("coordination-report-audit");
    let options = HttpKernelOptions::new()
        .with_auth(pilot_auth())
        .with_audit_log_path(audit.path().to_path_buf());
    let (base_url, server) = spawn_server_with_options(InMemoryKernelService::new(), options).await;
    let client = Client::new();

    let mut datoms = coordination_pilot_seed_history();
    for datom in &mut datoms {
        if datom.element.0 >= 6 {
            datom.policy = Some(PolicyEnvelope {
                capabilities: vec!["executor".into()],
                visibilities: Vec::new(),
            });
        }
    }

    let append = client
        .post(format!("{base_url}/v1/append"))
        .bearer_auth("pilot-operator-token")
        .json(&AppendRequest { datoms })
        .send()
        .await
        .expect("append coordination seed history");
    assert!(append.status().is_success());

    let operator_report = client
        .post(format!("{base_url}/v1/reports/pilot/coordination"))
        .bearer_auth("pilot-operator-token")
        .json(&CoordinationPilotReportRequest {
            policy_context: None,
        })
        .send()
        .await
        .expect("operator report request");
    assert!(operator_report.status().is_success());
    let operator_report = operator_report
        .json::<CoordinationPilotReport>()
        .await
        .expect("operator report response");
    assert_eq!(operator_report.history_len, 25);
    assert_eq!(operator_report.current_authorized.len(), 1);
    assert!(operator_report.trace.is_some());

    let public_report = client
        .post(format!("{base_url}/v1/reports/pilot/coordination"))
        .bearer_auth("pilot-query-token")
        .json(&CoordinationPilotReportRequest {
            policy_context: None,
        })
        .send()
        .await
        .expect("public report request");
    assert!(public_report.status().is_success());
    let public_report = public_report
        .json::<CoordinationPilotReport>()
        .await
        .expect("public report response");
    assert_eq!(public_report.history_len, 5);
    assert!(public_report.as_of_authorized.is_empty());
    assert!(public_report.current_authorized.is_empty());
    assert!(public_report.accepted_outcomes.is_empty());
    assert!(public_report.trace.is_none());

    let persisted = read_audit_entries(audit.path());
    assert!(persisted.iter().any(|entry| {
        entry.path == "/v1/reports/pilot/coordination"
            && entry.principal == "pilot-operator"
            && entry.context.temporal_view.as_deref() == Some("coordination_pilot_report")
            && entry.context.datom_count == Some(25)
            && entry.context.row_count.is_some()
            && entry.context.trace_tuple_count.is_some()
            && entry.context.policy_decision.as_deref() == Some("token_default")
    }));
    assert!(persisted.iter().any(|entry| {
        entry.path == "/v1/reports/pilot/coordination"
            && entry.principal == "query-client"
            && entry.context.temporal_view.as_deref() == Some("coordination_pilot_report")
            && entry.context.datom_count == Some(5)
            && entry.context.trace_tuple_count.is_none()
            && entry.context.policy_decision.as_deref() == Some("public")
    }));

    stop_server(server).await;
}

#[tokio::test]
async fn coordination_report_endpoint_matches_query_auth_behavior() {
    let options = HttpKernelOptions::new().with_auth(pilot_auth().with_token(
        "pilot-ops-only-token",
        "ops-only",
        [AuthScope::Ops],
    ));
    let (base_url, server) = spawn_server_with_options(InMemoryKernelService::new(), options).await;
    let client = Client::new();

    let unauthorized = client
        .post(format!("{base_url}/v1/reports/pilot/coordination"))
        .json(&CoordinationPilotReportRequest {
            policy_context: None,
        })
        .send()
        .await
        .expect("unauthorized report request");
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);

    let forbidden = client
        .post(format!("{base_url}/v1/reports/pilot/coordination"))
        .bearer_auth("pilot-ops-only-token")
        .json(&CoordinationPilotReportRequest {
            policy_context: None,
        })
        .send()
        .await
        .expect("forbidden report request");
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);

    stop_server(server).await;
}

#[tokio::test]
async fn http_service_exposes_status_and_supports_auth_reload() {
    let temp = TestTempDir::new("status-reload");
    let database_path = temp.path().join("pilot.sqlite");
    let audit_path = temp.path().join("audit.jsonl");
    let config_path = temp.path().join("pilot-service.json");

    let mut config = PilotServiceConfig {
        config_version: "test-config-v1".into(),
        schema_version: "test-schema-v1".into(),
        service_mode: ServiceMode::SingleNode,
        bind_addr: "127.0.0.1:0".into(),
        database_path: database_path.clone(),
        audit_log_path: Some(audit_path.clone()),
        auth: PilotAuthConfig {
            tokens: vec![
                PilotTokenConfig {
                    principal: "pilot-operator".into(),
                    principal_id: Some("principal:pilot-operator".into()),
                    token_id: Some("token:pilot-operator".into()),
                    scopes: vec![
                        AuthScope::Append,
                        AuthScope::Query,
                        AuthScope::Explain,
                        AuthScope::Ops,
                    ],
                    policy_context: Some(PolicyContext {
                        capabilities: vec!["executor".into()],
                        visibilities: Vec::new(),
                    }),
                    token: Some("pilot-operator-token".into()),
                    token_env: None,
                    token_file: None,
                    token_command: None,
                    revoked: false,
                },
                PilotTokenConfig {
                    principal: "query-client".into(),
                    principal_id: Some("principal:query-client".into()),
                    token_id: Some("token:query-client".into()),
                    scopes: vec![AuthScope::Query],
                    policy_context: None,
                    token: Some("pilot-query-token".into()),
                    token_env: None,
                    token_file: None,
                    token_command: None,
                    revoked: false,
                },
            ],
            revoked_token_ids: Vec::new(),
            revoked_principal_ids: Vec::new(),
        },
    };
    std::fs::write(
        &config_path,
        serde_json::to_string_pretty(&config).expect("encode config"),
    )
    .expect("write config");
    let resolved = config
        .clone()
        .resolve(&config_path)
        .expect("resolve pilot config");
    let options = HttpKernelOptions::new()
        .with_auth(resolved.auth.clone())
        .with_audit_log_path(resolved.audit_log_path.clone())
        .with_service_status(resolved.service_status())
        .with_auth_reload_config_path(config_path.clone());
    let (base_url, server) = spawn_server_with_options(InMemoryKernelService::new(), options).await;
    let client = Client::new();

    let status = client
        .get(format!("{base_url}/v1/status"))
        .bearer_auth("pilot-operator-token")
        .send()
        .await
        .expect("status request");
    assert!(status.status().is_success());
    let status = status
        .json::<ServiceStatusResponse>()
        .await
        .expect("status response");
    assert_eq!(status.service_mode, ServiceMode::SingleNode);
    assert_eq!(status.config_version, "test-config-v1");
    assert_eq!(status.schema_version, "test-schema-v1");
    assert_eq!(status.principals.len(), 2);
    assert!(status
        .principals
        .iter()
        .any(|principal| principal.token_id == "token:query-client" && !principal.revoked));

    config.config_version = "test-config-v2".into();
    config.auth.revoked_token_ids = vec!["token:query-client".into()];
    std::fs::write(
        &config_path,
        serde_json::to_string_pretty(&config).expect("encode updated config"),
    )
    .expect("write updated config");

    let reload = client
        .post(format!("{base_url}/v1/admin/auth/reload"))
        .bearer_auth("pilot-operator-token")
        .send()
        .await
        .expect("reload request");
    assert!(reload.status().is_success());

    let status = client
        .get(format!("{base_url}/v1/status"))
        .bearer_auth("pilot-operator-token")
        .send()
        .await
        .expect("status after reload");
    assert!(status.status().is_success());
    let status = status
        .json::<ServiceStatusResponse>()
        .await
        .expect("status response");
    assert_eq!(status.config_version, "test-config-v2");
    assert!(status
        .principals
        .iter()
        .any(|principal| principal.token_id == "token:query-client" && principal.revoked));

    let revoked = client
        .get(format!("{base_url}/v1/history"))
        .bearer_auth("pilot-query-token")
        .send()
        .await
        .expect("revoked token request");
    assert_eq!(revoked.status(), reqwest::StatusCode::FORBIDDEN);

    stop_server(server).await;
}

#[tokio::test]
async fn coordination_delta_report_endpoint_is_policy_aware() {
    let audit = TestAuditPath::new("coordination-delta-audit");
    let options = HttpKernelOptions::new()
        .with_auth(pilot_auth())
        .with_audit_log_path(audit.path().to_path_buf());
    let (base_url, server) = spawn_server_with_options(InMemoryKernelService::new(), options).await;
    let client = Client::new();

    let mut datoms = coordination_pilot_seed_history();
    for datom in &mut datoms {
        if datom.element.0 >= 6 {
            datom.policy = Some(PolicyEnvelope {
                capabilities: vec!["executor".into()],
                visibilities: Vec::new(),
            });
        }
    }

    let append = client
        .post(format!("{base_url}/v1/append"))
        .bearer_auth("pilot-operator-token")
        .json(&AppendRequest { datoms })
        .send()
        .await
        .expect("append coordination history");
    assert!(append.status().is_success());

    let request = CoordinationDeltaReportRequest {
        left: CoordinationCut::AsOf {
            element: ElementId::new(COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT),
        },
        right: CoordinationCut::Current,
        policy_context: None,
    };

    let operator = client
        .post(format!("{base_url}/v1/reports/pilot/coordination-delta"))
        .bearer_auth("pilot-operator-token")
        .json(&request)
        .send()
        .await
        .expect("operator delta request");
    assert!(operator.status().is_success());
    let operator = operator
        .json::<CoordinationDeltaReport>()
        .await
        .expect("operator delta response");
    assert_eq!(operator.right_history_len, 25);
    let operator_diff_count = operator.current_authorized.added.len()
        + operator.current_authorized.removed.len()
        + operator.current_authorized.changed.len()
        + operator.claimable.added.len()
        + operator.claimable.removed.len()
        + operator.claimable.changed.len()
        + operator.live_heartbeats.added.len()
        + operator.live_heartbeats.removed.len()
        + operator.live_heartbeats.changed.len()
        + operator.accepted_outcomes.added.len()
        + operator.accepted_outcomes.removed.len()
        + operator.accepted_outcomes.changed.len()
        + operator.rejected_outcomes.added.len()
        + operator.rejected_outcomes.removed.len()
        + operator.rejected_outcomes.changed.len();
    assert!(operator_diff_count > 0);
    let operator_has_trace = operator
        .current_authorized
        .added
        .iter()
        .chain(operator.current_authorized.removed.iter())
        .chain(operator.claimable.added.iter())
        .chain(operator.claimable.removed.iter())
        .chain(operator.live_heartbeats.added.iter())
        .chain(operator.live_heartbeats.removed.iter())
        .chain(operator.accepted_outcomes.added.iter())
        .chain(operator.accepted_outcomes.removed.iter())
        .chain(operator.rejected_outcomes.added.iter())
        .chain(operator.rejected_outcomes.removed.iter())
        .map(|row| row.trace.as_ref())
        .chain(
            operator
                .current_authorized
                .changed
                .iter()
                .chain(operator.claimable.changed.iter())
                .chain(operator.live_heartbeats.changed.iter())
                .chain(operator.accepted_outcomes.changed.iter())
                .chain(operator.rejected_outcomes.changed.iter())
                .flat_map(|row| [row.before_trace.as_ref(), row.after_trace.as_ref()]),
        )
        .any(|trace| trace.is_some());
    assert!(operator_has_trace);

    let public = client
        .post(format!("{base_url}/v1/reports/pilot/coordination-delta"))
        .bearer_auth("pilot-query-token")
        .json(&request)
        .send()
        .await
        .expect("public delta request");
    assert!(public.status().is_success());
    let public = public
        .json::<CoordinationDeltaReport>()
        .await
        .expect("public delta response");
    assert_eq!(public.right_history_len, 5);
    assert!(public.current_authorized.added.is_empty());
    assert!(public.live_heartbeats.added.is_empty());
    assert!(public.accepted_outcomes.added.is_empty());

    let persisted = read_audit_entries(audit.path());
    assert!(persisted.iter().any(|entry| {
        entry.path == "/v1/reports/pilot/coordination-delta"
            && entry.principal == "pilot-operator"
            && entry.context.temporal_view.as_deref() == Some("coordination_delta_report")
            && entry.context.datom_count == Some(25)
            && entry.context.policy_decision.as_deref() == Some("token_default")
    }));
    assert!(persisted.iter().any(|entry| {
        entry.path == "/v1/reports/pilot/coordination-delta"
            && entry.principal == "query-client"
            && entry.context.datom_count == Some(5)
            && entry.context.policy_decision.as_deref() == Some("public")
    }));

    stop_server(server).await;
}

#[tokio::test]
async fn partitioned_http_service_exposes_replication_and_federated_surfaces() {
    let temp = TestTempDir::new("partitioned-http");
    let partitioned = replicated_partition_service(temp.path());
    let options = HttpKernelOptions::new()
        .with_auth(pilot_auth())
        .with_service_status(ServiceStatusResponse {
            status: "ok".into(),
            build_version: env!("CARGO_PKG_VERSION").into(),
            config_version: "replicated-prototype".into(),
            schema_version: "v1".into(),
            bind_addr: None,
            service_mode: ServiceMode::Partitioned,
            storage: aether_api::ServiceStatusStorage {
                database_path: None,
                sidecar_path: None,
                audit_log_path: None,
                partition_root: Some(temp.path().to_path_buf()),
            },
            principals: Vec::new(),
            replicas: Vec::new(),
        });
    let (base_url, server) =
        spawn_partitioned_server_with_options(InMemoryKernelService::new(), partitioned, options)
            .await;
    let client = Client::new();

    for (partition, datoms) in [
        ("readiness", vec![policy_status_datom(1, "ready", 1, None)]),
        (
            "authority",
            vec![partition_owner_datom(1, "worker-a", 3, None)],
        ),
    ] {
        let append = client
            .post(format!("{base_url}/v1/partitions/append"))
            .bearer_auth("pilot-operator-token")
            .json(&PartitionAppendRequest {
                partition: PartitionId::new(partition),
                leader_epoch: None,
                datoms,
            })
            .send()
            .await
            .expect("partition append request");
        assert!(append.status().is_success());
    }

    let status = client
        .get(format!("{base_url}/v1/status"))
        .bearer_auth("pilot-operator-token")
        .send()
        .await
        .expect("status request");
    assert!(status.status().is_success());
    let status = status
        .json::<ServiceStatusResponse>()
        .await
        .expect("status response");
    assert_eq!(status.service_mode, ServiceMode::Partitioned);
    assert_eq!(status.replicas.len(), 4);

    let partition_status = client
        .get(format!("{base_url}/v1/partitions/status"))
        .bearer_auth("pilot-operator-token")
        .send()
        .await
        .expect("partition status request");
    assert!(partition_status.status().is_success());
    let partition_status = partition_status
        .json::<PartitionStatusResponse>()
        .await
        .expect("partition status response");
    assert_eq!(partition_status.partitions.len(), 2);
    assert!(partition_status
        .partitions
        .iter()
        .all(|partition| partition.replicas.len() == 2));

    let federated_request = FederatedRunDocumentRequest {
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
    };

    let federated = client
        .post(format!("{base_url}/v1/federated/run"))
        .bearer_auth("pilot-operator-token")
        .json(&federated_request)
        .send()
        .await
        .expect("federated run request");
    assert!(federated.status().is_success());
    let federated = federated
        .json::<aether_api::FederatedRunDocumentResponse>()
        .await
        .expect("federated run response");
    assert_eq!(federated.imports.len(), 2);
    assert_eq!(
        federated.run.query.as_ref().expect("primary query").rows[0].values,
        vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-a".into())
        ]
    );

    let report = client
        .post(format!("{base_url}/v1/federated/report"))
        .bearer_auth("pilot-operator-token")
        .json(&federated_request)
        .send()
        .await
        .expect("federated report request");
    assert!(report.status().is_success());
    let report = report
        .json::<FederatedExplainReport>()
        .await
        .expect("federated report response");
    assert_eq!(report.primary_query.len(), 1);
    assert_eq!(
        report.traces[0]
            .imported_cuts
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec!["authority@e3".to_string(), "readiness@e1".to_string()]
    );

    let joined_import = client
        .post(format!("{base_url}/v1/federated/run"))
        .bearer_auth("pilot-operator-token")
        .json(&FederatedRunDocumentRequest {
            dsl: federated_assignment_document(),
            imports: vec![ImportedFactQueryRequest {
                cut: PartitionCut::as_of("authority", ElementId::new(3)),
                dsl: joined_import_document(),
                predicate: PredicateRef {
                    id: PredicateId::new(99),
                    name: "bad_import".into(),
                    arity: 2,
                },
                query_name: Some("joined_now".into()),
            }],
            policy_context: None,
        })
        .send()
        .await
        .expect("joined import request");
    assert_eq!(joined_import.status(), reqwest::StatusCode::BAD_REQUEST);

    let promote = client
        .post(format!("{base_url}/v1/partitions/promote"))
        .bearer_auth("pilot-operator-token")
        .json(&PromoteReplicaRequest {
            partition: PartitionId::new("authority"),
            replica_id: ReplicaId::new(2),
        })
        .send()
        .await
        .expect("promote request");
    assert!(promote.status().is_success());

    let stale_append = client
        .post(format!("{base_url}/v1/partitions/append"))
        .bearer_auth("pilot-operator-token")
        .json(&PartitionAppendRequest {
            partition: PartitionId::new("authority"),
            leader_epoch: Some(aether_api::LeaderEpoch::new(1)),
            datoms: vec![partition_owner_datom(1, "worker-b", 4, None)],
        })
        .send()
        .await
        .expect("stale append request");
    assert_eq!(stale_append.status(), reqwest::StatusCode::BAD_REQUEST);

    let partition_status = client
        .get(format!("{base_url}/v1/partitions/status"))
        .bearer_auth("pilot-operator-token")
        .send()
        .await
        .expect("partition status after promote");
    let partition_status = partition_status
        .json::<PartitionStatusResponse>()
        .await
        .expect("partition status response");
    let authority = partition_status
        .partitions
        .iter()
        .find(|partition| partition.partition == PartitionId::new("authority"))
        .expect("authority partition");
    assert_eq!(authority.leader_epoch.0, 2);
    assert!(authority
        .replicas
        .iter()
        .any(|replica| replica.replica_id == ReplicaId::new(2)
            && replica.role == ReplicaRole::Leader));

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
async fn authenticated_http_service_binds_policy_context_to_tokens() {
    let audit = TestAuditPath::new("policy-binding-audit");
    let options = HttpKernelOptions::new()
        .with_auth(pilot_auth())
        .with_audit_log_path(audit.path().to_path_buf());
    let (base_url, server) = spawn_server_with_options(InMemoryKernelService::new(), options).await;
    let client = Client::new();

    let append = client
        .post(format!("{base_url}/v1/append"))
        .bearer_auth("pilot-operator-token")
        .json(&AppendRequest {
            datoms: vec![
                policy_status_datom(1, "ready", 1, None),
                policy_status_datom(
                    3,
                    "ready",
                    2,
                    Some(PolicyEnvelope {
                        capabilities: vec!["executor".into()],
                        visibilities: Vec::new(),
                    }),
                ),
            ],
        })
        .send()
        .await
        .expect("append policy datoms");
    assert!(append.status().is_success());

    let operator_default = client
        .post(format!("{base_url}/v1/documents/run"))
        .bearer_auth("pilot-operator-token")
        .json(&RunDocumentRequest {
            dsl: policy_document_dsl(),
            policy_context: None,
        })
        .send()
        .await
        .expect("operator run request");
    assert!(operator_default.status().is_success());
    let operator_rows = operator_default
        .json::<RunDocumentResponse>()
        .await
        .expect("operator run response")
        .query
        .expect("operator query result")
        .rows;
    assert_eq!(
        operator_rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Entity(EntityId::new(1))],
            vec![Value::Entity(EntityId::new(2))],
            vec![Value::Entity(EntityId::new(3))],
        ]
    );

    let public_only = client
        .post(format!("{base_url}/v1/documents/run"))
        .bearer_auth("pilot-query-token")
        .json(&RunDocumentRequest {
            dsl: policy_document_dsl(),
            policy_context: None,
        })
        .send()
        .await
        .expect("public-only run request");
    assert!(public_only.status().is_success());
    let public_rows = public_only
        .json::<RunDocumentResponse>()
        .await
        .expect("public-only response")
        .query
        .expect("public-only query result")
        .rows;
    assert_eq!(
        public_rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![vec![Value::Entity(EntityId::new(1))]]
    );

    let forbidden_escalation = client
        .post(format!("{base_url}/v1/documents/run"))
        .bearer_auth("pilot-query-token")
        .json(&RunDocumentRequest {
            dsl: policy_document_dsl(),
            policy_context: Some(PolicyContext {
                capabilities: vec!["executor".into()],
                visibilities: Vec::new(),
            }),
        })
        .send()
        .await
        .expect("forbidden escalation request");
    assert_eq!(
        forbidden_escalation.status(),
        reqwest::StatusCode::FORBIDDEN
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
        entry.path == "/v1/documents/run"
            && entry.principal == "pilot-operator"
            && entry.context.policy_decision.as_deref() == Some("token_default")
            && entry.context.effective_capabilities == vec!["executor".to_string()]
    }));
    assert!(audit_entries.iter().any(|entry| {
        entry.path == "/v1/documents/run"
            && entry.principal == "query-client"
            && entry.status == reqwest::StatusCode::FORBIDDEN.as_u16()
            && entry.context.policy_decision.as_deref() == Some("denied_escalation")
            && entry.context.requested_capabilities == vec!["executor".to_string()]
            && entry.context.granted_capabilities.is_empty()
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
                .json(&ExplainTupleRequest {
                    tuple_id,
                    policy_context: None,
                })
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

fn partition_owner_datom(
    entity: u64,
    owner: &str,
    element: u64,
    policy: Option<PolicyEnvelope>,
) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(1),
        value: Value::String(owner.into()),
        op: OperationKind::Assert,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy,
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

fn joined_import_document() -> String {
    r#"
schema {
  attr task.status: ScalarLWW<String>
  attr task.owner: ScalarLWW<String>
}

predicates {
  task_status(Entity, String)
  task_owner(Entity, String)
  joined_now(Entity, String)
}

rules {
  joined_now(t, worker) <- task_status(t, "ready"), task_owner(t, worker)
}

materialize {
  joined_now
}

query joined_now {
  current
  goal joined_now(t, worker)
  keep t, worker
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

async fn spawn_partitioned_server_with_options(
    service: impl KernelService + Send + 'static,
    partitioned: ReplicatedAuthorityPartitionService,
    options: HttpKernelOptions,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind partitioned test listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let router = http_router_with_partitioned_options(service, partitioned, options);
        axum::serve(listener, router)
            .await
            .expect("serve partitioned http kernel");
    });

    (format!("http://{address}"), server)
}

async fn stop_server(server: tokio::task::JoinHandle<()>) {
    server.abort();
    let _ = server.await;
}

struct TestTempDir {
    path: PathBuf,
}

impl TestTempDir {
    fn new(name: &str) -> Self {
        let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("aether-http-dir-{name}-{nanos}-{unique}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
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

fn replicated_partition_service(root: &Path) -> ReplicatedAuthorityPartitionService {
    ReplicatedAuthorityPartitionService::open(
        root,
        vec![
            AuthorityPartitionConfig {
                partition: PartitionId::new("readiness"),
                replicas: vec![
                    ReplicaConfig {
                        replica_id: ReplicaId::new(1),
                        database_path: PathBuf::from("readiness-leader.sqlite"),
                        role: ReplicaRole::Leader,
                    },
                    ReplicaConfig {
                        replica_id: ReplicaId::new(2),
                        database_path: PathBuf::from("readiness-follower.sqlite"),
                        role: ReplicaRole::Follower,
                    },
                ],
            },
            AuthorityPartitionConfig {
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
            },
        ],
    )
    .expect("open replicated partition service")
}
