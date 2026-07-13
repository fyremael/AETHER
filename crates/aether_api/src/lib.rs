pub use aether_service_core::*;

pub mod admission {
    pub use aether_service_core::admission::*;
}
#[doc(hidden)]
pub mod evaluation {
    pub use aether_service_core::evaluation::*;
}
pub mod execution {
    pub use aether_service_core::execution::*;
}
pub mod namespace {
    pub use aether_service_core::namespace::*;
}
pub mod sidecar {
    pub use aether_sidecar::*;
}

pub mod deployment;
pub mod http;
pub mod partitioned;
#[doc(hidden)]
pub mod perf;
pub mod pilot {
    pub use aether_pilot::pilot::*;
}
pub mod report {
    pub use aether_pilot::report::*;
}
pub mod status;

pub use aether_pilot::{
    build_coordination_delta_report, build_coordination_pilot_report,
    build_coordination_pilot_report_with_policy, render_coordination_delta_report_markdown,
    render_coordination_pilot_report_markdown, CoordinationCut, CoordinationDeltaReport,
    CoordinationDeltaReportRequest, CoordinationPilotReport, CoordinationPilotReportRequest,
    CoordinationTraceHandle, ReportRow, ReportRowChange, ReportRowDiff, ReportSectionDelta,
    TraceSummary, TraceTupleSummary,
};
pub use aether_pilot::{
    coordination_pilot_dsl, coordination_pilot_seed_history,
    COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT, COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
};
pub use deployment::{
    default_audit_log_path, serve_pilot_http_service, DeploymentError, PilotAuthConfig,
    PilotConcurrencyConfig, PilotHttpTransportConfig, PilotServiceConfig, PilotStorageConfig,
    PilotTokenConfig, ResolvedPilotHttpTransport, ResolvedPilotServiceConfig, ResolvedPilotStorage,
    ResolvedPilotTokenSummary,
};
pub use http::{
    http_router, http_router_with_options, http_router_with_partitioned_options,
    http_router_with_postgres_namespaces, http_router_with_postgres_namespaces_and_tls,
    http_router_with_sqlite_namespaces, AuditContext, AuditEntry, AuditLogResponse, AuthScope,
    HealthResponse, HttpAccessToken, HttpAuthConfig, HttpKernelOptions, HttpKernelState,
    HttpResourceLimits, PageInfo, PageRequest, PagedHistoryResponse, PagedRunDocumentResponse,
    PagedTraceResponse, StructuredErrorResponse, AETHER_NAMESPACE_HEADER, AETHER_REQUEST_ID_HEADER,
};
pub use partitioned::{
    render_federated_explain_report_markdown, AuthorityPartitionConfig, FederatedExplainReport,
    FederatedHistoryRequest, FederatedHistoryResponse, FederatedImportedSourceSummary,
    FederatedNamedQuerySummary, FederatedReportRow, FederatedRunDocumentRequest,
    FederatedRunDocumentResponse, FederatedTraceSummary, FederatedTraceTupleSummary,
    ImportedFactQueryRequest, ImportedFactQueryResponse, LeaderEpoch, PartitionAppendRequest,
    PartitionAppendResponse, PartitionHistoryRequest, PartitionHistoryResponse,
    PartitionStateRequest, PartitionStateResponse, PartitionStatus, PartitionStatusResponse,
    PartitionedInMemoryKernelService, PromoteReplicaRequest, PromoteReplicaResponse, ReplicaConfig,
    ReplicaRole, ReplicaStatus, ReplicatedAuthorityPartitionService,
    SqlitePartitionedKernelService,
};
pub use status::{
    AuthReloadResponse, NamespaceStatusSummary, PrincipalStatusSummary, ReplicaStatusSummary,
    ServiceMode, ServiceResourceControlStatus, ServiceStatusResponse, ServiceStatusStorage,
    ServiceTransportStatus,
};

#[cfg(test)]
mod tests {
    use super::{
        coordination_pilot_dsl, coordination_pilot_seed_history, ApiError, AppendRequest,
        AsOfRequest, CurrentStateRequest, ExplainArtifact, InMemoryKernelService, KernelService,
        ParseDocumentRequest, ResolveTraceHandleRequest, RunDocumentRequest,
        COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT, COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
    };
    use crate::execution::ExecutionError;
    use aether_ast::{ElementId, EntityId, PolicyContext, PolicyEnvelope, Value};

    #[test]
    fn service_models_multi_worker_lease_handoff_and_fencing() {
        let mut service = InMemoryKernelService::new();
        service
            .append(AppendRequest {
                datoms: coordination_pilot_seed_history(),
            })
            .expect("append journal");

        let parsed = service
            .parse_document(ParseDocumentRequest {
                dsl: coordination_pilot_dsl(
                    &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
                    "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
                ),
            })
            .expect("parse coordination document");
        assert_eq!(parsed.program.facts.len(), 7);

        let pre_heartbeat_authorized = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    &format!("as_of e{}", COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT),
                    "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
                ),
                policy_context: None,
            })
            .expect("run pre-heartbeat authorization document");
        assert_eq!(
            pre_heartbeat_authorized.state.as_of,
            Some(ElementId::new(COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT))
        );
        assert!(pre_heartbeat_authorized
            .query
            .as_ref()
            .expect("query result should exist")
            .rows
            .is_empty());

        let as_of_authorized = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
                    "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
                ),
                policy_context: None,
            })
            .expect("run as_of authorization document");
        assert_eq!(
            as_of_authorized.state.as_of,
            Some(ElementId::new(COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT))
        );
        let as_of_authorized_rows = &as_of_authorized
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(as_of_authorized_rows.len(), 1);
        assert_eq!(
            as_of_authorized_rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into()),
                Value::U64(1),
            ]
        );

        let current_authorized = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    "current",
                    "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
                ),
                policy_context: None,
            })
            .expect("run current authorization document");
        let authorized_rows = &current_authorized
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(authorized_rows.len(), 1);
        assert_eq!(
            authorized_rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into()),
                Value::U64(2),
            ]
        );
        let authorized_tuple = authorized_rows[0]
            .tuple_id
            .expect("execution_authorized tuple id");
        let handle = current_authorized
            .execution
            .as_ref()
            .expect("execution receipt")
            .trace_handles
            .iter()
            .find(|binding| binding.local_tuple_id == authorized_tuple)
            .expect("authorization trace handle")
            .handle
            .clone();
        let trace = service
            .resolve_trace_handle(ResolveTraceHandleRequest {
                handle,
                policy_context: None,
                verify_replay: true,
            })
            .expect("explain authorization tuple")
            .record
            .trace;
        assert!(!trace.tuples.is_empty());

        let claimable = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    "current",
                    "goal worker_can_claim(t, worker)\n  keep t, worker",
                ),
                policy_context: None,
            })
            .expect("run claimability document");
        let claimable_rows = &claimable
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(claimable_rows.len(), 2);
        assert_eq!(
            claimable_rows
                .iter()
                .map(|row| row.values.clone())
                .collect::<Vec<_>>(),
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

        let accepted_outcomes = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    "current",
                    "goal execution_outcome_accepted(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
                ),
                policy_context: None,
            })
            .expect("run accepted-outcome document");
        let accepted_rows = &accepted_outcomes
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(
            accepted_rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into()),
                Value::U64(2),
                Value::String("completed".into()),
                Value::String("current-worker-b".into()),
            ]
        );

        let rejected_outcomes = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    "current",
                    "goal execution_outcome_rejected_stale(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
                ),
                policy_context: None,
            })
            .expect("run rejected-outcome document");
        let rejected_rows = &rejected_outcomes
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(
            rejected_rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into()),
                Value::U64(1),
                Value::String("completed".into()),
                Value::String("stale-worker-a".into()),
            ]
        );
    }

    #[test]
    fn service_parses_and_runs_named_queries_and_explain_directives() {
        let mut service = InMemoryKernelService::new();
        service
            .append(AppendRequest {
                datoms: vec![dependency_datom(1, 2, 1), dependency_datom(2, 3, 2)],
            })
            .expect("append transitive chain");

        let parsed = service
            .parse_document(ParseDocumentRequest {
                dsl: transitive_document_dsl(),
            })
            .expect("parse transitive document");
        assert_eq!(parsed.query, Some(parsed.queries[0].spec.clone()));
        assert_eq!(parsed.queries.len(), 2);
        assert_eq!(parsed.explains.len(), 2);

        let response = service
            .run_document(RunDocumentRequest {
                dsl: transitive_document_dsl(),
                policy_context: None,
            })
            .expect("run named-query document");
        assert_eq!(response.query, Some(response.queries[0].result.clone()));
        assert_eq!(response.queries.len(), 2);
        assert_eq!(response.explains.len(), 2);
        assert_eq!(
            response.queries[0].result.rows[0].values,
            vec![Value::Entity(EntityId::new(2))]
        );
        assert_eq!(
            response.queries[1]
                .result
                .rows
                .iter()
                .map(|row| row.values.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Entity(EntityId::new(2))],
                vec![Value::Entity(EntityId::new(3))],
            ]
        );
        assert!(matches!(
            &response.explains[0].result,
            ExplainArtifact::Tuple(trace) if !trace.tuples.is_empty()
        ));
        assert!(matches!(
            &response.explains[1].result,
            ExplainArtifact::Plan(explanation) if !explanation.phase_graph.nodes.is_empty()
        ));
    }

    #[test]
    fn service_filters_state_and_derivation_by_policy_context() {
        let mut service = InMemoryKernelService::new();
        let dsl = r#"
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
"#;

        let parsed = service
            .parse_document(ParseDocumentRequest { dsl: dsl.into() })
            .expect("parse policy document");
        service
            .append(AppendRequest {
                datoms: vec![
                    status_datom(1, "ready", 1, None),
                    status_datom(
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
            .expect("append policy datoms");

        let default_state = service
            .current_state(CurrentStateRequest {
                schema: parsed.schema.clone(),
                datoms: Vec::new(),
                policy_context: None,
            })
            .expect("resolve default state");
        assert_eq!(default_state.state.entities.len(), 1);

        let executor_state = service
            .current_state(CurrentStateRequest {
                schema: parsed.schema.clone(),
                datoms: Vec::new(),
                policy_context: Some(PolicyContext {
                    capabilities: vec!["executor".into()],
                    visibilities: Vec::new(),
                }),
            })
            .expect("resolve executor state");
        assert_eq!(executor_state.state.entities.len(), 2);

        let default_result = service
            .run_document(RunDocumentRequest {
                dsl: dsl.into(),
                policy_context: None,
            })
            .expect("run default policy document");
        assert_eq!(
            default_result
                .query
                .expect("default query result")
                .rows
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>(),
            vec![vec![Value::Entity(EntityId::new(1))]]
        );

        let executor_result = service
            .run_document(RunDocumentRequest {
                dsl: dsl.into(),
                policy_context: Some(PolicyContext {
                    capabilities: vec!["executor".into()],
                    visibilities: Vec::new(),
                }),
            })
            .expect("run executor policy document");
        let executor_rows = executor_result
            .query
            .as_ref()
            .expect("executor query result")
            .rows
            .clone();
        assert_eq!(
            executor_rows
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Entity(EntityId::new(1))],
                vec![Value::Entity(EntityId::new(2))],
                vec![Value::Entity(EntityId::new(3))],
            ]
        );

        let protected_tuple = executor_result
            .query
            .as_ref()
            .expect("executor query result")
            .rows
            .iter()
            .find(|row| row.values == vec![Value::Entity(EntityId::new(3))])
            .and_then(|row| row.tuple_id)
            .expect("protected tuple id");
        let protected_handle = executor_result
            .execution
            .as_ref()
            .expect("executor execution receipt")
            .trace_handles
            .iter()
            .find(|binding| binding.local_tuple_id == protected_tuple)
            .expect("protected trace handle")
            .handle
            .clone();
        let mismatch = service
            .resolve_trace_handle(ResolveTraceHandleRequest {
                handle: protected_handle.clone(),
                policy_context: None,
                verify_replay: false,
            })
            .expect_err("explain should reject mismatched policy context");
        assert!(matches!(
            mismatch,
            ApiError::Execution(ExecutionError::InsufficientPolicy)
        ));
        let executor_trace = service
            .resolve_trace_handle(ResolveTraceHandleRequest {
                handle: protected_handle,
                policy_context: Some(PolicyContext {
                    capabilities: vec!["executor".into()],
                    visibilities: Vec::new(),
                }),
                verify_replay: true,
            })
            .expect("explain protected tuple with matching policy")
            .record
            .trace;
        assert!(!executor_trace.tuples.is_empty());
    }

    #[test]
    fn service_rejects_hidden_as_of_cuts_under_policy() {
        let mut service = InMemoryKernelService::new();
        let parsed = service
            .parse_document(ParseDocumentRequest {
                dsl: transitive_document_dsl(),
            })
            .expect("parse transitive document");
        service
            .append(AppendRequest {
                datoms: vec![dependency_datom(1, 2, 1), {
                    let mut datom = dependency_datom(2, 3, 2);
                    datom.policy = Some(PolicyEnvelope {
                        capabilities: vec!["executor".into()],
                        visibilities: Vec::new(),
                    });
                    datom
                }],
            })
            .expect("append mixed-visibility chain");

        let hidden_as_of = service.as_of(AsOfRequest {
            schema: parsed.schema.clone(),
            datoms: Vec::new(),
            at: ElementId::new(2),
            policy_context: None,
        });
        assert!(matches!(
            hidden_as_of,
            Err(ApiError::Validation(message)) if message == "unknown element 2"
        ));

        let visible_as_of = service
            .as_of(AsOfRequest {
                schema: parsed.schema,
                datoms: Vec::new(),
                at: ElementId::new(2),
                policy_context: Some(PolicyContext {
                    capabilities: vec!["executor".into()],
                    visibilities: Vec::new(),
                }),
            })
            .expect("authorized as_of should succeed");
        assert_eq!(visible_as_of.state.as_of, Some(ElementId::new(2)));
    }

    fn transitive_document_dsl() -> String {
        r#"
schema {
  attr task.depends_on: RefSet<Entity>
}

predicates {
  task_depends_on(Entity, Entity)
  depends_transitive(Entity, Entity)
}

rules {
  depends_transitive(x, y) <- task_depends_on(x, y)
  depends_transitive(x, z) <- depends_transitive(x, y), task_depends_on(y, z)
}

materialize {
  depends_transitive
}

query first_cut {
  as_of e1
  goal depends_transitive(entity(1), y)
  keep y
}

query current_cut {
  current
  goal depends_transitive(entity(1), y)
  keep y
}

explain current_path {
  tuple depends_transitive(entity(1), entity(3))
}

explain plan_shape {
  plan
}
"#
        .into()
    }

    fn dependency_datom(entity: u64, value: u64, element: u64) -> aether_ast::Datom {
        aether_ast::Datom {
            entity: EntityId::new(entity),
            attribute: aether_ast::AttributeId::new(1),
            value: Value::Entity(EntityId::new(value)),
            op: aether_ast::OperationKind::Add,
            element: ElementId::new(element),
            replica: aether_ast::ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: aether_ast::DatomProvenance::default(),
            policy: None,
        }
    }

    fn status_datom(
        entity: u64,
        status: &str,
        element: u64,
        policy: Option<PolicyEnvelope>,
    ) -> aether_ast::Datom {
        aether_ast::Datom {
            entity: EntityId::new(entity),
            attribute: aether_ast::AttributeId::new(1),
            value: Value::String(status.into()),
            op: aether_ast::OperationKind::Assert,
            element: ElementId::new(element),
            replica: aether_ast::ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: aether_ast::DatomProvenance::default(),
            policy,
        }
    }
}
