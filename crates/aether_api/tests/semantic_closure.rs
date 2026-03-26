use aether_api::{
    build_coordination_pilot_report_with_policy, coordination_pilot_dsl,
    coordination_pilot_seed_history, AppendRequest, CompileProgramRequest, EvaluateProgramRequest,
    ExplainTupleRequest, InMemoryKernelService, KernelService, RegisterArtifactReferenceRequest,
    RegisterVectorRecordRequest, RunDocumentRequest, SearchVectorsRequest, VectorFactProjection,
    VectorMetric, VectorRecordMetadata, COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT,
    COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
};
use aether_ast::{
    AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, PolicyContext,
    PolicyEnvelope, PredicateId, PredicateRef, ReplicaId, RuleAst, RuleId, RuleProgram, Term,
    Value, Variable,
};
use aether_resolver::{MaterializedResolver, ResolvedState, Resolver};
use aether_schema::{AttributeClass, AttributeSchema, PredicateSignature, Schema, ValueType};
use std::collections::BTreeMap;

#[test]
fn semantic_closure_acceptance_covers_coordination_fencing_and_explainability() {
    let mut service = InMemoryKernelService::new();
    service
        .append(AppendRequest {
            datoms: coordination_pilot_seed_history(),
        })
        .expect("append coordination pilot history");

    assert_eq!(
        service
            .history(aether_api::HistoryRequest {
                policy_context: None,
            })
            .expect("fetch history")
            .datoms
            .len(),
        25
    );

    let pre_heartbeat = service
        .run_document(RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                &format!("as_of e{}", COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT),
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
            policy_context: None,
        })
        .expect("run pre-heartbeat authorization");
    assert_eq!(
        pre_heartbeat.state.as_of,
        Some(ElementId::new(COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT))
    );
    assert!(pre_heartbeat
        .query
        .expect("pre-heartbeat query")
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
        .expect("run authorization at cut");
    let as_of_rows = as_of_authorized.query.expect("authorization query").rows;
    assert_eq!(
        as_of_rows[0].values,
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
        .expect("run current authorization");
    let current_rows = current_authorized.query.expect("current query").rows;
    assert_eq!(
        current_rows[0].values,
        vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-b".into()),
            Value::U64(2),
        ]
    );

    let accepted = service
        .run_document(RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal execution_outcome_accepted(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
            ),
            policy_context: None,
        })
        .expect("run accepted outcomes");
    assert_eq!(
        accepted.query.expect("accepted query").rows[0].values,
        vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-b".into()),
            Value::U64(2),
            Value::String("completed".into()),
            Value::String("current-worker-b".into()),
        ]
    );

    let rejected = service
        .run_document(RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal execution_outcome_rejected_stale(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
            ),
            policy_context: None,
        })
        .expect("run rejected outcomes");
    assert_eq!(
        rejected.query.expect("rejected query").rows[0].values,
        vec![
            Value::Entity(EntityId::new(1)),
            Value::String("worker-a".into()),
            Value::U64(1),
            Value::String("completed".into()),
            Value::String("stale-worker-a".into()),
        ]
    );

    let trace = service
        .explain_tuple(ExplainTupleRequest {
            tuple_id: current_rows[0]
                .tuple_id
                .expect("current authorization tuple id"),
            policy_context: None,
        })
        .expect("explain current authorization")
        .trace;
    assert!(!trace.tuples.is_empty());

    let report = build_coordination_pilot_report_with_policy(&mut service, None)
        .expect("build coordination report");
    assert_eq!(report.history_len, 25);
    assert_eq!(report.current_authorized.len(), 1);
    assert_eq!(report.accepted_outcomes.len(), 1);
    assert_eq!(report.rejected_outcomes.len(), 1);
    assert!(report.trace.is_some());
}

#[test]
fn semantic_closure_acceptance_covers_policy_recursion_aggregation_and_redaction() {
    let mut service = InMemoryKernelService::new();
    service
        .append(AppendRequest {
            datoms: vec![
                semantic_datom(
                    1,
                    1,
                    Value::Entity(EntityId::new(2)),
                    OperationKind::Add,
                    1,
                    None,
                ),
                semantic_datom(
                    2,
                    1,
                    Value::Entity(EntityId::new(3)),
                    OperationKind::Add,
                    2,
                    None,
                ),
                semantic_datom(1, 3, Value::U64(7), OperationKind::Assert, 3, None),
                semantic_datom(2, 3, Value::U64(5), OperationKind::Assert, 4, None),
                semantic_datom(
                    3,
                    2,
                    Value::String("blocked".into()),
                    OperationKind::Assert,
                    5,
                    Some(ops_policy()),
                ),
                semantic_datom(
                    3,
                    2,
                    Value::String("done".into()),
                    OperationKind::Assert,
                    6,
                    Some(ops_policy()),
                ),
            ],
        })
        .expect("append semantic closure datoms");

    let public_as_of = service
        .run_document(RunDocumentRequest {
            dsl: policy_semantic_dsl(
                "as_of e5",
                "goal blocked(t)\n  keep t",
                "dependency_closure\n  blocked\n  blocked_summary",
            ),
            policy_context: None,
        })
        .expect("run public as-of policy document");
    assert!(public_as_of.query.expect("public query").rows.is_empty());

    let privileged_as_of = service
        .run_document(RunDocumentRequest {
            dsl: policy_semantic_dsl(
                "as_of e5",
                "goal blocked(t)\n  keep t",
                "dependency_closure\n  blocked\n  blocked_summary",
            ),
            policy_context: Some(ops_context()),
        })
        .expect("run privileged as-of policy document");
    let mut blocked_rows = privileged_as_of.query.expect("blocked query").rows;
    blocked_rows.sort_by_key(|row| match &row.values[0] {
        Value::Entity(entity) => entity.0,
        _ => 0,
    });
    assert_eq!(
        blocked_rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Entity(EntityId::new(1))],
            vec![Value::Entity(EntityId::new(2))],
        ]
    );
    let blocked_derived = privileged_as_of
        .derived
        .tuples
        .iter()
        .filter(|tuple| tuple.tuple.values.len() == 1)
        .collect::<Vec<_>>();
    assert_eq!(blocked_derived.len(), 2);
    assert!(blocked_derived.iter().all(|tuple| {
        tuple
            .policy
            .as_ref()
            .is_some_and(|policy| policy.capabilities == vec!["ops".to_string()])
    }));

    let tuple_id = blocked_rows[0].tuple_id.expect("blocked tuple id");
    let privileged_trace = service
        .explain_tuple(ExplainTupleRequest {
            tuple_id,
            policy_context: Some(ops_context()),
        })
        .expect("explain privileged tuple")
        .trace;
    assert!(!privileged_trace.tuples.is_empty());

    let hidden_trace = service.explain_tuple(ExplainTupleRequest {
        tuple_id,
        policy_context: None,
    });
    assert!(matches!(
        hidden_trace,
        Err(aether_api::ApiError::Validation(message))
            if message == "requested tuple is not visible under the current policy"
    ));

    let aggregate_at_cut = service
        .run_document(RunDocumentRequest {
            dsl: policy_semantic_dsl(
                "as_of e5",
                "goal blocked_summary(count_blocked, max_priority)\n  keep count_blocked, max_priority",
                "dependency_closure\n  blocked\n  blocked_summary",
            ),
            policy_context: Some(ops_context()),
        })
        .expect("run privileged aggregate at cut");
    assert_eq!(
        aggregate_at_cut.query.expect("aggregate query").rows[0].values,
        vec![Value::U64(2), Value::U64(7)]
    );
    let aggregate_tuple = aggregate_at_cut
        .derived
        .tuples
        .iter()
        .find(|tuple| tuple.tuple.values == vec![Value::U64(2), Value::U64(7)])
        .expect("aggregate tuple should be materialized");
    assert!(aggregate_tuple
        .policy
        .as_ref()
        .is_some_and(|policy| { policy.capabilities == vec!["ops".to_string()] }));

    let current_privileged = service
        .run_document(RunDocumentRequest {
            dsl: policy_semantic_dsl(
                "current",
                "goal blocked(t)\n  keep t",
                "dependency_closure\n  blocked\n  blocked_summary",
            ),
            policy_context: Some(ops_context()),
        })
        .expect("run privileged current policy document");
    assert!(current_privileged
        .query
        .expect("current query")
        .rows
        .is_empty());
}

#[test]
fn semantic_closure_acceptance_covers_sidecar_policy_and_provenance() {
    let mut service = InMemoryKernelService::new();
    service
        .append(AppendRequest {
            datoms: vec![sidecar_anchor_datom(1)],
        })
        .expect("append artifact anchor");
    service
        .register_artifact_reference(RegisterArtifactReferenceRequest {
            reference: aether_api::ArtifactReference {
                sidecar_id: "semantic-memory".into(),
                artifact_id: "runbook-1".into(),
                entity: EntityId::new(41),
                uri: "s3://aether/runbooks/runbook-1.md".into(),
                media_type: "text/markdown".into(),
                byte_length: 1024,
                digest: Some("sha256:runbook-1".into()),
                metadata: BTreeMap::from([("kind".into(), Value::String("runbook".into()))]),
                provenance: DatomProvenance::default(),
                policy: Some(ops_policy()),
                registered_at: ElementId::new(1),
            },
        })
        .expect("register protected artifact");
    service
        .append(AppendRequest {
            datoms: vec![sidecar_anchor_datom(2)],
        })
        .expect("append vector anchor");
    service
        .register_vector_record(RegisterVectorRecordRequest {
            record: VectorRecordMetadata {
                sidecar_id: "semantic-memory".into(),
                vector_id: "vec-runbook-1".into(),
                entity: EntityId::new(41),
                source_artifact_id: Some("runbook-1".into()),
                embedding_ref: "s3://aether/vectors/runbook-1.bin".into(),
                dimensions: 3,
                metric: VectorMetric::Cosine,
                metadata: BTreeMap::from([("topic".into(), Value::String("handoff".into()))]),
                provenance: DatomProvenance::default(),
                policy: Some(ops_policy()),
                registered_at: ElementId::new(2),
            },
            embedding: vec![0.9, 0.1, 0.0],
        })
        .expect("register protected vector");

    let public_search = service
        .search_vectors(SearchVectorsRequest {
            sidecar_id: "semantic-memory".into(),
            query_embedding: vec![1.0, 0.0, 0.0],
            top_k: 1,
            metric: VectorMetric::Cosine,
            as_of: Some(ElementId::new(2)),
            projection: Some(VectorFactProjection {
                predicate: PredicateRef {
                    id: PredicateId::new(90),
                    name: "semantic_neighbor".into(),
                    arity: 3,
                },
                query_entity: EntityId::new(900),
            }),
            policy_context: None,
        })
        .expect("public search");
    assert!(public_search.matches.is_empty());
    assert!(public_search.facts.is_empty());

    let protected_search = service
        .search_vectors(SearchVectorsRequest {
            sidecar_id: "semantic-memory".into(),
            query_embedding: vec![1.0, 0.0, 0.0],
            top_k: 1,
            metric: VectorMetric::Cosine,
            as_of: Some(ElementId::new(2)),
            projection: Some(VectorFactProjection {
                predicate: PredicateRef {
                    id: PredicateId::new(90),
                    name: "semantic_neighbor".into(),
                    arity: 3,
                },
                query_entity: EntityId::new(900),
            }),
            policy_context: Some(ops_context()),
        })
        .expect("protected search");
    assert_eq!(protected_search.matches.len(), 1);
    assert_eq!(
        protected_search.facts[0]
            .policy
            .as_ref()
            .expect("fact policy")
            .capabilities,
        vec!["ops".to_string()]
    );
    assert_eq!(
        protected_search.facts[0]
            .provenance
            .as_ref()
            .expect("fact provenance")
            .source_datom_ids,
        vec![ElementId::new(2), ElementId::new(1)]
    );

    let schema = sidecar_projection_schema();
    let program = sidecar_projection_program(protected_search.facts.clone());
    let compiled = service
        .compile_program(CompileProgramRequest {
            schema: schema.clone(),
            program,
        })
        .expect("compile sidecar projection program")
        .program;

    let public_eval = service
        .evaluate_program(EvaluateProgramRequest {
            state: ResolvedState::default(),
            program: compiled.clone(),
            policy_context: None,
        })
        .expect("evaluate sidecar projection publicly");
    assert!(public_eval.derived.tuples.is_empty());

    let protected_eval = service
        .evaluate_program(EvaluateProgramRequest {
            state: ResolvedState::default(),
            program: compiled,
            policy_context: Some(ops_context()),
        })
        .expect("evaluate sidecar projection with policy");
    assert_eq!(protected_eval.derived.tuples.len(), 1);
    assert_eq!(
        protected_eval.derived.tuples[0]
            .policy
            .as_ref()
            .expect("derived policy")
            .capabilities,
        vec!["ops".to_string()]
    );
    assert_eq!(
        protected_eval.derived.tuples[0].metadata.source_datom_ids,
        vec![ElementId::new(2), ElementId::new(1)]
    );
}

#[test]
fn semantic_closure_acceptance_covers_sequence_and_lifecycle_replay() {
    let resolver = MaterializedResolver;
    let schema = replay_schema();
    let datoms = vec![
        semantic_datom(
            1,
            1,
            Value::String("worker-a".into()),
            OperationKind::Claim,
            1,
            None,
        ),
        semantic_datom(
            1,
            1,
            Value::String("worker-a".into()),
            OperationKind::Release,
            2,
            None,
        ),
        semantic_datom(
            1,
            2,
            Value::String("active".into()),
            OperationKind::LeaseOpen,
            3,
            None,
        ),
        semantic_datom(
            1,
            2,
            Value::String("active".into()),
            OperationKind::LeaseRenew,
            4,
            None,
        ),
        semantic_datom(
            1,
            2,
            Value::String("active".into()),
            OperationKind::LeaseExpire,
            5,
            None,
        ),
        sequence_datom(1, 3, "a", 6, &[]),
        sequence_datom(1, 3, "c", 7, &[6]),
        sequence_datom(1, 3, "b", 8, &[6]),
        semantic_datom(
            1,
            3,
            Value::String("a".into()),
            OperationKind::Remove,
            9,
            None,
        ),
        semantic_datom(
            1,
            3,
            Value::String("c".into()),
            OperationKind::Retract,
            10,
            None,
        ),
    ];

    let as_of_sequence = resolver
        .as_of(&schema, &datoms, &ElementId::new(8))
        .expect("resolve sequence cut");
    let as_of_lease = resolver
        .as_of(&schema, &datoms, &ElementId::new(4))
        .expect("resolve lease cut");
    let current = resolver.current(&schema, &datoms).expect("resolve current");

    let as_of_entity = as_of_sequence
        .entity(&EntityId::new(1))
        .expect("as_of sequence entity");
    assert_eq!(
        as_of_entity.attribute(&AttributeId::new(1)),
        Some(&aether_resolver::ResolvedValue::Scalar(None))
    );
    assert_eq!(
        as_of_lease
            .entity(&EntityId::new(1))
            .expect("as_of lease entity")
            .attribute(&AttributeId::new(2)),
        Some(&aether_resolver::ResolvedValue::Scalar(Some(
            Value::String("active".into())
        )))
    );
    assert_eq!(
        as_of_entity.attribute(&AttributeId::new(3)),
        Some(&aether_resolver::ResolvedValue::Sequence(vec![
            Value::String("a".into()),
            Value::String("c".into()),
            Value::String("b".into()),
        ]))
    );

    let current_entity = current.entity(&EntityId::new(1)).expect("current entity");
    assert_eq!(
        current_entity.attribute(&AttributeId::new(1)),
        Some(&aether_resolver::ResolvedValue::Scalar(None))
    );
    assert_eq!(
        current_entity.attribute(&AttributeId::new(2)),
        Some(&aether_resolver::ResolvedValue::Scalar(None))
    );
    assert_eq!(
        current_entity.attribute(&AttributeId::new(3)),
        Some(&aether_resolver::ResolvedValue::Sequence(vec![
            Value::String("b".into())
        ]))
    );
}

fn semantic_datom(
    entity: u64,
    attribute: u64,
    value: Value,
    op: OperationKind,
    element: u64,
    policy: Option<PolicyEnvelope>,
) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(attribute),
        value,
        op,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy,
    }
}

fn sequence_datom(
    entity: u64,
    attribute: u64,
    value: &str,
    element: u64,
    anchors: &[u64],
) -> Datom {
    let mut datom = semantic_datom(
        entity,
        attribute,
        Value::String(value.into()),
        OperationKind::InsertAfter,
        element,
        None,
    );
    datom.provenance.parent_datom_ids = anchors
        .iter()
        .copied()
        .map(ElementId::new)
        .collect::<Vec<_>>();
    datom
}

fn ops_policy() -> PolicyEnvelope {
    PolicyEnvelope {
        capabilities: vec!["ops".into()],
        visibilities: Vec::new(),
    }
}

fn ops_context() -> PolicyContext {
    PolicyContext {
        capabilities: vec!["ops".into()],
        visibilities: Vec::new(),
    }
}

fn policy_semantic_dsl(view: &str, query_body: &str, materialized: &str) -> String {
    format!(
        r#"
schema v1 {{
  attr task.depends_on: RefSet<Entity>
  attr task.status: ScalarLWW<String>
  attr task.priority: ScalarLWW<U64>
}}

predicates {{
  task_depends_on(Entity, Entity)
  task_status(Entity, String)
  task_priority(Entity, U64)
  dependency_closure(Entity, Entity)
  blocked(Entity)
  blocked_summary(U64, U64)
}}

rules {{
  dependency_closure(t, dep) <- task_depends_on(t, dep)
  dependency_closure(t, dep2) <- task_depends_on(t, dep1), dependency_closure(dep1, dep2)
  blocked(t) <- dependency_closure(t, dep), task_status(dep, "blocked")
  blocked_summary(count(t), max(priority)) <- blocked(t), task_priority(t, priority)
}}

materialize {{
  {materialized}
}}

query {{
  {view}
  {query_body}
}}
"#
    )
}

fn replay_schema() -> Schema {
    let mut schema = Schema::new("replay-v1");
    for (id, name, class) in [
        (
            AttributeId::new(1),
            "task.claimed_by",
            AttributeClass::ScalarLww,
        ),
        (
            AttributeId::new(2),
            "task.lease_state",
            AttributeClass::ScalarLww,
        ),
        (
            AttributeId::new(3),
            "task.steps",
            AttributeClass::SequenceRga,
        ),
    ] {
        schema
            .register_attribute(AttributeSchema {
                id,
                name: name.into(),
                class,
                value_type: ValueType::String,
            })
            .expect("register replay attribute");
    }
    schema
}

fn sidecar_anchor_datom(element: u64) -> Datom {
    semantic_datom(
        1,
        99,
        Value::String(format!("sidecar-anchor-{element}")),
        OperationKind::Annotate,
        element,
        None,
    )
}

fn sidecar_projection_schema() -> Schema {
    let mut schema = Schema::new("sidecar-v1");
    schema
        .register_predicate(PredicateSignature {
            id: PredicateId::new(90),
            name: "semantic_neighbor".into(),
            fields: vec![ValueType::Entity, ValueType::Entity, ValueType::F64],
        })
        .expect("register neighbor predicate");
    schema
        .register_predicate(PredicateSignature {
            id: PredicateId::new(91),
            name: "review_candidate".into(),
            fields: vec![ValueType::Entity],
        })
        .expect("register review predicate");
    schema
}

fn sidecar_projection_program(facts: Vec<aether_ast::ExtensionalFact>) -> RuleProgram {
    RuleProgram {
        predicates: vec![
            PredicateRef {
                id: PredicateId::new(90),
                name: "semantic_neighbor".into(),
                arity: 3,
            },
            PredicateRef {
                id: PredicateId::new(91),
                name: "review_candidate".into(),
                arity: 1,
            },
        ],
        rules: vec![RuleAst {
            id: RuleId::new(1),
            head: aether_ast::Atom {
                predicate: PredicateRef {
                    id: PredicateId::new(91),
                    name: "review_candidate".into(),
                    arity: 1,
                },
                terms: vec![Term::Variable(Variable::new("doc"))],
            },
            body: vec![aether_ast::Literal::Positive(aether_ast::Atom {
                predicate: PredicateRef {
                    id: PredicateId::new(90),
                    name: "semantic_neighbor".into(),
                    arity: 3,
                },
                terms: vec![
                    Term::Variable(Variable::new("query")),
                    Term::Variable(Variable::new("doc")),
                    Term::Variable(Variable::new("score")),
                ],
            })],
        }],
        materialized: vec![PredicateId::new(91)],
        facts,
    }
}
