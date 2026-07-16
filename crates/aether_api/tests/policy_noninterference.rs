use aether_api::{
    ApiError, AppendRequest, AsOfRequest, CurrentStateRequest, HistoryRequest,
    InMemoryKernelService, KernelService, KernelServiceCore, RunDocumentRequest,
    SqliteKernelService,
};
use aether_ast::{
    AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, PolicyContext,
    PolicyEnvelope, ReplicaId, Value,
};
use aether_resolver::{JournalDependencyKind, ResolveError, ResolvedState, ResolvedValue};
use aether_schema::{AttributeClass, AttributeSchema, Schema, ValueType};
use aether_storage::{InMemoryJournal, Journal};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

const ENTITY: EntityId = EntityId::new(1);
const SCALAR: AttributeId = AttributeId::new(1);
const SET: AttributeId = AttributeId::new(2);
const SEQUENCE: AttributeId = AttributeId::new(3);

#[test]
fn public_scalar_replay_ignores_hidden_assert_overwrite_and_removals() {
    struct Case {
        name: &'static str,
        history: Vec<Datom>,
        public: Option<Option<&'static str>>,
        authorized: Option<Option<&'static str>>,
        public_cut: Option<ElementId>,
    }

    let cases = vec![
        Case {
            name: "hidden assert",
            history: vec![datom(SCALAR, "secret", OperationKind::Assert, 1, true)],
            public: None,
            authorized: Some(Some("secret")),
            public_cut: None,
        },
        Case {
            name: "hidden overwrite",
            history: vec![
                datom(SCALAR, "public", OperationKind::Assert, 1, false),
                datom(SCALAR, "secret", OperationKind::Assert, 2, true),
            ],
            public: Some(Some("public")),
            authorized: Some(Some("secret")),
            public_cut: Some(ElementId::new(1)),
        },
        Case {
            name: "hidden retract",
            history: vec![
                datom(SCALAR, "public", OperationKind::Assert, 1, false),
                datom(SCALAR, "public", OperationKind::Retract, 2, true),
            ],
            public: Some(Some("public")),
            authorized: Some(None),
            public_cut: Some(ElementId::new(1)),
        },
        Case {
            name: "hidden release",
            history: vec![
                datom(SCALAR, "public", OperationKind::Assert, 1, false),
                datom(SCALAR, "public", OperationKind::Release, 2, true),
            ],
            public: Some(Some("public")),
            authorized: Some(None),
            public_cut: Some(ElementId::new(1)),
        },
        Case {
            name: "hidden lease expiry",
            history: vec![
                datom(SCALAR, "public", OperationKind::Assert, 1, false),
                datom(SCALAR, "public", OperationKind::LeaseExpire, 2, true),
            ],
            public: Some(Some("public")),
            authorized: Some(None),
            public_cut: Some(ElementId::new(1)),
        },
    ];

    for case in cases {
        let mut service = InMemoryKernelService::new();
        service
            .append(AppendRequest {
                datoms: case.history,
            })
            .unwrap_or_else(|error| panic!("{} append failed: {error}", case.name));

        let public = current(&service, None);
        assert_scalar(&public, case.public, case.name);
        assert_eq!(public.as_of, case.public_cut, "{} public cut", case.name);

        let authorized = current(&service, Some(restricted_context()));
        assert_scalar(&authorized, case.authorized, case.name);
        assert_eq!(
            authorized.as_of,
            Some(ElementId::new(if case.public_cut.is_some() {
                2
            } else {
                1
            })),
            "{} authorized cut",
            case.name
        );
    }
}

#[test]
fn public_set_replay_ignores_hidden_add_remove_and_retract() {
    struct Case {
        name: &'static str,
        history: Vec<Datom>,
        public: Option<Vec<&'static str>>,
        authorized: Option<Vec<&'static str>>,
        public_cut: Option<ElementId>,
    }

    let cases = vec![
        Case {
            name: "hidden add",
            history: vec![datom(SET, "secret", OperationKind::Add, 1, true)],
            public: None,
            authorized: Some(vec!["secret"]),
            public_cut: None,
        },
        Case {
            name: "hidden second add",
            history: vec![
                datom(SET, "public", OperationKind::Add, 1, false),
                datom(SET, "secret", OperationKind::Add, 2, true),
            ],
            public: Some(vec!["public"]),
            authorized: Some(vec!["public", "secret"]),
            public_cut: Some(ElementId::new(1)),
        },
        Case {
            name: "hidden remove",
            history: vec![
                datom(SET, "public", OperationKind::Add, 1, false),
                datom(SET, "public", OperationKind::Remove, 2, true),
            ],
            public: Some(vec!["public"]),
            authorized: Some(vec![]),
            public_cut: Some(ElementId::new(1)),
        },
        Case {
            name: "hidden retract",
            history: vec![
                datom(SET, "public", OperationKind::Add, 1, false),
                datom(SET, "public", OperationKind::Retract, 2, true),
            ],
            public: Some(vec!["public"]),
            authorized: Some(vec![]),
            public_cut: Some(ElementId::new(1)),
        },
    ];

    for case in cases {
        let mut service = InMemoryKernelService::new();
        service
            .append(AppendRequest {
                datoms: case.history,
            })
            .unwrap_or_else(|error| panic!("{} append failed: {error}", case.name));

        let public = current(&service, None);
        assert_set(&public, case.public.as_deref(), case.name);
        assert_eq!(public.as_of, case.public_cut, "{} public cut", case.name);

        let authorized = current(&service, Some(restricted_context()));
        assert_set(&authorized, case.authorized.as_deref(), case.name);
        assert_eq!(
            authorized.as_of,
            Some(ElementId::new(if case.public_cut.is_some() {
                2
            } else {
                1
            })),
            "{} authorized cut",
            case.name
        );
    }
}

#[test]
fn sequence_projection_ignores_hidden_insert_and_remove_but_rejects_hidden_anchor() {
    let mut service = InMemoryKernelService::new();
    service
        .append(AppendRequest {
            datoms: vec![
                sequence_datom("public", 1, &[], false),
                sequence_datom("hidden", 2, &[1], true),
                datom(SEQUENCE, "public", OperationKind::Remove, 3, true),
            ],
        })
        .expect("append mixed-policy sequence");

    let public = current(&service, None);
    assert_eq!(
        public
            .entity(&ENTITY)
            .and_then(|entity| entity.attribute(&SEQUENCE)),
        Some(&ResolvedValue::Sequence(vec![Value::String(
            "public".into()
        )]))
    );
    assert_eq!(public.as_of, Some(ElementId::new(1)));

    let privileged = current(&service, Some(restricted_context()));
    assert_eq!(
        privileged
            .entity(&ENTITY)
            .and_then(|entity| entity.attribute(&SEQUENCE)),
        Some(&ResolvedValue::Sequence(vec![Value::String(
            "hidden".into()
        )]))
    );
    assert_eq!(privileged.as_of, Some(ElementId::new(3)));

    // Replay still fails closed for a pre-R3 journal prefix created through
    // the deliberately low-level raw storage boundary.
    let mut journal = InMemoryJournal::new();
    journal
        .append(&[
            sequence_datom("hidden-parent", 1, &[], true),
            sequence_datom("public-child", 2, &[1], false),
        ])
        .expect("seed legacy invalid sequence for scanner coverage");
    let invalid: InMemoryKernelService = KernelServiceCore::from_journal(journal);
    let error = invalid
        .current_state(CurrentStateRequest {
            schema: schema(),
            datoms: Vec::new(),
            policy_context: None,
        })
        .expect_err("public child of hidden sequence anchor must fail closed");
    assert!(matches!(
        error,
        ApiError::Resolve(ResolveError::UnavailableScopedDependency {
            element,
            kind: JournalDependencyKind::SequenceAnchor,
        }) if element == ElementId::new(2)
    ));
}

#[test]
fn temporal_projection_uses_visible_cuts_and_hides_target_existence() {
    let mut service = InMemoryKernelService::new();
    service
        .append(AppendRequest {
            datoms: temporal_history(),
        })
        .expect("append temporal history");

    let at_first_public = as_of(&service, ElementId::new(1), None).expect("visible first cut");
    assert_scalar(
        &at_first_public,
        Some(Some("visible-1")),
        "first public cut",
    );
    assert_eq!(at_first_public.as_of, Some(ElementId::new(1)));

    let at_second_public = as_of(&service, ElementId::new(3), None).expect("visible second cut");
    assert_scalar(
        &at_second_public,
        Some(Some("visible-3")),
        "second public cut",
    );
    assert_eq!(at_second_public.as_of, Some(ElementId::new(3)));

    let hidden = as_of(&service, ElementId::new(2), None).expect_err("hidden cut must fail");
    let nonexistent =
        as_of(&service, ElementId::new(999), None).expect_err("missing cut must fail");
    assert_unknown_element(hidden, ElementId::new(2));
    assert_unknown_element(nonexistent, ElementId::new(999));

    let authorized = as_of(
        &service,
        ElementId::new(2),
        Some(PolicyContext {
            capabilities: vec!["restricted".into(), "restricted".into()],
            visibilities: Vec::new(),
        }),
    )
    .expect("authorized hidden cut");
    assert_scalar(&authorized, Some(Some("hidden-2")), "authorized hidden cut");
    assert_eq!(authorized.as_of, Some(ElementId::new(2)));

    let public_current = current(&service, None);
    assert_scalar(&public_current, Some(Some("visible-3")), "public current");
    assert_eq!(public_current.as_of, Some(ElementId::new(3)));

    let public_history = service
        .history(HistoryRequest {
            policy_context: None,
        })
        .expect("public history");
    assert_eq!(
        public_history
            .datoms
            .iter()
            .map(|datom| datom.element)
            .collect::<Vec<_>>(),
        vec![ElementId::new(1), ElementId::new(3)]
    );
}

#[test]
fn entirely_hidden_history_has_an_empty_public_projection() {
    let mut service = InMemoryKernelService::new();
    service
        .append(AppendRequest {
            datoms: vec![datom(SCALAR, "hidden", OperationKind::Assert, 1, true)],
        })
        .expect("append hidden history");

    for context in [None, Some(PolicyContext::public())] {
        let state = current(&service, context);
        assert!(state.entities.is_empty());
        assert_eq!(state.as_of, None);
    }
    assert!(service
        .history(HistoryRequest {
            policy_context: None,
        })
        .expect("public history")
        .datoms
        .is_empty());
    assert_unknown_element(
        as_of(&service, ElementId::new(1), None).expect_err("hidden target must fail"),
        ElementId::new(1),
    );
}

#[test]
fn sqlite_and_in_memory_services_have_identical_scoped_replay() {
    let temp = TestDbPath::new("policy-noninterference");
    let mut memory = InMemoryKernelService::new();
    let mut sqlite = SqliteKernelService::open(temp.path()).expect("open sqlite service");
    let history = temporal_history();
    memory
        .append(AppendRequest {
            datoms: history.clone(),
        })
        .expect("append memory history");
    sqlite
        .append(AppendRequest { datoms: history })
        .expect("append sqlite history");

    for context in [None, Some(restricted_context())] {
        assert_eq!(
            current(&memory, context.clone()),
            current(&sqlite, context.clone())
        );
        assert_eq!(
            memory
                .history(HistoryRequest {
                    policy_context: context.clone(),
                })
                .expect("memory history"),
            sqlite
                .history(HistoryRequest {
                    policy_context: context,
                })
                .expect("sqlite history")
        );
    }

    assert_eq!(
        as_of(&memory, ElementId::new(3), None).expect("memory as-of"),
        as_of(&sqlite, ElementId::new(3), None).expect("sqlite as-of")
    );
    assert_unknown_element(
        as_of(&sqlite, ElementId::new(2), None).expect_err("sqlite hidden target must fail"),
        ElementId::new(2),
    );
}

#[test]
fn equal_visible_program_projections_have_byte_equal_semantics_and_metadata() {
    let mut service = InMemoryKernelService::new();
    let projected_from_full = service
        .run_document(RunDocumentRequest {
            dsl: semantic_program(true),
            policy_context: None,
        })
        .expect("evaluate public projection of mixed-policy program");
    let explicit_control = service
        .run_document(RunDocumentRequest {
            dsl: semantic_program(false),
            policy_context: None,
        })
        .expect("evaluate explicit public control program");

    let projected_receipt = projected_from_full
        .execution
        .as_ref()
        .expect("projected execution receipt");
    let control_receipt = explicit_control
        .execution
        .as_ref()
        .expect("control execution receipt");
    assert_eq!(
        projected_receipt.manifest.execution_id,
        control_receipt.manifest.execution_id
    );
    assert_ne!(
        projected_receipt.trace_handles[0].handle,
        control_receipt.trace_handles[0].handle
    );
    let mut projected_semantics = projected_from_full.clone();
    projected_semantics.execution = None;
    projected_semantics.executions.clear();
    let mut control_semantics = explicit_control.clone();
    control_semantics.execution = None;
    control_semantics.executions.clear();
    assert_eq!(projected_semantics, control_semantics);
    assert_eq!(
        serde_json::to_vec(&projected_semantics).expect("serialize projected response"),
        serde_json::to_vec(&control_semantics).expect("serialize control response")
    );

    let privileged = service
        .run_document(RunDocumentRequest {
            dsl: semantic_program(true),
            policy_context: Some(restricted_context()),
        })
        .expect("evaluate privileged mixed-policy program");
    assert_ne!(privileged.derived, projected_from_full.derived);

    let path = privileged
        .program
        .rules
        .first()
        .expect("path rule")
        .head
        .predicate
        .id;
    assert!(privileged.derived.tuples.iter().any(|tuple| {
        tuple.tuple.predicate == path
            && tuple.tuple.values
                == vec![
                    Value::Entity(EntityId::new(1)),
                    Value::Entity(EntityId::new(3)),
                ]
    }));
    assert!(privileged
        .query
        .expect("privileged ready query")
        .rows
        .is_empty());
}

fn current(service: &impl KernelService, policy_context: Option<PolicyContext>) -> ResolvedState {
    service
        .current_state(CurrentStateRequest {
            schema: schema(),
            datoms: Vec::new(),
            policy_context,
        })
        .expect("resolve current state")
        .state
}

fn as_of(
    service: &impl KernelService,
    at: ElementId,
    policy_context: Option<PolicyContext>,
) -> Result<ResolvedState, ApiError> {
    service
        .as_of(AsOfRequest {
            schema: schema(),
            datoms: Vec::new(),
            at,
            policy_context,
        })
        .map(|response| response.state)
}

fn schema() -> Schema {
    let mut schema = Schema::new("policy-noninterference-v1");
    schema
        .register_attribute(AttributeSchema {
            id: SCALAR,
            name: "task.status".into(),
            class: AttributeClass::ScalarLww,
            value_type: ValueType::String,
        })
        .expect("register scalar attribute");
    schema
        .register_attribute(AttributeSchema {
            id: SET,
            name: "task.labels".into(),
            class: AttributeClass::SetAddWins,
            value_type: ValueType::String,
        })
        .expect("register set attribute");
    schema
        .register_attribute(AttributeSchema {
            id: SEQUENCE,
            name: "task.timeline".into(),
            class: AttributeClass::SequenceRga,
            value_type: ValueType::String,
        })
        .expect("register sequence attribute");
    schema
}

fn temporal_history() -> Vec<Datom> {
    vec![
        datom(SCALAR, "visible-1", OperationKind::Assert, 1, false),
        datom(SCALAR, "hidden-2", OperationKind::Assert, 2, true),
        datom(SCALAR, "visible-3", OperationKind::Assert, 3, false),
        datom(SCALAR, "visible-3", OperationKind::Retract, 4, true),
    ]
}

fn semantic_program(include_hidden: bool) -> String {
    let hidden = if include_hidden {
        r#"
  edge(entity(2), entity(3)) @capability("restricted")
  blocked(entity(1)) @capability("restricted")
  score(100) @capability("restricted")
"#
    } else {
        ""
    };
    format!(
        r#"
schema policy_program_v1 {{
}}

predicates {{
  edge(Entity, Entity)
  path(Entity, Entity)
  candidate(Entity)
  blocked(Entity)
  ready(Entity)
  score(U64)
  summary(U64, U64, U64, U64)
}}

facts {{
  edge(entity(1), entity(2))
  candidate(entity(1))
  blocked(entity(2))
  score(2)
  score(7)
{hidden}}}

rules {{
  path(x, y) <- edge(x, y)
  path(x, z) <- path(x, y), edge(y, z)
  ready(x) <- candidate(x), not blocked(x)
  summary(count(value), sum(value), min(value), max(value)) <- score(value)
}}

materialize {{
  path
  ready
  summary
}}

query {{
  current
  goal ready(task)
  keep task
}}
"#
    )
}

fn datom(
    attribute: AttributeId,
    value: &str,
    op: OperationKind,
    element: u64,
    hidden: bool,
) -> Datom {
    Datom {
        entity: ENTITY,
        attribute,
        value: Value::String(value.into()),
        op,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: hidden.then(restricted_policy),
    }
}

fn sequence_datom(value: &str, element: u64, anchors: &[u64], hidden: bool) -> Datom {
    let mut datom = datom(SEQUENCE, value, OperationKind::InsertAfter, element, hidden);
    datom.provenance.parent_datom_ids = anchors.iter().copied().map(ElementId::new).collect();
    datom
}

fn restricted_policy() -> PolicyEnvelope {
    PolicyEnvelope {
        capabilities: vec!["restricted".into()],
        visibilities: Vec::new(),
    }
}

fn restricted_context() -> PolicyContext {
    PolicyContext {
        capabilities: vec!["restricted".into()],
        visibilities: Vec::new(),
    }
}

fn assert_scalar(state: &ResolvedState, expected: Option<Option<&str>>, description: &str) {
    let actual = state
        .entity(&ENTITY)
        .and_then(|entity| entity.attribute(&SCALAR));
    let expected = expected
        .map(|value| ResolvedValue::Scalar(value.map(|value| Value::String(value.to_string()))));
    assert_eq!(actual, expected.as_ref(), "{description}");
}

fn assert_set(state: &ResolvedState, expected: Option<&[&str]>, description: &str) {
    let actual = state
        .entity(&ENTITY)
        .and_then(|entity| entity.attribute(&SET));
    let expected = expected.map(|values| {
        ResolvedValue::Set(
            values
                .iter()
                .map(|value| Value::String((*value).to_string()))
                .collect(),
        )
    });
    assert_eq!(actual, expected.as_ref(), "{description}");
}

fn assert_unknown_element(error: ApiError, expected: ElementId) {
    assert!(
        matches!(error, ApiError::Validation(message) if message == format!("unknown element {}", expected.0)),
        "expected opaque unknown-element error for {expected}"
    );
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
        let sidecar = PathBuf::from(format!("{}.sidecars.sqlite", self.path.display()));
        for path in [&self.path, &sidecar] {
            let _ = std::fs::remove_file(path);
            for suffix in ["-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
            }
        }
    }
}
