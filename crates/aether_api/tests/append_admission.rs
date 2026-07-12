use aether_api::{
    ActivateSchemaRequest, AppendAdmissionRequest, ArtifactReference, GetArtifactReferenceRequest,
    InMemoryKernelService, JournalCutRef, KernelService, PostgresKernelService,
    RegisterArtifactReferenceRequest, RegisterSchemaRequest, SchemaCompatibility, SchemaRef,
    SqliteKernelService,
};
use aether_ast::{
    AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, PolicyEnvelope,
    ReplicaId, Value,
};
use aether_schema::{AttributeClass, AttributeSchema, Schema, ValueType};
use aether_storage::{Journal, SqliteJournal};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn in_memory_append_admission_is_atomic_schema_bound_and_idempotent() {
    admission_contract(InMemoryKernelService::new());
}

#[test]
fn sqlite_append_admission_is_atomic_schema_bound_idempotent_and_durable() {
    let temp = TestDbPath::new("append-admission");
    {
        let service = SqliteKernelService::open(temp.path()).expect("open sqlite service");
        admission_contract(service);
    }
    let service = SqliteKernelService::open(temp.path()).expect("reopen sqlite service");
    assert_eq!(
        service
            .history(aether_api::HistoryRequest {
                policy_context: Some(aether_ast::PolicyContext {
                    capabilities: vec!["restricted".into()],
                    visibilities: Vec::new(),
                }),
            })
            .expect("durable history")
            .datoms
            .len(),
        2
    );
    assert!(service
        .schema_catalog()
        .expect("durable schema catalog")
        .active
        .is_some());
    let history = history_bytes(&service);
    let catalog = serde_json::to_vec(&service.schema_catalog().expect("durable catalog"))
        .expect("encode catalog");
    let receipts = serde_json::to_vec(&service.append_receipts().expect("durable receipts"))
        .expect("encode receipts");
    let sidecar = stable_sidecar_bytes(&service);
    drop(service);

    let backup = TestDbPath::new("append-admission-backup");
    std::fs::copy(temp.path(), backup.path()).expect("copy authority database backup");
    std::fs::copy(
        format!("{}.sidecars.sqlite", temp.path().display()),
        format!("{}.sidecars.sqlite", backup.path().display()),
    )
    .expect("copy sidecar database backup");
    let restored = SqliteKernelService::open(backup.path()).expect("open restored service");
    assert_eq!(history_bytes(&restored), history);
    assert_eq!(
        serde_json::to_vec(&restored.schema_catalog().expect("restored catalog"))
            .expect("encode restored catalog"),
        catalog
    );
    assert_eq!(
        serde_json::to_vec(&restored.append_receipts().expect("restored receipts"))
            .expect("encode restored receipts"),
        receipts
    );
    assert_eq!(stable_sidecar_bytes(&restored), sidecar);
}

#[test]
fn postgres_append_admission_contract_when_configured() {
    let Some(database_url) = std::env::var("AETHER_TEST_POSTGRES_URL")
        .or_else(|_| std::env::var("AETHER_POSTGRES_TEST_URL"))
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return;
    };
    let temp = TestDbPath::new("postgres-sidecars");
    let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
    let namespace = format!(
        "admission_{}_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos(),
        unique
    );
    let tls = std::env::var("AETHER_POSTGRES_TLS_CA")
        .ok()
        .map(|ca| aether_storage::PostgresTlsConfig {
            ca_certificate_paths: vec![ca.into()],
            disable_system_roots: true,
            ..Default::default()
        })
        .unwrap_or_default();
    let service = PostgresKernelService::open_postgres_with_tls(
        &database_url,
        "aether_test",
        &namespace,
        temp.path(),
        &tls,
    )
    .expect("open postgres admission service");
    admission_contract(service);
    let service = PostgresKernelService::open_postgres_with_tls(
        &database_url,
        "aether_test",
        &namespace,
        temp.path(),
        &tls,
    )
    .expect("reopen postgres admission service");
    assert_eq!(
        service.append_receipts().expect("postgres receipts").len(),
        2
    );
    assert_eq!(
        service
            .schema_catalog()
            .expect("postgres schema catalog")
            .baselines
            .len(),
        1
    );
}

#[test]
fn incompatible_existing_history_is_durably_quarantined() {
    let temp = TestDbPath::new("history-quarantine");
    let unsafe_datom = strict_datom(
        99,
        1,
        Value::String("unknown".into()),
        OperationKind::Assert,
        &SchemaRef::default(),
    );
    {
        let mut journal = SqliteJournal::open(temp.path()).expect("open raw migration journal");
        journal
            .append(&[unsafe_datom])
            .expect("seed pre-admission history");
    }
    let mut service = SqliteKernelService::open(temp.path()).expect("open migration service");
    let registered = service
        .register_schema(RegisterSchemaRequest {
            schema: strict_schema(),
            predecessor: None,
            compatibility: SchemaCompatibility::Exact,
        })
        .expect("register candidate schema");
    let before = history_bytes(&service);
    assert!(service
        .activate_schema(ActivateSchemaRequest {
            schema_ref: registered.schema_ref,
            expected_active: None,
        })
        .is_err());
    assert_eq!(history_bytes(&service), before);
    let catalog = service.schema_catalog().expect("quarantine catalog");
    assert!(catalog.active.is_none());
    assert_eq!(catalog.baselines.len(), 1);
    assert_eq!(
        catalog.baselines[0].status,
        aether_api::HistoryCertificationStatus::Quarantined
    );
    drop(service);
    let service = SqliteKernelService::open(temp.path()).expect("reopen quarantine service");
    assert_eq!(
        service
            .schema_catalog()
            .expect("durable quarantine")
            .baselines[0]
            .status,
        aether_api::HistoryCertificationStatus::Quarantined
    );
}

fn admission_contract(mut service: impl KernelService) {
    let schema = strict_schema();
    let registered = service
        .register_schema(RegisterSchemaRequest {
            schema: schema.clone(),
            predecessor: None,
            compatibility: SchemaCompatibility::Exact,
        })
        .expect("register schema");
    let active = service
        .activate_schema(ActivateSchemaRequest {
            schema_ref: registered.schema_ref.clone(),
            expected_active: None,
        })
        .expect("activate schema");
    let schema_ref = active.schema_ref;
    let initial_cut = JournalCutRef {
        last_element: None,
        entry_count: 0,
        prefix_digest: service
            .dry_run_append(AppendAdmissionRequest {
                schema_ref: Some(schema_ref.clone()),
                expected_cut: None,
                idempotency_key: None,
                datoms: vec![strict_datom(
                    1,
                    1,
                    Value::String("ready".into()),
                    OperationKind::Assert,
                    &schema_ref,
                )],
                principal: None,
            })
            .expect("dry run")
            .current_cut
            .expect("current cut")
            .prefix_digest,
    };
    let first_request = AppendAdmissionRequest {
        schema_ref: Some(schema_ref.clone()),
        expected_cut: Some(initial_cut.clone()),
        idempotency_key: Some("first-write".into()),
        datoms: vec![strict_datom(
            1,
            1,
            Value::String("ready".into()),
            OperationKind::Assert,
            &schema_ref,
        )],
        principal: None,
    };
    let first = service
        .admit_append(first_request.clone())
        .expect("commit admitted batch");
    assert_eq!(first.appended, 1);
    assert!(!first.idempotent_replay);
    assert_eq!(first.prior_cut, initial_cut);

    let replay = service
        .admit_append(first_request)
        .expect("idempotent retry");
    assert!(replay.idempotent_replay);
    assert_eq!(replay.batch_id, first.batch_id);
    assert_eq!(history_bytes(&service), history_bytes(&service));

    service
        .register_artifact_reference(RegisterArtifactReferenceRequest {
            reference: ArtifactReference {
                sidecar_id: "admission-test".into(),
                artifact_id: "stable-artifact".into(),
                entity: EntityId::new(1),
                uri: "memory://stable".into(),
                media_type: "text/plain".into(),
                byte_length: 6,
                digest: Some("sha256:stable".into()),
                metadata: BTreeMap::new(),
                provenance: DatomProvenance::default(),
                policy: None,
                registered_at: ElementId::new(1),
            },
        })
        .expect("register stable sidecar record");

    let stable_history = history_bytes(&service);
    let stable_catalog = serde_json::to_vec(&service.schema_catalog().expect("schema catalog"))
        .expect("encode schema catalog");
    let committed_cut = first.committed_cut.clone();

    assert_rejected_unchanged(
        &mut service,
        request(
            &schema_ref,
            &committed_cut,
            vec![strict_datom(
                99,
                2,
                Value::String("x".into()),
                OperationKind::Assert,
                &schema_ref,
            )],
        ),
        &stable_history,
        &stable_catalog,
    );
    assert_rejected_unchanged(
        &mut service,
        request(
            &schema_ref,
            &committed_cut,
            vec![strict_datom(
                4,
                2,
                Value::String("not-an-entity".into()),
                OperationKind::Assert,
                &schema_ref,
            )],
        ),
        &stable_history,
        &stable_catalog,
    );
    assert_rejected_unchanged(
        &mut service,
        request(
            &schema_ref,
            &committed_cut,
            vec![strict_datom(
                1,
                2,
                Value::U64(7),
                OperationKind::Assert,
                &schema_ref,
            )],
        ),
        &stable_history,
        &stable_catalog,
    );
    assert_rejected_unchanged(
        &mut service,
        request(
            &schema_ref,
            &committed_cut,
            vec![strict_datom(
                1,
                2,
                Value::String("bad-op".into()),
                OperationKind::Add,
                &schema_ref,
            )],
        ),
        &stable_history,
        &stable_catalog,
    );
    assert_rejected_unchanged(
        &mut service,
        request(
            &schema_ref,
            &committed_cut,
            vec![strict_datom(
                1,
                1,
                Value::String("duplicate".into()),
                OperationKind::Assert,
                &schema_ref,
            )],
        ),
        &stable_history,
        &stable_catalog,
    );

    let mut forward = strict_datom(
        1,
        2,
        Value::String("forward".into()),
        OperationKind::Assert,
        &schema_ref,
    );
    forward.provenance.parent_datom_ids = vec![ElementId::new(3)];
    assert_rejected_unchanged(
        &mut service,
        request(&schema_ref, &committed_cut, vec![forward]),
        &stable_history,
        &stable_catalog,
    );

    let mut missing_causal_parent = strict_datom(
        1,
        2,
        Value::String("missing-causal-parent".into()),
        OperationKind::Assert,
        &schema_ref,
    );
    missing_causal_parent.causal_context.frontier = vec![ElementId::new(99)];
    assert_rejected_unchanged(
        &mut service,
        request(&schema_ref, &committed_cut, vec![missing_causal_parent]),
        &stable_history,
        &stable_catalog,
    );

    let mut invalid_confidence = strict_datom(
        1,
        2,
        Value::String("confidence".into()),
        OperationKind::Assert,
        &schema_ref,
    );
    invalid_confidence.provenance.confidence = f32::NAN;
    assert_rejected_unchanged(
        &mut service,
        request(&schema_ref, &committed_cut, vec![invalid_confidence]),
        &stable_history,
        &stable_catalog,
    );

    let mut missing_provenance = strict_datom(
        1,
        2,
        Value::String("missing".into()),
        OperationKind::Assert,
        &schema_ref,
    );
    missing_provenance.provenance.author_principal.clear();
    assert_rejected_unchanged(
        &mut service,
        request(&schema_ref, &committed_cut, vec![missing_provenance]),
        &stable_history,
        &stable_catalog,
    );

    let mut wrong_schema = schema_ref.clone();
    wrong_schema.digest.0 = "wrong".into();
    assert_rejected_unchanged(
        &mut service,
        request(
            &wrong_schema,
            &committed_cut,
            vec![strict_datom(
                1,
                2,
                Value::String("wrong-schema".into()),
                OperationKind::Assert,
                &schema_ref,
            )],
        ),
        &stable_history,
        &stable_catalog,
    );

    assert_rejected_unchanged(
        &mut service,
        request(
            &schema_ref,
            &initial_cut,
            vec![strict_datom(
                1,
                2,
                Value::String("stale".into()),
                OperationKind::Assert,
                &schema_ref,
            )],
        ),
        &stable_history,
        &stable_catalog,
    );

    assert_rejected_unchanged(
        &mut service,
        request(
            &schema_ref,
            &committed_cut,
            vec![
                strict_datom(
                    1,
                    2,
                    Value::String("valid-half".into()),
                    OperationKind::Assert,
                    &schema_ref,
                ),
                strict_datom(
                    99,
                    3,
                    Value::String("invalid-half".into()),
                    OperationKind::Assert,
                    &schema_ref,
                ),
            ],
        ),
        &stable_history,
        &stable_catalog,
    );

    let mut hidden_anchor = strict_datom(
        3,
        2,
        Value::String("hidden-root".into()),
        OperationKind::InsertAfter,
        &schema_ref,
    );
    hidden_anchor.policy = Some(PolicyEnvelope {
        capabilities: vec!["restricted".into()],
        visibilities: Vec::new(),
    });
    let sequence = service
        .admit_append(request(&schema_ref, &committed_cut, vec![hidden_anchor]))
        .expect("protected sequence root");
    let mut public_child = strict_datom(
        3,
        3,
        Value::String("public-child".into()),
        OperationKind::InsertAfter,
        &schema_ref,
    );
    public_child.provenance.parent_datom_ids = vec![ElementId::new(2)];
    let sequence_history = history_bytes(&service);
    let sequence_catalog = serde_json::to_vec(&service.schema_catalog().expect("schema catalog"))
        .expect("encode schema catalog");
    assert_rejected_unchanged(
        &mut service,
        request(&schema_ref, &sequence.committed_cut, vec![public_child]),
        &sequence_history,
        &sequence_catalog,
    );
}

fn request(
    schema_ref: &SchemaRef,
    cut: &JournalCutRef,
    datoms: Vec<Datom>,
) -> AppendAdmissionRequest {
    AppendAdmissionRequest {
        schema_ref: Some(schema_ref.clone()),
        expected_cut: Some(cut.clone()),
        idempotency_key: None,
        datoms,
        principal: None,
    }
}

fn assert_rejected_unchanged(
    service: &mut impl KernelService,
    request: AppendAdmissionRequest,
    history: &[u8],
    catalog: &[u8],
) {
    let receipt_count = service
        .append_receipts()
        .expect("append receipts before rejection")
        .len();
    let sidecar = stable_sidecar_bytes(service);
    assert!(service.admit_append(request).is_err());
    assert_eq!(history_bytes(service), history);
    assert_eq!(
        serde_json::to_vec(&service.schema_catalog().expect("schema catalog"))
            .expect("encode schema catalog"),
        catalog
    );
    assert_eq!(
        service
            .append_receipts()
            .expect("append receipts after rejection")
            .len(),
        receipt_count
    );
    assert_eq!(stable_sidecar_bytes(service), sidecar);
}

fn stable_sidecar_bytes(service: &impl KernelService) -> Vec<u8> {
    serde_json::to_vec(
        &service
            .get_artifact_reference(GetArtifactReferenceRequest {
                sidecar_id: "admission-test".into(),
                artifact_id: "stable-artifact".into(),
                policy_context: None,
            })
            .expect("stable sidecar record"),
    )
    .expect("encode stable sidecar")
}

fn history_bytes(service: &impl KernelService) -> Vec<u8> {
    serde_json::to_vec(&service.history(Default::default()).expect("history").datoms)
        .expect("encode history")
}

fn strict_schema() -> Schema {
    let mut schema = Schema::new("strict-v1");
    schema
        .register_attribute(AttributeSchema {
            id: AttributeId::new(1),
            name: "task.status".into(),
            class: AttributeClass::ScalarLww,
            value_type: ValueType::String,
        })
        .expect("status attribute");
    schema
        .register_attribute(AttributeSchema {
            id: AttributeId::new(2),
            name: "task.tags".into(),
            class: AttributeClass::SetAddWins,
            value_type: ValueType::List(Box::new(ValueType::U64)),
        })
        .expect("tags attribute");
    schema
        .register_attribute(AttributeSchema {
            id: AttributeId::new(3),
            name: "task.steps".into(),
            class: AttributeClass::SequenceRga,
            value_type: ValueType::String,
        })
        .expect("steps attribute");
    schema
        .register_attribute(AttributeSchema {
            id: AttributeId::new(4),
            name: "task.owner".into(),
            class: AttributeClass::RefScalar,
            value_type: ValueType::Entity,
        })
        .expect("owner attribute");
    schema
}

fn strict_datom(
    attribute: u64,
    element: u64,
    value: Value,
    op: OperationKind,
    schema_ref: &SchemaRef,
) -> Datom {
    Datom {
        entity: EntityId::new(1),
        attribute: AttributeId::new(attribute),
        value,
        op,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance {
            author_principal: "operator".into(),
            agent_id: "agent".into(),
            tool_id: "test".into(),
            session_id: "session".into(),
            source_ref: Default::default(),
            parent_datom_ids: Vec::new(),
            confidence: 1.0,
            trust_domain: "test".into(),
            schema_version: schema_ref.version.clone(),
        },
        policy: None,
    }
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
        for path in [
            self.path.clone(),
            PathBuf::from(format!("{}.sidecars.sqlite", self.path.display())),
            PathBuf::from(format!("{}.executions.sqlite", self.path.display())),
        ] {
            let _ = std::fs::remove_file(&path);
            for suffix in ["-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
            }
        }
    }
}
