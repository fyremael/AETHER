use aether_api::{
    AppendRequest, GetArtifactReferenceRequest, InMemoryKernelService, KernelService,
    RegisterArtifactReferenceRequest, RegisterVectorRecordRequest, SearchVectorsRequest,
    SqliteKernelService, VectorFactProjection, VectorMetric, VectorRecordMetadata,
};
use aether_ast::{
    AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, PredicateId,
    PredicateRef, ReplicaId, RuleAst, RuleId, RuleProgram, Term, Value, Variable,
};
use aether_rules::{DefaultRuleCompiler, RuleCompiler};
use aether_runtime::{RuleRuntime, SemiNaiveRuntime};
use aether_schema::{PredicateSignature, Schema, ValueType};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

#[test]
fn vector_search_results_reenter_the_semantic_layer_with_provenance() {
    let mut service = InMemoryKernelService::new();
    service
        .append(AppendRequest {
            datoms: vec![anchor_datom(1)],
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
                policy: None,
                registered_at: ElementId::new(1),
            },
        })
        .expect("register artifact reference");
    service
        .append(AppendRequest {
            datoms: vec![anchor_datom(2)],
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
                metadata: BTreeMap::from([("topic".into(), Value::String("lease-handoff".into()))]),
                provenance: DatomProvenance::default(),
                policy: None,
                registered_at: ElementId::new(2),
            },
            embedding: vec![0.9, 0.1, 0.0],
        })
        .expect("register vector record");

    let artifact = service
        .get_artifact_reference(GetArtifactReferenceRequest {
            sidecar_id: "semantic-memory".into(),
            artifact_id: "runbook-1".into(),
        })
        .expect("fetch artifact")
        .reference;
    assert_eq!(artifact.uri, "s3://aether/runbooks/runbook-1.md");

    let search = service
        .search_vectors(SearchVectorsRequest {
            sidecar_id: "semantic-memory".into(),
            query_embedding: vec![1.0, 0.0, 0.0],
            top_k: 3,
            metric: VectorMetric::Cosine,
            as_of: Some(ElementId::new(2)),
            projection: Some(VectorFactProjection {
                predicate: PredicateRef {
                    id: PredicateId::new(50),
                    name: "semantic_neighbor".into(),
                    arity: 3,
                },
                query_entity: EntityId::new(900),
            }),
        })
        .expect("search vectors");
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

    let mut schema = Schema::new("sidecar-v1");
    schema
        .register_predicate(PredicateSignature {
            id: PredicateId::new(50),
            name: "semantic_neighbor".into(),
            fields: vec![ValueType::Entity, ValueType::Entity, ValueType::F64],
        })
        .expect("register extensional predicate");
    schema
        .register_predicate(PredicateSignature {
            id: PredicateId::new(51),
            name: "review_candidate".into(),
            fields: vec![ValueType::Entity],
        })
        .expect("register derived predicate");

    let program = RuleProgram {
        predicates: vec![
            PredicateRef {
                id: PredicateId::new(50),
                name: "semantic_neighbor".into(),
                arity: 3,
            },
            PredicateRef {
                id: PredicateId::new(51),
                name: "review_candidate".into(),
                arity: 1,
            },
        ],
        rules: vec![RuleAst {
            id: RuleId::new(1),
            head: aether_ast::Atom {
                predicate: PredicateRef {
                    id: PredicateId::new(51),
                    name: "review_candidate".into(),
                    arity: 1,
                },
                terms: vec![Term::Variable(Variable::new("doc"))],
            },
            body: vec![aether_ast::Literal::Positive(aether_ast::Atom {
                predicate: PredicateRef {
                    id: PredicateId::new(50),
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
        materialized: vec![PredicateId::new(51)],
        facts: search.facts.clone(),
    };

    let compiled = DefaultRuleCompiler
        .compile(&schema, &program)
        .expect("compile projected sidecar facts");
    let derived = SemiNaiveRuntime
        .evaluate(&Default::default(), &compiled)
        .expect("evaluate projected sidecar facts");

    assert_eq!(derived.tuples.len(), 1);
    assert_eq!(
        derived.tuples[0].tuple.values,
        vec![Value::Entity(EntityId::new(41))]
    );
    assert_eq!(
        derived.tuples[0].metadata.source_datom_ids,
        vec![ElementId::new(2), ElementId::new(1)]
    );
}

#[test]
fn sqlite_sidecar_federation_survives_restart() {
    let temp = TestDbPath::new("sidecar-federation");
    {
        let mut service = SqliteKernelService::open(temp.path()).expect("open sqlite kernel");
        service
            .append(AppendRequest {
                datoms: vec![anchor_datom(1)],
            })
            .expect("append artifact anchor");
        service
            .register_artifact_reference(RegisterArtifactReferenceRequest {
                reference: aether_api::ArtifactReference {
                    sidecar_id: "semantic-memory".into(),
                    artifact_id: "doc-1".into(),
                    entity: EntityId::new(31),
                    uri: "s3://aether/docs/doc-1.md".into(),
                    media_type: "text/markdown".into(),
                    byte_length: 256,
                    digest: Some("sha256:doc-1".into()),
                    metadata: BTreeMap::new(),
                    provenance: DatomProvenance::default(),
                    policy: None,
                    registered_at: ElementId::new(1),
                },
            })
            .expect("register artifact");
        service
            .append(AppendRequest {
                datoms: vec![anchor_datom(2)],
            })
            .expect("append vector anchor");
        service
            .register_vector_record(RegisterVectorRecordRequest {
                record: VectorRecordMetadata {
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
            .expect("register vector");
    }

    let service = SqliteKernelService::open(temp.path()).expect("reopen sqlite kernel");
    let artifact = service
        .get_artifact_reference(GetArtifactReferenceRequest {
            sidecar_id: "semantic-memory".into(),
            artifact_id: "doc-1".into(),
        })
        .expect("fetch persisted artifact")
        .reference;
    assert_eq!(artifact.uri, "s3://aether/docs/doc-1.md");

    let search = service
        .search_vectors(SearchVectorsRequest {
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
        })
        .expect("search persisted vectors");
    assert_eq!(search.matches.len(), 1);
    assert_eq!(
        search.facts[0]
            .provenance
            .as_ref()
            .expect("fact provenance")
            .source_datom_ids,
        vec![ElementId::new(2), ElementId::new(1)]
    );
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
        path.push(format!("aether-sidecars-{name}-{nanos}-{unique}.sqlite"));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDbPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let sidecars = PathBuf::from(format!("{}.sidecars.sqlite", self.path.display()));
        let _ = std::fs::remove_file(&sidecars);

        for suffix in ["-wal", "-shm"] {
            let _ =
                std::fs::remove_file(PathBuf::from(format!("{}{}", self.path.display(), suffix)));
            let _ =
                std::fs::remove_file(PathBuf::from(format!("{}{}", sidecars.display(), suffix)));
        }
    }
}
