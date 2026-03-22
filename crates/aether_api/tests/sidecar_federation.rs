use aether_api::{
    GetArtifactReferenceRequest, InMemoryKernelService, KernelService,
    RegisterArtifactReferenceRequest, RegisterVectorRecordRequest, SearchVectorsRequest,
    VectorFactProjection, VectorMetric, VectorRecordMetadata,
};
use aether_ast::{
    DatomProvenance, ElementId, EntityId, PredicateId, PredicateRef, RuleAst, RuleId, RuleProgram,
    Term, Value, Variable,
};
use aether_rules::{DefaultRuleCompiler, RuleCompiler};
use aether_runtime::{RuleRuntime, SemiNaiveRuntime};
use aether_schema::{PredicateSignature, Schema, ValueType};
use std::collections::BTreeMap;

#[test]
fn vector_search_results_reenter_the_semantic_layer_with_provenance() {
    let mut service = InMemoryKernelService::new();
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
        vec![ElementId::new(2)]
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
        vec![ElementId::new(2)]
    );
}
