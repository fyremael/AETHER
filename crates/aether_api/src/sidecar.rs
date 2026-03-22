use aether_ast::{
    DatomProvenance, ElementId, EntityId, ExtensionalFact, FactProvenance, PolicyEnvelope,
    PredicateRef, SidecarKind, SidecarOrigin, SourceRef, Value,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::BTreeMap};
use thiserror::Error;

pub trait SidecarFederation {
    fn register_artifact_reference(
        &mut self,
        request: RegisterArtifactReferenceRequest,
    ) -> Result<RegisterArtifactReferenceResponse, SidecarError>;
    fn get_artifact_reference(
        &self,
        request: GetArtifactReferenceRequest,
    ) -> Result<GetArtifactReferenceResponse, SidecarError>;
    fn register_vector_record(
        &mut self,
        request: RegisterVectorRecordRequest,
    ) -> Result<RegisterVectorRecordResponse, SidecarError>;
    fn search_vectors(
        &self,
        request: SearchVectorsRequest,
    ) -> Result<SearchVectorsResponse, SidecarError>;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorMetric {
    #[default]
    Cosine,
    DotProduct,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ArtifactReference {
    pub sidecar_id: String,
    pub artifact_id: String,
    pub entity: EntityId,
    pub uri: String,
    pub media_type: String,
    pub byte_length: u64,
    pub digest: Option<String>,
    pub metadata: BTreeMap<String, Value>,
    pub provenance: DatomProvenance,
    pub policy: Option<PolicyEnvelope>,
    pub registered_at: ElementId,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VectorRecordMetadata {
    pub sidecar_id: String,
    pub vector_id: String,
    pub entity: EntityId,
    pub source_artifact_id: Option<String>,
    pub embedding_ref: String,
    pub dimensions: usize,
    pub metric: VectorMetric,
    pub metadata: BTreeMap<String, Value>,
    pub provenance: DatomProvenance,
    pub policy: Option<PolicyEnvelope>,
    pub registered_at: ElementId,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VectorFactProjection {
    pub predicate: PredicateRef,
    pub query_entity: EntityId,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RegisterArtifactReferenceRequest {
    pub reference: ArtifactReference,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RegisterArtifactReferenceResponse {
    pub reference: ArtifactReference,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetArtifactReferenceRequest {
    pub sidecar_id: String,
    pub artifact_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GetArtifactReferenceResponse {
    pub reference: ArtifactReference,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RegisterVectorRecordRequest {
    pub record: VectorRecordMetadata,
    pub embedding: Vec<f32>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RegisterVectorRecordResponse {
    pub record: VectorRecordMetadata,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchVectorsRequest {
    pub sidecar_id: String,
    pub query_embedding: Vec<f32>,
    pub top_k: usize,
    pub metric: VectorMetric,
    pub as_of: Option<ElementId>,
    pub projection: Option<VectorFactProjection>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VectorSearchMatch {
    pub vector_id: String,
    pub entity: EntityId,
    pub source_artifact_id: Option<String>,
    pub source_artifact_uri: Option<String>,
    pub score: f64,
    pub provenance: FactProvenance,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchVectorsResponse {
    pub matches: Vec<VectorSearchMatch>,
    pub facts: Vec<ExtensionalFact>,
}

#[derive(Clone, Debug, Default)]
pub struct InMemorySidecarFederation {
    artifacts: IndexMap<(String, String), ArtifactReference>,
    vectors: IndexMap<(String, String), StoredVectorRecord>,
    catalog_positions: IndexMap<ElementId, usize>,
}

#[derive(Clone, Debug)]
struct StoredVectorRecord {
    metadata: VectorRecordMetadata,
    embedding: Vec<f32>,
}

impl SidecarFederation for InMemorySidecarFederation {
    fn register_artifact_reference(
        &mut self,
        request: RegisterArtifactReferenceRequest,
    ) -> Result<RegisterArtifactReferenceResponse, SidecarError> {
        self.ensure_catalog_element_is_available(request.reference.registered_at)?;
        let key = artifact_key(
            &request.reference.sidecar_id,
            &request.reference.artifact_id,
        );
        if self.artifacts.contains_key(&key) {
            return Err(SidecarError::DuplicateArtifactId {
                sidecar_id: request.reference.sidecar_id.clone(),
                artifact_id: request.reference.artifact_id.clone(),
            });
        }

        self.record_catalog_element(request.reference.registered_at);
        self.artifacts.insert(key, request.reference.clone());
        Ok(RegisterArtifactReferenceResponse {
            reference: request.reference,
        })
    }

    fn get_artifact_reference(
        &self,
        request: GetArtifactReferenceRequest,
    ) -> Result<GetArtifactReferenceResponse, SidecarError> {
        let key = artifact_key(&request.sidecar_id, &request.artifact_id);
        let reference =
            self.artifacts
                .get(&key)
                .cloned()
                .ok_or(SidecarError::UnknownArtifactId {
                    sidecar_id: request.sidecar_id,
                    artifact_id: request.artifact_id,
                })?;
        Ok(GetArtifactReferenceResponse { reference })
    }

    fn register_vector_record(
        &mut self,
        request: RegisterVectorRecordRequest,
    ) -> Result<RegisterVectorRecordResponse, SidecarError> {
        self.ensure_catalog_element_is_available(request.record.registered_at)?;
        if request.record.dimensions != request.embedding.len() {
            return Err(SidecarError::EmbeddingDimensionMismatch {
                vector_id: request.record.vector_id.clone(),
                expected: request.record.dimensions,
                actual: request.embedding.len(),
            });
        }
        if let Some(artifact_id) = &request.record.source_artifact_id {
            let artifact_key = artifact_key(&request.record.sidecar_id, artifact_id);
            if !self.artifacts.contains_key(&artifact_key) {
                return Err(SidecarError::UnknownArtifactId {
                    sidecar_id: request.record.sidecar_id.clone(),
                    artifact_id: artifact_id.clone(),
                });
            }
        }

        let key = vector_key(&request.record.sidecar_id, &request.record.vector_id);
        if self.vectors.contains_key(&key) {
            return Err(SidecarError::DuplicateVectorId {
                sidecar_id: request.record.sidecar_id.clone(),
                vector_id: request.record.vector_id.clone(),
            });
        }

        self.record_catalog_element(request.record.registered_at);
        self.vectors.insert(
            key,
            StoredVectorRecord {
                metadata: request.record.clone(),
                embedding: request.embedding,
            },
        );
        Ok(RegisterVectorRecordResponse {
            record: request.record,
        })
    }

    fn search_vectors(
        &self,
        request: SearchVectorsRequest,
    ) -> Result<SearchVectorsResponse, SidecarError> {
        let cutoff_position = match request.as_of {
            Some(element) => Some(self.catalog_position(element)?),
            None => None,
        };
        if let Some(projection) = &request.projection {
            if projection.predicate.arity != 3 {
                return Err(SidecarError::UnsupportedProjectionArity {
                    predicate: projection.predicate.name.clone(),
                    arity: projection.predicate.arity,
                });
            }
        }

        let mut matches = self
            .vectors
            .values()
            .filter(|record| {
                record.metadata.sidecar_id == request.sidecar_id
                    && record.metadata.metric == request.metric
                    && record.metadata.dimensions == request.query_embedding.len()
                    && match cutoff_position {
                        Some(cutoff) => self
                            .catalog_positions
                            .get(&record.metadata.registered_at)
                            .copied()
                            .map(|position| position <= cutoff)
                            .unwrap_or(false),
                        None => true,
                    }
            })
            .map(|record| {
                let artifact =
                    record
                        .metadata
                        .source_artifact_id
                        .as_ref()
                        .and_then(|artifact_id| {
                            self.artifacts
                                .get(&artifact_key(&record.metadata.sidecar_id, artifact_id))
                        });
                let provenance = FactProvenance {
                    source_datom_ids: vec![record.metadata.registered_at],
                    sidecar_origin: Some(SidecarOrigin {
                        kind: SidecarKind::Vector,
                        sidecar_id: record.metadata.sidecar_id.clone(),
                        record_id: record.metadata.vector_id.clone(),
                    }),
                    source_ref: Some(artifact.map_or_else(
                        || SourceRef {
                            uri: record.metadata.embedding_ref.clone(),
                            digest: None,
                        },
                        |artifact| SourceRef {
                            uri: artifact.uri.clone(),
                            digest: artifact.digest.clone(),
                        },
                    )),
                };
                VectorSearchMatch {
                    vector_id: record.metadata.vector_id.clone(),
                    entity: record.metadata.entity,
                    source_artifact_id: record.metadata.source_artifact_id.clone(),
                    source_artifact_uri: artifact.map(|artifact| artifact.uri.clone()),
                    score: similarity_score(
                        request.metric,
                        &request.query_embedding,
                        &record.embedding,
                    ),
                    provenance,
                    metadata: record.metadata.metadata.clone(),
                }
            })
            .collect::<Vec<_>>();

        matches.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.vector_id.cmp(&right.vector_id))
        });
        let top_k = request.top_k.max(1);
        matches.truncate(top_k);

        let facts = match &request.projection {
            Some(projection) => matches
                .iter()
                .map(|item| ExtensionalFact {
                    predicate: projection.predicate.clone(),
                    values: vec![
                        Value::Entity(projection.query_entity),
                        Value::Entity(item.entity),
                        Value::F64(item.score),
                    ],
                    policy: None,
                    provenance: Some(item.provenance.clone()),
                })
                .collect(),
            None => Vec::new(),
        };

        Ok(SearchVectorsResponse { matches, facts })
    }
}

impl InMemorySidecarFederation {
    fn ensure_catalog_element_is_available(&self, element: ElementId) -> Result<(), SidecarError> {
        if self.catalog_positions.contains_key(&element) {
            return Err(SidecarError::DuplicateCatalogElement(element));
        }
        Ok(())
    }

    fn record_catalog_element(&mut self, element: ElementId) {
        let index = self.catalog_positions.len();
        self.catalog_positions.insert(element, index);
    }

    fn catalog_position(&self, element: ElementId) -> Result<usize, SidecarError> {
        self.catalog_positions
            .get(&element)
            .copied()
            .ok_or(SidecarError::UnknownCatalogElement(element))
    }
}

fn artifact_key(sidecar_id: &str, artifact_id: &str) -> (String, String) {
    (sidecar_id.to_string(), artifact_id.to_string())
}

fn vector_key(sidecar_id: &str, vector_id: &str) -> (String, String) {
    (sidecar_id.to_string(), vector_id.to_string())
}

fn similarity_score(metric: VectorMetric, query: &[f32], candidate: &[f32]) -> f64 {
    match metric {
        VectorMetric::Cosine => cosine_similarity(query, candidate),
        VectorMetric::DotProduct => dot_product(query, candidate),
    }
}

fn cosine_similarity(query: &[f32], candidate: &[f32]) -> f64 {
    let numerator = dot_product(query, candidate);
    let query_norm = query
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    let candidate_norm = candidate
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if query_norm == 0.0 || candidate_norm == 0.0 {
        0.0
    } else {
        numerator / (query_norm * candidate_norm)
    }
}

fn dot_product(query: &[f32], candidate: &[f32]) -> f64 {
    query
        .iter()
        .zip(candidate)
        .map(|(left, right)| f64::from(*left) * f64::from(*right))
        .sum()
}

#[derive(Debug, Error)]
pub enum SidecarError {
    #[error("sidecar catalog already contains element {0}")]
    DuplicateCatalogElement(ElementId),
    #[error("sidecar catalog does not contain element {0}")]
    UnknownCatalogElement(ElementId),
    #[error("sidecar {sidecar_id} already contains artifact {artifact_id}")]
    DuplicateArtifactId {
        sidecar_id: String,
        artifact_id: String,
    },
    #[error("sidecar {sidecar_id} does not contain artifact {artifact_id}")]
    UnknownArtifactId {
        sidecar_id: String,
        artifact_id: String,
    },
    #[error("sidecar {sidecar_id} already contains vector {vector_id}")]
    DuplicateVectorId {
        sidecar_id: String,
        vector_id: String,
    },
    #[error("vector {vector_id} declared dimension {expected}, but received {actual}")]
    EmbeddingDimensionMismatch {
        vector_id: String,
        expected: usize,
        actual: usize,
    },
    #[error("vector fact projection for predicate {predicate} requires arity 3, found {arity}")]
    UnsupportedProjectionArity { predicate: String, arity: usize },
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactReference, GetArtifactReferenceRequest, InMemorySidecarFederation,
        RegisterArtifactReferenceRequest, RegisterVectorRecordRequest, SearchVectorsRequest,
        SidecarError, SidecarFederation, VectorFactProjection, VectorMetric, VectorRecordMetadata,
    };
    use aether_ast::{
        DatomProvenance, ElementId, EntityId, PredicateId, PredicateRef, SidecarKind, Value,
    };
    use std::collections::BTreeMap;

    #[test]
    fn artifact_references_remain_external_metadata() {
        let mut federation = InMemorySidecarFederation::default();
        let reference = ArtifactReference {
            sidecar_id: "artifact-store".into(),
            artifact_id: "artifact-1".into(),
            entity: EntityId::new(10),
            uri: "s3://aether/artifacts/artifact-1.pdf".into(),
            media_type: "application/pdf".into(),
            byte_length: 4096,
            digest: Some("sha256:artifact-1".into()),
            metadata: BTreeMap::from([("title".into(), Value::String("Runbook".into()))]),
            provenance: DatomProvenance::default(),
            policy: None,
            registered_at: ElementId::new(1),
        };

        federation
            .register_artifact_reference(RegisterArtifactReferenceRequest {
                reference: reference.clone(),
            })
            .expect("register artifact reference");
        let fetched = federation
            .get_artifact_reference(GetArtifactReferenceRequest {
                sidecar_id: "artifact-store".into(),
                artifact_id: "artifact-1".into(),
            })
            .expect("fetch artifact reference")
            .reference;

        assert_eq!(fetched, reference);
        assert_eq!(fetched.uri, "s3://aether/artifacts/artifact-1.pdf");
    }

    #[test]
    fn vector_search_projects_provenance_bearing_semantic_facts() {
        let mut federation = InMemorySidecarFederation::default();
        federation
            .register_artifact_reference(RegisterArtifactReferenceRequest {
                reference: ArtifactReference {
                    sidecar_id: "vector-store".into(),
                    artifact_id: "doc-1".into(),
                    entity: EntityId::new(21),
                    uri: "s3://aether/docs/doc-1.md".into(),
                    media_type: "text/markdown".into(),
                    byte_length: 512,
                    digest: Some("sha256:doc-1".into()),
                    metadata: BTreeMap::new(),
                    provenance: DatomProvenance::default(),
                    policy: None,
                    registered_at: ElementId::new(1),
                },
            })
            .expect("register artifact");
        federation
            .register_vector_record(RegisterVectorRecordRequest {
                record: VectorRecordMetadata {
                    sidecar_id: "vector-store".into(),
                    vector_id: "vec-1".into(),
                    entity: EntityId::new(21),
                    source_artifact_id: Some("doc-1".into()),
                    embedding_ref: "s3://aether/vectors/vec-1.bin".into(),
                    dimensions: 3,
                    metric: VectorMetric::Cosine,
                    metadata: BTreeMap::from([(
                        "topic".into(),
                        Value::String("coordination".into()),
                    )]),
                    provenance: DatomProvenance::default(),
                    policy: None,
                    registered_at: ElementId::new(2),
                },
                embedding: vec![0.8, 0.1, 0.0],
            })
            .expect("register vector");

        let response = federation
            .search_vectors(SearchVectorsRequest {
                sidecar_id: "vector-store".into(),
                query_embedding: vec![1.0, 0.0, 0.0],
                top_k: 1,
                metric: VectorMetric::Cosine,
                as_of: Some(ElementId::new(2)),
                projection: Some(VectorFactProjection {
                    predicate: PredicateRef {
                        id: PredicateId::new(90),
                        name: "similar_document".into(),
                        arity: 3,
                    },
                    query_entity: EntityId::new(1),
                }),
            })
            .expect("search vectors");

        assert_eq!(response.matches.len(), 1);
        assert_eq!(
            response.matches[0].source_artifact_uri.as_deref(),
            Some("s3://aether/docs/doc-1.md")
        );
        assert_eq!(
            response.matches[0]
                .provenance
                .sidecar_origin
                .as_ref()
                .map(|origin| origin.kind),
            Some(SidecarKind::Vector)
        );
        assert_eq!(
            response.facts[0]
                .provenance
                .as_ref()
                .expect("fact provenance")
                .source_datom_ids,
            vec![ElementId::new(2)]
        );
    }

    #[test]
    fn vector_search_respects_catalog_as_of_cut() {
        let mut federation = InMemorySidecarFederation::default();
        for (vector_id, element, embedding) in
            [("vec-1", 1, vec![1.0, 0.0]), ("vec-2", 2, vec![0.0, 1.0])]
        {
            federation
                .register_vector_record(RegisterVectorRecordRequest {
                    record: VectorRecordMetadata {
                        sidecar_id: "vector-store".into(),
                        vector_id: vector_id.into(),
                        entity: EntityId::new(element),
                        source_artifact_id: None,
                        embedding_ref: format!("mem://{}", vector_id),
                        dimensions: 2,
                        metric: VectorMetric::DotProduct,
                        metadata: BTreeMap::new(),
                        provenance: DatomProvenance::default(),
                        policy: None,
                        registered_at: ElementId::new(element),
                    },
                    embedding,
                })
                .expect("register vector");
        }

        let before_second = federation
            .search_vectors(SearchVectorsRequest {
                sidecar_id: "vector-store".into(),
                query_embedding: vec![0.0, 1.0],
                top_k: 4,
                metric: VectorMetric::DotProduct,
                as_of: Some(ElementId::new(1)),
                projection: None,
            })
            .expect("search vectors before second insert");
        assert_eq!(
            before_second
                .matches
                .iter()
                .map(|item| item.vector_id.as_str())
                .collect::<Vec<_>>(),
            vec!["vec-1"]
        );

        let unknown = federation.search_vectors(SearchVectorsRequest {
            sidecar_id: "vector-store".into(),
            query_embedding: vec![0.0, 1.0],
            top_k: 4,
            metric: VectorMetric::DotProduct,
            as_of: Some(ElementId::new(9)),
            projection: None,
        });
        assert!(matches!(
            unknown,
            Err(SidecarError::UnknownCatalogElement(id)) if id == ElementId::new(9)
        ));
    }
}
