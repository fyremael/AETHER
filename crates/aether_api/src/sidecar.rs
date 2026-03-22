use aether_ast::{
    DatomProvenance, ElementId, EntityId, ExtensionalFact, FactProvenance, PolicyEnvelope,
    PredicateRef, SidecarKind, SidecarOrigin, SourceRef, Value,
};
use indexmap::IndexMap;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};
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

#[derive(Debug)]
pub struct SqliteSidecarFederation {
    connection: Connection,
    path: PathBuf,
}

#[derive(Clone, Debug)]
struct StoredVectorRecord {
    metadata: VectorRecordMetadata,
    embedding: Vec<f32>,
}

pub fn sidecar_catalog_path_for_journal(journal_path: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(format!(
        "{}.sidecars.sqlite",
        journal_path.as_ref().display()
    ))
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
        validate_projection(request.projection.as_ref())?;
        let cutoff_position = match request.as_of {
            Some(element) => Some(self.catalog_position(element)?),
            None => None,
        };
        let records = self
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
                let artifact = record
                    .metadata
                    .source_artifact_id
                    .as_ref()
                    .and_then(|artifact_id| {
                        self.artifacts
                            .get(&artifact_key(&record.metadata.sidecar_id, artifact_id))
                    })
                    .cloned();
                (record.clone(), artifact)
            })
            .collect::<Vec<_>>();
        build_search_response(request, records)
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

impl SqliteSidecarFederation {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SidecarError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let connection = Connection::open(&path)?;
        initialize_sqlite_schema(&connection)?;

        Ok(Self { connection, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn catalog_position(&self, element: ElementId) -> Result<usize, SidecarError> {
        self.connection
            .query_row(
                "SELECT seq FROM sidecar_catalog WHERE element = ?1",
                params![element_key(element)],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .map(|seq| seq as usize)
            .ok_or(SidecarError::UnknownCatalogElement(element))
    }

    fn artifact_exists(&self, sidecar_id: &str, artifact_id: &str) -> Result<bool, SidecarError> {
        Ok(self
            .connection
            .query_row(
                "SELECT 1 FROM artifact_references WHERE sidecar_id = ?1 AND artifact_id = ?2",
                params![sidecar_id, artifact_id],
                |_row| Ok(()),
            )
            .optional()?
            .is_some())
    }

    fn vector_exists(&self, sidecar_id: &str, vector_id: &str) -> Result<bool, SidecarError> {
        Ok(self
            .connection
            .query_row(
                "SELECT 1 FROM vector_records WHERE sidecar_id = ?1 AND vector_id = ?2",
                params![sidecar_id, vector_id],
                |_row| Ok(()),
            )
            .optional()?
            .is_some())
    }

    fn catalog_element_exists(&self, element: ElementId) -> Result<bool, SidecarError> {
        Ok(self
            .connection
            .query_row(
                "SELECT 1 FROM sidecar_catalog WHERE element = ?1",
                params![element_key(element)],
                |_row| Ok(()),
            )
            .optional()?
            .is_some())
    }

    fn lookup_artifact(
        &self,
        sidecar_id: &str,
        artifact_id: &str,
    ) -> Result<Option<ArtifactReference>, SidecarError> {
        let json = self
            .connection
            .query_row(
                "SELECT reference_json FROM artifact_references WHERE sidecar_id = ?1 AND artifact_id = ?2",
                params![sidecar_id, artifact_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        json.map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(SidecarError::from)
    }

    fn visible_vector_records(
        &self,
        request: &SearchVectorsRequest,
        cutoff_position: Option<usize>,
    ) -> Result<Vec<(StoredVectorRecord, Option<ArtifactReference>)>, SidecarError> {
        let mut statement = self.connection.prepare(
            "SELECT metadata_json, embedding_json
             FROM vector_records
             WHERE sidecar_id = ?1",
        )?;
        let rows = statement.query_map(params![&request.sidecar_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut records = Vec::new();
        for row in rows {
            let (metadata_json, embedding_json) = row?;
            let metadata: VectorRecordMetadata = serde_json::from_str(&metadata_json)?;
            if metadata.metric != request.metric
                || metadata.dimensions != request.query_embedding.len()
            {
                continue;
            }
            if let Some(cutoff) = cutoff_position {
                let position = self.catalog_position(metadata.registered_at)?;
                if position > cutoff {
                    continue;
                }
            }
            let embedding: Vec<f32> = serde_json::from_str(&embedding_json)?;
            let artifact = match &metadata.source_artifact_id {
                Some(artifact_id) => self.lookup_artifact(&metadata.sidecar_id, artifact_id)?,
                None => None,
            };
            records.push((
                StoredVectorRecord {
                    metadata,
                    embedding,
                },
                artifact,
            ));
        }
        Ok(records)
    }
}

impl SidecarFederation for SqliteSidecarFederation {
    fn register_artifact_reference(
        &mut self,
        request: RegisterArtifactReferenceRequest,
    ) -> Result<RegisterArtifactReferenceResponse, SidecarError> {
        if self.catalog_element_exists(request.reference.registered_at)? {
            return Err(SidecarError::DuplicateCatalogElement(
                request.reference.registered_at,
            ));
        }
        if self.artifact_exists(
            &request.reference.sidecar_id,
            &request.reference.artifact_id,
        )? {
            return Err(SidecarError::DuplicateArtifactId {
                sidecar_id: request.reference.sidecar_id.clone(),
                artifact_id: request.reference.artifact_id.clone(),
            });
        }

        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO sidecar_catalog (element, kind, sidecar_id, record_id)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                element_key(request.reference.registered_at),
                "artifact",
                &request.reference.sidecar_id,
                &request.reference.artifact_id,
            ],
        )?;
        transaction.execute(
            "INSERT INTO artifact_references (sidecar_id, artifact_id, catalog_element, reference_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                &request.reference.sidecar_id,
                &request.reference.artifact_id,
                element_key(request.reference.registered_at),
                serde_json::to_string(&request.reference)?,
            ],
        )?;
        transaction.commit()?;

        Ok(RegisterArtifactReferenceResponse {
            reference: request.reference,
        })
    }

    fn get_artifact_reference(
        &self,
        request: GetArtifactReferenceRequest,
    ) -> Result<GetArtifactReferenceResponse, SidecarError> {
        let Some(reference) = self.lookup_artifact(&request.sidecar_id, &request.artifact_id)?
        else {
            return Err(SidecarError::UnknownArtifactId {
                sidecar_id: request.sidecar_id,
                artifact_id: request.artifact_id,
            });
        };
        Ok(GetArtifactReferenceResponse { reference })
    }

    fn register_vector_record(
        &mut self,
        request: RegisterVectorRecordRequest,
    ) -> Result<RegisterVectorRecordResponse, SidecarError> {
        if self.catalog_element_exists(request.record.registered_at)? {
            return Err(SidecarError::DuplicateCatalogElement(
                request.record.registered_at,
            ));
        }
        if request.record.dimensions != request.embedding.len() {
            return Err(SidecarError::EmbeddingDimensionMismatch {
                vector_id: request.record.vector_id.clone(),
                expected: request.record.dimensions,
                actual: request.embedding.len(),
            });
        }
        if let Some(artifact_id) = &request.record.source_artifact_id {
            if !self.artifact_exists(&request.record.sidecar_id, artifact_id)? {
                return Err(SidecarError::UnknownArtifactId {
                    sidecar_id: request.record.sidecar_id.clone(),
                    artifact_id: artifact_id.clone(),
                });
            }
        }
        if self.vector_exists(&request.record.sidecar_id, &request.record.vector_id)? {
            return Err(SidecarError::DuplicateVectorId {
                sidecar_id: request.record.sidecar_id.clone(),
                vector_id: request.record.vector_id.clone(),
            });
        }

        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO sidecar_catalog (element, kind, sidecar_id, record_id)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                element_key(request.record.registered_at),
                "vector",
                &request.record.sidecar_id,
                &request.record.vector_id,
            ],
        )?;
        transaction.execute(
            "INSERT INTO vector_records (sidecar_id, vector_id, catalog_element, metadata_json, embedding_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                &request.record.sidecar_id,
                &request.record.vector_id,
                element_key(request.record.registered_at),
                serde_json::to_string(&request.record)?,
                serde_json::to_string(&request.embedding)?,
            ],
        )?;
        transaction.commit()?;

        Ok(RegisterVectorRecordResponse {
            record: request.record,
        })
    }

    fn search_vectors(
        &self,
        request: SearchVectorsRequest,
    ) -> Result<SearchVectorsResponse, SidecarError> {
        validate_projection(request.projection.as_ref())?;
        let cutoff_position = match request.as_of {
            Some(element) => Some(self.catalog_position(element)?),
            None => None,
        };
        let records = self.visible_vector_records(&request, cutoff_position)?;
        build_search_response(request, records)
    }
}

fn initialize_sqlite_schema(connection: &Connection) -> Result<(), SidecarError> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS sidecar_catalog (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            element TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL,
            sidecar_id TEXT NOT NULL,
            record_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS artifact_references (
            sidecar_id TEXT NOT NULL,
            artifact_id TEXT NOT NULL,
            catalog_element TEXT NOT NULL UNIQUE,
            reference_json TEXT NOT NULL,
            PRIMARY KEY (sidecar_id, artifact_id)
        );
        CREATE TABLE IF NOT EXISTS vector_records (
            sidecar_id TEXT NOT NULL,
            vector_id TEXT NOT NULL,
            catalog_element TEXT NOT NULL UNIQUE,
            metadata_json TEXT NOT NULL,
            embedding_json TEXT NOT NULL,
            PRIMARY KEY (sidecar_id, vector_id)
        );
        CREATE INDEX IF NOT EXISTS sidecar_catalog_by_element
            ON sidecar_catalog(element);
        CREATE INDEX IF NOT EXISTS artifact_references_by_sidecar
            ON artifact_references(sidecar_id, artifact_id);
        CREATE INDEX IF NOT EXISTS vector_records_by_sidecar
            ON vector_records(sidecar_id, vector_id);
        ",
    )?;
    Ok(())
}

fn artifact_key(sidecar_id: &str, artifact_id: &str) -> (String, String) {
    (sidecar_id.to_string(), artifact_id.to_string())
}

fn vector_key(sidecar_id: &str, vector_id: &str) -> (String, String) {
    (sidecar_id.to_string(), vector_id.to_string())
}

fn element_key(element: ElementId) -> String {
    element.0.to_string()
}

fn validate_projection(projection: Option<&VectorFactProjection>) -> Result<(), SidecarError> {
    if let Some(projection) = projection {
        if projection.predicate.arity != 3 {
            return Err(SidecarError::UnsupportedProjectionArity {
                predicate: projection.predicate.name.clone(),
                arity: projection.predicate.arity,
            });
        }
    }
    Ok(())
}

fn build_search_response(
    request: SearchVectorsRequest,
    records: Vec<(StoredVectorRecord, Option<ArtifactReference>)>,
) -> Result<SearchVectorsResponse, SidecarError> {
    let mut matches = records
        .into_iter()
        .map(|(record, artifact)| {
            let provenance = FactProvenance {
                source_datom_ids: vec![record.metadata.registered_at],
                sidecar_origin: Some(SidecarOrigin {
                    kind: SidecarKind::Vector,
                    sidecar_id: record.metadata.sidecar_id.clone(),
                    record_id: record.metadata.vector_id.clone(),
                }),
                source_ref: Some(artifact.as_ref().map_or_else(
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
                source_artifact_uri: artifact.as_ref().map(|artifact| artifact.uri.clone()),
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
    matches.truncate(request.top_k.max(1));

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
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
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
