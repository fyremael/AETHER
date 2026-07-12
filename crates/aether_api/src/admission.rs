use crate::{ContentDigest, SchemaRef};
use aether_ast::{Datom, ElementId, OperationKind, Value};
use aether_resolver::{certify_history_dependencies, MaterializedResolver, Resolver};
use aether_schema::{AttributeClass, AttributeSchema, Schema, ValueType};
use aether_storage::{
    AppendReceiptDraft, ConditionalAppend, Journal, JournalCutRef, StoredAppendReceipt,
    StoredHistoryCertification, StoredSchemaRevision,
};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

pub(crate) const ADMISSION_ENGINE_VERSION: &str = "aether-append-admission-v1";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaCompatibility {
    #[default]
    Exact,
    Additive,
    LegacyInferred,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaStatus {
    #[default]
    Registered,
    Active,
    Superseded,
    Quarantined,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NamespaceSchemaRevision {
    pub schema_ref: SchemaRef,
    pub schema: Schema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor: Option<SchemaRef>,
    pub compatibility: SchemaCompatibility,
    pub status: SchemaStatus,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RegisterSchemaRequest {
    pub schema: Schema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor: Option<SchemaRef>,
    #[serde(default)]
    pub compatibility: SchemaCompatibility,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActivateSchemaRequest {
    pub schema_ref: SchemaRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_active: Option<SchemaRef>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SchemaCatalogResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<NamespaceSchemaRevision>,
    pub revisions: Vec<NamespaceSchemaRevision>,
    #[serde(default)]
    pub baselines: Vec<SchemaBaselineReceipt>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryCertificationStatus {
    #[default]
    Certified,
    Quarantined,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SchemaBaselineReceipt {
    pub schema_ref: SchemaRef,
    pub cut: JournalCutRef,
    pub status: HistoryCertificationStatus,
    pub validation_engine_version: String,
    #[serde(default)]
    pub diagnostics: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_manifest_json: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AppendAdmissionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<SchemaRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_cut: Option<JournalCutRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub datoms: Vec<Datom>,
    #[serde(skip)]
    pub principal: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BatchId(pub String);

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendReceipt {
    pub batch_id: BatchId,
    pub schema_ref: SchemaRef,
    pub prior_cut: JournalCutRef,
    pub committed_cut: JournalCutRef,
    pub batch_digest: ContentDigest,
    pub appended: usize,
    pub idempotent_replay: bool,
    pub schema_ref_was_implicit: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AppendDryRunResponse {
    pub valid: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<SchemaRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_cut: Option<JournalCutRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_digest: Option<ContentDigest>,
    pub diagnostics: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ValidatedAppendBatch {
    datoms: Vec<Datom>,
    schema_ref: SchemaRef,
    batch_digest: ContentDigest,
    schema_ref_was_implicit: bool,
}

impl ValidatedAppendBatch {
    pub(crate) fn datoms(&self) -> &[Datom] {
        &self.datoms
    }

    pub(crate) fn schema_ref(&self) -> &SchemaRef {
        &self.schema_ref
    }

    pub(crate) fn batch_digest(&self) -> &ContentDigest {
        &self.batch_digest
    }
}

pub(crate) fn schema_ref(schema: &Schema) -> Result<SchemaRef, AdmissionError> {
    Ok(SchemaRef {
        version: schema.version.clone(),
        digest: ContentDigest(digest_schema(schema)?),
    })
}

pub(crate) fn register_schema(
    journal: &mut dyn Journal,
    request: RegisterSchemaRequest,
) -> Result<NamespaceSchemaRevision, AdmissionError> {
    let reference = schema_ref(&request.schema)?;
    if let Some(predecessor) = &request.predecessor {
        let known = journal
            .schema_revisions()?
            .into_iter()
            .find(|revision| revision.digest == predecessor.digest.0);
        let Some(known) = known else {
            return Err(AdmissionError::UnknownSchema(predecessor.clone()));
        };
        let known = decode_revision(known)?;
        ensure_schema_evolution(&known.schema, &request.schema, request.compatibility)?;
    } else if request.compatibility == SchemaCompatibility::Additive {
        return Err(AdmissionError::DocumentSchemaMismatch(
            "an additive revision requires a predecessor".into(),
        ));
    }
    let revision = NamespaceSchemaRevision {
        schema_ref: reference,
        schema: request.schema,
        predecessor: request.predecessor,
        compatibility: request.compatibility,
        status: SchemaStatus::Registered,
    };
    journal.register_schema_revision(&encode_revision(&revision)?)?;
    Ok(revision)
}

fn ensure_schema_evolution(
    predecessor: &Schema,
    candidate: &Schema,
    compatibility: SchemaCompatibility,
) -> Result<(), AdmissionError> {
    match compatibility {
        SchemaCompatibility::Exact => {
            if digest_schema(predecessor)? != digest_schema(candidate)? {
                return Err(AdmissionError::DocumentSchemaMismatch(
                    "exact revision does not match its predecessor".into(),
                ));
            }
        }
        SchemaCompatibility::Additive | SchemaCompatibility::LegacyInferred => {
            for existing in predecessor.attributes.values() {
                let Some(next) = candidate.attribute(&existing.id) else {
                    return Err(AdmissionError::DocumentSchemaMismatch(format!(
                        "revision removed attribute {}",
                        existing.id.0
                    )));
                };
                if existing.name != next.name
                    || existing.class != next.class
                    || existing.value_type != next.value_type
                {
                    return Err(AdmissionError::DocumentSchemaMismatch(format!(
                        "revision changed attribute {} semantics",
                        existing.id.0
                    )));
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn activate_schema(
    journal: &mut dyn Journal,
    request: ActivateSchemaRequest,
) -> Result<NamespaceSchemaRevision, AdmissionError> {
    let stored = journal
        .schema_revisions()?
        .into_iter()
        .find(|revision| revision.digest == request.schema_ref.digest.0)
        .ok_or_else(|| AdmissionError::UnknownSchema(request.schema_ref.clone()))?;
    let mut revision = decode_revision(stored)?;
    if revision.schema_ref != request.schema_ref {
        return Err(AdmissionError::SchemaMismatch {
            expected: revision.schema_ref,
            provided: Some(request.schema_ref),
        });
    }
    if revision.predecessor != request.expected_active {
        return Err(AdmissionError::SchemaActivationPrecondition);
    }
    let history = journal.history()?;
    let cut = journal.cut()?;
    let certification_request = AppendAdmissionRequest {
        schema_ref: (revision.compatibility != SchemaCompatibility::LegacyInferred)
            .then(|| revision.schema_ref.clone()),
        expected_cut: Some(cut.clone()),
        idempotency_key: None,
        datoms: history,
        principal: Some("schema-baseline-certifier".into()),
    };
    let certification = match validate_append(&[], &revision, &certification_request) {
        Ok(_) => SchemaBaselineReceipt {
            schema_ref: revision.schema_ref.clone(),
            cut: cut.clone(),
            status: HistoryCertificationStatus::Certified,
            validation_engine_version: ADMISSION_ENGINE_VERSION.into(),
            diagnostics: Vec::new(),
            migration_manifest_json: None,
        },
        Err(error) => {
            let certification = SchemaBaselineReceipt {
                schema_ref: revision.schema_ref.clone(),
                cut,
                status: HistoryCertificationStatus::Quarantined,
                validation_engine_version: ADMISSION_ENGINE_VERSION.into(),
                diagnostics: vec![error.to_string()],
                migration_manifest_json: None,
            };
            journal.seal_history_certification(&encode_certification(&certification)?)?;
            return Err(AdmissionError::ExistingHistoryQuarantined(
                certification.diagnostics.join("; "),
            ));
        }
    };
    journal.seal_history_certification(&encode_certification(&certification)?)?;
    journal.activate_schema_revision(
        request
            .expected_active
            .as_ref()
            .map(|reference| reference.digest.0.as_str()),
        &request.schema_ref.digest.0,
        &certification.cut,
    )?;
    revision.status = SchemaStatus::Active;
    Ok(revision)
}

pub(crate) fn schema_catalog(
    journal: &dyn Journal,
) -> Result<SchemaCatalogResponse, AdmissionError> {
    let mut revisions = journal
        .schema_revisions()?
        .into_iter()
        .map(decode_revision)
        .collect::<Result<Vec<_>, _>>()?;
    let active = journal
        .active_schema_revision()?
        .map(decode_revision)
        .transpose()?
        .map(|mut revision| {
            revision.status = SchemaStatus::Active;
            revision
        });
    if let Some(active) = &active {
        for revision in &mut revisions {
            if revision.schema_ref == active.schema_ref {
                revision.status = SchemaStatus::Active;
            }
        }
    }
    let baselines = journal
        .history_certifications()?
        .into_iter()
        .map(decode_certification)
        .collect::<Result<Vec<_>, _>>()?;
    for revision in &mut revisions {
        if baselines.iter().any(|baseline| {
            baseline.schema_ref == revision.schema_ref
                && baseline.status == HistoryCertificationStatus::Quarantined
        }) {
            revision.status = SchemaStatus::Quarantined;
        }
    }
    Ok(SchemaCatalogResponse {
        active,
        revisions,
        baselines,
    })
}

pub(crate) fn ensure_legacy_schema(
    journal: &mut dyn Journal,
    request: &AppendAdmissionRequest,
) -> Result<NamespaceSchemaRevision, AdmissionError> {
    if let Some(active) = schema_catalog(journal)?.active {
        return Ok(active);
    }
    let current_cut = journal.cut()?;
    if let Some(expected) = &request.expected_cut {
        if expected != &current_cut {
            return Err(aether_storage::JournalError::StaleCut {
                expected: expected.clone(),
                actual: current_cut,
            }
            .into());
        }
    }
    let schema = infer_legacy_schema(&request.datoms)?;
    let candidate = NamespaceSchemaRevision {
        schema_ref: schema_ref(&schema)?,
        schema: schema.clone(),
        predecessor: None,
        compatibility: SchemaCompatibility::LegacyInferred,
        status: SchemaStatus::Registered,
    };
    validate_append(&journal.history()?, &candidate, request)?;
    let registered = register_schema(
        journal,
        RegisterSchemaRequest {
            schema,
            predecessor: None,
            compatibility: SchemaCompatibility::LegacyInferred,
        },
    )?;
    activate_schema(
        journal,
        ActivateSchemaRequest {
            schema_ref: registered.schema_ref.clone(),
            expected_active: None,
        },
    )
}

pub(crate) fn extend_legacy_schema_if_needed(
    journal: &mut dyn Journal,
    active: NamespaceSchemaRevision,
    request: &AppendAdmissionRequest,
) -> Result<NamespaceSchemaRevision, AdmissionError> {
    if active.compatibility != SchemaCompatibility::LegacyInferred || request.datoms.is_empty() {
        return Ok(active);
    }
    let mut schema = active.schema.clone();
    let unknown_datoms = request
        .datoms
        .iter()
        .filter(|datom| schema.attribute(&datom.attribute).is_none())
        .cloned()
        .collect::<Vec<_>>();
    if unknown_datoms.is_empty() {
        return Ok(active);
    }
    let inferred = infer_legacy_schema(&unknown_datoms)?;
    let mut changed = false;
    for candidate in inferred.attributes.values() {
        match schema.attribute(&candidate.id) {
            Some(existing)
                if existing.class == candidate.class
                    && existing.value_type == candidate.value_type => {}
            Some(_) => return Err(AdmissionError::LegacyInferenceConflict(candidate.id.0)),
            None => {
                schema.register_attribute(candidate.clone())?;
                changed = true;
            }
        }
    }
    if !changed {
        return Ok(active);
    }
    schema.version = format!("legacy-inferred-v{}", journal.schema_revisions()?.len() + 1);
    let candidate = NamespaceSchemaRevision {
        schema_ref: schema_ref(&schema)?,
        schema: schema.clone(),
        predecessor: Some(active.schema_ref.clone()),
        compatibility: SchemaCompatibility::LegacyInferred,
        status: SchemaStatus::Registered,
    };
    validate_append(&journal.history()?, &candidate, request)?;
    let registered = register_schema(
        journal,
        RegisterSchemaRequest {
            schema,
            predecessor: Some(active.schema_ref.clone()),
            compatibility: SchemaCompatibility::LegacyInferred,
        },
    )?;
    activate_schema(
        journal,
        ActivateSchemaRequest {
            schema_ref: registered.schema_ref.clone(),
            expected_active: Some(active.schema_ref),
        },
    )
}

pub(crate) fn validate_append(
    history: &[Datom],
    active: &NamespaceSchemaRevision,
    request: &AppendAdmissionRequest,
) -> Result<ValidatedAppendBatch, AdmissionError> {
    let implicit = request.schema_ref.is_none();
    if let Some(provided) = &request.schema_ref {
        if provided != &active.schema_ref {
            return Err(AdmissionError::SchemaMismatch {
                expected: active.schema_ref.clone(),
                provided: Some(provided.clone()),
            });
        }
    }
    let mut elements = history
        .iter()
        .map(|datom| datom.element)
        .collect::<BTreeSet<_>>();
    for datom in &request.datoms {
        if !elements.insert(datom.element) {
            return Err(AdmissionError::DuplicateElement(datom.element));
        }
        let attribute = active
            .schema
            .attribute(&datom.attribute)
            .ok_or(AdmissionError::UnknownAttribute(datom.attribute.0))?;
        if !value_matches(&datom.value, &attribute.value_type) {
            return Err(AdmissionError::ValueTypeMismatch {
                attribute: datom.attribute.0,
                expected: attribute.value_type.clone(),
            });
        }
        if !operation_matches(attribute.class, datom.op) {
            return Err(AdmissionError::InvalidOperation {
                attribute: datom.attribute.0,
                class: attribute.class,
                operation: datom.op,
            });
        }
        if !datom.provenance.confidence.is_finite()
            || !(0.0..=1.0).contains(&datom.provenance.confidence)
        {
            return Err(AdmissionError::InvalidConfidence(datom.element));
        }
        if !implicit && datom.provenance.schema_version != active.schema_ref.version {
            return Err(AdmissionError::MissingSchemaIdentity(datom.element));
        }
        if !implicit
            && (datom.provenance.author_principal.trim().is_empty()
                || datom.provenance.tool_id.trim().is_empty()
                || datom.provenance.session_id.trim().is_empty()
                || datom.provenance.trust_domain.trim().is_empty())
        {
            return Err(AdmissionError::MissingRequiredProvenance(datom.element));
        }
    }
    let mut combined = history.to_vec();
    combined.extend_from_slice(&request.datoms);
    let certification = certify_history_dependencies(&combined);
    if !certification.is_valid() {
        return Err(AdmissionError::DependencyViolation(serde_json::to_string(
            &certification.violations,
        )?));
    }
    MaterializedResolver.current(&active.schema, &combined)?;
    Ok(ValidatedAppendBatch {
        datoms: request.datoms.clone(),
        schema_ref: active.schema_ref.clone(),
        batch_digest: ContentDigest(digest_json(&request.datoms)?),
        schema_ref_was_implicit: implicit,
    })
}

pub(crate) fn commit_append(
    journal: &mut dyn Journal,
    expected: &JournalCutRef,
    request: &AppendAdmissionRequest,
    batch: ValidatedAppendBatch,
    principal: &str,
) -> Result<AppendReceipt, AdmissionError> {
    let draft = AppendReceiptDraft {
        batch_id: random_id(),
        schema_version: batch.schema_ref.version.clone(),
        schema_digest: batch.schema_ref.digest.0.clone(),
        batch_digest: batch.batch_digest.0.clone(),
        principal: principal.into(),
        admission_engine_version: ADMISSION_ENGINE_VERSION.into(),
        idempotency_key: request.idempotency_key.clone(),
        schema_ref_was_implicit: batch.schema_ref_was_implicit,
    };
    let outcome = journal.append_if_cut(expected, batch.datoms(), &draft)?;
    receipt_from_storage(outcome, batch.schema_ref_was_implicit)
}

pub(crate) fn receipt_from_storage(
    outcome: ConditionalAppend,
    implicit: bool,
) -> Result<AppendReceipt, AdmissionError> {
    let StoredAppendReceipt {
        draft,
        prior_cut,
        committed_cut,
        appended,
    } = outcome.receipt;
    Ok(AppendReceipt {
        batch_id: BatchId(draft.batch_id),
        schema_ref: SchemaRef {
            version: draft.schema_version,
            digest: ContentDigest(draft.schema_digest),
        },
        prior_cut,
        committed_cut,
        batch_digest: ContentDigest(draft.batch_digest),
        appended,
        idempotent_replay: outcome.idempotent_replay,
        schema_ref_was_implicit: implicit || draft.schema_ref_was_implicit,
    })
}

pub(crate) fn resolve_idempotent_append(
    journal: &dyn Journal,
    request: &AppendAdmissionRequest,
) -> Result<Option<AppendReceipt>, AdmissionError> {
    let Some(key) = &request.idempotency_key else {
        return Ok(None);
    };
    let Some(receipt) = journal
        .append_receipts()?
        .into_iter()
        .find(|receipt| receipt.draft.idempotency_key.as_ref() == Some(key))
    else {
        return Ok(None);
    };
    let batch_digest = digest_json(&request.datoms)?;
    let schema_matches = request.schema_ref.as_ref().map_or(true, |reference| {
        reference.digest.0 == receipt.draft.schema_digest
    });
    if batch_digest != receipt.draft.batch_digest || !schema_matches {
        return Err(aether_storage::JournalError::IdempotencyConflict(key.clone()).into());
    }
    Ok(Some(receipt_from_storage(
        ConditionalAppend {
            receipt,
            idempotent_replay: true,
        },
        request.schema_ref.is_none(),
    )?))
}

pub(crate) fn append_receipts(journal: &dyn Journal) -> Result<Vec<AppendReceipt>, AdmissionError> {
    journal
        .append_receipts()?
        .into_iter()
        .map(|receipt| {
            let implicit = receipt.draft.schema_ref_was_implicit;
            receipt_from_storage(
                ConditionalAppend {
                    receipt,
                    idempotent_replay: false,
                },
                implicit,
            )
        })
        .collect()
}

pub(crate) fn replicate_append_receipt(
    journal: &mut dyn Journal,
    revision: &NamespaceSchemaRevision,
    leader_receipt: &AppendReceipt,
    datoms: Vec<Datom>,
) -> Result<AppendReceipt, AdmissionError> {
    let catalog = schema_catalog(journal)?;
    if !catalog
        .revisions
        .iter()
        .any(|known| known.schema_ref == revision.schema_ref)
    {
        register_schema(
            journal,
            RegisterSchemaRequest {
                schema: revision.schema.clone(),
                predecessor: revision.predecessor.clone(),
                compatibility: revision.compatibility,
            },
        )?;
    }
    let active = schema_catalog(journal)?.active;
    if active.as_ref().map(|active| &active.schema_ref) != Some(&revision.schema_ref) {
        activate_schema(
            journal,
            ActivateSchemaRequest {
                schema_ref: revision.schema_ref.clone(),
                expected_active: active.map(|active| active.schema_ref),
            },
        )?;
    }
    let request = AppendAdmissionRequest {
        schema_ref: (!leader_receipt.schema_ref_was_implicit)
            .then(|| leader_receipt.schema_ref.clone()),
        expected_cut: Some(leader_receipt.prior_cut.clone()),
        idempotency_key: Some(format!("replication:{}", leader_receipt.batch_id.0)),
        datoms,
        principal: Some("partition-leader-replication".into()),
    };
    let batch = validate_append(&journal.history()?, revision, &request)?;
    if batch.batch_digest != leader_receipt.batch_digest {
        return Err(AdmissionError::ReplicationReceiptMismatch);
    }
    let draft = AppendReceiptDraft {
        batch_id: leader_receipt.batch_id.0.clone(),
        schema_version: leader_receipt.schema_ref.version.clone(),
        schema_digest: leader_receipt.schema_ref.digest.0.clone(),
        batch_digest: leader_receipt.batch_digest.0.clone(),
        principal: "partition-leader-replication".into(),
        admission_engine_version: ADMISSION_ENGINE_VERSION.into(),
        idempotency_key: request.idempotency_key.clone(),
        schema_ref_was_implicit: leader_receipt.schema_ref_was_implicit,
    };
    let outcome = journal.append_if_cut(&leader_receipt.prior_cut, batch.datoms(), &draft)?;
    let replicated = receipt_from_storage(outcome, leader_receipt.schema_ref_was_implicit)?;
    if replicated.batch_id != leader_receipt.batch_id
        || replicated.schema_ref != leader_receipt.schema_ref
        || replicated.prior_cut != leader_receipt.prior_cut
        || replicated.committed_cut != leader_receipt.committed_cut
        || replicated.batch_digest != leader_receipt.batch_digest
        || replicated.appended != leader_receipt.appended
    {
        return Err(AdmissionError::ReplicationReceiptMismatch);
    }
    Ok(replicated)
}

pub(crate) fn document_schema_compatible(
    active: &NamespaceSchemaRevision,
    document: &Schema,
) -> Result<(), AdmissionError> {
    if active.compatibility != SchemaCompatibility::LegacyInferred
        && active.schema.attributes.len() != document.attributes.len()
    {
        return Err(AdmissionError::DocumentSchemaMismatch(
            "document attribute set does not equal the active namespace schema".into(),
        ));
    }
    for active_attribute in active.schema.attributes.values() {
        let Some(candidate) = document.attribute(&active_attribute.id) else {
            return Err(AdmissionError::DocumentSchemaMismatch(format!(
                "document omitted active attribute {}",
                active_attribute.id.0
            )));
        };
        let name_matches = candidate.name == active_attribute.name
            || active.compatibility == SchemaCompatibility::LegacyInferred;
        if !name_matches
            || candidate.class != active_attribute.class
            || candidate.value_type != active_attribute.value_type
        {
            return Err(AdmissionError::DocumentSchemaMismatch(format!(
                "document redefined active attribute {}",
                active_attribute.id.0
            )));
        }
    }
    Ok(())
}

fn infer_legacy_schema(datoms: &[Datom]) -> Result<Schema, AdmissionError> {
    if datoms.is_empty() {
        return Err(AdmissionError::NoActiveSchema);
    }
    let mut grouped = BTreeMap::<u64, Vec<&Datom>>::new();
    for datom in datoms {
        grouped.entry(datom.attribute.0).or_default().push(datom);
    }
    let mut attributes = BTreeMap::<u64, (AttributeClass, ValueType)>::new();
    for (attribute, datoms) in grouped {
        let value_type = infer_value_type(&datoms[0].value)?;
        if !datoms
            .iter()
            .all(|datom| value_matches(&datom.value, &value_type))
        {
            return Err(AdmissionError::LegacyInferenceConflict(attribute));
        }
        let class = if datoms
            .iter()
            .any(|datom| datom.op == OperationKind::InsertAfter)
        {
            AttributeClass::SequenceRga
        } else if datoms
            .iter()
            .any(|datom| matches!(datom.op, OperationKind::Add | OperationKind::Remove))
        {
            if value_type == ValueType::Entity {
                AttributeClass::RefSet
            } else {
                AttributeClass::SetAddWins
            }
        } else if value_type == ValueType::Entity {
            AttributeClass::RefScalar
        } else {
            AttributeClass::ScalarLww
        };
        attributes.insert(attribute, (class, value_type));
    }
    let mut schema = Schema::new("legacy-inferred-v1");
    for (id, (class, value_type)) in attributes {
        schema.register_attribute(AttributeSchema {
            id: aether_ast::AttributeId::new(id),
            name: format!("attribute_{id}"),
            class,
            value_type,
        })?;
    }
    Ok(schema)
}

fn infer_value_type(value: &Value) -> Result<ValueType, AdmissionError> {
    Ok(match value {
        Value::Null => return Err(AdmissionError::CannotInferNull),
        Value::Bool(_) => ValueType::Bool,
        Value::I64(_) => ValueType::I64,
        Value::U64(_) => ValueType::U64,
        Value::F64(value) if value.is_finite() => ValueType::F64,
        Value::F64(_) => return Err(AdmissionError::NonFiniteValue),
        Value::String(_) => ValueType::String,
        Value::Bytes(_) => ValueType::Bytes,
        Value::Entity(_) => ValueType::Entity,
        Value::List(values) => {
            let first = values.first().ok_or(AdmissionError::CannotInferEmptyList)?;
            let element_type = infer_value_type(first)?;
            if !values
                .iter()
                .all(|value| value_matches(value, &element_type))
            {
                return Err(AdmissionError::HeterogeneousList);
            }
            ValueType::List(Box::new(element_type))
        }
    })
}

fn value_matches(value: &Value, expected: &ValueType) -> bool {
    match (value, expected) {
        (Value::Bool(_), ValueType::Bool)
        | (Value::I64(_), ValueType::I64)
        | (Value::U64(_), ValueType::U64)
        | (Value::String(_), ValueType::String)
        | (Value::Bytes(_), ValueType::Bytes)
        | (Value::Entity(_), ValueType::Entity) => true,
        (Value::F64(value), ValueType::F64) => value.is_finite(),
        (Value::List(values), ValueType::List(element)) => {
            values.iter().all(|value| value_matches(value, element))
        }
        _ => false,
    }
}

fn operation_matches(class: AttributeClass, operation: OperationKind) -> bool {
    match class {
        AttributeClass::ScalarLww | AttributeClass::RefScalar => matches!(
            operation,
            OperationKind::Assert
                | OperationKind::Claim
                | OperationKind::LeaseOpen
                | OperationKind::LeaseRenew
                | OperationKind::Annotate
                | OperationKind::Retract
                | OperationKind::Release
                | OperationKind::LeaseExpire
        ),
        AttributeClass::SetAddWins | AttributeClass::RefSet => matches!(
            operation,
            OperationKind::Add
                | OperationKind::Claim
                | OperationKind::Annotate
                | OperationKind::Remove
                | OperationKind::Release
                | OperationKind::LeaseExpire
                | OperationKind::Retract
        ),
        AttributeClass::SequenceRga => matches!(
            operation,
            OperationKind::InsertAfter | OperationKind::Remove | OperationKind::Retract
        ),
    }
}

fn digest_schema(schema: &Schema) -> Result<String, AdmissionError> {
    let mut attributes = schema.attributes.values().cloned().collect::<Vec<_>>();
    attributes.sort_by_key(|attribute| attribute.id);
    let material = (&schema.version, attributes);
    digest_json(&material)
}

fn digest_json<T: Serialize + ?Sized>(value: &T) -> Result<String, AdmissionError> {
    let encoded = serde_json::to_vec(value)?;
    let mut hasher = Sha256::new();
    hasher.update(b"aether-admission-canonical-v1");
    hasher.update((encoded.len() as u64).to_be_bytes());
    hasher.update(encoded);
    Ok(hex_encode(&hasher.finalize()))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn random_id() -> String {
    let mut bytes = [0u8; 16];
    OsRng.fill_bytes(&mut bytes);
    hex_encode(&bytes)
}

fn encode_revision(
    revision: &NamespaceSchemaRevision,
) -> Result<StoredSchemaRevision, AdmissionError> {
    Ok(StoredSchemaRevision {
        version: revision.schema_ref.version.clone(),
        digest: revision.schema_ref.digest.0.clone(),
        schema_json: serde_json::to_string(&revision.schema)?,
        predecessor_digest: revision
            .predecessor
            .as_ref()
            .map(|reference| reference.digest.0.clone()),
        predecessor_version: revision
            .predecessor
            .as_ref()
            .map(|reference| reference.version.clone()),
        compatibility: serde_json::to_string(&revision.compatibility)?,
        status: serde_json::to_string(&revision.status)?,
    })
}

fn decode_revision(
    revision: StoredSchemaRevision,
) -> Result<NamespaceSchemaRevision, AdmissionError> {
    Ok(NamespaceSchemaRevision {
        schema_ref: SchemaRef {
            version: revision.version,
            digest: ContentDigest(revision.digest),
        },
        schema: serde_json::from_str(&revision.schema_json)?,
        predecessor: revision.predecessor_digest.map(|digest| SchemaRef {
            version: revision.predecessor_version.unwrap_or_default(),
            digest: ContentDigest(digest),
        }),
        compatibility: serde_json::from_str(&revision.compatibility)?,
        status: serde_json::from_str(&revision.status)?,
    })
}

fn encode_certification(
    certification: &SchemaBaselineReceipt,
) -> Result<StoredHistoryCertification, AdmissionError> {
    Ok(StoredHistoryCertification {
        schema_version: certification.schema_ref.version.clone(),
        schema_digest: certification.schema_ref.digest.0.clone(),
        cut: certification.cut.clone(),
        status: serde_json::to_string(&certification.status)?,
        validation_engine_version: certification.validation_engine_version.clone(),
        diagnostics: certification.diagnostics.clone(),
        migration_manifest_json: certification.migration_manifest_json.clone(),
    })
}

fn decode_certification(
    certification: StoredHistoryCertification,
) -> Result<SchemaBaselineReceipt, AdmissionError> {
    Ok(SchemaBaselineReceipt {
        schema_ref: SchemaRef {
            version: certification.schema_version,
            digest: ContentDigest(certification.schema_digest),
        },
        cut: certification.cut,
        status: serde_json::from_str(&certification.status)?,
        validation_engine_version: certification.validation_engine_version,
        diagnostics: certification.diagnostics,
        migration_manifest_json: certification.migration_manifest_json,
    })
}

#[derive(Debug, Error)]
pub enum AdmissionError {
    #[error("no active namespace schema is registered")]
    NoActiveSchema,
    #[error("unknown schema {0:?}")]
    UnknownSchema(SchemaRef),
    #[error("schema mismatch: expected {expected:?}, provided {provided:?}")]
    SchemaMismatch {
        expected: SchemaRef,
        provided: Option<SchemaRef>,
    },
    #[error("unknown attribute {0}")]
    UnknownAttribute(u64),
    #[error("attribute {attribute} does not match value type {expected:?}")]
    ValueTypeMismatch { attribute: u64, expected: ValueType },
    #[error("operation {operation:?} is invalid for attribute {attribute} with class {class:?}")]
    InvalidOperation {
        attribute: u64,
        class: AttributeClass,
        operation: OperationKind,
    },
    #[error("duplicate element id {0}")]
    DuplicateElement(ElementId),
    #[error("invalid confidence on element {0}")]
    InvalidConfidence(ElementId),
    #[error("element {0} is missing required provenance")]
    MissingRequiredProvenance(ElementId),
    #[error("element {0} does not bind the active schema version")]
    MissingSchemaIdentity(ElementId),
    #[error("journal dependency closure failed: {0}")]
    DependencyViolation(String),
    #[error("document schema is incompatible with the active namespace schema: {0}")]
    DocumentSchemaMismatch(String),
    #[error("schema activation does not name the registered predecessor as its expected active revision")]
    SchemaActivationPrecondition,
    #[error("legacy schema inference conflicted for attribute {0}")]
    LegacyInferenceConflict(u64),
    #[error("cannot infer a value type from null")]
    CannotInferNull,
    #[error("cannot infer a value type from an empty list")]
    CannotInferEmptyList,
    #[error("list values must have one recursive value type")]
    HeterogeneousList,
    #[error("floating-point values must be finite")]
    NonFiniteValue,
    #[error("partition follower receipt did not match the leader admission receipt")]
    ReplicationReceiptMismatch,
    #[error("existing journal generation is quarantined: {0}")]
    ExistingHistoryQuarantined(String),
    #[error(transparent)]
    Resolve(#[from] aether_resolver::ResolveError),
    #[error(transparent)]
    Schema(#[from] aether_schema::SchemaError),
    #[error(transparent)]
    Storage(#[from] aether_storage::JournalError),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}
