use aether_api::{
    AppendRequest, ArtifactReference, GetArtifactReferenceRequest, HistoryRequest,
    InMemoryKernelService, KernelService, RegisterArtifactReferenceRequest,
    RegisterVectorRecordRequest, ResolveTraceHandleRequest, RunDocumentRequest,
    SearchVectorsRequest, SearchVectorsResponse, VectorFactProjection, VectorMetric,
    VectorRecordMetadata, VectorSearchMatch,
};
use aether_ast::{
    AttributeId, Datom, DatomProvenance, DerivationTrace, ElementId, EntityId, ExtensionalFact,
    OperationKind, PredicateId, PredicateRef, QueryRow, ReplicaId, Value,
};
use std::collections::BTreeMap;

const BASELINE_CASES: usize = 1;
const BASELINE_RESOLUTIONS: usize = 2;
const BASELINE_EVIDENCE_RECORDS: usize = 1;
const BASELINE_ASSIGNMENT_ATTEMPTS: usize = 4;
const BASELINE_JOURNAL_DATOMS: usize = 24;
const SCALED_CASES: usize = 13;
const DATOMS_PER_SCALED_CASE: usize = 21;
const CATALOG_ANCHOR_DATOMS_PER_SCALED_CASE: usize = 2;

#[derive(Clone, Copy)]
struct SupportCaseSpec {
    case_id: u64,
    step_id: u64,
    preferred_resolution_id: u64,
    fallback_resolution_id: u64,
    evidence_id: u64,
    subject: &'static str,
    preferred_title: &'static str,
    fallback_title: &'static str,
    priority: &'static str,
    channel: &'static str,
    current_owner: &'static str,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let case_specs = support_case_specs();
    let mut service = InMemoryKernelService::new();
    service.append(AppendRequest {
        datoms: support_case_history(),
    })?;

    service.register_artifact_reference(RegisterArtifactReferenceRequest {
        reference: ArtifactReference {
            sidecar_id: "support-memory".into(),
            artifact_id: "kb-apply-credit".into(),
            entity: EntityId::new(9_101),
            uri: "s3://aether/support/runbooks/migration-credit.md".into(),
            media_type: "text/markdown".into(),
            byte_length: 1_824,
            digest: Some("sha256:kb-apply-credit".into()),
            metadata: BTreeMap::from([
                ("kind".into(), Value::String("runbook".into())),
                (
                    "title".into(),
                    Value::String("Apply migration credit".into()),
                ),
            ]),
            provenance: DatomProvenance::default(),
            policy: None,
            registered_at: ElementId::new(19),
        },
    })?;

    service.append(AppendRequest {
        datoms: vec![annotate_string_datom(
            501,
            16,
            "support-memory-anchor-2",
            20,
        )],
    })?;

    service.register_vector_record(RegisterVectorRecordRequest {
        record: VectorRecordMetadata {
            sidecar_id: "support-memory".into(),
            vector_id: "vec-apply-credit".into(),
            entity: EntityId::new(9_101),
            source_artifact_id: Some("kb-apply-credit".into()),
            embedding_ref: "s3://aether/support/vectors/vec-apply-credit.bin".into(),
            dimensions: SCALED_CASES,
            metric: VectorMetric::Cosine,
            metadata: BTreeMap::from([("topic".into(), Value::String("migration-credit".into()))]),
            provenance: DatomProvenance::default(),
            policy: None,
            registered_at: ElementId::new(20),
        },
        embedding: one_hot_embedding(0),
    })?;

    service.append(AppendRequest {
        datoms: support_handoff_history(),
    })?;

    service.append(AppendRequest {
        datoms: scaled_support_case_history(&case_specs[1..]),
    })?;
    register_scaled_support_memory(&mut service, &case_specs[1..])?;

    let searches = search_support_memory(&mut service, &case_specs)?;
    let search = &searches[0];
    let projected_facts = searches
        .iter()
        .flat_map(|search| search.facts.iter().cloned())
        .collect::<Vec<_>>();

    let evidence_artifact = service.get_artifact_reference(GetArtifactReferenceRequest {
        sidecar_id: "support-memory".into(),
        artifact_id: "kb-apply-credit".into(),
        policy_context: None,
    })?;

    println!("AETHER Demo 05: AI Support Resolution Desk");
    println!("==========================================");
    println!();
    println!("This is the flagship ML-facing AETHER application pack:");
    println!("  - one governed support desk for agents, retrieval, and human leads");
    println!("  - customer issues, evidence, candidate resolutions, and assignments");
    println!("  - one resolution derived as truly ready");
    println!("  - controlled handoff through live assignment authority");
    println!("  - temporal replay plus a proof trace for the chosen path");
    println!();
    let history = service
        .history(HistoryRequest {
            policy_context: None,
        })?
        .datoms;
    print_scale_contract(&case_specs, history.len(), projected_facts.len())?;
    println!();
    println!("Published support-case history (hero case drill-down):");
    for datom in history.iter().filter(|datom| is_hero_datom(datom)) {
        println!("  - {}", describe_datom(datom));
    }
    println!(
        "  - ... {} additional governed journal datoms remain queryable without flooding the console",
        history
            .len()
            .saturating_sub(history.iter().filter(|datom| is_hero_datom(datom)).count())
    );

    let active_cases = service.run_document(RunDocumentRequest {
        dsl: support_dsl(
            "current",
            "goal active_case(case, subject, priority, channel)\n  keep case, subject, priority, channel",
            projected_facts.as_slice(),
            &case_specs,
        ),
        policy_context: None,
    })?;
    print_section(
        "Act I: Active support cases on the desk (Current)",
        active_cases
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "The case desk starts with the customer issue, not with infrastructure jargon.",
        5,
    );
    print_evidence_section(
        "Retrieved evidence from the support-memory sidecar",
        search.matches.as_slice(),
        evidence_artifact.reference.uri.as_str(),
    );

    let candidate_resolutions = service.run_document(RunDocumentRequest {
        dsl: support_dsl(
            "current",
            "goal resolution_board(resolution, title, approval, suppression, confidence)\n  keep resolution, title, approval, suppression, confidence",
            projected_facts.as_slice(),
            &case_specs,
        ),
        policy_context: None,
    })?;
    print_section(
        "Published candidate resolutions (Current)",
        candidate_resolutions
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "One path is clear once evidence, approval, and dependencies line up. The fallback path stays visibly suppressed.",
        6,
    );

    let ready_resolution = service.run_document(RunDocumentRequest {
        dsl: support_dsl(
            "as_of e20",
            "goal ready_resolution_detail(case, subject, resolution, title)\n  keep case, subject, resolution, title",
            projected_facts.as_slice(),
            &case_specs,
        ),
        policy_context: None,
    })?;
    print_section(
        "Act II: Which resolution is actually ready? (AsOf e20)",
        ready_resolution
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "Retrieved evidence, approval, confidence, suppression, and dependency state all have to line up before the desk marks a path as ready.",
        5,
    );

    let current_selection = service.run_document(RunDocumentRequest {
        dsl: support_dsl(
            "current",
            "goal case_resolution_selected_detail(case, subject, title, owner, epoch)\n  keep case, subject, title, owner, epoch",
            projected_facts.as_slice(),
            &case_specs,
        ),
        policy_context: None,
    })?;
    let selected_rows = current_selection
        .query
        .as_ref()
        .expect("query should exist")
        .rows
        .clone();
    print_section(
        "Act III: Who owns the case now? (Current)",
        selected_rows.as_slice(),
        "The desk now names a single current owner for the selected resolution because assignment authority has advanced to the live holder.",
        5,
    );

    let before_handoff = service.run_document(RunDocumentRequest {
        dsl: support_dsl(
            "as_of e23",
            "goal case_resolution_selected_detail(case, subject, title, owner, epoch)\n  keep case, subject, title, owner, epoch",
            projected_facts.as_slice(),
            &case_specs,
        ),
        policy_context: None,
    })?;
    print_section(
        "Act IV: The same case before the handoff (AsOf e23)",
        before_handoff
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "Replay shows that ownership really did change. This is a semantic handoff, not a dashboard illusion.",
        5,
    );

    let stale_recommendations = service.run_document(RunDocumentRequest {
        dsl: support_dsl(
            "current",
            "goal stale_assignment_attempt_detail(case, subject, owner, epoch)\n  keep case, subject, owner, epoch",
            projected_facts.as_slice(),
            &case_specs,
        ),
        policy_context: None,
    })?;
    print_section(
        "Fenced stale recommendations at Current",
        stale_recommendations
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "AETHER keeps assignment history, but it still distinguishes what merely happened from what is semantically valid now.",
        6,
    );

    assert_scaled_semantics(
        active_cases
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .len(),
        candidate_resolutions
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .len(),
        selected_rows.len(),
        stale_recommendations
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .len(),
    )?;

    if let Some(tuple_id) = selected_rows
        .iter()
        .find(|row| {
            row.values.first() == Some(&Value::Entity(EntityId::new(case_specs[0].case_id)))
        })
        .and_then(|row| row.tuple_id)
    {
        let handle = current_selection
            .execution
            .as_ref()
            .and_then(|receipt| {
                receipt
                    .trace_handles
                    .iter()
                    .find(|binding| binding.local_tuple_id == tuple_id)
            })
            .expect("selected row should carry a trace handle")
            .handle
            .clone();
        let trace = service
            .resolve_trace_handle(ResolveTraceHandleRequest {
                handle,
                policy_context: None,
                verify_replay: true,
            })?
            .record
            .trace;
        print_trace_summary(&trace);
    }

    println!();
    println!("Bottom line:");
    println!("  - support cases, retrieved evidence, candidate resolutions, and ownership all live in one fabric");
    println!("  - Current tells the operator which resolution is selected and who owns it now");
    println!("  - AsOf shows what changed across the handoff");
    println!("  - the proof trace preserves why the current answer is true");

    Ok(())
}

fn support_case_specs() -> Vec<SupportCaseSpec> {
    let mut specs = vec![SupportCaseSpec {
        case_id: 501,
        step_id: 801,
        preferred_resolution_id: 901,
        fallback_resolution_id: 902,
        evidence_id: 9_101,
        subject: "duplicate charge after plan migration",
        preferred_title: "apply-migration-credit",
        fallback_title: "escalate-to-billing-specialist",
        priority: "high",
        channel: "chat",
        current_owner: "lead-ana",
    }];
    let scenarios = [
        (
            "customer trapped in password reset loop",
            "invalidate-reset-session",
            "escalate-to-identity-specialist",
            "high",
            "email",
            "lead-marcus",
        ),
        (
            "invoice missing tax registration details",
            "regenerate-compliant-invoice",
            "escalate-to-billing-operations",
            "medium",
            "chat",
            "specialist-nia",
        ),
        (
            "shipment address locked after checkout",
            "apply-carrier-address-correction",
            "escalate-to-fulfillment-lead",
            "high",
            "phone",
            "lead-ana",
        ),
        (
            "workspace entitlement missing after renewal",
            "reconcile-workspace-entitlements",
            "escalate-to-subscription-operations",
            "high",
            "chat",
            "lead-marcus",
        ),
        (
            "data export remains queued overnight",
            "restart-governed-export-job",
            "escalate-to-data-operations",
            "medium",
            "email",
            "specialist-nia",
        ),
        (
            "approved refund has not reached card",
            "reconcile-refund-settlement",
            "escalate-to-payments-specialist",
            "high",
            "phone",
            "lead-ana",
        ),
        (
            "team invitation resolves to expired tenant",
            "reissue-tenant-bound-invitation",
            "escalate-to-identity-specialist",
            "medium",
            "chat",
            "lead-marcus",
        ),
        (
            "regional tax rate changed mid-cycle",
            "apply-tax-adjustment-credit",
            "escalate-to-tax-operations",
            "high",
            "email",
            "specialist-nia",
        ),
        (
            "webhook retries exhausted after outage",
            "replay-idempotent-webhook-batch",
            "escalate-to-integration-engineering",
            "high",
            "chat",
            "lead-ana",
        ),
        (
            "subscription pause ignored billing date",
            "correct-pause-effective-date",
            "escalate-to-billing-operations",
            "medium",
            "phone",
            "lead-marcus",
        ),
        (
            "sso certificate rotation rejected metadata",
            "restore-validated-sso-metadata",
            "escalate-to-identity-engineering",
            "critical",
            "chat",
            "specialist-nia",
        ),
        (
            "api quota remains stale after upgrade",
            "reconcile-api-quota-entitlement",
            "escalate-to-platform-operations",
            "high",
            "email",
            "lead-ana",
        ),
    ];

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let offset = index as u64 + 1;
        specs.push(SupportCaseSpec {
            case_id: 501 + offset,
            step_id: 801 + offset,
            preferred_resolution_id: 901 + offset * 2,
            fallback_resolution_id: 902 + offset * 2,
            evidence_id: 9_101 + offset,
            subject: scenario.0,
            preferred_title: scenario.1,
            fallback_title: scenario.2,
            priority: scenario.3,
            channel: scenario.4,
            current_owner: scenario.5,
        });
    }
    specs
}

fn register_scaled_support_memory(
    service: &mut InMemoryKernelService,
    specs: &[SupportCaseSpec],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut next_element = scaled_history_tail_element().0 + 1;
    for (index, spec) in specs.iter().enumerate() {
        service.append(AppendRequest {
            datoms: vec![annotate_string_datom(
                spec.case_id,
                16,
                &format!("artifact-catalog-anchor-{}", spec.case_id),
                next_element,
            )],
        })?;
        let artifact_registered_at = ElementId::new(next_element);
        next_element += 1;
        let artifact_id = format!("kb-{}", spec.preferred_title);
        service.register_artifact_reference(RegisterArtifactReferenceRequest {
            reference: ArtifactReference {
                sidecar_id: "support-memory".into(),
                artifact_id: artifact_id.clone(),
                entity: EntityId::new(spec.evidence_id),
                uri: format!("s3://aether/support/runbooks/{}.md", spec.preferred_title),
                media_type: "text/markdown".into(),
                byte_length: 1_500 + index as u64 * 37,
                digest: Some(format!("sha256:{artifact_id}")),
                metadata: BTreeMap::from([
                    ("kind".into(), Value::String("runbook".into())),
                    ("title".into(), Value::String(spec.preferred_title.into())),
                ]),
                provenance: DatomProvenance::default(),
                policy: None,
                registered_at: artifact_registered_at,
            },
        })?;
        service.append(AppendRequest {
            datoms: vec![annotate_string_datom(
                spec.case_id,
                16,
                &format!("vector-catalog-anchor-{}", spec.case_id),
                next_element,
            )],
        })?;
        let vector_registered_at = ElementId::new(next_element);
        next_element += 1;
        service.register_vector_record(RegisterVectorRecordRequest {
            record: VectorRecordMetadata {
                sidecar_id: "support-memory".into(),
                vector_id: format!("vec-{}", spec.preferred_title),
                entity: EntityId::new(spec.evidence_id),
                source_artifact_id: Some(artifact_id),
                embedding_ref: format!("s3://aether/support/vectors/{}.bin", spec.preferred_title),
                dimensions: SCALED_CASES,
                metric: VectorMetric::Cosine,
                metadata: BTreeMap::from([(
                    "topic".into(),
                    Value::String(spec.preferred_title.into()),
                )]),
                provenance: DatomProvenance::default(),
                policy: None,
                registered_at: vector_registered_at,
            },
            embedding: one_hot_embedding(index + 1),
        })?;
    }
    Ok(())
}

fn search_support_memory(
    service: &mut InMemoryKernelService,
    specs: &[SupportCaseSpec],
) -> Result<Vec<SearchVectorsResponse>, Box<dyn std::error::Error>> {
    specs
        .iter()
        .enumerate()
        .map(|(index, spec)| {
            let as_of = if index == 0 {
                ElementId::new(20)
            } else {
                scaled_catalog_tail_element()
            };
            Ok(service.search_vectors(SearchVectorsRequest {
                sidecar_id: "support-memory".into(),
                query_embedding: one_hot_embedding(index),
                top_k: 1,
                metric: VectorMetric::Cosine,
                as_of: Some(as_of),
                projection: Some(VectorFactProjection {
                    predicate: PredicateRef {
                        id: PredicateId::new(81),
                        name: "retrieved_support_evidence".into(),
                        arity: 3,
                    },
                    query_entity: EntityId::new(spec.case_id),
                }),
                policy_context: None,
            })?)
        })
        .collect()
}

fn one_hot_embedding(index: usize) -> Vec<f32> {
    let mut embedding = vec![0.0; SCALED_CASES];
    embedding[index] = 1.0;
    embedding
}

fn print_scale_contract(
    specs: &[SupportCaseSpec],
    journal_datoms: usize,
    evidence_records: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let resolutions = specs.len() * 2;
    let assignment_attempts = specs.len() * 4;
    let expected_datoms = BASELINE_JOURNAL_DATOMS
        + (specs.len() - BASELINE_CASES)
            * (DATOMS_PER_SCALED_CASE + CATALOG_ANCHOR_DATOMS_PER_SCALED_CASE);
    let scaled = specs.len() >= BASELINE_CASES * 10
        && resolutions >= BASELINE_RESOLUTIONS * 10
        && evidence_records >= BASELINE_EVIDENCE_RECORDS * 10
        && assignment_attempts >= BASELINE_ASSIGNMENT_ATTEMPTS * 10
        && journal_datoms >= BASELINE_JOURNAL_DATOMS * 10
        && journal_datoms == expected_datoms;
    if !scaled {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "scaled workload contract failed: cases={} resolutions={} evidence={} attempts={} datoms={} expected_datoms={expected_datoms}",
                specs.len(),
                resolutions,
                evidence_records,
                assignment_attempts,
                journal_datoms
            ),
        )
        .into());
    }

    println!("Scale contract: >=10x baseline");
    println!(
        "  - {} governed cases ({:.1}x)",
        specs.len(),
        specs.len() as f64 / BASELINE_CASES as f64
    );
    println!(
        "  - {resolutions} candidate resolutions ({:.1}x)",
        resolutions as f64 / BASELINE_RESOLUTIONS as f64
    );
    println!(
        "  - {evidence_records} governed evidence records ({:.1}x)",
        evidence_records as f64 / BASELINE_EVIDENCE_RECORDS as f64
    );
    println!(
        "  - {assignment_attempts} assignment attempts ({:.1}x)",
        assignment_attempts as f64 / BASELINE_ASSIGNMENT_ATTEMPTS as f64
    );
    println!(
        "  - {journal_datoms} journal datoms ({:.1}x)",
        journal_datoms as f64 / BASELINE_JOURNAL_DATOMS as f64
    );
    Ok(())
}

fn assert_scaled_semantics(
    active_cases: usize,
    candidate_resolutions: usize,
    selected_resolutions: usize,
    stale_attempts: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected_stale_attempts = SCALED_CASES * 3;
    if active_cases != SCALED_CASES
        || candidate_resolutions != SCALED_CASES * 2
        || selected_resolutions != SCALED_CASES
        || stale_attempts != expected_stale_attempts
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "scaled semantic outcomes failed: active={active_cases} candidates={candidate_resolutions} selected={selected_resolutions} stale={stale_attempts}"
            ),
        )
        .into());
    }
    println!();
    println!("Scaled semantic outcome");
    println!("-----------------------");
    println!("  - {active_cases} active cases");
    println!("  - {candidate_resolutions} published candidate resolutions");
    println!("  - {selected_resolutions} current selected resolutions");
    println!("  - {stale_attempts} stale assignment attempts fenced");
    Ok(())
}

fn is_hero_datom(datom: &Datom) -> bool {
    matches!(datom.entity.0, 501 | 801 | 901 | 902)
}

fn print_section(title: &str, rows: &[QueryRow], note: &str, max_rows: usize) {
    println!();
    println!("{title}");
    println!("{}", "-".repeat(title.len()));
    if rows.is_empty() {
        println!("  - none");
    } else {
        println!("  - rows: {}", rows.len());
        for row in rows.iter().take(max_rows) {
            println!("  - {}", format_values(&row.values));
        }
        if rows.len() > max_rows {
            println!("  - ... {} more rows", rows.len() - max_rows);
        }
    }
    println!("  {note}");
}

fn print_evidence_section(title: &str, matches: &[VectorSearchMatch], artifact_uri: &str) {
    println!();
    println!("{title}");
    println!("{}", "-".repeat(title.len()));
    if matches.is_empty() {
        println!("  - none");
    } else {
        for entry in matches {
            println!(
                "  - case/501 matched {} at score {:.4} via {}",
                describe_entity(entry.entity.0),
                entry.score,
                artifact_uri
            );
        }
    }
    println!(
        "  Retrieval is useful here because it re-enters the governed case desk as evidence rather than floating beside it."
    );
}

fn print_trace_summary(trace: &DerivationTrace) {
    println!();
    println!("Act V: Why the current selected resolution is true");
    println!("-----------------------------------------------");
    println!("  - root tuple: t{}", trace.root.0);
    println!("  - tuples in trace: {}", trace.tuples.len());
    for tuple in &trace.tuples {
        println!(
            "  - t{} via r{} -> {} | iteration {} | sources {}",
            tuple.tuple.id.0,
            tuple.metadata.rule_id.0,
            format_values(&tuple.tuple.values),
            tuple.metadata.iteration,
            format_elements(&tuple.metadata.source_datom_ids)
        );
    }
    println!(
        "  The chosen support path stays explainable after handoff, not only while the recommendation is being formed."
    );
}

fn describe_datom(datom: &Datom) -> String {
    let subject = describe_entity(datom.entity.0);
    match datom.attribute.0 {
        1 => format!(
            "e{}: {subject} subject = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        2 => format!(
            "e{}: {subject} priority = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        3 => format!(
            "e{}: {subject} channel = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        4 => format!(
            "e{}: {subject} step_title = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        5 => format!(
            "e{}: {subject} status = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        6 => format!(
            "e{}: {subject} title = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        7 => format!(
            "e{}: {subject} case = {}",
            datom.element.0,
            describe_entity(entity_value(&datom.value))
        ),
        8 => format!(
            "e{}: {subject} depends_on {}",
            datom.element.0,
            describe_entity(entity_value(&datom.value))
        ),
        9 => format!(
            "e{}: {subject} approval_state = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        10 => format!(
            "e{}: {subject} suppression_state = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        11 => format!(
            "e{}: {subject} confidence_band = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        12 => format!(
            "e{}: {subject} claimed_by = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        13 => format!(
            "e{}: {subject} lease_epoch = {}",
            datom.element.0,
            u64_value(&datom.value)
        ),
        14 => format!(
            "e{}: {subject} lease_state = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        15 => format!(
            "e{}: {subject} case_status = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        _ => format!(
            "e{}: {subject} -> {}",
            datom.element.0,
            format_value(&datom.value)
        ),
    }
}

fn support_case_history() -> Vec<Datom> {
    vec![
        assert_string_datom(501, 1, "duplicate charge after plan migration", 1),
        assert_string_datom(501, 2, "high", 2),
        assert_string_datom(501, 3, "chat", 3),
        assert_string_datom(801, 4, "collect billing timeline", 4),
        assert_string_datom(801, 5, "done", 5),
        assert_string_datom(901, 6, "apply-migration-credit", 6),
        ref_scalar_datom(901, 7, 501, 7),
        ref_set_datom(901, 8, 801, 8),
        assert_string_datom(901, 9, "approved", 9),
        assert_string_datom(901, 10, "clear", 10),
        assert_string_datom(901, 11, "high", 11),
        assert_string_datom(902, 6, "escalate-to-billing-specialist", 12),
        ref_scalar_datom(902, 7, 501, 13),
        assert_string_datom(902, 9, "approved", 14),
        assert_string_datom(902, 10, "suppressed", 15),
        assert_string_datom(902, 11, "medium", 16),
        assert_string_datom(501, 15, "open", 17),
        annotate_string_datom(501, 16, "support-memory-anchor-1", 19),
    ]
}

fn support_handoff_history() -> Vec<Datom> {
    vec![
        assert_string_datom(501, 12, "triage-agent", 21),
        assert_u64_datom(501, 13, 1, 22),
        assert_string_datom(501, 14, "active", 23),
        assert_string_datom(501, 12, "lead-ana", 24),
        assert_u64_datom(501, 13, 2, 25),
    ]
}

fn scaled_support_case_history(specs: &[SupportCaseSpec]) -> Vec<Datom> {
    let mut datoms = Vec::with_capacity(specs.len() * DATOMS_PER_SCALED_CASE);
    for (index, spec) in specs.iter().enumerate() {
        let mut element = 26 + index as u64 * DATOMS_PER_SCALED_CASE as u64;
        datoms.push(assert_string_datom(spec.case_id, 1, spec.subject, element));
        element += 1;
        datoms.push(assert_string_datom(spec.case_id, 2, spec.priority, element));
        element += 1;
        datoms.push(assert_string_datom(spec.case_id, 3, spec.channel, element));
        element += 1;
        datoms.push(assert_string_datom(
            spec.step_id,
            4,
            "complete case-specific verification",
            element,
        ));
        element += 1;
        datoms.push(assert_string_datom(spec.step_id, 5, "done", element));
        element += 1;
        datoms.push(assert_string_datom(
            spec.preferred_resolution_id,
            6,
            spec.preferred_title,
            element,
        ));
        element += 1;
        datoms.push(ref_scalar_datom(
            spec.preferred_resolution_id,
            7,
            spec.case_id,
            element,
        ));
        element += 1;
        datoms.push(ref_set_datom(
            spec.preferred_resolution_id,
            8,
            spec.step_id,
            element,
        ));
        element += 1;
        datoms.push(assert_string_datom(
            spec.preferred_resolution_id,
            9,
            "approved",
            element,
        ));
        element += 1;
        datoms.push(assert_string_datom(
            spec.preferred_resolution_id,
            10,
            "clear",
            element,
        ));
        element += 1;
        datoms.push(assert_string_datom(
            spec.preferred_resolution_id,
            11,
            "high",
            element,
        ));
        element += 1;
        datoms.push(assert_string_datom(
            spec.fallback_resolution_id,
            6,
            spec.fallback_title,
            element,
        ));
        element += 1;
        datoms.push(ref_scalar_datom(
            spec.fallback_resolution_id,
            7,
            spec.case_id,
            element,
        ));
        element += 1;
        datoms.push(assert_string_datom(
            spec.fallback_resolution_id,
            9,
            "approved",
            element,
        ));
        element += 1;
        datoms.push(assert_string_datom(
            spec.fallback_resolution_id,
            10,
            "suppressed",
            element,
        ));
        element += 1;
        datoms.push(assert_string_datom(
            spec.fallback_resolution_id,
            11,
            "medium",
            element,
        ));
        element += 1;
        datoms.push(assert_string_datom(spec.case_id, 15, "open", element));
        element += 1;
        datoms.push(annotate_string_datom(
            spec.case_id,
            16,
            &format!("support-memory-anchor-{}", spec.case_id),
            element,
        ));
        element += 1;
        datoms.push(assert_string_datom(
            spec.case_id,
            12,
            spec.current_owner,
            element,
        ));
        element += 1;
        datoms.push(assert_u64_datom(spec.case_id, 13, 2, element));
        element += 1;
        datoms.push(assert_string_datom(spec.case_id, 14, "active", element));
    }
    datoms
}

fn scaled_history_tail_element() -> ElementId {
    ElementId::new(26 + (SCALED_CASES - BASELINE_CASES) as u64 * DATOMS_PER_SCALED_CASE as u64 - 1)
}

fn scaled_catalog_tail_element() -> ElementId {
    ElementId::new(
        scaled_history_tail_element().0
            + (SCALED_CASES - BASELINE_CASES) as u64 * CATALOG_ANCHOR_DATOMS_PER_SCALED_CASE as u64,
    )
}

fn support_dsl(
    view: &str,
    query_body: &str,
    projected_facts: &[ExtensionalFact],
    specs: &[SupportCaseSpec],
) -> String {
    let rendered_projected_facts = render_extensional_facts(projected_facts);
    let rendered_support_facts = render_support_facts(specs);
    format!(
        r#"
schema v1 {{
  attr case.subject: ScalarLWW<String>
  attr case.priority: ScalarLWW<String>
  attr case.channel: ScalarLWW<String>
  attr step.title: ScalarLWW<String>
  attr step.status: ScalarLWW<String>
  attr resolution.title: ScalarLWW<String>
  attr resolution.case: RefScalar<Entity>
  attr resolution.depends_on: RefSet<Entity>
  attr resolution.approval_state: ScalarLWW<String>
  attr resolution.suppression_state: ScalarLWW<String>
  attr resolution.confidence_band: ScalarLWW<String>
  attr case.claimed_by: ScalarLWW<String>
  attr case.lease_epoch: ScalarLWW<U64>
  attr case.lease_state: ScalarLWW<String>
  attr case.status: ScalarLWW<String>
  attr system.anchor: ScalarLWW<String>
}}

predicates {{
  support_case(Entity)
  candidate_resolution(Entity)
  assignment_attempt(Entity, String, U64)
  retrieved_support_evidence(Entity, Entity, F64)
  case_subject(Entity, String)
  case_priority(Entity, String)
  case_channel(Entity, String)
  step_status(Entity, String)
  resolution_title(Entity, String)
  resolution_case(Entity, Entity)
  resolution_depends_on(Entity, Entity)
  resolution_approval_state(Entity, String)
  resolution_suppression_state(Entity, String)
  resolution_confidence_band(Entity, String)
  case_claimed_by(Entity, String)
  case_lease_epoch(Entity, U64)
  case_lease_state(Entity, String)
  case_status(Entity, String)
  active_case(Entity, String, String, String)
  resolution_dependency_closure(Entity, Entity)
  step_complete(Entity)
  resolution_blocked(Entity)
  resolution_policy_approved(Entity)
  resolution_suppressed(Entity)
  resolution_confident(Entity)
  resolution_has_retrieved_evidence(Entity)
  active_assignment(Entity, String, U64)
  case_claimed(Entity)
  resolution_board(Entity, String, String, String, String)
  case_action_ready(Entity)
  ready_resolution_detail(Entity, String, Entity, String)
  case_resolution_selected(Entity, Entity, String, U64)
  case_resolution_selected_detail(Entity, String, String, String, U64)
  stale_assignment_attempt(Entity, String, U64)
  stale_assignment_attempt_detail(Entity, String, String, U64)
}}

facts {{
{rendered_support_facts}
{rendered_projected_facts}
}}

rules {{
  active_case(case, subject, priority, channel) <- support_case(case), case_status(case, "open"), case_subject(case, subject), case_priority(case, priority), case_channel(case, channel)
  resolution_dependency_closure(resolution, dep) <- resolution_depends_on(resolution, dep)
  resolution_dependency_closure(resolution, dep) <- resolution_depends_on(resolution, mid), resolution_dependency_closure(mid, dep)
  step_complete(step) <- step_status(step, "done")
  resolution_blocked(resolution) <- resolution_dependency_closure(resolution, dep), not step_complete(dep)
  resolution_policy_approved(resolution) <- resolution_approval_state(resolution, "approved")
  resolution_suppressed(resolution) <- resolution_suppression_state(resolution, "suppressed")
  resolution_confident(resolution) <- resolution_confidence_band(resolution, "high")
  resolution_has_retrieved_evidence(resolution) <- resolution_case(resolution, case), retrieved_support_evidence(case, evidence, score)
  active_assignment(case, owner, epoch) <- case_claimed_by(case, owner), case_lease_epoch(case, epoch), case_lease_state(case, "active")
  case_claimed(case) <- active_assignment(case, owner, epoch)
  resolution_board(resolution, title, approval, suppression, confidence) <- candidate_resolution(resolution), resolution_title(resolution, title), resolution_approval_state(resolution, approval), resolution_suppression_state(resolution, suppression), resolution_confidence_band(resolution, confidence)
  case_action_ready(resolution) <- candidate_resolution(resolution), resolution_policy_approved(resolution), resolution_confident(resolution), resolution_has_retrieved_evidence(resolution), not resolution_blocked(resolution), not resolution_suppressed(resolution), resolution_case(resolution, case), not case_claimed(case)
  ready_resolution_detail(case, subject, resolution, title) <- case_action_ready(resolution), resolution_case(resolution, case), case_subject(case, subject), resolution_title(resolution, title)
  case_resolution_selected(resolution, case, owner, epoch) <- candidate_resolution(resolution), resolution_policy_approved(resolution), resolution_confident(resolution), resolution_has_retrieved_evidence(resolution), not resolution_blocked(resolution), not resolution_suppressed(resolution), resolution_case(resolution, case), active_assignment(case, owner, epoch)
  case_resolution_selected_detail(case, subject, title, owner, epoch) <- case_resolution_selected(resolution, case, owner, epoch), case_subject(case, subject), resolution_title(resolution, title)
  stale_assignment_attempt(case, owner, epoch) <- assignment_attempt(case, owner, epoch), not active_assignment(case, owner, epoch)
  stale_assignment_attempt_detail(case, subject, owner, epoch) <- stale_assignment_attempt(case, owner, epoch), case_subject(case, subject)
}}

materialize {{
  active_case
  resolution_dependency_closure
  resolution_blocked
  resolution_policy_approved
  resolution_suppressed
  resolution_confident
  resolution_has_retrieved_evidence
  active_assignment
  case_claimed
  resolution_board
  case_action_ready
  ready_resolution_detail
  case_resolution_selected
  case_resolution_selected_detail
  stale_assignment_attempt
  stale_assignment_attempt_detail
}}

query {{
  {view}
  {query_body}
}}
"#
    )
}

fn render_extensional_facts(facts: &[ExtensionalFact]) -> String {
    let mut rendered = String::new();
    for fact in facts {
        let values = fact
            .values
            .iter()
            .map(render_fact_value)
            .collect::<Vec<_>>()
            .join(", ");
        rendered.push_str("  ");
        rendered.push_str(fact.predicate.name.as_str());
        rendered.push('(');
        rendered.push_str(values.as_str());
        rendered.push_str(")\n");
    }
    rendered
}

fn render_support_facts(specs: &[SupportCaseSpec]) -> String {
    let mut rendered = String::new();
    for spec in specs {
        let facts = [
            format!("support_case(entity({}))", spec.case_id),
            format!(
                "candidate_resolution(entity({}))",
                spec.preferred_resolution_id
            ),
            format!(
                "candidate_resolution(entity({}))",
                spec.fallback_resolution_id
            ),
            format!(
                "assignment_attempt(entity({}), \"triage-agent\", 1)",
                spec.case_id
            ),
            format!(
                "assignment_attempt(entity({}), \"{}\", 1)",
                spec.case_id, spec.current_owner
            ),
            format!(
                "assignment_attempt(entity({}), \"triage-agent\", 2)",
                spec.case_id
            ),
            format!(
                "assignment_attempt(entity({}), \"{}\", 2)",
                spec.case_id, spec.current_owner
            ),
        ];
        for fact in facts {
            rendered.push_str("  ");
            rendered.push_str(&fact);
            rendered.push('\n');
        }
    }
    rendered
}

fn render_fact_value(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(value) => value.to_string(),
        Value::I64(value) => value.to_string(),
        Value::U64(value) => value.to_string(),
        Value::F64(value) => format!("{value:.4}"),
        Value::String(value) => {
            format!("\"{}\"", value.replace('\\', "\\\\").replace('\"', "\\\""))
        }
        Value::Bytes(value) => format!("\"{} bytes\"", value.len()),
        Value::Entity(entity) => format!("entity({})", entity.0),
        Value::List(values) => format!(
            "[{}]",
            values
                .iter()
                .map(render_fact_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn format_values(values: &[Value]) -> String {
    values
        .iter()
        .map(format_value)
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(value) => value.to_string(),
        Value::I64(value) => value.to_string(),
        Value::U64(value) => value.to_string(),
        Value::F64(value) => format!("{value:.4}"),
        Value::String(value) => value.clone(),
        Value::Bytes(value) => format!("<{} bytes>", value.len()),
        Value::Entity(id) => describe_entity(id.0),
        Value::List(values) => format!("[{}]", format_values(values)),
    }
}

fn format_elements(elements: &[ElementId]) -> String {
    if elements.is_empty() {
        return "none".into();
    }

    elements
        .iter()
        .map(|element| format!("e{}", element.0))
        .collect::<Vec<_>>()
        .join(", ")
}

fn describe_entity(entity: u64) -> String {
    match entity {
        9_000..=9_999 => format!("evidence/{entity}"),
        900..=999 => format!("resolution/{entity}"),
        800..=899 => format!("step/{entity}"),
        _ => format!("case/{entity}"),
    }
}

fn entity_value(value: &Value) -> u64 {
    match value {
        Value::Entity(entity) => entity.0,
        other => panic!("expected entity value, found {other:?}"),
    }
}

fn string_value(value: &Value) -> &str {
    match value {
        Value::String(value) => value.as_str(),
        other => panic!("expected string value, found {other:?}"),
    }
}

fn u64_value(value: &Value) -> u64 {
    match value {
        Value::U64(value) => *value,
        other => panic!("expected u64 value, found {other:?}"),
    }
}

fn assert_string_datom(entity: u64, attribute: u64, value: &str, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(attribute),
        value: Value::String(value.into()),
        op: OperationKind::Assert,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn annotate_string_datom(entity: u64, attribute: u64, value: &str, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(attribute),
        value: Value::String(value.into()),
        op: OperationKind::Annotate,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn assert_u64_datom(entity: u64, attribute: u64, value: u64, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(attribute),
        value: Value::U64(value),
        op: OperationKind::Assert,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn ref_scalar_datom(entity: u64, attribute: u64, value: u64, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(attribute),
        value: Value::Entity(EntityId::new(value)),
        op: OperationKind::Assert,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn ref_set_datom(entity: u64, attribute: u64, value: u64, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(attribute),
        value: Value::Entity(EntityId::new(value)),
        op: OperationKind::Add,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn scaled_workload_exceeds_every_ten_x_baseline() {
        let specs = support_case_specs();
        let scaled_history = scaled_support_case_history(&specs[1..]);
        let elements = scaled_history
            .iter()
            .map(|datom| datom.element)
            .collect::<BTreeSet<_>>();

        assert_eq!(specs.len(), SCALED_CASES);
        assert!(specs.len() >= BASELINE_CASES * 10);
        assert!(specs.len() * 2 >= BASELINE_RESOLUTIONS * 10);
        assert!(specs.len() >= BASELINE_EVIDENCE_RECORDS * 10);
        assert!(specs.len() * 4 >= BASELINE_ASSIGNMENT_ATTEMPTS * 10);
        assert_eq!(
            scaled_history.len(),
            (SCALED_CASES - BASELINE_CASES) * DATOMS_PER_SCALED_CASE
        );
        assert_eq!(elements.len(), scaled_history.len());

        let total_datoms = BASELINE_JOURNAL_DATOMS
            + scaled_history.len()
            + (SCALED_CASES - BASELINE_CASES) * CATALOG_ANCHOR_DATOMS_PER_SCALED_CASE;
        assert_eq!(total_datoms, 300);
        assert!(total_datoms >= BASELINE_JOURNAL_DATOMS * 10);
        assert_eq!(scaled_catalog_tail_element(), ElementId::new(301));
    }
}
