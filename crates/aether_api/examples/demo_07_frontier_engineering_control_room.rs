use aether_api::{
    AppendRequest, ArtifactReference, GetArtifactReferenceRequest, InMemoryKernelService,
    KernelService, RegisterArtifactReferenceRequest, ResolveTraceHandleRequest, RunDocumentRequest,
    RunDocumentResponse,
};
use aether_ast::{
    AttributeId, Datom, DatomProvenance, DerivationTrace, ElementId, EntityId, OperationKind,
    QueryRow, ReplicaId, Value,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, path::PathBuf};

const PRODUCT_SHA: &str = "62272646689b726bdb54bd94b86f42efc812f618";
const DRIFTED_PRODUCT_SHA: &str = "73383757790c837cea65ce05c97f53f0d9230729";
const OLD_TOOLING_SHA: &str = "84494868801d948dfb76df16da806401ea34183a";
const CURRENT_TOOLING_SHA: &str = "95505979912e059efc87ef27eb917512fb45294b";
const WORK_PACKAGE_COUNT: usize = 24;
const CANDIDATE_COUNT: usize = 3;
const GATE_COUNT: usize = 4;
const ENGINEERING_AGENT_COUNT: usize = 6;
const ROOT_WORK_ID: u64 = 1_024;
const RUNNER_ID: u64 = 4_001;
const REPAIR_WORK_ID: u64 = 1_018;

struct LocalArtifact {
    path: PathBuf,
    uri: String,
    digest: String,
    byte_length: u64,
}

struct CampaignFixture {
    datoms: Vec<Datom>,
    replay_cut: ElementId,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let package = current_executable_artifact()?;
    let fixture = campaign_fixture(&package.digest);
    let replay_cut = fixture.replay_cut;
    let journal_datoms = fixture.datoms.len();
    let mut service = InMemoryKernelService::new();
    service.append(AppendRequest {
        datoms: fixture.datoms,
    })?;
    let journal_tail = service
        .history(Default::default())?
        .datoms
        .last()
        .map(|datom| datom.element)
        .expect("campaign history should not be empty");
    service.register_artifact_reference(RegisterArtifactReferenceRequest {
        reference: ArtifactReference {
            sidecar_id: "engineering-evidence".into(),
            artifact_id: "canonical-candidate-package".into(),
            entity: EntityId::new(8_001),
            uri: package.uri.clone(),
            media_type: "application/octet-stream".into(),
            byte_length: package.byte_length,
            digest: Some(package.digest.clone()),
            metadata: BTreeMap::from([
                ("kind".into(), Value::String("candidate-package".into())),
                (
                    "source".into(),
                    Value::String("current-demo-executable".into()),
                ),
            ]),
            provenance: DatomProvenance::default(),
            policy: None,
            registered_at: journal_tail,
        },
    })?;

    let response = service.run_document(RunDocumentRequest {
        dsl: control_room_dsl(&package.digest, replay_cut),
        policy_context: None,
    })?;
    assert_campaign_contract(&response, journal_datoms)?;

    let artifact = service.get_artifact_reference(GetArtifactReferenceRequest {
        sidecar_id: "engineering-evidence".into(),
        artifact_id: "canonical-candidate-package".into(),
        policy_context: None,
    })?;

    print_header(&package, replay_cut, journal_tail, journal_datoms);
    print_section(
        "ACT I / WORK GRAPH BEFORE REPAIR",
        named_rows(&response, "work_before"),
        6,
        "The release root is blocked recursively by the alternate-runner repair.",
    );
    print_section(
        "ACT II / WORK GRAPH AT CURRENT",
        named_rows(&response, "work_current"),
        6,
        "The same dependency graph is now closed; no blocked work remains.",
    );
    print_section(
        "ACT III / COMPETING CANDIDATES",
        named_rows(&response, "candidate_board"),
        6,
        "Three plausible changes exist, but activity is not authority.",
    );
    print_section(
        "ACT IV / STALE RUNNER EVIDENCE FENCED",
        named_rows(&response, "stale_evidence"),
        6,
        "Successful results from runner epoch 1 remain visible but cannot qualify epoch 2.",
    );
    print_section(
        "ACT V / IDENTITY AND POLICY REJECTIONS",
        named_rows(&response, "rejected_candidates"),
        8,
        "Product drift, stale tooling, package drift, and incomplete current evidence fail independently.",
    );
    print_section(
        &format!("ACT VI / PROMOTION AT AS OF(e{})", replay_cut.0),
        named_rows(&response, "promotion_before"),
        4,
        "Before the repair and authority handoff, no candidate is promotable.",
    );
    print_section(
        "ACT VII / PROMOTION AT CURRENT",
        named_rows(&response, "promotion_current"),
        4,
        "Only the release-control repair preserves product identity, exact bytes, current evidence, and review authority.",
    );
    print_section(
        "ACT VIII / ROUTING UPDATES ACCEPTED",
        named_rows(&response, "accepted_learning"),
        4,
        "The successful alternate-runner route becomes an explicit accepted learning update.",
    );
    print_section(
        "ACT VIII / ROUTING UPDATES RETAINED",
        named_rows(&response, "retained_learning"),
        4,
        "The experimental route remains evidence without silently changing policy.",
    );

    println!();
    println!("EVIDENCE ANCHOR");
    println!("---------------");
    println!("  artifact: {}", artifact.reference.artifact_id);
    println!("  bytes:    {}", artifact.reference.byte_length);
    println!(
        "  digest:   {}",
        short_identity(
            artifact
                .reference
                .digest
                .as_deref()
                .unwrap_or("digest unavailable")
        )
    );
    println!("  local:    {}", package.path.display());

    let trace = promotion_trace(&mut service, &response)?;
    print_trace(&trace);

    println!();
    println!("FRONTIER ENGINEERING VERDICT");
    println!("----------------------------");
    println!("  promoted: candidate-c-release-control-repair");
    println!("  fenced:   candidate-a-stale-runner");
    println!("  rejected: candidate-b-product-drift");
    println!("  boundary: AETHER governs engineering truth; agents and CI remain external workers");
    println!("  claim:    functional control-room proof, not autonomous software delivery");

    Ok(())
}

fn current_executable_artifact() -> Result<LocalArtifact, Box<dyn std::error::Error>> {
    let path = std::env::current_exe()?.canonicalize()?;
    let bytes = fs::read(&path)?;
    let digest = format!(
        "sha256:{}",
        Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let uri_path = path.to_string_lossy().replace('\\', "/");
    Ok(LocalArtifact {
        uri: format!("file:///{uri_path}"),
        path,
        digest,
        byte_length: bytes.len() as u64,
    })
}

fn print_header(
    package: &LocalArtifact,
    replay_cut: ElementId,
    journal_tail: ElementId,
    journal_datoms: usize,
) {
    println!("AETHER Demo 07: Frontier Engineering Control Room");
    println!("=================================================");
    println!();
    println!("+--------------------------------------------------------------------+");
    println!("| GOVERNED CHANGE CAMPAIGN                                           |");
    println!("+--------------------------------------------------------------------+");
    println!("| Product SHA : {:<52} |", short_identity(PRODUCT_SHA));
    println!(
        "| Tooling SHA : {:<52} |",
        short_identity(CURRENT_TOOLING_SHA)
    );
    println!("| Package     : {:<52} |", short_identity(&package.digest));
    println!("| Replay cut  : AsOf(e{:<43}) |", replay_cut.0);
    println!("| Current cut : e{:<49} |", journal_tail.0);
    println!("+--------------------------------------------------------------------+");
    println!();
    println!("Scale contract");
    println!("  - {WORK_PACKAGE_COUNT} recursively dependent work packages");
    println!("  - {ENGINEERING_AGENT_COUNT} engineering agents and specialist workers");
    println!("  - {CANDIDATE_COUNT} competing change candidates");
    println!(
        "  - {} exact prerequisite receipts across {GATE_COUNT} gate lanes",
        CANDIDATE_COUNT * GATE_COUNT
    );
    println!("  - 2 runner authority epochs");
    println!("  - {journal_datoms} append-only engineering datoms");
}

fn print_section(title: &str, rows: &[QueryRow], max_rows: usize, note: &str) {
    println!();
    println!("{title}");
    println!("{}", "-".repeat(title.len()));
    println!("  rows: {}", rows.len());
    if rows.is_empty() {
        println!("  - none");
    } else {
        for row in rows.iter().take(max_rows) {
            println!("  - {}", values(&row.values));
        }
        if rows.len() > max_rows {
            println!("  - ... {} more rows", rows.len() - max_rows);
        }
    }
    println!("  {note}");
}

fn print_trace(trace: &DerivationTrace) {
    println!();
    println!("ACT IX / WHY THE PROMOTION IS TRUE");
    println!("---------------------------------");
    println!("  root tuple: t{}", trace.root.0);
    println!("  tuples in trace: {}", trace.tuples.len());
    for tuple in trace.tuples.iter().take(10) {
        println!(
            "  - t{} via r{} -> {} | iteration {} | sources {}",
            tuple.tuple.id.0,
            tuple.metadata.rule_id.0,
            values(&tuple.tuple.values),
            tuple.metadata.iteration,
            element_ids(&tuple.metadata.source_datom_ids)
        );
    }
    if trace.tuples.len() > 10 {
        println!("  - ... {} more proof tuples", trace.tuples.len() - 10);
    }
}

fn named_rows<'a>(response: &'a RunDocumentResponse, name: &str) -> &'a [QueryRow] {
    response
        .queries
        .iter()
        .find(|query| query.name.as_deref() == Some(name))
        .unwrap_or_else(|| panic!("missing named query {name}"))
        .result
        .rows
        .as_slice()
}

fn promotion_trace(
    service: &mut InMemoryKernelService,
    response: &RunDocumentResponse,
) -> Result<DerivationTrace, Box<dyn std::error::Error>> {
    let query = response
        .queries
        .iter()
        .find(|query| query.name.as_deref() == Some("promotion_current"))
        .expect("promotion query should exist");
    let tuple_id = query
        .result
        .rows
        .first()
        .and_then(|row| row.tuple_id)
        .expect("promotion row should have a tuple id");
    let execution_id = query
        .execution_id
        .as_ref()
        .expect("promotion query should have an execution id");
    let receipt = response
        .executions
        .iter()
        .find(|receipt| receipt.manifest.execution_id == *execution_id)
        .expect("promotion execution should be persisted");
    let handle = receipt
        .trace_handles
        .iter()
        .find(|binding| binding.local_tuple_id == tuple_id)
        .expect("promotion tuple should have a trace handle")
        .handle
        .clone();
    Ok(service
        .resolve_trace_handle(ResolveTraceHandleRequest {
            handle,
            policy_context: None,
            verify_replay: true,
        })?
        .record
        .trace)
}

fn assert_campaign_contract(
    response: &RunDocumentResponse,
    journal_datoms: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let expectations = [
        ("work_before", 6),
        ("work_current", 0),
        ("candidate_board", 3),
        ("stale_evidence", 4),
        ("promotion_before", 0),
        ("promotion_current", 1),
        ("accepted_learning", 1),
        ("retained_learning", 1),
    ];
    for (query, expected) in expectations {
        let observed = named_rows(response, query).len();
        if observed != expected {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "frontier engineering contract failed for {query}: expected {expected}, observed {observed}"
                ),
            )
            .into());
        }
    }
    if journal_datoms < 180 || response.executions.len() != 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "frontier engineering scale/evaluation contract failed: datoms={journal_datoms} executions={}",
                response.executions.len()
            ),
        )
        .into());
    }
    Ok(())
}

fn campaign_fixture(package_digest: &str) -> CampaignFixture {
    let mut datoms = Vec::new();
    let mut element = 1u64;
    let work_names = [
        "reproduce-hosted-runner-timeout",
        "capture-runner-diagnostics",
        "isolate-report-query-amplification",
        "design-batched-temporal-evaluation",
        "implement-named-query-document",
        "preserve-trace-handle-contract",
        "preserve-policy-filtering",
        "add-report-batching-regression",
        "run-rust-unit-suite",
        "run-http-integration-suite",
        "run-go-client-suite",
        "run-tui-regression-suite",
        "measure-packaged-client-boundary",
        "verify-canonical-package-digest",
        "review-release-control-scope",
        "prepare-alternate-runner",
        "qualify-tooling-only-diff",
        "repair-alternate-runner",
        "collect-ci-prerequisite",
        "collect-supply-chain-prerequisite",
        "collect-pages-prerequisite",
        "collect-capacity-prerequisite",
        "assemble-two-identity-evidence",
        "authorize-qualified-candidate",
    ];
    for (index, name) in work_names.iter().enumerate() {
        let work_id = 1_001 + index as u64;
        push_string(&mut datoms, work_id, 1, name, &mut element);
        let status = if work_id == REPAIR_WORK_ID {
            "pending"
        } else {
            "done"
        };
        push_string(&mut datoms, work_id, 2, status, &mut element);
        if index > 0 {
            push_entity(
                &mut datoms,
                work_id,
                3,
                work_id - 1,
                OperationKind::Add,
                &mut element,
            );
        }
    }

    push_string(
        &mut datoms,
        RUNNER_ID,
        20,
        "hosted-windows-primary",
        &mut element,
    );
    push_u64(&mut datoms, RUNNER_ID, 21, 1, &mut element);

    add_candidate(
        &mut datoms,
        &mut element,
        2_001,
        "candidate-a-stale-runner",
        PRODUCT_SHA,
        OLD_TOOLING_SHA,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "release_control_only",
        "agent-delta",
    );
    add_candidate(
        &mut datoms,
        &mut element,
        2_002,
        "candidate-b-product-drift",
        DRIFTED_PRODUCT_SHA,
        CURRENT_TOOLING_SHA,
        package_digest,
        "product_and_tooling",
        "agent-epsilon",
    );
    add_candidate(
        &mut datoms,
        &mut element,
        2_003,
        "candidate-c-release-control-repair",
        PRODUCT_SHA,
        CURRENT_TOOLING_SHA,
        package_digest,
        "release_control_only",
        "agent-zeta",
    );

    let gates = ["ci", "supply_chain", "pages", "capacity"];
    for (candidate_index, candidate_id) in [2_001u64, 2_002, 2_003].into_iter().enumerate() {
        let (product_sha, tooling_sha, digest, epoch) = match candidate_id {
            2_001 => (
                PRODUCT_SHA,
                OLD_TOOLING_SHA,
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                1,
            ),
            2_002 => (DRIFTED_PRODUCT_SHA, CURRENT_TOOLING_SHA, package_digest, 2),
            _ => (PRODUCT_SHA, CURRENT_TOOLING_SHA, package_digest, 2),
        };
        for (gate_index, gate) in gates.iter().enumerate() {
            add_evidence(
                &mut datoms,
                &mut element,
                3_001 + candidate_index as u64 * 10 + gate_index as u64,
                candidate_id,
                gate,
                product_sha,
                tooling_sha,
                digest,
                epoch,
                90_000 + candidate_index as u64 * 100 + gate_index as u64,
            );
        }
    }

    add_learning_update(
        &mut datoms,
        &mut element,
        6_001,
        "windows-ci -> alternate-windows-runner",
        "accepted_local",
        "current-epoch evidence completed below client deadline",
    );
    add_learning_update(
        &mut datoms,
        &mut element,
        6_002,
        "capacity -> experimental-gpu-runner",
        "retained_evidence",
        "useful signal but insufficient repeated evidence",
    );

    let replay_cut = ElementId::new(element - 1);
    push_string(&mut datoms, REPAIR_WORK_ID, 2, "done", &mut element);
    push_string(
        &mut datoms,
        RUNNER_ID,
        20,
        "alternate-windows-runner",
        &mut element,
    );
    push_u64(&mut datoms, RUNNER_ID, 21, 2, &mut element);

    CampaignFixture { datoms, replay_cut }
}

#[allow(clippy::too_many_arguments)]
fn add_candidate(
    datoms: &mut Vec<Datom>,
    element: &mut u64,
    candidate: u64,
    label: &str,
    product_sha: &str,
    tooling_sha: &str,
    package_digest: &str,
    scope: &str,
    proposed_by: &str,
) {
    push_string(datoms, candidate, 4, label, element);
    push_string(datoms, candidate, 5, product_sha, element);
    push_string(datoms, candidate, 6, tooling_sha, element);
    push_string(datoms, candidate, 7, package_digest, element);
    push_string(datoms, candidate, 8, "approved", element);
    push_string(datoms, candidate, 9, scope, element);
    push_string(datoms, candidate, 10, proposed_by, element);
    push_entity(
        datoms,
        candidate,
        11,
        ROOT_WORK_ID,
        OperationKind::Assert,
        element,
    );
}

#[allow(clippy::too_many_arguments)]
fn add_evidence(
    datoms: &mut Vec<Datom>,
    element: &mut u64,
    evidence: u64,
    candidate: u64,
    gate: &str,
    product_sha: &str,
    tooling_sha: &str,
    package_digest: &str,
    runner_epoch: u64,
    run_id: u64,
) {
    push_entity(
        datoms,
        evidence,
        12,
        candidate,
        OperationKind::Assert,
        element,
    );
    push_string(datoms, evidence, 13, gate, element);
    push_string(datoms, evidence, 14, "success", element);
    push_string(datoms, evidence, 15, product_sha, element);
    push_string(datoms, evidence, 16, tooling_sha, element);
    push_string(datoms, evidence, 17, package_digest, element);
    push_u64(datoms, evidence, 18, runner_epoch, element);
    push_u64(datoms, evidence, 19, run_id, element);
}

fn add_learning_update(
    datoms: &mut Vec<Datom>,
    element: &mut u64,
    update: u64,
    route: &str,
    status: &str,
    reason: &str,
) {
    push_string(datoms, update, 22, route, element);
    push_string(datoms, update, 23, status, element);
    push_string(datoms, update, 24, reason, element);
}

fn control_room_dsl(package_digest: &str, replay_cut: ElementId) -> String {
    let replay_element = replay_cut.0;
    let mut work_facts = String::new();
    for index in 0..WORK_PACKAGE_COUNT {
        work_facts.push_str(&format!("  work_package(entity({}))\n", 1_001 + index));
    }
    format!(
        r#"
schema v1 {{
  attr work.name: ScalarLWW<String>
  attr work.status: ScalarLWW<String>
  attr work.depends_on: RefSet<Entity>
  attr candidate.label: ScalarLWW<String>
  attr candidate.product_sha: ScalarLWW<String>
  attr candidate.tooling_sha: ScalarLWW<String>
  attr candidate.package_digest: ScalarLWW<String>
  attr candidate.review_state: ScalarLWW<String>
  attr candidate.change_scope: ScalarLWW<String>
  attr candidate.proposed_by: ScalarLWW<String>
  attr candidate.release_work: RefScalar<Entity>
  attr evidence.candidate: RefScalar<Entity>
  attr evidence.gate: ScalarLWW<String>
  attr evidence.conclusion: ScalarLWW<String>
  attr evidence.product_sha: ScalarLWW<String>
  attr evidence.tooling_sha: ScalarLWW<String>
  attr evidence.package_digest: ScalarLWW<String>
  attr evidence.runner_epoch: ScalarLWW<U64>
  attr evidence.run_id: ScalarLWW<U64>
  attr runner.name: ScalarLWW<String>
  attr runner.epoch: ScalarLWW<U64>
  attr update.route: ScalarLWW<String>
  attr update.status: ScalarLWW<String>
  attr update.reason: ScalarLWW<String>
}}

predicates {{
  work_package(Entity)
  candidate(Entity)
  selected_product_sha(String)
  protected_tooling_sha(String)
  canonical_package_digest(String)
  work_name(Entity, String)
  work_status(Entity, String)
  work_depends_on(Entity, Entity)
  candidate_label(Entity, String)
  candidate_product_sha(Entity, String)
  candidate_tooling_sha(Entity, String)
  candidate_package_digest(Entity, String)
  candidate_review_state(Entity, String)
  candidate_change_scope(Entity, String)
  candidate_proposed_by(Entity, String)
  candidate_release_work(Entity, Entity)
  evidence_candidate(Entity, Entity)
  evidence_gate(Entity, String)
  evidence_conclusion(Entity, String)
  evidence_product_sha(Entity, String)
  evidence_tooling_sha(Entity, String)
  evidence_package_digest(Entity, String)
  evidence_runner_epoch(Entity, U64)
  evidence_run_id(Entity, U64)
  runner_name(Entity, String)
  runner_epoch(Entity, U64)
  update_route(Entity, String)
  update_status(Entity, String)
  update_reason(Entity, String)
  work_complete(Entity)
  work_dependency_closure(Entity, Entity)
  work_blocked(Entity)
  work_ready(Entity)
  blocked_work_detail(Entity, String, Entity, String)
  candidate_product_exact(Entity)
  candidate_tooling_admissible(Entity)
  candidate_package_unchanged(Entity)
  candidate_review_approved(Entity)
  candidate_work_ready(Entity)
  evidence_identity_exact(Entity)
  evidence_current_epoch(Entity)
  gate_green_current(Entity, String)
  candidate_all_gates_current(Entity)
  candidate_board(Entity, String, String, String, String, String)
  stale_runner_evidence_detail(Entity, String, String, U64, U64, U64)
  candidate_rejected_reason(Entity, String, String)
  promotion_allowed_detail(Entity, String, String, String, String)
  accepted_routing_update(Entity, String, String)
  retained_routing_update(Entity, String, String)
}}

facts {{
{work_facts}
  candidate(entity(2001))
  candidate(entity(2002))
  candidate(entity(2003))
  selected_product_sha("{PRODUCT_SHA}")
  protected_tooling_sha("{CURRENT_TOOLING_SHA}")
  canonical_package_digest("{package_digest}")
}}

rules {{
  work_complete(w) <- work_status(w, "done")
  work_dependency_closure(w, dep) <- work_depends_on(w, dep)
  work_dependency_closure(w, dep) <- work_depends_on(w, mid), work_dependency_closure(mid, dep)
  work_blocked(w) <- work_dependency_closure(w, dep), not work_complete(dep)
  work_ready(w) <- work_package(w), work_complete(w), not work_blocked(w)
  blocked_work_detail(w, name, dep, dep_name) <- work_blocked(w), work_name(w, name), work_dependency_closure(w, dep), work_name(dep, dep_name), not work_complete(dep)

  candidate_product_exact(c) <- candidate_product_sha(c, product), selected_product_sha(product)
  candidate_tooling_admissible(c) <- candidate_change_scope(c, "release_control_only"), candidate_tooling_sha(c, tooling), protected_tooling_sha(tooling)
  candidate_package_unchanged(c) <- candidate_package_digest(c, digest), canonical_package_digest(digest)
  candidate_review_approved(c) <- candidate_review_state(c, "approved")
  candidate_work_ready(c) <- candidate_release_work(c, work), work_ready(work)

  evidence_identity_exact(e) <- evidence_candidate(e, c), evidence_product_sha(e, product), candidate_product_sha(c, product), evidence_tooling_sha(e, tooling), candidate_tooling_sha(c, tooling), evidence_package_digest(e, digest), candidate_package_digest(c, digest)
  evidence_current_epoch(e) <- evidence_runner_epoch(e, epoch), runner_epoch(runner, epoch)
  gate_green_current(c, gate) <- evidence_candidate(e, c), evidence_gate(e, gate), evidence_conclusion(e, "success"), evidence_identity_exact(e), evidence_current_epoch(e)
  candidate_all_gates_current(c) <- gate_green_current(c, "ci"), gate_green_current(c, "supply_chain"), gate_green_current(c, "pages"), gate_green_current(c, "capacity")

  candidate_board(c, label, product, tooling, scope, proposer) <- candidate(c), candidate_label(c, label), candidate_product_sha(c, product), candidate_tooling_sha(c, tooling), candidate_change_scope(c, scope), candidate_proposed_by(c, proposer)
  stale_runner_evidence_detail(e, label, gate, run_id, recorded, current) <- evidence_candidate(e, c), candidate_label(c, label), evidence_gate(e, gate), evidence_run_id(e, run_id), evidence_runner_epoch(e, recorded), runner_epoch(runner, current), not evidence_current_epoch(e)

  candidate_rejected_reason(c, label, "product_sha_mismatch") <- candidate(c), candidate_label(c, label), not candidate_product_exact(c)
  candidate_rejected_reason(c, label, "tooling_or_scope_not_admissible") <- candidate(c), candidate_label(c, label), not candidate_tooling_admissible(c)
  candidate_rejected_reason(c, label, "package_digest_changed") <- candidate(c), candidate_label(c, label), not candidate_package_unchanged(c)
  candidate_rejected_reason(c, label, "current_gate_evidence_incomplete") <- candidate(c), candidate_label(c, label), not candidate_all_gates_current(c)

  promotion_allowed_detail(c, label, product, tooling, digest) <- candidate(c), candidate_label(c, label), candidate_product_sha(c, product), candidate_tooling_sha(c, tooling), candidate_package_digest(c, digest), candidate_product_exact(c), candidate_tooling_admissible(c), candidate_package_unchanged(c), candidate_review_approved(c), candidate_work_ready(c), candidate_all_gates_current(c)

  accepted_routing_update(update, route, reason) <- update_route(update, route), update_status(update, "accepted_local"), update_reason(update, reason)
  retained_routing_update(update, route, reason) <- update_route(update, route), update_status(update, "retained_evidence"), update_reason(update, reason)
}}

materialize {{
  work_dependency_closure
  work_blocked
  work_ready
  blocked_work_detail
  candidate_product_exact
  candidate_tooling_admissible
  candidate_package_unchanged
  candidate_review_approved
  candidate_work_ready
  evidence_identity_exact
  evidence_current_epoch
  gate_green_current
  candidate_all_gates_current
  candidate_board
  stale_runner_evidence_detail
  candidate_rejected_reason
  promotion_allowed_detail
  accepted_routing_update
  retained_routing_update
}}

query work_before {{
  as_of e{replay_element}
  goal blocked_work_detail(work, name, dep, dep_name)
  keep work, name, dep, dep_name
}}

query work_current {{
  current
  goal blocked_work_detail(work, name, dep, dep_name)
  keep work, name, dep, dep_name
}}

query candidate_board {{
  current
  goal candidate_board(candidate, label, product, tooling, scope, proposer)
  keep candidate, label, product, tooling, scope, proposer
}}

query stale_evidence {{
  current
  goal stale_runner_evidence_detail(evidence, label, gate, run_id, recorded, current_epoch)
  keep evidence, label, gate, run_id, recorded, current_epoch
}}

query rejected_candidates {{
  current
  goal candidate_rejected_reason(candidate, label, reason)
  keep candidate, label, reason
}}

query promotion_before {{
  as_of e{replay_element}
  goal promotion_allowed_detail(candidate, label, product, tooling, digest)
  keep candidate, label, product, tooling, digest
}}

query promotion_current {{
  current
  goal promotion_allowed_detail(candidate, label, product, tooling, digest)
  keep candidate, label, product, tooling, digest
}}

query accepted_learning {{
  current
  goal accepted_routing_update(update, route, reason)
  keep update, route, reason
}}

query retained_learning {{
  current
  goal retained_routing_update(update, route, reason)
  keep update, route, reason
}}
"#
    )
}

fn push_string(
    datoms: &mut Vec<Datom>,
    entity: u64,
    attribute: u64,
    value: &str,
    element: &mut u64,
) {
    datoms.push(datom(
        entity,
        attribute,
        Value::String(value.into()),
        OperationKind::Assert,
        *element,
    ));
    *element += 1;
}

fn push_u64(datoms: &mut Vec<Datom>, entity: u64, attribute: u64, value: u64, element: &mut u64) {
    datoms.push(datom(
        entity,
        attribute,
        Value::U64(value),
        OperationKind::Assert,
        *element,
    ));
    *element += 1;
}

fn push_entity(
    datoms: &mut Vec<Datom>,
    entity: u64,
    attribute: u64,
    value: u64,
    op: OperationKind,
    element: &mut u64,
) {
    datoms.push(datom(
        entity,
        attribute,
        Value::Entity(EntityId::new(value)),
        op,
        *element,
    ));
    *element += 1;
}

fn datom(entity: u64, attribute: u64, value: Value, op: OperationKind, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(attribute),
        value,
        op,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn values(values: &[Value]) -> String {
    values
        .iter()
        .map(display_value)
        .collect::<Vec<_>>()
        .join(", ")
}

fn display_value(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(value) => value.to_string(),
        Value::I64(value) => value.to_string(),
        Value::U64(value) => value.to_string(),
        Value::F64(value) => format!("{value:.4}"),
        Value::String(value) => short_identity(value),
        Value::Bytes(bytes) => format!("<{} bytes>", bytes.len()),
        Value::Entity(entity) => match entity.0 {
            1_000..=1_999 => format!("work/{}", entity.0),
            2_000..=2_999 => format!("candidate/{}", entity.0),
            3_000..=3_999 => format!("evidence/{}", entity.0),
            4_000..=4_999 => format!("runner/{}", entity.0),
            6_000..=6_999 => format!("update/{}", entity.0),
            _ => format!("entity/{}", entity.0),
        },
        Value::List(items) => format!("[{}]", values(items)),
    }
}

fn short_identity(value: &str) -> String {
    if value.len() <= 32 {
        return value.into();
    }
    format!("{}...{}", &value[..12], &value[value.len() - 8..])
}

fn element_ids(elements: &[ElementId]) -> String {
    if elements.is_empty() {
        return "none".into();
    }
    elements
        .iter()
        .map(|element| format!("e{}", element.0))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn campaign_fixture_preserves_scale_and_replay_boundary() {
        let fixture = campaign_fixture(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        );
        assert!(fixture.datoms.len() >= 180);
        assert_eq!(
            fixture.datoms.last().map(|datom| datom.element.0),
            Some(fixture.replay_cut.0 + 3)
        );
        assert_eq!(
            fixture
                .datoms
                .iter()
                .filter(|datom| datom.attribute == AttributeId::new(13))
                .count(),
            CANDIDATE_COUNT * GATE_COUNT
        );
    }
}
