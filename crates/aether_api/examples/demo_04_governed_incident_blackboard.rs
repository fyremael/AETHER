use aether_api::{
    AppendRequest, ExplainTupleRequest, HistoryRequest, InMemoryKernelService, KernelService,
    RunDocumentRequest,
};
use aether_ast::{Datom, DatomProvenance, DerivationTrace, EntityId, QueryRow, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut service = InMemoryKernelService::new();
    service.append(AppendRequest {
        datoms: board_history(),
    })?;

    println!("AETHER Demo 04: Governed Incident Blackboard");
    println!("=============================================");
    println!();
    println!("This is the product-facing AETHER exemplar:");
    println!("  - one shared incident board for agents and operators");
    println!("  - active observations and published remediation actions");
    println!("  - one action derived as truly ready");
    println!("  - governed claiming through live lease authority");
    println!("  - temporal replay plus a proof trace for the current answer");
    println!();
    println!("Published board history:");
    for datom in service
        .history(HistoryRequest {
            policy_context: None,
        })?
        .datoms
    {
        println!("  - {}", describe_datom(&datom));
    }

    let observations = service.run_document(RunDocumentRequest {
        dsl: board_dsl(
            "current",
            "goal observation_active(incident, signal)\n  keep incident, signal",
        ),
        policy_context: None,
    })?;
    print_section(
        "Act I: Active observations on the board (Current)",
        observations
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "The incident board starts with the live operating picture rather than a queue entry.",
    );

    let board_actions = service.run_document(RunDocumentRequest {
        dsl: board_dsl(
            "current",
            "goal board_action(action, title, approval, suppression)\n  keep action, title, approval, suppression",
        ),
        policy_context: None,
    })?;
    print_section(
        "Published candidate actions (Current)",
        board_actions
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "One action is clear for use once its dependency chain closes. The other is visibly suppressed.",
    );

    let ready_action = service.run_document(RunDocumentRequest {
        dsl: board_dsl(
            "as_of e15",
            "goal ready_action_detail(action, title)\n  keep action, title",
        ),
        policy_context: None,
    })?;
    print_section(
        "Act II: Which action is actually ready? (AsOf e15)",
        ready_action
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "Approval, required board signals, and dependency completion all have to line up before the action becomes ready.",
    );

    let current_authority = service.run_document(RunDocumentRequest {
        dsl: board_dsl(
            "current",
            "goal execution_authorized_detail(action, title, worker, epoch)\n  keep action, title, worker, epoch",
        ),
        policy_context: None,
    })?;
    let authorized_rows = current_authority
        .query
        .as_ref()
        .expect("query should exist")
        .rows
        .clone();
    print_section(
        "Act III: Who may act now? (Current)",
        authorized_rows.as_slice(),
        "The board now names a single authorized remediator because the lease has advanced to the live holder.",
    );

    let before_handoff = service.run_document(RunDocumentRequest {
        dsl: board_dsl(
            "as_of e18",
            "goal execution_authorized_detail(action, title, worker, epoch)\n  keep action, title, worker, epoch",
        ),
        policy_context: None,
    })?;
    print_section(
        "Act IV: The same board before the handoff (AsOf e18)",
        before_handoff
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "Replay shows that the authorized actor really did change. This is a semantic handoff, not a presentation trick.",
    );

    let stale_attempts = service.run_document(RunDocumentRequest {
        dsl: board_dsl(
            "current",
            "goal execution_rejected_stale_detail(action, title, worker, epoch)\n  keep action, title, worker, epoch",
        ),
        policy_context: None,
    })?;
    print_section(
        "Fenced stale attempts at Current",
        stale_attempts
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "AETHER keeps history, but it still distinguishes what merely happened from what is semantically valid now.",
    );

    if let Some(tuple_id) = authorized_rows.first().and_then(|row| row.tuple_id) {
        let trace = service
            .explain_tuple(ExplainTupleRequest {
                tuple_id,
                policy_context: None,
            })?
            .trace;
        print_trace_summary(&trace);
    }

    println!();
    println!("Bottom line:");
    println!("  - observations, candidate actions, authority, and fencing all live in one fabric");
    println!("  - Current tells the operator who may act now");
    println!("  - AsOf shows what changed across the handoff");
    println!("  - the proof trace preserves why the current answer is true");

    Ok(())
}

fn print_section(title: &str, rows: &[QueryRow], note: &str) {
    println!();
    println!("{title}");
    println!("{}", "-".repeat(title.len()));
    if rows.is_empty() {
        println!("  - none");
    } else {
        for row in rows {
            println!("  - {}", format_values(&row.values));
        }
    }
    println!("  {note}");
}

fn print_trace_summary(trace: &DerivationTrace) {
    println!();
    println!("Act V: Why the current authorization is true");
    println!("--------------------------------------------");
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
        "  The current answer is explainable after the handoff, not only while it is happening."
    );
}

fn describe_datom(datom: &Datom) -> String {
    let subject = describe_entity(datom.entity.0);
    match datom.attribute.0 {
        1 => format!(
            "e{}: {subject} observation = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        2 => format!(
            "e{}: {subject} title = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        3 => format!(
            "e{}: {subject} belongs_to {}",
            datom.element.0,
            describe_entity(entity_value(&datom.value))
        ),
        4 => format!(
            "e{}: {subject} depends_on {}",
            datom.element.0,
            describe_entity(entity_value(&datom.value))
        ),
        5 => format!(
            "e{}: {subject} requires_signal = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        6 => format!(
            "e{}: {subject} approval_state = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        7 => format!(
            "e{}: {subject} suppression_state = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        8 => format!(
            "e{}: {subject} status = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        9 => format!(
            "e{}: {subject} claimed_by = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        10 => format!(
            "e{}: {subject} lease_epoch = {}",
            datom.element.0,
            u64_value(&datom.value)
        ),
        11 => format!(
            "e{}: {subject} lease_state = {}",
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

fn board_history() -> Vec<Datom> {
    vec![
        add_string_datom(1, 1, "latency_spike", 1),
        add_string_datom(1, 1, "saturation_alert", 2),
        assert_string_datom(202, 2, "drain-read-replica", 3),
        assert_string_datom(202, 8, "done", 4),
        assert_string_datom(201, 2, "shift-read-traffic", 5),
        ref_scalar_datom(201, 3, 1, 6),
        ref_set_datom(201, 4, 202, 7),
        add_string_datom(201, 5, "latency_spike", 8),
        assert_string_datom(201, 6, "approved", 9),
        assert_string_datom(201, 7, "clear", 10),
        assert_string_datom(203, 2, "restart-primary", 11),
        ref_scalar_datom(203, 3, 1, 12),
        add_string_datom(203, 5, "saturation_alert", 13),
        assert_string_datom(203, 6, "approved", 14),
        assert_string_datom(203, 7, "suppressed", 15),
        assert_string_datom(201, 9, "remediator-a", 16),
        assert_u64_datom(201, 10, 1, 17),
        assert_string_datom(201, 11, "active", 18),
        assert_string_datom(201, 9, "remediator-b", 19),
        assert_u64_datom(201, 10, 2, 20),
    ]
}

fn board_dsl(view: &str, query_body: &str) -> String {
    format!(
        r#"
schema v1 {{
  attr incident.observation: SetAddWins<String>
  attr action.title: ScalarLWW<String>
  attr action.incident: RefScalar<Entity>
  attr action.depends_on: RefSet<Entity>
  attr action.requires_signal: SetAddWins<String>
  attr action.approval_state: ScalarLWW<String>
  attr action.suppression_state: ScalarLWW<String>
  attr action.status: ScalarLWW<String>
  attr action.claimed_by: ScalarLWW<String>
  attr action.lease_epoch: ScalarLWW<U64>
  attr action.lease_state: ScalarLWW<String>
}}

predicates {{
  candidate_action(Entity)
  execution_attempt(Entity, String, U64)
  incident_observation(Entity, String)
  action_title(Entity, String)
  action_incident(Entity, Entity)
  action_depends_on(Entity, Entity)
  action_requires_signal(Entity, String)
  action_approval_state(Entity, String)
  action_suppression_state(Entity, String)
  action_status(Entity, String)
  action_claimed_by(Entity, String)
  action_lease_epoch(Entity, U64)
  action_lease_state(Entity, String)
  observation_active(Entity, String)
  dependency_closure(Entity, Entity)
  action_complete(Entity)
  dependency_blocked(Entity)
  action_missing_signal(Entity)
  action_suppressed(Entity)
  action_approved(Entity)
  lease_active(Entity, String, U64)
  active_claim(Entity)
  action_ready(Entity)
  board_action(Entity, String, String, String)
  ready_action_detail(Entity, String)
  execution_authorized(Entity, String, U64)
  execution_authorized_detail(Entity, String, String, U64)
  execution_rejected_stale(Entity, String, U64)
  execution_rejected_stale_detail(Entity, String, String, U64)
}}

facts {{
  candidate_action(entity(201))
  candidate_action(entity(203))
  execution_attempt(entity(201), "remediator-a", 1)
  execution_attempt(entity(201), "remediator-b", 1)
  execution_attempt(entity(201), "remediator-a", 2)
  execution_attempt(entity(201), "remediator-b", 2)
}}

rules {{
  observation_active(incident, signal) <- incident_observation(incident, signal)
  dependency_closure(action, dep) <- action_depends_on(action, dep)
  dependency_closure(action, dep) <- action_depends_on(action, mid), dependency_closure(mid, dep)
  action_complete(action) <- action_status(action, "done")
  dependency_blocked(action) <- dependency_closure(action, dep), not action_complete(dep)
  action_missing_signal(action) <- action_requires_signal(action, signal), action_incident(action, incident), not incident_observation(incident, signal)
  action_suppressed(action) <- action_suppression_state(action, "suppressed")
  action_approved(action) <- action_approval_state(action, "approved")
  lease_active(action, worker, epoch) <- action_claimed_by(action, worker), action_lease_epoch(action, epoch), action_lease_state(action, "active")
  active_claim(action) <- lease_active(action, worker, epoch)
  action_ready(action) <- candidate_action(action), action_approved(action), not dependency_blocked(action), not action_missing_signal(action), not action_suppressed(action), not active_claim(action), not action_complete(action)
  board_action(action, title, approval, suppression) <- candidate_action(action), action_title(action, title), action_approval_state(action, approval), action_suppression_state(action, suppression)
  ready_action_detail(action, title) <- action_ready(action), action_title(action, title)
  execution_authorized(action, worker, epoch) <- execution_attempt(action, worker, epoch), lease_active(action, worker, epoch)
  execution_authorized_detail(action, title, worker, epoch) <- execution_authorized(action, worker, epoch), action_title(action, title)
  execution_rejected_stale(action, worker, epoch) <- execution_attempt(action, worker, epoch), not lease_active(action, worker, epoch)
  execution_rejected_stale_detail(action, title, worker, epoch) <- execution_rejected_stale(action, worker, epoch), action_title(action, title)
}}

materialize {{
  observation_active
  dependency_closure
  dependency_blocked
  action_missing_signal
  action_suppressed
  action_ready
  board_action
  ready_action_detail
  execution_authorized
  execution_authorized_detail
  execution_rejected_stale
  execution_rejected_stale_detail
}}

query {{
  {view}
  {query_body}
}}
"#
    )
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
        Value::F64(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Bytes(value) => format!("<{} bytes>", value.len()),
        Value::Entity(id) => describe_entity(id.0),
        Value::List(values) => format!("[{}]", format_values(values)),
    }
}

fn format_elements(elements: &[aether_ast::ElementId]) -> String {
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
    if entity >= 200 {
        format!("action/{entity}")
    } else {
        format!("incident/{entity}")
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

fn add_string_datom(entity: u64, attribute: u64, value: &str, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: aether_ast::AttributeId::new(attribute),
        value: Value::String(value.into()),
        op: aether_ast::OperationKind::Add,
        element: aether_ast::ElementId::new(element),
        replica: aether_ast::ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn assert_string_datom(entity: u64, attribute: u64, value: &str, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: aether_ast::AttributeId::new(attribute),
        value: Value::String(value.into()),
        op: aether_ast::OperationKind::Assert,
        element: aether_ast::ElementId::new(element),
        replica: aether_ast::ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn assert_u64_datom(entity: u64, attribute: u64, value: u64, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: aether_ast::AttributeId::new(attribute),
        value: Value::U64(value),
        op: aether_ast::OperationKind::Assert,
        element: aether_ast::ElementId::new(element),
        replica: aether_ast::ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn ref_scalar_datom(entity: u64, attribute: u64, value: u64, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: aether_ast::AttributeId::new(attribute),
        value: Value::Entity(EntityId::new(value)),
        op: aether_ast::OperationKind::Assert,
        element: aether_ast::ElementId::new(element),
        replica: aether_ast::ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn ref_set_datom(entity: u64, attribute: u64, value: u64, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: aether_ast::AttributeId::new(attribute),
        value: Value::Entity(EntityId::new(value)),
        op: aether_ast::OperationKind::Add,
        element: aether_ast::ElementId::new(element),
        replica: aether_ast::ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}
