use aether_api::{AppendRequest, ExplainTupleRequest, InMemoryKernelService, KernelService, RunDocumentRequest};
use aether_ast::{Datom, DatomProvenance, DerivationTrace, EntityId, QueryRow, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut service = InMemoryKernelService::new();
    service.append(AppendRequest { datoms: routing_history() })?;

    println!("AETHER-Learn Demo 06: Service-backed routing report");
    println!("=====================================================");
    println!("Mapping: task, proposal, decision, outcome, router update, promotion");

    let mapped = run(&mut service, "goal service_record(task, proposal, decision, outcome, update, status)\n  keep task, proposal, decision, outcome, update, status")?;
    print_section("Service-backed records", query_rows(&mapped), "Each row joins the six proof tuple families through the AETHER service.");

    let accepted = run(&mut service, "goal accepted_update(update, task, family, worker, reason)\n  keep update, task, family, worker, reason")?;
    let accepted_rows = query_rows(&accepted).to_vec();
    print_section("Accepted router updates", &accepted_rows, "Promoted local router updates.");

    let retained = run(&mut service, "goal retained_update(update, task, family, worker, reason)\n  keep update, task, family, worker, reason")?;
    print_section("Retained router updates", query_rows(&retained), "Updates retained as evidence for the router.");

    if let Some(tuple_id) = accepted_rows.first().and_then(|row| row.tuple_id) {
        let trace = service.explain_tuple(ExplainTupleRequest { tuple_id, policy_context: None })?.trace;
        print_trace_summary(&trace);
    }

    Ok(())
}

fn run(service: &mut InMemoryKernelService, query_body: &str) -> Result<aether_api::RunDocumentResponse, aether_api::ApiError> {
    service.run_document(RunDocumentRequest { dsl: routing_dsl("current", query_body), policy_context: None })
}

fn query_rows(response: &aether_api::RunDocumentResponse) -> &[QueryRow] {
    response.query.as_ref().expect("query result expected").rows.as_slice()
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
    println!("Proof trace for the first accepted update:");
    println!("  - root tuple: t{}", trace.root.0);
    println!("  - tuples in trace: {}", trace.tuples.len());
    for tuple in &trace.tuples {
        println!(
            "  - t{} via r{} -> {} | sources {}",
            tuple.tuple.id.0,
            tuple.metadata.rule_id.0,
            format_values(&tuple.tuple.values),
            format_elements(&tuple.metadata.source_datom_ids)
        );
    }
}

fn routing_history() -> Vec<Datom> {
    let mut datoms = Vec::new();
    let mut e = 1u64;
    add_case(&mut datoms, &mut e, 101, 201, 301, 401, 501, 601, "A", "simple", "fast_cheap_worker", "accepted_local", "good utility");
    add_case(&mut datoms, &mut e, 102, 202, 302, 402, 502, 602, "B", "math", "math_specialist_worker", "retained_evidence", "needs more evidence");
    add_case(&mut datoms, &mut e, 103, 203, 303, 403, 503, 603, "B", "code", "code_specialist_worker", "accepted_local", "good utility");
    datoms
}

#[allow(clippy::too_many_arguments)]
fn add_case(datoms: &mut Vec<Datom>, e: &mut u64, task: u64, proposal: u64, decision: u64, outcome: u64, update: u64, promotion: u64, phase: &str, family: &str, worker: &str, status: &str, reason: &str) {
    push_str(datoms, task, 1, phase, e);
    push_str(datoms, task, 2, family, e);
    push_entity(datoms, proposal, 3, task, e);
    push_str(datoms, proposal, 4, worker, e);
    push_entity(datoms, decision, 5, task, e);
    push_str(datoms, decision, 6, worker, e);
    push_entity(datoms, outcome, 7, task, e);
    push_str(datoms, outcome, 8, worker, e);
    push_entity(datoms, update, 9, task, e);
    push_str(datoms, update, 10, worker, e);
    push_entity(datoms, promotion, 11, update, e);
    push_str(datoms, promotion, 12, status, e);
    push_str(datoms, promotion, 13, reason, e);
}

fn routing_dsl(view: &str, query_body: &str) -> String {
    format!(r#"
schema v1 {{
  attr task.phase: ScalarLWW<String>
  attr task.family: ScalarLWW<String>
  attr proposal.task: RefScalar<Entity>
  attr proposal.worker: ScalarLWW<String>
  attr decision.task: RefScalar<Entity>
  attr decision.worker: ScalarLWW<String>
  attr outcome.task: RefScalar<Entity>
  attr outcome.worker: ScalarLWW<String>
  attr router_update.task: RefScalar<Entity>
  attr router_update.worker: ScalarLWW<String>
  attr promotion.artifact: RefScalar<Entity>
  attr promotion.status: ScalarLWW<String>
  attr promotion.reason: ScalarLWW<String>
}}

predicates {{
  task_phase(Entity, String)
  task_family(Entity, String)
  proposal_task(Entity, Entity)
  proposal_worker(Entity, String)
  decision_task(Entity, Entity)
  decision_worker(Entity, String)
  outcome_task(Entity, Entity)
  outcome_worker(Entity, String)
  router_update_task(Entity, Entity)
  router_update_worker(Entity, String)
  promotion_artifact(Entity, Entity)
  promotion_status(Entity, String)
  promotion_reason(Entity, String)
  service_record(Entity, Entity, Entity, Entity, Entity, String)
  accepted_update(Entity, Entity, String, String, String)
  retained_update(Entity, Entity, String, String, String)
}}

rules {{
  service_record(task, proposal, decision, outcome, update, status) <- proposal_task(proposal, task), decision_task(decision, task), outcome_task(outcome, task), router_update_task(update, task), promotion_artifact(promotion, update), promotion_status(promotion, status)
  accepted_update(update, task, family, worker, reason) <- router_update_task(update, task), router_update_worker(update, worker), task_family(task, family), promotion_artifact(promotion, update), promotion_status(promotion, "accepted_local"), promotion_reason(promotion, reason)
  retained_update(update, task, family, worker, reason) <- router_update_task(update, task), router_update_worker(update, worker), task_family(task, family), promotion_artifact(promotion, update), promotion_status(promotion, "retained_evidence"), promotion_reason(promotion, reason)
}}

materialize {{
  service_record
  accepted_update
  retained_update
}}

query {{
  {view}
  {query_body}
}}
"#)
}

fn push_str(datoms: &mut Vec<Datom>, entity: u64, attr: u64, value: &str, e: &mut u64) {
    datoms.push(datom(entity, attr, Value::String(value.into()), *e));
    *e += 1;
}

fn push_entity(datoms: &mut Vec<Datom>, entity: u64, attr: u64, value: u64, e: &mut u64) {
    datoms.push(datom(entity, attr, Value::Entity(EntityId::new(value)), *e));
    *e += 1;
}

fn datom(entity: u64, attribute: u64, value: Value, element: u64) -> Datom {
    Datom { entity: EntityId::new(entity), attribute: aether_ast::AttributeId::new(attribute), value, op: aether_ast::OperationKind::Assert, element: aether_ast::ElementId::new(element), replica: aether_ast::ReplicaId::new(1), causal_context: Default::default(), provenance: DatomProvenance::default(), policy: None }
}

fn format_values(values: &[Value]) -> String {
    values.iter().map(format_value).collect::<Vec<_>>().join(", ")
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
        Value::Entity(id) => format!("entity/{}", id.0),
        Value::List(values) => format!("[{}]", format_values(values)),
    }
}

fn format_elements(elements: &[aether_ast::ElementId]) -> String {
    if elements.is_empty() { return "none".into(); }
    elements.iter().map(|element| format!("e{}", element.0)).collect::<Vec<_>>().join(", ")
}
