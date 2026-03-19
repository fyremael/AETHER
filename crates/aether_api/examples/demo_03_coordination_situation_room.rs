use aether_api::{
    AppendRequest, ExplainTupleRequest, HistoryRequest, InMemoryKernelService, KernelService,
    RunDocumentRequest,
};
use aether_ast::{Datom, DatomProvenance, DerivationTrace, EntityId, QueryRow, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut service = InMemoryKernelService::new();
    service.append(AppendRequest {
        datoms: showcase_history(),
    })?;

    println!("AETHER Demo 03: Coordination Situation Room");
    println!("===========================================");
    println!();
    println!("This showcase compresses the kernel's current strengths into one run:");
    println!("  - recursive dependency closure");
    println!("  - temporal replay with AsOf views");
    println!("  - readiness and claimability");
    println!("  - lease handoff and stale fencing");
    println!("  - service-backed explanation");
    println!();
    println!("Journal:");
    for datom in service.history(HistoryRequest)?.datoms {
        println!("  - {}", describe_datom(&datom));
    }

    let before_completion = service.run_document(RunDocumentRequest {
        dsl: showcase_dsl(
            "as_of e2",
            "goal depends_transitive(task, dep)\n  keep task, dep",
        ),
    })?;
    print_section(
        "Act I: Before completion closes the chain (AsOf e2)",
        before_completion
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "Recursive closure already knows that task/1 reaches task/3 through task/2.",
    );

    let blocked = service.run_document(RunDocumentRequest {
        dsl: showcase_dsl("as_of e2", "goal dependency_blocked(task)\n  keep task"),
    })?;
    print_section(
        "Blocked work at AsOf e2",
        blocked
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "Both task/1 and task/2 are still blocked because the completion wave has not landed yet.",
    );

    let claim_window = service.run_document(RunDocumentRequest {
        dsl: showcase_dsl(
            "as_of e4",
            "goal worker_can_claim(task, worker)\n  keep task, worker",
        ),
    })?;
    print_section(
        "Act II: Claim window opens (AsOf e4)",
        claim_window
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "Once task/2 is completed, task/1 becomes claimable. Task/4 is claimable throughout because it is independent and unclaimed.",
    );

    let worker_a = service.run_document(RunDocumentRequest {
        dsl: showcase_dsl(
            "as_of e7",
            "goal execution_authorized(task, worker, epoch)\n  keep task, worker, epoch",
        ),
    })?;
    print_section(
        "Act III: Worker A is authoritative (AsOf e7)",
        worker_a
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "At this cut, the active lease authorizes only worker-a at epoch 1.",
    );

    let worker_b = service.run_document(RunDocumentRequest {
        dsl: showcase_dsl(
            "current",
            "goal execution_authorized(task, worker, epoch)\n  keep task, worker, epoch",
        ),
    })?;
    let authorized_rows = worker_b
        .query
        .as_ref()
        .expect("query should exist")
        .rows
        .clone();
    print_section(
        "Act IV: Worker B takes over (Current)",
        authorized_rows.as_slice(),
        "The same rules now authorize worker-b at epoch 2 because the journal advanced.",
    );

    if let Some(tuple_id) = authorized_rows.first().and_then(|row| row.tuple_id) {
        let trace = service
            .explain_tuple(ExplainTupleRequest { tuple_id })?
            .trace;
        print_trace_summary(&trace);
    }

    let fenced = service.run_document(RunDocumentRequest {
        dsl: showcase_dsl(
            "current",
            "goal execution_rejected_stale(task, worker, epoch)\n  keep task, worker, epoch",
        ),
    })?;
    print_section(
        "Fenced stale work at Current",
        fenced
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "The kernel separates work that merely happened from work that is still semantically valid.",
    );

    let current_claimable = service.run_document(RunDocumentRequest {
        dsl: showcase_dsl(
            "current",
            "goal worker_can_claim(task, worker)\n  keep task, worker",
        ),
    })?;
    print_section(
        "Open work at Current",
        current_claimable
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
        "Task/1 is no longer open because it is under an active lease. Task/4 remains available to both workers.",
    );

    println!();
    println!("Bottom line:");
    println!("  - one append-only history");
    println!("  - one DSL-authored program");
    println!("  - multiple principled semantic answers at different cut points");
    println!("  - one proof trace for why the current answer is true");

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
    println!("Proof trace for the current authorized execution:");
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
}

fn describe_datom(datom: &Datom) -> String {
    let subject = format!("task/{}", datom.entity.0);
    match datom.attribute.0 {
        1 => format!(
            "e{}: {subject} depends_on task/{}",
            datom.element.0,
            entity_value(&datom.value)
        ),
        2 => format!(
            "e{}: {subject} status = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        3 => format!(
            "e{}: {subject} claimed_by = {}",
            datom.element.0,
            string_value(&datom.value)
        ),
        4 => format!(
            "e{}: {subject} lease_epoch = {}",
            datom.element.0,
            u64_value(&datom.value)
        ),
        5 => format!(
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

fn showcase_history() -> Vec<Datom> {
    vec![
        dependency_datom(1, 2, 1),
        dependency_datom(2, 3, 2),
        datom(3, 2, Value::String("done".into()), 3),
        datom(2, 2, Value::String("done".into()), 4),
        datom(1, 3, Value::String("worker-a".into()), 5),
        datom(1, 4, Value::U64(1), 6),
        datom(1, 5, Value::String("active".into()), 7),
        datom(1, 3, Value::String("worker-b".into()), 8),
        datom(1, 4, Value::U64(2), 9),
    ]
}

fn showcase_dsl(view: &str, query_body: &str) -> String {
    format!(
        r#"
schema v1 {{
  attr task.depends_on: RefSet<Entity>
  attr task.status: ScalarLWW<String>
  attr task.claimed_by: ScalarLWW<String>
  attr task.lease_epoch: ScalarLWW<U64>
  attr task.lease_state: ScalarLWW<String>
}}

predicates {{
  task(Entity)
  worker(String)
  worker_capability(String, String)
  execution_attempt(Entity, String, U64)
  task_depends_on(Entity, Entity)
  task_status(Entity, String)
  task_claimed_by(Entity, String)
  task_lease_epoch(Entity, U64)
  task_lease_state(Entity, String)
  depends_transitive(Entity, Entity)
  task_complete(Entity)
  dependency_blocked(Entity)
  lease_active(Entity, String, U64)
  active_claim(Entity)
  task_ready(Entity)
  worker_can_claim(Entity, String)
  execution_authorized(Entity, String, U64)
  execution_rejected_stale(Entity, String, U64)
}}

facts {{
  task(entity(1))
  task(entity(2))
  task(entity(3))
  task(entity(4))
  worker("worker-a")
  worker("worker-b")
  worker_capability("worker-a", "executor")
  worker_capability("worker-b", "executor")
  execution_attempt(entity(1), "worker-a", 1)
  execution_attempt(entity(1), "worker-b", 1)
  execution_attempt(entity(1), "worker-a", 2)
  execution_attempt(entity(1), "worker-b", 2) @capability("executor") @visibility("ops")
}}

rules {{
  depends_transitive(t, dep) <- task_depends_on(t, dep)
  depends_transitive(t, dep) <- task_depends_on(t, mid), depends_transitive(mid, dep)
  task_complete(t) <- task_status(t, "done")
  dependency_blocked(t) <- depends_transitive(t, dep), not task_complete(dep)
  lease_active(t, w, epoch) <- task_claimed_by(t, w), task_lease_epoch(t, epoch), task_lease_state(t, "active")
  active_claim(t) <- lease_active(t, w, epoch)
  task_ready(t) <- task(t), not task_complete(t), not dependency_blocked(t), not active_claim(t)
  worker_can_claim(t, w) <- task_ready(t), worker(w), worker_capability(w, "executor")
  execution_authorized(t, w, epoch) <- execution_attempt(t, w, epoch), lease_active(t, w, epoch)
  execution_rejected_stale(t, w, epoch) <- execution_attempt(t, w, epoch), not lease_active(t, w, epoch)
}}

materialize {{
  depends_transitive
  dependency_blocked
  task_ready
  worker_can_claim
  execution_authorized
  execution_rejected_stale
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
        Value::Entity(id) => format!("task/{}", id.0),
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

fn dependency_datom(entity: u64, value: u64, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: aether_ast::AttributeId::new(1),
        value: Value::Entity(EntityId::new(value)),
        op: aether_ast::OperationKind::Add,
        element: aether_ast::ElementId::new(element),
        replica: aether_ast::ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn datom(entity: u64, attribute: u64, value: Value, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: aether_ast::AttributeId::new(attribute),
        value,
        op: aether_ast::OperationKind::Assert,
        element: aether_ast::ElementId::new(element),
        replica: aether_ast::ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}
