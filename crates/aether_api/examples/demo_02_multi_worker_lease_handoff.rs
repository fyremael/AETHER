use aether_api::{
    coordination_pilot_dsl, coordination_pilot_seed_history, AppendRequest, ExplainTupleRequest,
    HistoryRequest, InMemoryKernelService, KernelService, RunDocumentRequest,
    COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT, COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
};
use aether_ast::{Datom, DerivationTrace, QueryRow, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut service = InMemoryKernelService::new();
    let datoms = coordination_pilot_seed_history();
    service.append(AppendRequest {
        datoms: datoms.clone(),
    })?;

    println!("AETHER Demo 02: Multi-Worker Lease Handoff");
    println!("==========================================");
    println!();
    println!("Journal:");
    for datom in service.history(HistoryRequest)?.datoms {
        println!("  - {}", describe_datom(&datom));
    }

    let pre_heartbeat = service.run_document(RunDocumentRequest {
        dsl: coordination_pilot_dsl(
            &format!("as_of e{}", COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT),
            "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
        ),
    })?;
    print_query(
        "Authorization before heartbeat at AsOf(e5)",
        pre_heartbeat
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
    );

    let as_of = service.run_document(RunDocumentRequest {
        dsl: coordination_pilot_dsl(
            &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
            "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
        ),
    })?;
    print_query(
        "Authorized execution after heartbeat at AsOf(e9)",
        as_of
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
    );

    let heartbeats = service.run_document(RunDocumentRequest {
        dsl: coordination_pilot_dsl(
            "current",
            "goal live_authority(t, worker, epoch, beat)\n  keep t, worker, epoch, beat",
        ),
    })?;
    print_query(
        "Live heartbeats at Current",
        heartbeats
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
    );

    let current_authorized = service.run_document(RunDocumentRequest {
        dsl: coordination_pilot_dsl(
            "current",
            "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
        ),
    })?;
    let authorized_rows = current_authorized
        .query
        .as_ref()
        .expect("query should exist")
        .rows
        .clone();
    print_query(
        "Authorized execution at Current",
        authorized_rows.as_slice(),
    );

    if let Some(tuple_id) = authorized_rows.first().and_then(|row| row.tuple_id) {
        let trace = service
            .explain_tuple(ExplainTupleRequest { tuple_id })?
            .trace;
        print_trace_summary(&trace);
    }

    let claimable = service.run_document(RunDocumentRequest {
        dsl: coordination_pilot_dsl(
            "current",
            "goal worker_can_claim(t, worker)\n  keep t, worker",
        ),
    })?;
    print_query(
        "Claimable tasks at Current",
        claimable
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
    );

    let accepted = service.run_document(RunDocumentRequest {
        dsl: coordination_pilot_dsl(
            "current",
            "goal execution_outcome_accepted(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
        ),
    })?;
    print_query(
        "Accepted outcomes at Current",
        accepted
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
    );

    let stale = service.run_document(RunDocumentRequest {
        dsl: coordination_pilot_dsl(
            "current",
            "goal execution_outcome_rejected_stale(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
        ),
    })?;
    print_query(
        "Fenced stale outcomes at Current",
        stale
            .query
            .as_ref()
            .expect("query should exist")
            .rows
            .as_slice(),
    );

    Ok(())
}

fn print_query(title: &str, rows: &[QueryRow]) {
    println!();
    println!("{title}:");
    if rows.is_empty() {
        println!("  - none");
        return;
    }

    for row in rows {
        println!("  - {}", format_values(&row.values));
    }
}

fn print_trace_summary(trace: &DerivationTrace) {
    println!();
    println!("Proof trace for current authorized execution:");
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
        6 => format!(
            "e{}: heartbeat/{id} task = task/{}",
            datom.element.0,
            entity_value(&datom.value),
            id = datom.entity.0
        ),
        7 => format!(
            "e{}: heartbeat/{id} worker = {}",
            datom.element.0,
            string_value(&datom.value),
            id = datom.entity.0
        ),
        8 => format!(
            "e{}: heartbeat/{id} epoch = {}",
            datom.element.0,
            u64_value(&datom.value),
            id = datom.entity.0
        ),
        9 => format!(
            "e{}: heartbeat/{id} at = {}",
            datom.element.0,
            u64_value(&datom.value),
            id = datom.entity.0
        ),
        10 => format!(
            "e{}: outcome/{id} task = task/{}",
            datom.element.0,
            entity_value(&datom.value),
            id = datom.entity.0
        ),
        11 => format!(
            "e{}: outcome/{id} worker = {}",
            datom.element.0,
            string_value(&datom.value),
            id = datom.entity.0
        ),
        12 => format!(
            "e{}: outcome/{id} epoch = {}",
            datom.element.0,
            u64_value(&datom.value),
            id = datom.entity.0
        ),
        13 => format!(
            "e{}: outcome/{id} status = {}",
            datom.element.0,
            string_value(&datom.value),
            id = datom.entity.0
        ),
        14 => format!(
            "e{}: outcome/{id} detail = {}",
            datom.element.0,
            string_value(&datom.value),
            id = datom.entity.0
        ),
        _ => format!(
            "e{}: {subject} -> {}",
            datom.element.0,
            format_value(&datom.value)
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
