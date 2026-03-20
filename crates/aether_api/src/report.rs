use crate::{ApiError, ExplainTupleRequest, HistoryRequest, KernelService, RunDocumentRequest};
use aether_ast::{Datom, ElementId, QueryRow, TupleId, Value};
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CoordinationPilotReport {
    pub generated_at_ms: u64,
    pub history_len: usize,
    pub as_of_authorized: Vec<ReportRow>,
    pub current_authorized: Vec<ReportRow>,
    pub claimable: Vec<ReportRow>,
    pub stale: Vec<ReportRow>,
    pub trace: Option<TraceSummary>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReportRow {
    pub tuple_id: Option<TupleId>,
    pub values: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TraceSummary {
    pub root: TupleId,
    pub tuple_count: usize,
    pub tuples: Vec<TraceTupleSummary>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TraceTupleSummary {
    pub tuple_id: TupleId,
    pub values: Vec<Value>,
    pub iteration: usize,
    pub source_datom_ids: Vec<ElementId>,
    pub parent_tuple_ids: Vec<TupleId>,
}

pub fn build_coordination_pilot_report(
    service: &mut impl KernelService,
) -> Result<CoordinationPilotReport, ApiError> {
    let history_len = service.history(HistoryRequest)?.datoms.len();
    let as_of_authorized = service
        .run_document(RunDocumentRequest {
            dsl: coordination_dsl(
                "as_of e5",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
        })?
        .query
        .unwrap_or_default()
        .rows;
    let current_authorized = service
        .run_document(RunDocumentRequest {
            dsl: coordination_dsl(
                "current",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
        })?
        .query
        .unwrap_or_default()
        .rows;
    let claimable = service
        .run_document(RunDocumentRequest {
            dsl: coordination_dsl(
                "current",
                "goal worker_can_claim(t, worker)\n  keep t, worker",
            ),
        })?
        .query
        .unwrap_or_default()
        .rows;
    let stale = service
        .run_document(RunDocumentRequest {
            dsl: coordination_dsl(
                "current",
                "goal execution_rejected_stale(t, worker, epoch)\n  keep t, worker, epoch",
            ),
        })?
        .query
        .unwrap_or_default()
        .rows;

    let trace = current_authorized
        .first()
        .and_then(|row| row.tuple_id)
        .map(|tuple_id| -> Result<TraceSummary, ApiError> {
            let trace = service
                .explain_tuple(ExplainTupleRequest { tuple_id })?
                .trace;
            Ok(TraceSummary {
                root: trace.root,
                tuple_count: trace.tuples.len(),
                tuples: trace
                    .tuples
                    .into_iter()
                    .map(|tuple| TraceTupleSummary {
                        tuple_id: tuple.tuple.id,
                        values: tuple.tuple.values,
                        iteration: tuple.metadata.iteration,
                        source_datom_ids: tuple.metadata.source_datom_ids,
                        parent_tuple_ids: tuple.metadata.parent_tuple_ids,
                    })
                    .collect(),
            })
        })
        .transpose()?;

    Ok(CoordinationPilotReport {
        generated_at_ms: now_millis(),
        history_len,
        as_of_authorized: into_report_rows(as_of_authorized),
        current_authorized: into_report_rows(current_authorized),
        claimable: into_report_rows(claimable),
        stale: into_report_rows(stale),
        trace,
    })
}

pub fn render_coordination_pilot_report_markdown(report: &CoordinationPilotReport) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AETHER Coordination Pilot Report");
    let _ = writeln!(output);
    let _ = writeln!(output, "- Generated at: `{}`", report.generated_at_ms);
    let _ = writeln!(output, "- Journal entries: `{}`", report.history_len);
    let _ = writeln!(output);

    render_row_section(
        &mut output,
        "Authorization At AsOf(e5)",
        &report.as_of_authorized,
    );
    render_row_section(
        &mut output,
        "Authorization At Current",
        &report.current_authorized,
    );
    render_row_section(&mut output, "Current Claimable Work", &report.claimable);
    render_row_section(&mut output, "Current Stale Rejections", &report.stale);

    let _ = writeln!(output, "## Proof Trace");
    let _ = writeln!(output);
    match &report.trace {
        Some(trace) => {
            let _ = writeln!(output, "- Root tuple: `{}`", trace.root.0);
            let _ = writeln!(output, "- Tuples in trace: `{}`", trace.tuple_count);
            let _ = writeln!(output);
            for tuple in &trace.tuples {
                let _ = writeln!(
                    output,
                    "- `t{}` | values `{}` | iteration `{}` | sources `{}` | parents `{}`",
                    tuple.tuple_id.0,
                    format_values(&tuple.values),
                    tuple.iteration,
                    format_element_ids(&tuple.source_datom_ids),
                    format_tuple_ids(&tuple.parent_tuple_ids),
                );
            }
        }
        None => {
            let _ = writeln!(
                output,
                "No current authorization tuple was available to explain."
            );
        }
    }

    output
}

pub fn coordination_pilot_seed_history() -> Vec<Datom> {
    vec![
        dependency_datom(1, 2, 1),
        datom(2, 2, Value::String("done".into()), 2),
        datom(1, 3, Value::String("worker-a".into()), 3),
        datom(1, 4, Value::U64(1), 4),
        datom(1, 5, Value::String("active".into()), 5),
        datom(1, 3, Value::String("worker-b".into()), 6),
        datom(1, 4, Value::U64(2), 7),
    ]
}

fn into_report_rows(rows: Vec<QueryRow>) -> Vec<ReportRow> {
    rows.into_iter()
        .map(|row| ReportRow {
            tuple_id: row.tuple_id,
            values: row.values,
        })
        .collect()
}

fn render_row_section(output: &mut String, title: &str, rows: &[ReportRow]) {
    let _ = writeln!(output, "## {title}");
    let _ = writeln!(output);
    if rows.is_empty() {
        let _ = writeln!(output, "No rows.");
        let _ = writeln!(output);
        return;
    }

    for row in rows {
        let tuple_id = row
            .tuple_id
            .map(|tuple_id| format!("t{}", tuple_id.0))
            .unwrap_or_else(|| "-".into());
        let _ = writeln!(
            output,
            "- `{}` | `{}`",
            tuple_id,
            format_values(&row.values)
        );
    }
    let _ = writeln!(output);
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
        Value::String(value) => format!("\"{value}\""),
        Value::Bytes(bytes) => format!("{bytes:?}"),
        Value::Entity(entity) => format!("entity({})", entity.0),
        Value::List(values) => format!("[{}]", format_values(values)),
    }
}

fn format_element_ids(ids: &[ElementId]) -> String {
    if ids.is_empty() {
        return "-".into();
    }
    ids.iter()
        .map(|id| format!("e{}", id.0))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_tuple_ids(ids: &[TupleId]) -> String {
    if ids.is_empty() {
        return "-".into();
    }
    ids.iter()
        .map(|id| format!("t{}", id.0))
        .collect::<Vec<_>>()
        .join(", ")
}

fn now_millis() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn coordination_dsl(view: &str, query_body: &str) -> String {
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
  task_complete(t) <- task_status(t, "done")
  dependency_blocked(t) <- task_depends_on(t, dep), not task_complete(dep)
  lease_active(t, w, epoch) <- task_claimed_by(t, w), task_lease_epoch(t, epoch), task_lease_state(t, "active")
  active_claim(t) <- lease_active(t, w, epoch)
  task_ready(t) <- task(t), not task_complete(t), not dependency_blocked(t), not active_claim(t)
  worker_can_claim(t, w) <- task_ready(t), worker(w), worker_capability(w, "executor")
  execution_authorized(t, w, epoch) <- execution_attempt(t, w, epoch), lease_active(t, w, epoch)
  execution_rejected_stale(t, worker, epoch) <- execution_attempt(t, worker, epoch), not lease_active(t, worker, epoch)
}}

materialize {{
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

fn dependency_datom(entity: u64, value: u64, element: u64) -> Datom {
    Datom {
        entity: aether_ast::EntityId::new(entity),
        attribute: aether_ast::AttributeId::new(1),
        value: Value::Entity(aether_ast::EntityId::new(value)),
        op: aether_ast::OperationKind::Add,
        element: ElementId::new(element),
        replica: aether_ast::ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: aether_ast::DatomProvenance::default(),
        policy: None,
    }
}

fn datom(entity: u64, attribute: u64, value: Value, element: u64) -> Datom {
    Datom {
        entity: aether_ast::EntityId::new(entity),
        attribute: aether_ast::AttributeId::new(attribute),
        value,
        op: aether_ast::OperationKind::Assert,
        element: ElementId::new(element),
        replica: aether_ast::ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: aether_ast::DatomProvenance::default(),
        policy: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{build_coordination_pilot_report, coordination_pilot_seed_history};
    use crate::{AppendRequest, InMemoryKernelService, KernelService};
    use aether_ast::{EntityId, Value};

    #[test]
    fn coordination_pilot_report_captures_expected_answers() {
        let mut service = InMemoryKernelService::new();
        service
            .append(AppendRequest {
                datoms: coordination_pilot_seed_history(),
            })
            .expect("append seed history");

        let report =
            build_coordination_pilot_report(&mut service).expect("build coordination report");

        assert_eq!(report.history_len, 7);
        assert_eq!(
            report.as_of_authorized[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into()),
                Value::U64(1),
            ]
        );
        assert_eq!(
            report.current_authorized[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into()),
                Value::U64(2),
            ]
        );
        assert_eq!(report.claimable.len(), 2);
        assert_eq!(report.stale.len(), 3);
        assert!(
            report
                .trace
                .as_ref()
                .map(|trace| trace.tuple_count)
                .unwrap_or(0)
                > 0
        );
    }
}
