use crate::{
    pilot::{
        coordination_pilot_dsl, COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT,
        COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
    },
    ApiError, ExplainTupleRequest, HistoryRequest, KernelService, RunDocumentRequest,
};
use aether_ast::{ElementId, PolicyContext, QueryRow, TupleId, Value};
use aether_resolver::ResolveError;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CoordinationPilotReport {
    pub generated_at_ms: u64,
    pub policy_context: Option<PolicyContext>,
    pub history_len: usize,
    pub pre_heartbeat_authorized: Vec<ReportRow>,
    pub as_of_authorized: Vec<ReportRow>,
    pub live_heartbeats: Vec<ReportRow>,
    pub current_authorized: Vec<ReportRow>,
    pub claimable: Vec<ReportRow>,
    pub accepted_outcomes: Vec<ReportRow>,
    pub rejected_outcomes: Vec<ReportRow>,
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
    build_coordination_pilot_report_with_policy(service, None)
}

pub fn build_coordination_pilot_report_with_policy(
    service: &mut impl KernelService,
    policy_context: Option<PolicyContext>,
) -> Result<CoordinationPilotReport, ApiError> {
    let history_len = service
        .history(HistoryRequest {
            policy_context: policy_context.clone(),
        })?
        .datoms
        .len();
    let pre_heartbeat_authorized = run_report_query(
        service,
        RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                &format!("as_of e{}", COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT),
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
            policy_context: policy_context.clone(),
        },
    )?;
    let as_of_authorized = run_report_query(
        service,
        RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
            policy_context: policy_context.clone(),
        },
    )?;
    let live_heartbeats = run_report_query(
        service,
        RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal live_authority(t, worker, epoch, beat)\n  keep t, worker, epoch, beat",
            ),
            policy_context: policy_context.clone(),
        },
    )?;
    let current_authorized = run_report_query(
        service,
        RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            ),
            policy_context: policy_context.clone(),
        },
    )?;
    let claimable = run_report_query(
        service,
        RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal worker_can_claim(t, worker)\n  keep t, worker",
            ),
            policy_context: policy_context.clone(),
        },
    )?;
    let accepted_outcomes = run_report_query(
        service,
        RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal execution_outcome_accepted(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
            ),
            policy_context: policy_context.clone(),
        },
    )?;
    let rejected_outcomes = run_report_query(
        service,
        RunDocumentRequest {
            dsl: coordination_pilot_dsl(
                "current",
                "goal execution_outcome_rejected_stale(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
            ),
            policy_context: policy_context.clone(),
        },
    )?;

    let trace = current_authorized
        .first()
        .and_then(|row| row.tuple_id)
        .map(|tuple_id| -> Result<TraceSummary, ApiError> {
            let trace = service
                .explain_tuple(ExplainTupleRequest {
                    tuple_id,
                    policy_context: policy_context.clone(),
                })?
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
        policy_context,
        history_len,
        pre_heartbeat_authorized: into_report_rows(pre_heartbeat_authorized),
        as_of_authorized: into_report_rows(as_of_authorized),
        live_heartbeats: into_report_rows(live_heartbeats),
        current_authorized: into_report_rows(current_authorized),
        claimable: into_report_rows(claimable),
        accepted_outcomes: into_report_rows(accepted_outcomes),
        rejected_outcomes: into_report_rows(rejected_outcomes),
        trace,
    })
}

pub fn render_coordination_pilot_report_markdown(report: &CoordinationPilotReport) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AETHER Coordination Pilot Report");
    let _ = writeln!(output);
    let _ = writeln!(output, "- Generated at: `{}`", report.generated_at_ms);
    let _ = writeln!(
        output,
        "- Effective policy: `{}`",
        format_policy_context(report.policy_context.as_ref())
    );
    let _ = writeln!(output, "- Journal entries: `{}`", report.history_len);
    let _ = writeln!(output);

    render_row_section(
        &mut output,
        &format!(
            "Authorization Before Heartbeat At AsOf(e{})",
            COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT
        ),
        &report.pre_heartbeat_authorized,
    );
    render_row_section(
        &mut output,
        &format!(
            "Authorization At AsOf(e{})",
            COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT
        ),
        &report.as_of_authorized,
    );
    render_row_section(
        &mut output,
        "Current Live Heartbeats",
        &report.live_heartbeats,
    );
    render_row_section(
        &mut output,
        "Authorization At Current",
        &report.current_authorized,
    );
    render_row_section(&mut output, "Current Claimable Work", &report.claimable);
    render_row_section(
        &mut output,
        "Current Accepted Outcomes",
        &report.accepted_outcomes,
    );
    render_row_section(
        &mut output,
        "Current Rejected Outcomes",
        &report.rejected_outcomes,
    );

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

fn format_policy_context(policy_context: Option<&PolicyContext>) -> String {
    match policy_context {
        None => "public".into(),
        Some(policy_context) => {
            let capabilities = if policy_context.capabilities.is_empty() {
                "-".into()
            } else {
                policy_context.capabilities.join(", ")
            };
            let visibilities = if policy_context.visibilities.is_empty() {
                "-".into()
            } else {
                policy_context.visibilities.join(", ")
            };
            format!("capabilities=[{capabilities}] visibilities=[{visibilities}]")
        }
    }
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

fn run_report_query(
    service: &mut impl KernelService,
    request: RunDocumentRequest,
) -> Result<Vec<QueryRow>, ApiError> {
    match service.run_document(request) {
        Ok(response) => Ok(response.query.unwrap_or_default().rows),
        Err(ApiError::Resolve(ResolveError::UnknownElementId(_))) => Ok(Vec::new()),
        Err(error) => Err(error),
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

#[cfg(test)]
mod tests {
    use super::{build_coordination_pilot_report, build_coordination_pilot_report_with_policy};
    use crate::{
        coordination_pilot_seed_history, AppendRequest, InMemoryKernelService, KernelService,
    };
    use aether_ast::{EntityId, PolicyContext, PolicyEnvelope, Value};

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

        assert_eq!(report.history_len, 25);
        assert!(report.pre_heartbeat_authorized.is_empty());
        assert_eq!(
            report.as_of_authorized[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into()),
                Value::U64(1),
            ]
        );
        assert_eq!(
            report.live_heartbeats[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into()),
                Value::U64(2),
                Value::U64(200),
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
        assert_eq!(
            report.accepted_outcomes[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into()),
                Value::U64(2),
                Value::String("completed".into()),
                Value::String("current-worker-b".into()),
            ]
        );
        assert_eq!(
            report.rejected_outcomes[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into()),
                Value::U64(1),
                Value::String("completed".into()),
                Value::String("stale-worker-a".into()),
            ]
        );
        assert!(
            report
                .trace
                .as_ref()
                .map(|trace| trace.tuple_count)
                .unwrap_or(0)
                > 0
        );
    }

    #[test]
    fn coordination_pilot_report_respects_policy_context() {
        let mut service = InMemoryKernelService::new();
        let mut datoms = coordination_pilot_seed_history();
        for datom in &mut datoms {
            if datom.element.0 >= 6 {
                datom.policy = Some(PolicyEnvelope {
                    capability: Some("executor".into()),
                    visibility: None,
                });
            }
        }
        service
            .append(AppendRequest { datoms })
            .expect("append policy-filtered seed history");

        let public_report = build_coordination_pilot_report_with_policy(&mut service, None)
            .expect("build public coordination report");
        assert_eq!(public_report.policy_context, None);
        assert_eq!(public_report.history_len, 5);
        assert!(public_report.as_of_authorized.is_empty());
        assert!(public_report.current_authorized.is_empty());
        assert!(public_report.accepted_outcomes.is_empty());
        assert!(public_report.trace.is_none());

        let executor_report = build_coordination_pilot_report_with_policy(
            &mut service,
            Some(PolicyContext {
                capabilities: vec!["executor".into()],
                visibilities: Vec::new(),
            }),
        )
        .expect("build executor coordination report");
        assert_eq!(executor_report.history_len, 25);
        assert_eq!(
            executor_report.current_authorized[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into()),
                Value::U64(2),
            ]
        );
        assert!(executor_report.trace.is_some());
    }
}
