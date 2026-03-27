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
use std::collections::{BTreeMap, VecDeque};
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

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoordinationCut {
    #[default]
    Current,
    AsOf {
        element: ElementId,
    },
}

impl CoordinationCut {
    fn view_label(&self) -> String {
        match self {
            Self::Current => "current".into(),
            Self::AsOf { element } => format!("as_of e{}", element.0),
        }
    }

    fn human_label(&self) -> String {
        match self {
            Self::Current => "Current".into(),
            Self::AsOf { element } => format!("AsOf(e{})", element.0),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoordinationDeltaReportRequest {
    #[serde(default)]
    pub left: CoordinationCut,
    #[serde(default)]
    pub right: CoordinationCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoordinationTraceHandle {
    pub tuple_id: TupleId,
    pub tuple_count: usize,
    pub source_datom_ids: Vec<ElementId>,
    pub parent_tuple_ids: Vec<TupleId>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReportRowDiff {
    pub row: ReportRow,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<CoordinationTraceHandle>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReportRowChange {
    pub before: ReportRow,
    pub after: ReportRow,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_trace: Option<CoordinationTraceHandle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_trace: Option<CoordinationTraceHandle>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReportSectionDelta {
    pub added: Vec<ReportRowDiff>,
    pub removed: Vec<ReportRowDiff>,
    pub changed: Vec<ReportRowChange>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CoordinationDeltaReport {
    pub generated_at_ms: u64,
    pub left: CoordinationCut,
    pub right: CoordinationCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
    pub left_history_len: usize,
    pub right_history_len: usize,
    pub current_authorized: ReportSectionDelta,
    pub claimable: ReportSectionDelta,
    pub live_heartbeats: ReportSectionDelta,
    pub accepted_outcomes: ReportSectionDelta,
    pub rejected_outcomes: ReportSectionDelta,
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

pub fn build_coordination_delta_report(
    service: &mut impl KernelService,
    request: CoordinationDeltaReportRequest,
) -> Result<CoordinationDeltaReport, ApiError> {
    let policy_context = request.policy_context.clone();
    let left = build_coordination_snapshot(service, &request.left, request.policy_context.clone())?;
    let right =
        build_coordination_snapshot(service, &request.right, request.policy_context.clone())?;

    Ok(CoordinationDeltaReport {
        generated_at_ms: now_millis(),
        left: request.left,
        right: request.right,
        policy_context: policy_context.clone(),
        left_history_len: left.history_len,
        right_history_len: right.history_len,
        current_authorized: diff_report_rows(
            service,
            left.current_authorized,
            right.current_authorized,
            policy_context.as_ref(),
        )?,
        claimable: diff_report_rows(
            service,
            left.claimable,
            right.claimable,
            policy_context.as_ref(),
        )?,
        live_heartbeats: diff_report_rows(
            service,
            left.live_heartbeats,
            right.live_heartbeats,
            policy_context.as_ref(),
        )?,
        accepted_outcomes: diff_report_rows(
            service,
            left.accepted_outcomes,
            right.accepted_outcomes,
            policy_context.as_ref(),
        )?,
        rejected_outcomes: diff_report_rows(
            service,
            left.rejected_outcomes,
            right.rejected_outcomes,
            policy_context.as_ref(),
        )?,
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

pub fn render_coordination_delta_report_markdown(report: &CoordinationDeltaReport) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AETHER Coordination Delta Report");
    let _ = writeln!(output);
    let _ = writeln!(output, "- Generated at: `{}`", report.generated_at_ms);
    let _ = writeln!(output, "- Left cut: `{}`", report.left.human_label());
    let _ = writeln!(output, "- Right cut: `{}`", report.right.human_label());
    let _ = writeln!(
        output,
        "- Effective policy: `{}`",
        format_policy_context(report.policy_context.as_ref())
    );
    let _ = writeln!(
        output,
        "- Left journal entries: `{}`",
        report.left_history_len
    );
    let _ = writeln!(
        output,
        "- Right journal entries: `{}`",
        report.right_history_len
    );
    let _ = writeln!(output);

    render_delta_section(
        &mut output,
        "Authorization Delta",
        &report.current_authorized,
    );
    render_delta_section(&mut output, "Claimable Work Delta", &report.claimable);
    render_delta_section(
        &mut output,
        "Live Heartbeats Delta",
        &report.live_heartbeats,
    );
    render_delta_section(
        &mut output,
        "Accepted Outcomes Delta",
        &report.accepted_outcomes,
    );
    render_delta_section(
        &mut output,
        "Rejected Outcomes Delta",
        &report.rejected_outcomes,
    );

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

fn render_delta_section(output: &mut String, title: &str, delta: &ReportSectionDelta) {
    let _ = writeln!(output, "## {title}");
    let _ = writeln!(output);
    if delta.added.is_empty() && delta.removed.is_empty() && delta.changed.is_empty() {
        let _ = writeln!(output, "No changes.");
        let _ = writeln!(output);
        return;
    }

    render_diff_rows(output, "Added", &delta.added);
    render_diff_rows(output, "Removed", &delta.removed);
    render_changed_rows(output, &delta.changed);
}

fn render_diff_rows(output: &mut String, title: &str, rows: &[ReportRowDiff]) {
    if rows.is_empty() {
        return;
    }
    let _ = writeln!(output, "### {title}");
    let _ = writeln!(output);
    for row in rows {
        let tuple = row
            .row
            .tuple_id
            .map(|tuple_id| format!("t{}", tuple_id.0))
            .unwrap_or_else(|| "-".into());
        let _ = writeln!(
            output,
            "- `{}` | `{}`",
            tuple,
            format_values(&row.row.values)
        );
        if let Some(trace) = &row.trace {
            let _ = writeln!(
                output,
                "  trace `t{}` | tuples `{}` | sources `{}` | parents `{}`",
                trace.tuple_id.0,
                trace.tuple_count,
                format_element_ids(&trace.source_datom_ids),
                format_tuple_ids(&trace.parent_tuple_ids),
            );
        }
    }
    let _ = writeln!(output);
}

fn render_changed_rows(output: &mut String, rows: &[ReportRowChange]) {
    if rows.is_empty() {
        return;
    }
    let _ = writeln!(output, "### Changed");
    let _ = writeln!(output);
    for row in rows {
        let _ = writeln!(
            output,
            "- before `{}` | after `{}`",
            format_values(&row.before.values),
            format_values(&row.after.values),
        );
        if let Some(trace) = &row.before_trace {
            let _ = writeln!(
                output,
                "  before trace `t{}` | tuples `{}` | sources `{}` | parents `{}`",
                trace.tuple_id.0,
                trace.tuple_count,
                format_element_ids(&trace.source_datom_ids),
                format_tuple_ids(&trace.parent_tuple_ids),
            );
        }
        if let Some(trace) = &row.after_trace {
            let _ = writeln!(
                output,
                "  after trace `t{}` | tuples `{}` | sources `{}` | parents `{}`",
                trace.tuple_id.0,
                trace.tuple_count,
                format_element_ids(&trace.source_datom_ids),
                format_tuple_ids(&trace.parent_tuple_ids),
            );
        }
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

#[derive(Clone, Debug, Default)]
struct CoordinationSnapshot {
    history_len: usize,
    current_authorized: Vec<ReportRow>,
    claimable: Vec<ReportRow>,
    live_heartbeats: Vec<ReportRow>,
    accepted_outcomes: Vec<ReportRow>,
    rejected_outcomes: Vec<ReportRow>,
}

fn build_coordination_snapshot(
    service: &mut impl KernelService,
    cut: &CoordinationCut,
    policy_context: Option<PolicyContext>,
) -> Result<CoordinationSnapshot, ApiError> {
    let history_len = run_report_history_len(service, cut, policy_context.clone())?;
    Ok(CoordinationSnapshot {
        history_len,
        current_authorized: into_report_rows(run_report_query_for_cut(
            service,
            cut,
            "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
            policy_context.clone(),
        )?),
        claimable: into_report_rows(run_report_query_for_cut(
            service,
            cut,
            "goal worker_can_claim(t, worker)\n  keep t, worker",
            policy_context.clone(),
        )?),
        live_heartbeats: into_report_rows(run_report_query_for_cut(
            service,
            cut,
            "goal live_authority(t, worker, epoch, beat)\n  keep t, worker, epoch, beat",
            policy_context.clone(),
        )?),
        accepted_outcomes: into_report_rows(run_report_query_for_cut(
            service,
            cut,
            "goal execution_outcome_accepted(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
            policy_context.clone(),
        )?),
        rejected_outcomes: into_report_rows(run_report_query_for_cut(
            service,
            cut,
            "goal execution_outcome_rejected_stale(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
            policy_context,
        )?),
    })
}

fn run_report_history_len(
    service: &mut impl KernelService,
    cut: &CoordinationCut,
    policy_context: Option<PolicyContext>,
) -> Result<usize, ApiError> {
    let history = service.history(HistoryRequest {
        policy_context: policy_context.clone(),
    })?;
    match cut {
        CoordinationCut::Current => Ok(history.datoms.len()),
        CoordinationCut::AsOf { element } => history
            .datoms
            .iter()
            .position(|datom| datom.element == *element)
            .map(|index| index + 1)
            .ok_or_else(|| ApiError::Validation(format!("unknown element {}", element.0))),
    }
}

fn run_report_query_for_cut(
    service: &mut impl KernelService,
    cut: &CoordinationCut,
    query_body: &str,
    policy_context: Option<PolicyContext>,
) -> Result<Vec<QueryRow>, ApiError> {
    run_report_query(
        service,
        RunDocumentRequest {
            dsl: coordination_pilot_dsl(&cut.view_label(), query_body),
            policy_context,
        },
    )
}

fn diff_report_rows(
    service: &mut impl KernelService,
    left: Vec<ReportRow>,
    right: Vec<ReportRow>,
    policy_context: Option<&PolicyContext>,
) -> Result<ReportSectionDelta, ApiError> {
    let mut left_exact = rows_by_signature(left);
    let mut right_exact = rows_by_signature(right);
    let exact_keys = left_exact
        .keys()
        .filter(|signature| right_exact.contains_key(*signature))
        .cloned()
        .collect::<Vec<_>>();
    for signature in exact_keys {
        let left_rows = left_exact
            .get_mut(&signature)
            .expect("left signature exists");
        let right_rows = right_exact
            .get_mut(&signature)
            .expect("right signature exists");
        let pair_count = left_rows.len().min(right_rows.len());
        for _ in 0..pair_count {
            left_rows.pop_front();
            right_rows.pop_front();
        }
        if left_rows.is_empty() {
            left_exact.remove(&signature);
        }
        if right_rows.is_empty() {
            right_exact.remove(&signature);
        }
    }

    let mut left_grouped = rows_by_primary_key(left_exact);
    let mut right_grouped = rows_by_primary_key(right_exact);
    let group_keys = left_grouped
        .keys()
        .chain(right_grouped.keys())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    for key in group_keys {
        let mut left_rows = left_grouped.remove(&key).unwrap_or_default();
        let mut right_rows = right_grouped.remove(&key).unwrap_or_default();
        while let (Some(before), Some(after)) = (left_rows.pop_front(), right_rows.pop_front()) {
            changed.push(ReportRowChange {
                before_trace: trace_handle_for_row(service, &before, policy_context)?,
                after_trace: trace_handle_for_row(service, &after, policy_context)?,
                before,
                after,
            });
        }
        for row in left_rows {
            removed.push(ReportRowDiff {
                trace: trace_handle_for_row(service, &row, policy_context)?,
                row,
            });
        }
        for row in right_rows {
            added.push(ReportRowDiff {
                trace: trace_handle_for_row(service, &row, policy_context)?,
                row,
            });
        }
    }

    Ok(ReportSectionDelta {
        added,
        removed,
        changed,
    })
}

fn rows_by_signature(rows: Vec<ReportRow>) -> BTreeMap<String, VecDeque<ReportRow>> {
    let mut grouped = BTreeMap::new();
    for row in rows {
        grouped
            .entry(row_signature(&row))
            .or_insert_with(VecDeque::new)
            .push_back(row);
    }
    grouped
}

fn rows_by_primary_key(
    rows: BTreeMap<String, VecDeque<ReportRow>>,
) -> BTreeMap<String, VecDeque<ReportRow>> {
    let mut grouped = BTreeMap::new();
    for (_, mut bucket) in rows {
        while let Some(row) = bucket.pop_front() {
            grouped
                .entry(row_primary_key(&row))
                .or_insert_with(VecDeque::new)
                .push_back(row);
        }
    }
    grouped
}

fn row_signature(row: &ReportRow) -> String {
    format!(
        "{}|{}",
        row.tuple_id
            .map(|tuple_id| tuple_id.0.to_string())
            .unwrap_or_else(|| "-".into()),
        format_values(&row.values)
    )
}

fn row_primary_key(row: &ReportRow) -> String {
    row.values
        .first()
        .map(format_value)
        .or_else(|| row.tuple_id.map(|tuple_id| format!("t{}", tuple_id.0)))
        .unwrap_or_else(|| "-".into())
}

fn trace_handle_for_row(
    service: &mut impl KernelService,
    row: &ReportRow,
    policy_context: Option<&PolicyContext>,
) -> Result<Option<CoordinationTraceHandle>, ApiError> {
    let Some(tuple_id) = row.tuple_id else {
        return Ok(None);
    };
    let trace = match service.explain_tuple(ExplainTupleRequest {
        tuple_id,
        policy_context: policy_context.cloned(),
    }) {
        Ok(response) => response.trace,
        Err(ApiError::Validation(message))
            if message == "requested tuple is not visible under the current policy" =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let root = trace
        .tuples
        .iter()
        .find(|tuple| tuple.tuple.id == trace.root)
        .or_else(|| trace.tuples.first())
        .ok_or_else(|| ApiError::Validation("empty explain trace".into()))?;
    Ok(Some(CoordinationTraceHandle {
        tuple_id: trace.root,
        tuple_count: trace.tuples.len(),
        source_datom_ids: root.metadata.source_datom_ids.clone(),
        parent_tuple_ids: root.metadata.parent_tuple_ids.clone(),
    }))
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
                    capabilities: vec!["executor".into()],
                    visibilities: Vec::new(),
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
