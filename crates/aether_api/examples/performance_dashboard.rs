use aether_api::perf::{
    default_performance_report_with_events, FootprintEstimate, PerfEvent, PerfMeasurement,
};
use std::io::{self, IsTerminal, Write};
use std::time::{Duration, Instant};

struct DashboardState {
    started: Instant,
    interactive: bool,
    total_workloads: usize,
    samples_per_workload: usize,
    current: Option<ActiveMeasurement>,
    completed: Vec<PerfMeasurement>,
    footprints: Vec<FootprintEstimate>,
}

#[derive(Clone, Debug)]
struct ActiveMeasurement {
    workload: String,
    scale: String,
    total_samples: usize,
    units: usize,
    unit_label: String,
    notes: Vec<String>,
    completed_samples: usize,
    last_elapsed: Duration,
    last_throughput: f64,
    mean_so_far: Duration,
    min_so_far: Duration,
    max_so_far: Duration,
    sample_throughputs: Vec<f64>,
}

impl DashboardState {
    fn new(interactive: bool) -> Self {
        Self {
            started: Instant::now(),
            interactive,
            total_workloads: 0,
            samples_per_workload: 0,
            current: None,
            completed: Vec::new(),
            footprints: Vec::new(),
        }
    }

    fn apply(&mut self, event: PerfEvent) {
        match event {
            PerfEvent::SuiteStart {
                total_workloads,
                samples_per_workload,
            } => {
                self.total_workloads = total_workloads;
                self.samples_per_workload = samples_per_workload;
            }
            PerfEvent::MeasurementStart {
                workload,
                scale,
                total_samples,
                units,
                unit_label,
                notes,
            } => {
                self.current = Some(ActiveMeasurement {
                    workload: workload.into(),
                    scale,
                    total_samples,
                    units,
                    unit_label: unit_label.into(),
                    notes,
                    completed_samples: 0,
                    last_elapsed: Duration::default(),
                    last_throughput: 0.0,
                    mean_so_far: Duration::default(),
                    min_so_far: Duration::default(),
                    max_so_far: Duration::default(),
                    sample_throughputs: Vec::new(),
                });
            }
            PerfEvent::SampleRecorded {
                workload: _,
                scale: _,
                sample_index,
                total_samples: _,
                elapsed,
                throughput_per_second,
                mean_so_far,
                min_so_far,
                max_so_far,
            } => {
                if let Some(current) = self.current.as_mut() {
                    current.completed_samples = sample_index;
                    current.last_elapsed = elapsed;
                    current.last_throughput = throughput_per_second;
                    current.mean_so_far = mean_so_far;
                    current.min_so_far = min_so_far;
                    current.max_so_far = max_so_far;
                    current.sample_throughputs.push(throughput_per_second);
                }
            }
            PerfEvent::MeasurementComplete { measurement } => {
                self.current = None;
                self.completed.push(measurement);
            }
            PerfEvent::FootprintComputed { footprint } => {
                self.footprints.push(footprint);
            }
        }
    }

    fn should_render(&self, event: &PerfEvent) -> bool {
        self.interactive || !matches!(event, PerfEvent::SampleRecorded { .. })
    }

    fn render(&self) -> io::Result<()> {
        let mut output = String::new();
        if self.interactive {
            output.push_str("\x1B[2J\x1B[H");
        }

        let total_samples = self.total_workloads * self.samples_per_workload;
        let completed_samples = self.completed.len() * self.samples_per_workload
            + self
                .current
                .as_ref()
                .map(|current| current.completed_samples)
                .unwrap_or_default();

        output.push_str("AETHER Performance Dashboard\n");
        output.push_str("============================\n");
        output.push_str("Live console view of real-time and collected kernel measures.\n\n");
        output.push_str(&format!(
            "Elapsed: {} | Workloads: {}/{} | Samples: {}/{}\n",
            format_elapsed(self.started.elapsed()),
            self.completed.len(),
            self.total_workloads,
            completed_samples,
            total_samples
        ));
        output.push_str(&format!(
            "Overall: {}\n\n",
            render_bar(completed_samples, total_samples.max(1), 36)
        ));

        output.push_str("Current Measurement\n");
        output.push_str("-------------------\n");
        if let Some(current) = &self.current {
            let max_throughput = self.max_throughput().max(current.last_throughput);
            output.push_str(&format!("{} | {}\n", current.workload, current.scale));
            output.push_str(&format!(
                "Samples: {} {}/{}\n",
                render_bar(current.completed_samples, current.total_samples.max(1), 24),
                current.completed_samples,
                current.total_samples
            ));
            output.push_str(&format!(
                "Last latency: {} | Mean/min/max: {} / {} / {}\n",
                format_duration(current.last_elapsed),
                format_duration(current.mean_so_far),
                format_duration(current.min_so_far),
                format_duration(current.max_so_far)
            ));
            output.push_str(&format!(
                "Last throughput: {:>9}/{} {}\n",
                format_rate(current.last_throughput),
                current.unit_label,
                render_ratio_bar(current.last_throughput, max_throughput, 18)
            ));
            output.push_str(&format!("Units/sample: {}\n", format_count(current.units)));
            if !current.sample_throughputs.is_empty() {
                output.push_str("Sample history:\n");
                for (index, throughput) in current.sample_throughputs.iter().enumerate() {
                    output.push_str(&format!(
                        "  {:>2}. {:>9}/{} {}\n",
                        index + 1,
                        format_rate(*throughput),
                        current.unit_label,
                        render_ratio_bar(*throughput, max_throughput, 16)
                    ));
                }
            }
            if !current.notes.is_empty() {
                output.push_str("Notes:\n");
                for note in &current.notes {
                    output.push_str(&format!("  - {note}\n"));
                }
            }
        } else {
            output.push_str("Waiting for the next workload or finishing footprint summaries.\n");
        }
        output.push('\n');

        output.push_str("Collected Measures\n");
        output.push_str("------------------\n");
        if self.completed.is_empty() {
            output.push_str("No completed measurements yet.\n");
        } else {
            let max_throughput = self.max_throughput();
            output.push_str(&format!(
                "{:<28} {:<16} {:>10} {:>14} {}\n",
                "Workload", "Scale", "Mean", "Throughput", "Visual"
            ));
            for measurement in &self.completed {
                output.push_str(&format!(
                    "{:<28} {:<16} {:>10} {:>14} {}\n",
                    fit(measurement.workload, 28),
                    fit(&measurement.scale, 16),
                    format_duration(measurement.latency.mean),
                    format!(
                        "{}/{}",
                        format_rate(measurement.throughput_per_second),
                        measurement.unit_label
                    ),
                    render_ratio_bar(measurement.throughput_per_second, max_throughput, 16)
                ));
            }
        }
        output.push('\n');

        output.push_str("Footprints\n");
        output.push_str("----------\n");
        if self.footprints.is_empty() {
            output.push_str("Pending after timed workloads complete.\n");
        } else {
            for footprint in &self.footprints {
                output.push_str(&format!(
                    "{} | {} | {} bytes\n",
                    footprint.workload,
                    footprint.scale,
                    format_count(footprint.estimated_bytes)
                ));
            }
        }
        output.push('\n');

        output.push_str("After the live run, use `scripts/run-performance-report.cmd` for a saved markdown capture.\n");

        let mut stdout = io::stdout();
        stdout.write_all(output.as_bytes())?;
        stdout.flush()
    }

    fn max_throughput(&self) -> f64 {
        self.completed
            .iter()
            .map(|measurement| measurement.throughput_per_second)
            .fold(0.0, f64::max)
    }
}

fn main() -> Result<(), aether_api::ApiError> {
    let interactive = io::stdout().is_terminal();
    let mut dashboard = DashboardState::new(interactive);

    default_performance_report_with_events(|event| {
        let should_render = dashboard.should_render(&event);
        dashboard.apply(event);
        if should_render {
            dashboard.render().expect("render dashboard");
        }
    })?;

    dashboard.render().expect("render final dashboard");
    if interactive {
        println!();
    }
    Ok(())
}

fn render_bar(current: usize, total: usize, width: usize) -> String {
    let total = total.max(1);
    let filled = current.saturating_mul(width) / total;
    format!(
        "[{}{}]",
        "#".repeat(filled),
        "-".repeat(width.saturating_sub(filled))
    )
}

fn render_ratio_bar(value: f64, max: f64, width: usize) -> String {
    if max <= f64::EPSILON {
        return format!("[{}]", "-".repeat(width));
    }
    let ratio = (value / max).clamp(0.0, 1.0);
    let filled = (ratio * width as f64).round() as usize;
    format!(
        "[{}{}]",
        "=".repeat(filled.min(width)),
        "-".repeat(width.saturating_sub(filled.min(width)))
    )
}

fn fit(text: &str, width: usize) -> String {
    let mut chars = text.chars();
    let truncated = chars.by_ref().take(width).collect::<String>();
    if text.chars().count() <= width {
        format!("{truncated:<width$}")
    } else if width > 3 {
        let mut prefix = truncated.chars().take(width - 3).collect::<String>();
        prefix.push_str("...");
        prefix
    } else {
        truncated
    }
}

fn format_duration(duration: Duration) -> String {
    if duration.as_secs_f64() >= 1.0 {
        format!("{:.2}s", duration.as_secs_f64())
    } else if duration.as_secs_f64() >= 0.001 {
        format!("{:.2}ms", duration.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.2}us", duration.as_secs_f64() * 1_000_000.0)
    }
}

fn format_elapsed(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

fn format_rate(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("{:.2}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.2}K", value / 1_000.0)
    } else {
        format!("{value:.2}")
    }
}

fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut output = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            output.push(',');
        }
        output.push(ch);
    }
    output
}
