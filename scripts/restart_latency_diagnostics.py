#!/usr/bin/env python3
"""Aggregate fresh-process restart telemetry without producing a release verdict."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "aether.restart-latency-diagnostics.v1"
EXPECTED_PASSES_PER_SAMPLE = 4


class DiagnosticError(ValueError):
    pass


def load_bundle(path: Path) -> tuple[dict[str, Any], bytes]:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as error:
        raise DiagnosticError(f"{path}: invalid JSON: {error}") from error
    if not isinstance(payload, dict):
        raise DiagnosticError(f"{path}: bundle root must be an object")
    return payload, raw


def percentile(values: list[int], quantile: float) -> int:
    if not values:
        raise DiagnosticError("cannot summarize an empty duration collection")
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def summarize(values: Iterable[int]) -> dict[str, int]:
    observed = list(values)
    if not observed:
        raise DiagnosticError("cannot summarize an empty duration collection")
    return {
        "count": len(observed),
        "min_ns": min(observed),
        "mean_ns": sum(observed) // len(observed),
        "p50_ns": percentile(observed, 0.50),
        "p95_ns": percentile(observed, 0.95),
        "max_ns": max(observed),
    }


def _require_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise DiagnosticError(f"{label} must be a non-empty string")
    return value


def _require_positive_int(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise DiagnosticError(f"{label} must be a positive integer")
    return value


def extract_passes(
    bundle: dict[str, Any],
    *,
    source_index: int,
    source_sha256: str,
) -> list[dict[str, Any]]:
    report = bundle.get("report")
    if not isinstance(report, dict):
        raise DiagnosticError(f"bundle {source_index}: report must be an object")
    samples = _require_positive_int(
        report.get("samples_per_workload"),
        f"bundle {source_index}: report.samples_per_workload",
    )
    measurements = report.get("measurements")
    if not isinstance(measurements, list):
        raise DiagnosticError(f"bundle {source_index}: measurements must be an array")

    retained: list[dict[str, Any]] = []
    for measurement_index, measurement in enumerate(measurements, start=1):
        if not isinstance(measurement, dict):
            raise DiagnosticError(
                f"bundle {source_index}: measurement {measurement_index} must be an object"
            )
        pass_timings = measurement.get("pass_timings", [])
        if not pass_timings:
            continue
        workload = _require_string(
            measurement.get("workload"),
            f"bundle {source_index}: measurement {measurement_index}.workload",
        )
        if not workload.startswith("Durable restart "):
            raise DiagnosticError(
                f"bundle {source_index}: unexpected pass telemetry on {workload!r}"
            )
        if not isinstance(pass_timings, list):
            raise DiagnosticError(
                f"bundle {source_index}: {workload}: pass_timings must be an array"
            )
        expected_count = samples * EXPECTED_PASSES_PER_SAMPLE
        if len(pass_timings) != expected_count:
            raise DiagnosticError(
                f"bundle {source_index}: {workload}: expected {expected_count} retained passes, "
                f"found {len(pass_timings)}"
            )

        seen_coordinates: set[tuple[int, int]] = set()
        first_count = 0
        for pass_timing in pass_timings:
            if not isinstance(pass_timing, dict):
                raise DiagnosticError(
                    f"bundle {source_index}: {workload}: pass timing must be an object"
                )
            sample_index = _require_positive_int(
                pass_timing.get("sample_index"),
                f"bundle {source_index}: {workload}.sample_index",
            )
            pass_index = _require_positive_int(
                pass_timing.get("pass_index"),
                f"bundle {source_index}: {workload}.pass_index",
            )
            coordinate = (sample_index, pass_index)
            if coordinate in seen_coordinates:
                raise DiagnosticError(
                    f"bundle {source_index}: {workload}: duplicate pass {coordinate}"
                )
            seen_coordinates.add(coordinate)
            if sample_index > samples or pass_index > EXPECTED_PASSES_PER_SAMPLE:
                raise DiagnosticError(
                    f"bundle {source_index}: {workload}: out-of-range pass {coordinate}"
                )

            expected_classification = (
                "first_observed_restart"
                if coordinate == (1, 1)
                else "subsequent_restart"
            )
            classification = _require_string(
                pass_timing.get("classification"),
                f"bundle {source_index}: {workload}.classification",
            )
            if classification != expected_classification:
                raise DiagnosticError(
                    f"bundle {source_index}: {workload}: pass {coordinate} classification "
                    f"{classification!r} != {expected_classification!r}"
                )
            first_count += int(classification == "first_observed_restart")
            total_ns = _require_positive_int(
                pass_timing.get("total_duration_ns"),
                f"bundle {source_index}: {workload}.total_duration_ns",
            )
            phases = pass_timing.get("phases")
            if not isinstance(phases, list) or not phases:
                raise DiagnosticError(
                    f"bundle {source_index}: {workload}: pass {coordinate} has no phases"
                )
            normalized_phases: list[dict[str, Any]] = []
            phase_names: set[str] = set()
            attributed_ns = 0
            for phase in phases:
                if not isinstance(phase, dict):
                    raise DiagnosticError(
                        f"bundle {source_index}: {workload}: pass {coordinate} phase must be an object"
                    )
                phase_name = _require_string(
                    phase.get("phase"),
                    f"bundle {source_index}: {workload}: pass {coordinate}.phase",
                )
                if phase_name in phase_names:
                    raise DiagnosticError(
                        f"bundle {source_index}: {workload}: pass {coordinate} duplicates phase {phase_name}"
                    )
                phase_names.add(phase_name)
                duration_ns = phase.get("duration_ns")
                if not isinstance(duration_ns, int) or isinstance(duration_ns, bool) or duration_ns < 0:
                    raise DiagnosticError(
                        f"bundle {source_index}: {workload}: pass {coordinate} phase {phase_name} "
                        "duration must be a non-negative integer"
                    )
                attributed_ns += duration_ns
                normalized_phases.append({"phase": phase_name, "duration_ns": duration_ns})
            if attributed_ns > total_ns:
                raise DiagnosticError(
                    f"bundle {source_index}: {workload}: pass {coordinate} attributes "
                    f"{attributed_ns} ns over total {total_ns} ns"
                )
            retained.append(
                {
                    "source_index": source_index,
                    "source_sha256": source_sha256,
                    "workload": workload,
                    "sample_index": sample_index,
                    "pass_index": pass_index,
                    "classification": classification,
                    "total_duration_ns": total_ns,
                    "phases": normalized_phases,
                }
            )
        if first_count != 1:
            raise DiagnosticError(
                f"bundle {source_index}: {workload}: expected one first-observed restart, found {first_count}"
            )
    if not retained:
        raise DiagnosticError(f"bundle {source_index}: no durable restart pass telemetry found")
    return retained


def build_diagnostics(
    bundle_paths: list[Path],
    *,
    expected_commit: str,
    expected_tree: str,
    expected_ref: str,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if len(bundle_paths) < 2:
        raise DiagnosticError("at least two fresh-process bundles are required")
    expected_commit = _require_string(expected_commit, "expected_commit")
    expected_tree = _require_string(expected_tree, "expected_tree")
    expected_ref = _require_string(expected_ref, "expected_ref")

    sources: list[dict[str, Any]] = []
    raw_passes: list[dict[str, Any]] = []
    host_id: str | None = None
    suite_id: str | None = None
    for source_index, path in enumerate(bundle_paths, start=1):
        bundle, raw = load_bundle(path)
        run = bundle.get("run")
        if not isinstance(run, dict):
            raise DiagnosticError(f"{path}: run must be an object")
        commit = run.get("git_commit")
        if commit != expected_commit:
            raise DiagnosticError(f"{path}: commit {commit!r} != {expected_commit!r}")
        if run.get("git_dirty") is not False:
            raise DiagnosticError(f"{path}: diagnostic bundle must come from a clean worktree")
        observed_suite = _require_string(run.get("suite_id"), f"{path}: run.suite_id")
        manifest = bundle.get("host_manifest")
        if not isinstance(manifest, dict):
            raise DiagnosticError(f"{path}: host_manifest must be an object")
        observed_host = _require_string(manifest.get("host_id"), f"{path}: host_manifest.host_id")
        if host_id is None:
            host_id = observed_host
            suite_id = observed_suite
        elif observed_host != host_id or observed_suite != suite_id:
            raise DiagnosticError(
                f"{path}: host/suite {observed_host}/{observed_suite} does not match "
                f"{host_id}/{suite_id}"
            )
        sha256 = hashlib.sha256(raw).hexdigest()
        sources.append(
            {
                "index": source_index,
                "path": path.as_posix(),
                "size_bytes": len(raw),
                "sha256": sha256,
                "generated_at": bundle.get("generated_at"),
            }
        )
        raw_passes.extend(
            extract_passes(bundle, source_index=source_index, source_sha256=sha256)
        )

    totals: dict[tuple[str, str], list[int]] = defaultdict(list)
    phases: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    for pass_timing in raw_passes:
        workload = pass_timing["workload"]
        classification = pass_timing["classification"]
        totals[(workload, classification)].append(pass_timing["total_duration_ns"])
        for phase in pass_timing["phases"]:
            phases[(workload, classification, phase["phase"])].append(phase["duration_ns"])

    summaries: list[dict[str, Any]] = []
    for workload, classification in sorted(totals):
        phase_summaries = [
            {"phase": phase, **summarize(values)}
            for (phase_workload, phase_classification, phase), values in sorted(phases.items())
            if phase_workload == workload and phase_classification == classification
        ]
        summaries.append(
            {
                "workload": workload,
                "classification": classification,
                "total": summarize(totals[(workload, classification)]),
                "phases": phase_summaries,
            }
        )

    return {
        "schema": SCHEMA,
        "generated_at": generated_at
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "diagnostic_only": True,
        "claim_effect": "none",
        "candidate": {
            "commit": expected_commit,
            "tree": expected_tree,
            "ref": expected_ref,
        },
        "host_id": host_id,
        "suite_id": suite_id,
        "fresh_process_repetitions": len(bundle_paths),
        "sources": sources,
        "summaries": summaries,
        "raw_passes": raw_passes,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    candidate = payload["candidate"]
    lines = [
        "# AETHER Restart Latency Diagnostics",
        "",
        "> Diagnostic only. This report does not change or satisfy any release gate.",
        "",
        f"- Commit: `{candidate['commit']}`",
        f"- Tree: `{candidate['tree']}`",
        f"- Ref: `{candidate['ref']}`",
        f"- Host: `{payload['host_id']}`",
        f"- Suite: `{payload['suite_id']}`",
        f"- Fresh-process repetitions: `{payload['fresh_process_repetitions']}`",
        "",
        "## Restart distributions",
        "",
        "| Workload | Classification | Count | Min | Mean | P50 | P95 | Max |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for summary in payload["summaries"]:
        total = summary["total"]
        lines.append(
            "| {workload} | `{classification}` | {count} | {min:.3f} ms | {mean:.3f} ms | "
            "{p50:.3f} ms | {p95:.3f} ms | {max:.3f} ms |".format(
                workload=summary["workload"],
                classification=summary["classification"],
                count=total["count"],
                min=total["min_ns"] / 1_000_000,
                mean=total["mean_ns"] / 1_000_000,
                p50=total["p50_ns"] / 1_000_000,
                p95=total["p95_ns"] / 1_000_000,
                max=total["max_ns"] / 1_000_000,
            )
        )
    for summary in payload["summaries"]:
        lines.extend(
            [
                "",
                f"### {summary['workload']} - {summary['classification']}",
                "",
                "| Phase | Count | Mean | P95 | Max |",
                "| --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for phase in summary["phases"]:
            lines.append(
                "| `{phase}` | {count} | {mean:.3f} ms | {p95:.3f} ms | {max:.3f} ms |".format(
                    phase=phase["phase"],
                    count=phase["count"],
                    mean=phase["mean_ns"] / 1_000_000,
                    p95=phase["p95_ns"] / 1_000_000,
                    max=phase["max_ns"] / 1_000_000,
                )
            )
    lines.extend(
        [
            "",
            "## Immutable source bytes",
            "",
            "| Run | Size | SHA-256 | Path |",
            "| ---: | ---: | --- | --- |",
        ]
    )
    for source in payload["sources"]:
        lines.append(
            f"| {source['index']} | {source['size_bytes']} | `{source['sha256']}` | "
            f"`{source['path']}` |"
        )
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", action="append", required=True, type=Path)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-tree", required=True)
    parser.add_argument("--expected-ref", required=True)
    parser.add_argument("--generated-at")
    parser.add_argument("--output-json", required=True, type=Path)
    parser.add_argument("--output-report", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        payload = build_diagnostics(
            args.bundle,
            expected_commit=args.expected_commit,
            expected_tree=args.expected_tree,
            expected_ref=args.expected_ref,
            generated_at=args.generated_at,
        )
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_report.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(payload, indent=2) + "\n", encoding="utf-8", newline="\n"
        )
        args.output_report.write_text(
            render_markdown(payload), encoding="utf-8", newline="\n"
        )
    except (DiagnosticError, OSError) as error:
        print(f"restart latency diagnostics failed: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
