from __future__ import annotations

import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path


REPOSITORY = "https://github.com/fyremael/AETHER.git"
QUALIFICATION_STATUS = "diagnostic_only"
COMMERCIAL_BETA_AUTHORITY = False
WORKSPACE = Path("/content/AETHER")
OUTPUT = Path("/content/aether-colab-diagnostic")
ARCHIVE = Path("/content/aether-colab-diagnostic.zip")


def run(command: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    print("[aether-colab]", " ".join(command), flush=True)
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def ensure_rust() -> None:
    if shutil.which("cargo") is None:
        subprocess.run(
            [
                "bash",
                "-lc",
                "curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal",
            ],
            check=True,
        )
        os.environ["PATH"] = f"{Path.home() / '.cargo' / 'bin'}:{os.environ['PATH']}"
    run(["rustup", "toolchain", "install", "stable", "--profile", "minimal"])
    run(["rustup", "default", "stable"])


def measurement(bundle: dict, workload: str, scale: str) -> dict:
    matches = [
        item
        for item in bundle["report"]["measurements"]
        if item["workload"] == workload and item["scale"] == scale
    ]
    if len(matches) != 1:
        raise RuntimeError(f"expected one {workload!r}/{scale!r} measurement, got {len(matches)}")
    item = matches[0]
    return {
        "throughput_per_second": item["throughput_per_second"],
        "mean_ns": item["latency"]["mean"]["secs"] * 1_000_000_000
        + item["latency"]["mean"]["nanos"],
        "sample_durations_ns": item["latency"]["sample_durations_ns"],
    }


def execute_report(binary: Path, host_manifest: Path, label: str) -> dict:
    bundle_path = OUTPUT / f"{label}.json"
    report_path = OUTPUT / f"{label}.md"
    completed = run(
        [
            str(binary),
            "--suite",
            "core_kernel",
            "--samples",
            "5",
            "--host-manifest",
            str(host_manifest),
            "--bundle-path",
            str(bundle_path),
            "--report-path",
            str(report_path),
        ],
        cwd=WORKSPACE,
    )
    (OUTPUT / f"{label}.stdout.txt").write_text(completed.stdout, encoding="utf-8")
    return json.loads(bundle_path.read_text(encoding="utf-8"))


def main() -> None:
    started = time.time()
    candidate = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.environ.get("AETHER_CANDIDATE_SHA", "")
    ).strip()
    if re.fullmatch(r"[0-9a-f]{40}", candidate) is None:
        raise RuntimeError("AETHER_CANDIDATE_SHA must be a full lowercase commit SHA")

    shutil.rmtree(WORKSPACE, ignore_errors=True)
    shutil.rmtree(OUTPUT, ignore_errors=True)
    ARCHIVE.unlink(missing_ok=True)
    OUTPUT.mkdir(parents=True)

    run(["git", "clone", "--filter=blob:none", "--no-checkout", REPOSITORY, str(WORKSPACE)])
    run(["git", "fetch", "--depth", "1", "origin", candidate], cwd=WORKSPACE)
    run(["git", "checkout", "--detach", candidate], cwd=WORKSPACE)
    head = run(["git", "rev-parse", "HEAD"], cwd=WORKSPACE).stdout.strip()
    tree = run(["git", "rev-parse", "HEAD^{tree}"], cwd=WORKSPACE).stdout.strip()
    if head != candidate:
        raise RuntimeError(f"checkout mismatch: expected {candidate}, got {head}")

    ensure_rust()
    test_output = run(
        ["cargo", "test", "-p", "aether_explain", "-p", "aether_perf", "--release"],
        cwd=WORKSPACE,
    )
    (OUTPUT / "cargo-tests.txt").write_text(test_output.stdout, encoding="utf-8")
    build_output = run(
        [
            "cargo",
            "build",
            "-p",
            "aether_api",
            "--example",
            "performance_report",
            "--release",
        ],
        cwd=WORKSPACE,
    )
    (OUTPUT / "cargo-build.txt").write_text(build_output.stdout, encoding="utf-8")

    host_manifest = OUTPUT / "colab-host.json"
    host_manifest.write_text(
        json.dumps(
            {
                "host_id": "colab-cpu-diagnostic",
                "display_name": "Google Colab CPU diagnostic runtime",
                "host_class": "diagnostic",
                "execution_environment": "native_linux",
                "owner": "google-colab",
                "notes": [
                    "Ephemeral Colab CPU runtime used for contained diagnostics.",
                    "Never qualifies the Windows x86_64 commercial package or cross-host drift.",
                ],
                "tags": ["diagnostic", "colab", "linux", "ephemeral"],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    binary = WORKSPACE / "target" / "release" / "examples" / "performance_report"
    warmup = execute_report(binary, host_manifest, "warmup-discarded")
    retained = [execute_report(binary, host_manifest, f"retained-{index}") for index in range(1, 4)]
    warmup_explain = measurement(warmup, "Tuple explanation runtime", "chain 128")
    runs = []
    for index, bundle in enumerate(retained, start=1):
        runs.append(
            {
                "index": index,
                "generated_at": bundle["generated_at"],
                "host_snapshot": bundle["host_snapshot"],
                "explain": measurement(bundle, "Tuple explanation runtime", "chain 128"),
                "closure": measurement(bundle, "Recursive closure runtime", "chain 128"),
            }
        )

    explain_throughputs = [item["explain"]["throughput_per_second"] for item in runs]
    closure_throughputs = [item["closure"]["throughput_per_second"] for item in runs]
    summary = {
        "schema_version": "aether.colab-diagnostic.v1",
        "qualification_status": QUALIFICATION_STATUS,
        "candidate": {"commit_sha": head, "tree_sha": tree},
        "environment": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
            "python": platform.python_version(),
            "rustc": run(["rustc", "-Vv"]).stdout,
        },
        "policy": {
            "warmup_runs_discarded": 1,
            "retained_runs": 3,
            "samples_per_workload_per_run": 5,
            "commercial_beta_authority": COMMERCIAL_BETA_AUTHORITY,
        },
        "warmup_explain": warmup_explain,
        "runs": runs,
        "explain_across_runs": {
            "mean_throughput_per_second": statistics.fmean(explain_throughputs),
            "min_throughput_per_second": min(explain_throughputs),
            "max_throughput_per_second": max(explain_throughputs),
            "coefficient_of_variation": statistics.stdev(explain_throughputs)
            / statistics.fmean(explain_throughputs),
        },
        "closure_across_runs": {
            "mean_throughput_per_second": statistics.fmean(closure_throughputs),
            "min_throughput_per_second": min(closure_throughputs),
            "max_throughput_per_second": max(closure_throughputs),
            "coefficient_of_variation": statistics.stdev(closure_throughputs)
            / statistics.fmean(closure_throughputs),
        },
        "elapsed_seconds": time.time() - started,
    }
    (OUTPUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    shutil.make_archive(str(ARCHIVE.with_suffix("")), "zip", OUTPUT)
    print("AETHER_COLAB_SUMMARY=" + json.dumps(summary, separators=(",", ":")), flush=True)


if __name__ == "__main__":
    main()
