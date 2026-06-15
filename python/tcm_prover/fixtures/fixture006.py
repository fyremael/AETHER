from __future__ import annotations

import argparse
import json
import os
import shlex
import struct
import subprocess
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

WEIGHTS: list[list[int]] = [
    [10, 6, 5, 4, 3],
    [12, 17, 11, 10, 9],
    [8, 7, 13, 6, 5],
    [16, 15, 14, 21, 13],
    [18, 17, 16, 15, 24],
]
DUAL_ROWS: list[int] = [10, 17, 13, 21, 24]
DUAL_COLS: list[int] = [0, 0, 0, 0, 0]
SELECTED_COLS: list[int] = [0, 1, 2, 3, 4]
OPTIMUM = 85


@dataclass(frozen=True)
class SemiringResult:
    optimum: int
    optimum_count: int
    witness_cols: list[int]
    trace: list[dict[str, Any]]


def solve_assignment_max_plus_count(weights: list[list[int]]) -> SemiringResult:
    """Exact max-plus/count contraction for a square assignment tensor network.

    Rows are eliminated one at a time. Each DP table maps a used-column mask to
    the best score and degeneracy count for the processed prefix. This is the
    assignment-problem specialization of exact semiring contraction: ordinary
    addition over alternatives is replaced by max, and ties accumulate counts.
    """

    n = len(weights)
    dp: dict[int, tuple[int, int, tuple[int, ...]]] = {0: (0, 1, ())}
    trace: list[dict[str, Any]] = []

    for row in range(n):
        next_dp: dict[int, tuple[int, int, tuple[int, ...]]] = {}
        for mask, (score, count, cols) in dp.items():
            for col in range(n):
                if mask & (1 << col):
                    continue
                new_mask = mask | (1 << col)
                candidate_score = score + weights[row][col]
                incumbent = next_dp.get(new_mask)
                if incumbent is None or candidate_score > incumbent[0]:
                    next_dp[new_mask] = (candidate_score, count, cols + (col,))
                elif candidate_score == incumbent[0]:
                    next_dp[new_mask] = (
                        incumbent[0], incumbent[1] + count, incumbent[2]
                    )
        dp = next_dp
        trace.append(
            {
                "eliminated_row": row,
                "table_entries": len(dp),
                "mask_width": row + 1,
                "best_prefix_score": max(score for score, _, _ in dp.values()),
            }
        )

    final_mask = (1 << n) - 1
    optimum, optimum_count, witness = dp[final_mask]
    return SemiringResult(optimum, optimum_count, list(witness), trace)


def write_opb(path: Path, weights: list[list[int]]) -> None:
    n = len(weights)
    terms = []
    for i in range(n):
        for j in range(n):
            terms.append(f"+{weights[i][j]} x_{i}_{j}")

    lines = [
        f"* #variable= {n * n} #constraint= {2 * n}",
        "max: " + " ".join(terms) + " ;",
    ]
    for i in range(n):
        row_terms = " ".join(f"+1 x_{i}_{j}" for j in range(n))
        lines.append(f"{row_terms} = 1 ;")
    for j in range(n):
        col_terms = " ".join(f"+1 x_{i}_{j}" for i in range(n))
        lines.append(f"{col_terms} = 1 ;")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def witness_assignment(cols: list[int]) -> dict[str, int]:
    return {f"x_{i}_{col}": 1 for i, col in enumerate(cols)}


def validate_witness(weights: list[list[int]], cols: list[int]) -> int:
    n = len(weights)
    if len(cols) != n:
        raise ValueError("witness must pick one column per row")
    if sorted(cols) != list(range(n)):
        raise ValueError("witness must use each column exactly once")
    return sum(weights[i][col] for i, col in enumerate(cols))


def validate_dual_certificate(
    weights: list[list[int]], row_duals: list[int], col_duals: list[int]
) -> int:
    n = len(weights)
    if len(row_duals) != n or len(col_duals) != n:
        raise ValueError("dual certificate dimensions do not match instance")
    violations = []
    for i in range(n):
        for j in range(n):
            if row_duals[i] + col_duals[j] < weights[i][j]:
                violations.append(
                    {
                        "row": i,
                        "col": j,
                        "lhs": row_duals[i] + col_duals[j],
                        "rhs": weights[i][j],
                    }
                )
    if violations:
        raise ValueError(f"dual bound violation: {violations[:3]}")
    return sum(row_duals) + sum(col_duals)


def local_reference_check(out_dir: Path) -> dict[str, Any]:
    witness = json.loads((out_dir / "primal_witness.json").read_text(encoding="utf-8"))
    dual = json.loads((out_dir / "pb_dual_certificate.json").read_text(encoding="utf-8"))
    cols = witness["selected_cols"]
    primal = validate_witness(WEIGHTS, cols)
    upper = validate_dual_certificate(WEIGHTS, dual["row_duals"], dual["col_duals"])
    if primal != upper:
        raise ValueError(f"optimum mismatch: witness={primal}, upper_bound={upper}")
    return {"checker": "local_reference_pb_dual_checker", "optimum": primal, "upper_bound": upper}


def run_external_checker(out_dir: Path) -> tuple[str, int, str]:
    command_template = os.environ.get("TCM_FIXTURE006_CHECKER")
    if not command_template:
        return "missing", 127, "TCM_FIXTURE006_CHECKER not set; used local fallback.\n"

    values = {
        "opb": str(out_dir / "instance.opb"),
        "witness": str(out_dir / "primal_witness.json"),
        "dual": str(out_dir / "pb_dual_certificate.json"),
        "out": str(out_dir),
    }
    command = command_template.format(**values)
    proc = subprocess.run(
        shlex.split(command),
        cwd=str(out_dir),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return command, proc.returncode, proc.stdout


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _png_chunk(tag: bytes, data: bytes) -> bytes:
    return (
        struct.pack("!I", len(data))
        + tag
        + data
        + struct.pack("!I", zlib.crc32(tag + data) & 0xFFFFFFFF)
    )


def write_rgb_png(
    path: Path, width: int, height: int, pixels: list[tuple[int, int, int]]
) -> None:
    if len(pixels) != width * height:
        raise ValueError("pixel buffer size mismatch")
    raw = bytearray()
    for y in range(height):
        raw.append(0)
        row = pixels[y * width : (y + 1) * width]
        for r, g, b in row:
            raw.extend((r, g, b))
    png = bytearray(b"\x89PNG\r\n\x1a\n")
    png.extend(
        _png_chunk(b"IHDR", struct.pack("!IIBBBBB", width, height, 8, 2, 0, 0, 0))
    )
    png.extend(_png_chunk(b"IDAT", zlib.compress(bytes(raw), level=9)))
    png.extend(_png_chunk(b"IEND", b""))
    path.write_bytes(bytes(png))


def blank(width: int, height: int, color: tuple[int, int, int]) -> list[tuple[int, int, int]]:
    return [color for _ in range(width * height)]


def fill_rect(
    pixels: list[tuple[int, int, int]],
    width: int,
    x0: int,
    y0: int,
    x1: int,
    y1: int,
    color: tuple[int, int, int],
) -> None:
    height = len(pixels) // width
    x0, x1 = max(0, x0), min(width, x1)
    y0, y1 = max(0, y0), min(height, y1)
    for y in range(y0, y1):
        base = y * width
        for x in range(x0, x1):
            pixels[base + x] = color


def write_visuals(out_dir: Path, result: SemiringResult) -> None:
    visual_dir = out_dir / "visuals"
    visual_dir.mkdir(parents=True, exist_ok=True)
    n = len(WEIGHTS)

    w, h = 360, 360
    pixels = blank(w, h, (248, 250, 252))
    margin, cell = 40, 56
    for i in range(n):
        for j in range(n):
            value = WEIGHTS[i][j]
            shade = 255 - min(180, value * 6)
            color = (shade, shade + 10 if shade < 245 else 245, 255)
            if result.witness_cols[i] == j:
                color = (22, 163, 74)
            fill_rect(
                pixels,
                w,
                margin + j * cell,
                margin + i * cell,
                margin + (j + 1) * cell - 4,
                margin + (i + 1) * cell - 4,
                color,
            )
    write_rgb_png(visual_dir / "01_weight_matrix_matching.png", w, h, pixels)

    pixels = blank(w, h, (255, 255, 255))
    max_slack = max(
        DUAL_ROWS[i] + DUAL_COLS[j] - WEIGHTS[i][j] for i in range(n) for j in range(n)
    )
    for i in range(n):
        for j in range(n):
            slack = DUAL_ROWS[i] + DUAL_COLS[j] - WEIGHTS[i][j]
            intensity = int(255 - 180 * (slack / max_slack if max_slack else 0))
            color = (255, intensity, 128)
            if slack == 0:
                color = (34, 197, 94)
            fill_rect(
                pixels,
                w,
                margin + j * cell,
                margin + i * cell,
                margin + (j + 1) * cell - 4,
                margin + (i + 1) * cell - 4,
                color,
            )
    write_rgb_png(visual_dir / "02_dual_slack_certificate.png", w, h, pixels)

    w2, h2 = 640, 220
    pixels = blank(w2, h2, (248, 250, 252))
    colors = [(59, 130, 246), (20, 184, 166), (22, 163, 74)]
    xs = [40, 245, 450]
    for x, color in zip(xs, colors):
        fill_rect(pixels, w2, x, 70, x + 150, 150, color)
    for x in [205, 410]:
        fill_rect(pixels, w2, x, 105, x + 25, 115, (15, 23, 42))
        fill_rect(pixels, w2, x + 20, 98, x + 36, 122, (15, 23, 42))
    write_rgb_png(visual_dir / "03_roundtrip_flow.png", w2, h2, pixels)


def build_artifacts(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    result = solve_assignment_max_plus_count(WEIGHTS)
    if result.optimum != OPTIMUM:
        raise AssertionError(f"unexpected fixture optimum {result.optimum}")

    write_opb(out_dir / "instance.opb", WEIGHTS)

    primal_payload = {
        "problem": "max_weight_assignment_pb",
        "selected_cols": result.witness_cols,
        "assignment": witness_assignment(result.witness_cols),
        "objective": result.optimum,
    }
    write_json(out_dir / "primal_witness.json", primal_payload)

    slack = [
        {"row": i, "col": j, "slack": DUAL_ROWS[i] + DUAL_COLS[j] - WEIGHTS[i][j]}
        for i in range(len(WEIGHTS))
        for j in range(len(WEIGHTS))
    ]
    dual_payload = {
        "type": "assignment_lp_dual_upper_bound",
        "row_duals": DUAL_ROWS,
        "col_duals": DUAL_COLS,
        "upper_bound": sum(DUAL_ROWS) + sum(DUAL_COLS),
        "inequalities": slack,
        "trusted_rule": "For every feasible assignment, sum W_ij x_ij <= sum row_duals + sum col_duals when row_dual_i + col_dual_j >= W_ij for every edge.",
    }
    write_json(out_dir / "pb_dual_certificate.json", dual_payload)

    external_command, exit_code, external_output = run_external_checker(out_dir)
    transcript_lines = [
        "TCM Fixture 006 checker transcript",
        "",
        f"external_command: {external_command}",
        f"external_exit_code: {exit_code}",
        "",
        external_output,
    ]
    local_status = local_reference_check(out_dir)
    transcript_lines.extend(["", "local_reference_check: ok", json.dumps(local_status, sort_keys=True)])
    (out_dir / "checker_transcript.txt").write_text("\n".join(transcript_lines), encoding="utf-8")

    status = "externally_checked" if exit_code == 0 and external_command != "missing" else "locally_checked_fallback"
    if external_command != "missing" and exit_code != 0:
        status = "failed"

    result_card = {
        "fixture": "TCM-Prover Fixture 006",
        "route_family": "SEMIRING-CONTRACTION/TCM",
        "status": status,
        "external_checker": {
            "enabled": external_command != "missing",
            "command": None if external_command == "missing" else external_command,
            "exit_code": exit_code,
            "transcript_path": "artifacts/fixture006/checker_transcript.txt",
        },
        "claim": {
            "problem": "max_weight_assignment_pb",
            "optimum": result.optimum,
            "optimum_count": result.optimum_count,
        },
        "certificate": {
            "opb_path": "artifacts/fixture006/instance.opb",
            "primal_witness_path": "artifacts/fixture006/primal_witness.json",
            "dual_certificate_path": "artifacts/fixture006/pb_dual_certificate.json",
            "proof_import_stub_path": "artifacts/fixture006/proof_import_stub.lean",
        },
        "trace": result.trace,
        "trust_boundary": "TCM search is untrusted; certificate replay is trusted.",
    }
    write_json(out_dir / "result_card.json", result_card)

    (out_dir / "failure_mode_ledger.md").write_text(
        "# Fixture 006 Failure-Mode Ledger\n\n"
        "| Failure mode | Status discipline |\n"
        "| --- | --- |\n"
        "| Missing external checker | Downgrade to `locally_checked_fallback`; do not claim external certification. |\n"
        "| Malformed certificate | Reject before checking optimum. |\n"
        "| Witness infeasibility | Reject primal lower bound. |\n"
        "| Dual-bound failure | Reject upper-bound certificate. |\n"
        "| Optimum mismatch | Mark `failed`; search result and certificate disagree. |\n"
        "| Floating-point dependency | Reject from trusted path. |\n",
        encoding="utf-8",
    )

    (out_dir / "proof_import_stub.lean").write_text(
        "-- TCM Fixture 006 proof-import stub.\n"
        "-- This is an audit scaffold, not a completed Lean proof.\n"
        "-- Intended theorem shape:\n"
        "-- theorem fixture006_assignment_optimum : optimum instance = 85 := by\n"
        "--   exact pbCertificateChecked fixture006_opb fixture006_dual\n",
        encoding="utf-8",
    )

    (out_dir / "fixture006_report.md").write_text(
        "# TCM-Prover Fixture 006: External Checker Round-Trip\n\n"
        "## Claim\n\n"
        "The max-weight 5x5 assignment OPB instance has optimum `85` with a unique optimum.\n\n"
        "## Search\n\n"
        "Exact max-plus/count semiring contraction recovers the diagonal witness and counts one optimum.\n\n"
        "## Certificate\n\n"
        "The primal witness proves a lower bound of `85`. The row/column dual certificate proves every feasible assignment is at most `85`. The bounds agree.\n\n"
        f"## Status\n\n`{status}`\n\n"
        "TCM search is untrusted. The certificate replay path is trusted.\n",
        encoding="utf-8",
    )

    write_visuals(out_dir, result)
    return result_card


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run TCM-Prover Fixture 006.")
    parser.add_argument("--out", type=Path, default=Path("artifacts/fixture006"))
    args = parser.parse_args(argv)
    result_card = build_artifacts(args.out)
    print(json.dumps(result_card, indent=2, sort_keys=True))
    return 0 if result_card["status"] != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
