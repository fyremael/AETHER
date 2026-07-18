from __future__ import annotations

import copy
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "restart_latency_diagnostics.py"
COMMIT = "a" * 40
TREE = "b" * 40
REF = "refs/heads/main"


def load_module():
    spec = importlib.util.spec_from_file_location("restart_latency_diagnostics", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pass_timing(pass_index: int) -> dict[str, object]:
    return {
        "sample_index": 1,
        "pass_index": pass_index,
        "classification": (
            "first_observed_restart" if pass_index == 1 else "subsequent_restart"
        ),
        "total_duration_ns": 1_000_000 + pass_index,
        "phases": [
            {"phase": "open.journal_open_configure_schema", "duration_ns": 300_000},
            {"phase": "run.policy_replay", "duration_ns": 200_000},
            {"phase": "harness.unattributed", "duration_ns": 400_000},
        ],
    }


def bundle() -> dict[str, object]:
    return {
        "generated_at": "2026-07-17T12:00:00Z",
        "host_manifest": {"host_id": "test-windows-native"},
        "run": {
            "suite_id": "service_in_process",
            "git_commit": COMMIT,
            "git_dirty": False,
        },
        "report": {
            "samples_per_workload": 1,
            "measurements": [
                {
                    "workload": "Durable restart coordination replay",
                    "pass_timings": [pass_timing(index) for index in range(1, 5)],
                }
            ],
        },
    }


class RestartLatencyDiagnosticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_module()

    def write_bundle(self, root: Path, name: str, payload: dict[str, object]) -> Path:
        path = root / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def build(self, root: Path, payloads: list[dict[str, object]]):
        paths = [
            self.write_bundle(root, f"bundle-{index}.json", payload)
            for index, payload in enumerate(payloads, start=1)
        ]
        return self.module.build_diagnostics(
            paths,
            expected_commit=COMMIT,
            expected_tree=TREE,
            expected_ref=REF,
            generated_at="2026-07-17T12:05:00Z",
        )

    def test_aggregates_fresh_process_first_and_subsequent_distributions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            payload = self.build(Path(tmp), [bundle(), bundle()])

        self.assertEqual(payload["schema"], "aether.restart-latency-diagnostics.v1")
        self.assertTrue(payload["diagnostic_only"])
        self.assertEqual(payload["claim_effect"], "none")
        self.assertEqual(payload["fresh_process_repetitions"], 2)
        self.assertEqual(len(payload["raw_passes"]), 8)
        summaries = {
            summary["classification"]: summary for summary in payload["summaries"]
        }
        self.assertEqual(summaries["first_observed_restart"]["total"]["count"], 2)
        self.assertEqual(summaries["subsequent_restart"]["total"]["count"], 6)
        report = self.module.render_markdown(payload)
        self.assertIn("Diagnostic only", report)
        self.assertIn(COMMIT, report)

    def test_rejects_missing_restart_pass(self) -> None:
        incomplete = bundle()
        incomplete["report"]["measurements"][0]["pass_timings"].pop()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(self.module.DiagnosticError, "expected 4 retained passes"):
                self.build(Path(tmp), [incomplete, bundle()])

    def test_rejects_dirty_or_cross_candidate_bundle(self) -> None:
        dirty = bundle()
        dirty["run"]["git_dirty"] = True
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(self.module.DiagnosticError, "clean worktree"):
                self.build(Path(tmp), [dirty, bundle()])

        wrong_candidate = bundle()
        wrong_candidate["run"]["git_commit"] = "c" * 40
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(self.module.DiagnosticError, "commit"):
                self.build(Path(tmp), [wrong_candidate, bundle()])

    def test_rejects_duplicate_or_over_attributed_phases(self) -> None:
        duplicate = bundle()
        phases = duplicate["report"]["measurements"][0]["pass_timings"][0]["phases"]
        phases.append(copy.deepcopy(phases[0]))
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(self.module.DiagnosticError, "duplicates phase"):
                self.build(Path(tmp), [duplicate, bundle()])

        over_attributed = bundle()
        over_attributed["report"]["measurements"][0]["pass_timings"][0]["phases"][0][
            "duration_ns"
        ] = 2_000_000
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(self.module.DiagnosticError, "attributes"):
                self.build(Path(tmp), [over_attributed, bundle()])


if __name__ == "__main__":
    unittest.main()
