from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "commercial_readiness.py"


def load_module():
    spec = importlib.util.spec_from_file_location("commercial_readiness", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CommercialReadinessTests(unittest.TestCase):
    def test_stage_status_separates_alpha_from_beta(self) -> None:
        module = load_module()
        payload = {
            "schema_version": 1,
            "current_target_stage": "alpha",
            "stages": [
                {
                    "id": "alpha",
                    "label": "Alpha",
                    "intent": "supported pilot",
                    "claim_boundary": "single-node pilot",
                    "gates": [
                        {
                            "id": "kernel",
                            "title": "Kernel",
                            "owner": "kernel",
                            "gate_class": "blocking",
                            "status": "ready",
                            "evidence": [{"label": "command", "command": "cargo test"}],
                            "blockers": [],
                            "next_actions": [],
                        }
                    ],
                },
                {
                    "id": "beta",
                    "label": "Beta",
                    "intent": "commercial beta",
                    "claim_boundary": "customer deployment",
                    "gates": [
                        {
                            "id": "service",
                            "title": "Service",
                            "owner": "service",
                            "gate_class": "blocking",
                            "status": "blocked",
                            "evidence": [{"label": "command", "command": "service drill"}],
                            "blockers": ["missing drill"],
                            "next_actions": ["add drill"],
                        }
                    ],
                },
            ],
        }

        errors = module.validate_ledger(payload, REPO_ROOT)
        self.assertEqual(errors, [])

        with tempfile.TemporaryDirectory() as tmp:
            summary = module.build_summary(
                payload=payload,
                ledger_path=Path(tmp) / "ledger.json",
                root=Path(tmp),
                generated_at="2026-07-02T00:00:00+00:00",
            )

        by_stage = {stage["id"]: stage for stage in summary["stages"]}
        self.assertEqual(by_stage["alpha"]["status"], "ready")
        self.assertEqual(by_stage["beta"]["status"], "blocked")
        self.assertEqual(summary["current_target_status"], "ready")

    def test_blocked_blocking_gate_requires_explicit_blocker(self) -> None:
        module = load_module()
        payload = {
            "schema_version": 1,
            "current_target_stage": "beta",
            "stages": [
                {
                    "id": "beta",
                    "label": "Beta",
                    "intent": "commercial beta",
                    "claim_boundary": "customer deployment",
                    "gates": [
                        {
                            "id": "service",
                            "title": "Service",
                            "owner": "service",
                            "gate_class": "blocking",
                            "status": "blocked",
                            "evidence": [{"label": "command", "command": "service drill"}],
                            "blockers": [],
                            "next_actions": ["add drill"],
                        }
                    ],
                }
            ],
        }

        errors = module.validate_ledger(payload, REPO_ROOT)
        self.assertTrue(
            any("is blocking and not ready but has no blockers" in error for error in errors),
            errors,
        )

    def test_tracked_ledger_validates(self) -> None:
        module = load_module()
        payload = module.load_json(
            REPO_ROOT / "fixtures" / "release" / "commercial-readiness-ledger.json"
        )
        self.assertEqual(module.validate_ledger(payload, REPO_ROOT), [])


if __name__ == "__main__":
    unittest.main()
