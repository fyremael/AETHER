from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "hardening_promotion.py"


def load_module():
    spec = importlib.util.spec_from_file_location("hardening_promotion", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HardeningPromotionSummaryTests(unittest.TestCase):
    def test_gate_summary_marks_blocking_and_diagnostic_statuses(self) -> None:
        module = load_module()
        config = {
            "minimum_consecutive_scheduled_green_runs": 3,
            "workflow_file": "qa-hardening.yml",
            "promotion_order": ["admin", "operator", "user"],
            "groups": {
                "admin": {
                    "label": "Admin",
                    "workflow_target": "CI",
                    "blocking": True,
                },
                "operator": {
                    "label": "Operator",
                    "workflow_target": "CI",
                    "blocking": False,
                },
                "user": {
                    "label": "User",
                    "workflow_target": "CI",
                    "blocking": False,
                },
            },
        }
        hardening = {
            "results": [
                {"persona": "admin", "status": "passed"},
                {"persona": "operator", "status": "failed"},
            ]
        }

        summary = module.build_gate_summary(
            config,
            hardening_payload=hardening,
            hardening_source="latest.json",
            generated_at="2026-07-02T00:00:00+00:00",
        )
        by_group = {item["group"]: item for item in summary["groups"]}

        self.assertEqual(by_group["admin"]["mode"], "blocking")
        self.assertEqual(by_group["admin"]["release_readiness_status"], "passed")
        self.assertEqual(by_group["operator"]["mode"], "diagnostic")
        self.assertEqual(by_group["operator"]["latest_hardening_status"], "failed")
        self.assertEqual(by_group["operator"]["release_readiness_status"], "diagnostic")
        self.assertEqual(by_group["user"]["latest_hardening_status"], "skipped")


if __name__ == "__main__":
    unittest.main()
