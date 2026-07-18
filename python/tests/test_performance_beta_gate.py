from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "performance_beta_gate.py"


def load_module():
    spec = importlib.util.spec_from_file_location("performance_beta_gate", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PerformanceBetaGateTests(unittest.TestCase):
    def test_tracked_threshold_manifest_has_required_surfaces(self) -> None:
        module = load_module()
        payload = module.load_json(
            REPO_ROOT / "fixtures" / "release" / "performance-beta-thresholds.json"
        )
        threshold_ids = {item["id"] for item in payload["latency_thresholds"]}

        self.assertEqual(payload["schema_version"], 2)
        self.assertEqual(
            payload["allowed_host_ids"],
            ["dev-chad-windows-native", "github-windows-latest"],
        )
        self.assertEqual(payload["suite_id"], "full_stack")
        self.assertIn("core_restart_replay", threshold_ids)
        self.assertIn("service_restart_replay", threshold_ids)
        self.assertIn("http_coordination_report", threshold_ids)
        self.assertIn("http_coordination_delta", threshold_ids)

    def test_drift_status_reads_gated_overall(self) -> None:
        module = load_module()

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "drift.md"
            path.write_text("- Gated overall: `ok`\n", encoding="utf-8")

            self.assertEqual(module.drift_status(path), "ok")

    def test_find_measurement_uses_group_workload_and_scale(self) -> None:
        module = load_module()
        bundle = {
            "report": {
                "measurements": [
                    {
                        "group": "http_pilot_boundary",
                        "workload": "HTTP coordination report endpoint",
                        "scale": "pilot coordination",
                        "latency": {"mean": {"secs": 0, "nanos": 3_000_000}},
                    }
                ]
            }
        }
        threshold = {
            "group": "http_pilot_boundary",
            "workload": "HTTP coordination report endpoint",
            "scale": "pilot coordination",
        }

        measurement = module.find_measurement(bundle, threshold)
        self.assertIsNotNone(measurement)
        self.assertEqual(module.duration_ms(measurement["latency"]["mean"]), 3.0)

    def test_bundle_identity_accepts_only_explicitly_approved_hosts(self) -> None:
        module = load_module()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            thresholds = root / "thresholds.json"
            bundle = root / "bundle.json"
            module.write_json(
                thresholds,
                {
                    "schema_version": 2,
                    "allowed_host_ids": [
                        "dev-chad-windows-native",
                        "github-windows-latest",
                    ],
                    "suite_id": "full_stack",
                    "drift_reports": [],
                    "latency_thresholds": [],
                },
            )

            for host_id in ("dev-chad-windows-native", "github-windows-latest"):
                module.write_json(
                    bundle,
                    {
                        "host_manifest": {"host_id": host_id},
                        "run": {"suite_id": "full_stack"},
                    },
                )
                report = module.build_report(
                    SimpleNamespace(
                        thresholds=str(thresholds),
                        bundle=str(bundle),
                        generated_at="2026-07-18T00:00:00+00:00",
                    )
                )
                self.assertTrue(report["beta_ready"])
                self.assertEqual(report["gates"][0]["status"], "passed")

            module.write_json(
                bundle,
                {
                    "host_manifest": {"host_id": "unapproved-windows-host"},
                    "run": {"suite_id": "full_stack"},
                },
            )
            report = module.build_report(
                SimpleNamespace(
                    thresholds=str(thresholds),
                    bundle=str(bundle),
                    generated_at="2026-07-18T00:00:00+00:00",
                )
            )
            self.assertFalse(report["beta_ready"])
            self.assertEqual(report["gates"][0]["status"], "blocked")

    def test_malformed_host_policy_fails_closed(self) -> None:
        module = load_module()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            thresholds = root / "thresholds.json"
            bundle = root / "bundle.json"
            module.write_json(
                thresholds,
                {
                    "schema_version": 2,
                    "allowed_host_ids": [
                        "github-windows-latest",
                        "github-windows-latest",
                    ],
                    "suite_id": "full_stack",
                    "drift_reports": [],
                    "latency_thresholds": [],
                },
            )
            module.write_json(
                bundle,
                {
                    "host_manifest": {"host_id": "github-windows-latest"},
                    "run": {"suite_id": "full_stack"},
                },
            )
            report = module.build_report(
                SimpleNamespace(
                    thresholds=str(thresholds),
                    bundle=str(bundle),
                    generated_at="2026-07-18T00:00:00+00:00",
                )
            )
            self.assertFalse(report["beta_ready"])
            self.assertEqual(report["gates"][0]["allowed_host_ids"], [])
            self.assertEqual(report["gates"][0]["threshold_schema_version"], 2)

            payload = module.load_json(thresholds)
            payload["schema_version"] = 1
            payload["allowed_host_ids"] = ["github-windows-latest"]
            module.write_json(thresholds, payload)
            report = module.build_report(
                SimpleNamespace(
                    thresholds=str(thresholds),
                    bundle=str(bundle),
                    generated_at="2026-07-18T00:00:00+00:00",
                )
            )
            self.assertFalse(report["beta_ready"])
            self.assertEqual(report["gates"][0]["threshold_schema_version"], 1)


if __name__ == "__main__":
    unittest.main()
