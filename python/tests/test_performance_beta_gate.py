from __future__ import annotations

import copy
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
        self.assertEqual(module.validate_threshold_policy(payload), [])

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
        policy = module.load_json(
            REPO_ROOT / "fixtures" / "release" / "performance-beta-thresholds.json"
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            thresholds = root / "thresholds.json"
            bundle = root / "bundle.json"
            module.write_json(thresholds, policy)

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
                gates = {gate["id"]: gate for gate in report["gates"]}
                self.assertEqual(gates["policy_integrity"]["status"], "passed")
                self.assertEqual(gates["bundle_identity"]["status"], "passed")

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
            gates = {gate["id"]: gate for gate in report["gates"]}
            self.assertEqual(gates["policy_integrity"]["status"], "passed")
            self.assertEqual(gates["bundle_identity"]["status"], "blocked")

    def test_policy_integrity_rejects_every_structural_weakening(self) -> None:
        module = load_module()
        baseline = module.load_json(
            REPO_ROOT / "fixtures" / "release" / "performance-beta-thresholds.json"
        )

        weakened: dict[str, dict] = {}

        payload = copy.deepcopy(baseline)
        payload["schema_version"] = 1
        weakened["old schema"] = payload

        payload = copy.deepcopy(baseline)
        payload["allowed_host_ids"].append("unapproved-windows-host")
        weakened["unapproved policy host"] = payload

        payload = copy.deepcopy(baseline)
        payload["allowed_host_ids"][0] = " dev-chad-windows-native"
        weakened["untrimmed host"] = payload

        payload = copy.deepcopy(baseline)
        payload["drift_reports"] = []
        weakened["empty drift gates"] = payload

        payload = copy.deepcopy(baseline)
        payload["drift_reports"].append(copy.deepcopy(payload["drift_reports"][0]))
        weakened["duplicate drift suite"] = payload

        payload = copy.deepcopy(baseline)
        payload["drift_reports"][0]["allowed_gated_overall"] = ["missing"]
        weakened["unknown drift status"] = payload

        payload = copy.deepcopy(baseline)
        payload["latency_thresholds"] = []
        weakened["empty latency gates"] = payload

        payload = copy.deepcopy(baseline)
        payload["latency_thresholds"][1]["id"] = payload["latency_thresholds"][0]["id"]
        weakened["duplicate latency id"] = payload

        payload = copy.deepcopy(baseline)
        payload["latency_thresholds"][0]["max_mean_ms"] = float("inf")
        weakened["non-finite ceiling"] = payload

        payload = copy.deepcopy(baseline)
        payload["latency_thresholds"][0]["max_mean_ms"] = True
        weakened["boolean ceiling"] = payload

        payload = copy.deepcopy(baseline)
        payload["latency_thresholds"] = payload["latency_thresholds"][1:]
        weakened["missing required latency"] = payload

        payload = copy.deepcopy(baseline)
        payload["latency_thresholds"][0]["workload"] = "easier workload"
        weakened["changed required surface"] = payload

        for label, policy in weakened.items():
            with self.subTest(label=label):
                self.assertTrue(module.validate_threshold_policy(policy))

    def test_invalid_policy_emits_blocking_integrity_gate(self) -> None:
        module = load_module()
        policy = module.load_json(
            REPO_ROOT / "fixtures" / "release" / "performance-beta-thresholds.json"
        )
        policy["drift_reports"] = []

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            thresholds = root / "thresholds.json"
            bundle = root / "bundle.json"
            module.write_json(thresholds, policy)
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
            self.assertEqual(len(report["gates"]), 1)
            self.assertEqual(report["gates"][0]["id"], "policy_integrity")
            self.assertEqual(report["gates"][0]["status"], "blocked")


if __name__ == "__main__":
    unittest.main()
