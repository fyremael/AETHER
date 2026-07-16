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


def policy_payload() -> dict:
    return {
        "schema_version": 2,
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
                        "title": "Kernel contract",
                        "owner": "kernel",
                        "gate_class": "blocking",
                        "requirement": "the candidate kernel passes",
                        "evidence_requirement": {
                            "gate_ids": ["semantic.full_acceptance"],
                            "bundle_subjects": [],
                        },
                    }
                ],
            }
        ],
    }


class CommercialReadinessTests(unittest.TestCase):
    def test_policy_summary_never_computes_readiness_from_declarations(self) -> None:
        module = load_module()
        payload = policy_payload()
        self.assertEqual(module.validate_ledger(payload, REPO_ROOT), [])
        with tempfile.TemporaryDirectory() as tmp:
            summary = module.build_summary(
                payload=payload,
                ledger_path=Path(tmp) / "ledger.json",
                root=Path(tmp),
                generated_at="2026-07-12T00:00:00+00:00",
            )
        self.assertEqual(summary["computed_status"], "not_computed_from_policy")
        self.assertEqual(summary["readiness_source"], "immutable_evidence_bundle_only")
        rendered = module.render_markdown(summary)
        self.assertIn("contains no observed gate outcomes", rendered)

    def test_authored_outcome_fields_are_rejected(self) -> None:
        module = load_module()
        for field, value in [
            ("status", "ready"),
            ("evidence", [{"path": "somewhere"}]),
            ("blockers", []),
        ]:
            payload = policy_payload()
            payload["stages"][0]["gates"][0][field] = value
            errors = module.validate_ledger(payload, REPO_ROOT)
            self.assertTrue(any("authored outcome fields" in error for error in errors), errors)

    def test_tracked_claim_policy_validates(self) -> None:
        module = load_module()
        payload = module.load_json(
            REPO_ROOT / "fixtures" / "release" / "commercial-readiness-ledger.json"
        )
        self.assertEqual(module.validate_ledger(payload, REPO_ROOT), [])

    def test_commercial_beta_requirements_are_enforced_by_gate_policy(self) -> None:
        module = load_module()
        ledger = module.load_json(
            REPO_ROOT / "fixtures" / "release" / "commercial-readiness-ledger.json"
        )
        gate_policy = module.load_json(REPO_ROOT / "fixtures" / "release" / "gate-policy.json")
        policy_gate_ids = {gate["id"] for gate in gate_policy["gates"]}
        policy_subjects = set(gate_policy["future_required_bundle_subjects"])
        commercial_beta = next(
            stage for stage in ledger["stages"] if stage["id"] == "commercial_beta"
        )
        for gate in commercial_beta["gates"]:
            requirement = gate["evidence_requirement"]
            self.assertLessEqual(set(requirement["gate_ids"]), policy_gate_ids)
            self.assertLessEqual(set(requirement["bundle_subjects"]), policy_subjects)


if __name__ == "__main__":
    unittest.main()
