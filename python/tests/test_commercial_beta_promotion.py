from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_modules():
    evidence_spec = importlib.util.spec_from_file_location(
        "release_evidence", REPO_ROOT / "scripts" / "release_evidence.py"
    )
    assert evidence_spec and evidence_spec.loader
    evidence = importlib.util.module_from_spec(evidence_spec)
    sys.modules["release_evidence"] = evidence
    evidence_spec.loader.exec_module(evidence)
    subject_spec = importlib.util.spec_from_file_location(
        "release_subjects", REPO_ROOT / "scripts" / "release_subjects.py"
    )
    assert subject_spec and subject_spec.loader
    subjects = importlib.util.module_from_spec(subject_spec)
    sys.modules["release_subjects"] = subjects
    subject_spec.loader.exec_module(subjects)
    verify_spec = importlib.util.spec_from_file_location(
        "verify_release_evidence", REPO_ROOT / "scripts" / "verify_release_evidence.py"
    )
    assert verify_spec and verify_spec.loader
    verify = importlib.util.module_from_spec(verify_spec)
    sys.modules["verify_release_evidence"] = verify
    verify_spec.loader.exec_module(verify)
    promotion_spec = importlib.util.spec_from_file_location(
        "commercial_beta_promotion", REPO_ROOT / "scripts" / "commercial_beta_promotion.py"
    )
    assert promotion_spec and promotion_spec.loader
    promotion = importlib.util.module_from_spec(promotion_spec)
    promotion_spec.loader.exec_module(promotion)
    return evidence, promotion


class CommercialBetaPromotionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.evidence, cls.promotion = load_modules()

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.verdict = {
            "schema_version": "aether.release-evidence-verdict.v1",
            "bundle_id": "1" * 64,
            "candidate": {
                "repository": "fyremael/AETHER",
                "commit_sha": "a" * 40,
                "tree_sha": "b" * 40,
                "ref": "refs/heads/main",
                "dirty": False,
            },
            "policy_id": "commercial-beta-r8-v1",
            "official": True,
            "computed_verdict": "passed",
            "blockers": [],
            "evidence_ids": ["2" * 64],
            "package_sha256": "3" * 64,
            "verifier": {
                "version": self.evidence.VERIFIER_VERSION,
                "algorithm": self.evidence.ALGORITHM,
            },
            "workflow": {"run_id": "42", "attempt": 1},
            "official_repository": "fyremael/AETHER",
            "official_artifact": {
                "artifact_id": 10,
                "artifact_name": "aether-release-evidence-aaa-42-1",
                "sha256": "4" * 64,
                "byte_size": 100,
                "run_id": "42",
                "attempt": 1,
            },
        }
        self.official = self.root / "official.json"
        self.independent = self.root / "independent.json"
        self.bundle = self.root / "aether-release-evidence-aaa-42-1.zip"
        self.bundle.write_bytes(b"bundle placeholder")
        self.write_verdicts()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_verdicts(self) -> None:
        self.evidence.write_canonical_json(self.official, self.verdict)
        self.evidence.write_canonical_json(self.independent, self.verdict)

    def args(self) -> argparse.Namespace:
        return argparse.Namespace(
            official_verdict=str(self.official),
            independent_verdict=str(self.independent),
            bundle=str(self.bundle),
            expected_commit_sha=self.verdict["candidate"]["commit_sha"],
            expected_tree_sha=self.verdict["candidate"]["tree_sha"],
            expected_ref="refs/heads/main",
        )

    def generate(self, args: argparse.Namespace | None = None) -> dict:
        return self.promotion.generate(
            args or self.args(),
            bundle_verifier=lambda _bundle, **_kwargs: self.evidence.load_json(self.official),
            verdict_artifact_verifier=lambda _verdict, _bytes: {
                "artifact_id": 11,
                "artifact_name": "aether-release-evidence-verdict-aaa-42-1",
                "sha256": "5" * 64,
                "byte_size": 100,
                "run_id": "42",
                "attempt": 1,
            },
        )

    def test_generates_immutable_record_with_narrow_beta_and_ga_zero_of_four(self) -> None:
        record = self.generate()
        self.promotion.verify_record(record)
        self.assertEqual(record["stage"], "commercial_beta")
        self.assertEqual(record["ga"]["passed_gates"], 0)
        self.assertEqual(record["ga"]["required_gates"], 4)
        self.assertEqual(len(record["ga"]["blockers"]), 4)
        self.assertIn("generalized-distributed-truth", record["beta_boundary"]["excluded_claims"])

    def test_diagnostic_failed_disagreeing_and_latest_inputs_cannot_promote(self) -> None:
        self.verdict["official"] = False
        self.write_verdicts()
        with self.assertRaisesRegex(ValueError, "diagnostic"):
            self.generate()
        self.verdict["official"] = True
        self.verdict["computed_verdict"] = "blocked"
        self.verdict["blockers"] = ["blocked"]
        self.write_verdicts()
        with self.assertRaisesRegex(ValueError, "did not pass"):
            self.generate()
        self.verdict["computed_verdict"] = "passed"
        self.verdict["blockers"] = []
        self.write_verdicts()
        independent = json.loads(self.independent.read_text(encoding="utf-8"))
        independent["package_sha256"] = "6" * 64
        self.evidence.write_canonical_json(self.independent, independent)
        with self.assertRaisesRegex(ValueError, "disagree"):
            self.generate()
        self.write_verdicts()
        args = self.args()
        latest = self.root / "latest.zip"
        latest.write_bytes(b"bundle placeholder")
        args.bundle = str(latest)
        with self.assertRaisesRegex(ValueError, "latest"):
            self.generate(args)

    def test_tampering_and_any_ga_promotion_are_rejected(self) -> None:
        record = self.generate()
        record["package_sha256"] = "7" * 64
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            self.promotion.verify_record(record)
        record = self.generate()
        record["ga"]["status"] = "passed"
        record["promotion_id"] = self.evidence.identity_digest(record, "promotion_id")
        with self.assertRaisesRegex(ValueError, "GA must remain blocked"):
            self.promotion.verify_record(record)


if __name__ == "__main__":
    unittest.main()
