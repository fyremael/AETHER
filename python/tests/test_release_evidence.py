from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import sys
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
EVIDENCE_SCRIPT = REPO_ROOT / "scripts" / "release_evidence.py"
VERIFY_SCRIPT = REPO_ROOT / "scripts" / "verify_release_evidence.py"


def load_modules():
    evidence_spec = importlib.util.spec_from_file_location("release_evidence", EVIDENCE_SCRIPT)
    assert evidence_spec and evidence_spec.loader
    evidence_module = importlib.util.module_from_spec(evidence_spec)
    sys.modules["release_evidence"] = evidence_module
    evidence_spec.loader.exec_module(evidence_module)
    verify_spec = importlib.util.spec_from_file_location("verify_release_evidence", VERIFY_SCRIPT)
    assert verify_spec and verify_spec.loader
    verify_module = importlib.util.module_from_spec(verify_spec)
    verify_spec.loader.exec_module(verify_module)
    return evidence_module, verify_module


class ReleaseEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.evidence, cls.verify = load_modules()

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.policy = self.evidence.load_json(REPO_ROOT / "fixtures/release/gate-policy.json")
        self.candidate = {
            "repository": "https://example.invalid/aether.git",
            "commit_sha": "a" * 40,
            "tree_sha": "b" * 40,
            "ref": "refs/tags/v-test",
            "dirty": False,
        }
        self.workflow = {
            "workflow_file": "local",
            "run_id": "local-test",
            "attempt": 1,
            "job_id": "local",
            "runner": "Windows",
            "host": "test-host",
            "tool_versions": {"test": "1"},
        }
        self.evidence_dir = self.root / "capture"
        (self.evidence_dir / "envelopes").mkdir(parents=True)
        (self.evidence_dir / "outputs").mkdir(parents=True)
        for gate in self.policy["gates"]:
            self._write_envelope(gate)
        self.package = self.root / "aether-package.zip"
        self.package.write_bytes(b"exact package bytes")
        output = self.root / "bundles"
        args = argparse.Namespace(
            policy="fixtures/release/gate-policy.json",
            evidence_dir=str(self.evidence_dir),
            package=str(self.package),
            package_attestation_sha256=None,
            sbom=None,
            subject=None,
            waiver=None,
            output_dir=str(output),
        )
        self.assertEqual(self.evidence.assemble(args), 0)
        self.bundle = next(output.glob("*.zip"))
        self.now = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_envelope(self, gate: dict, *, status: str = "passed") -> dict:
        log = self.evidence_dir / "outputs" / f"{gate['id']}.log"
        log.write_text(f"{gate['id']} test output\n", encoding="utf-8")
        attempt = {
            "attempt": 1,
            "started_at": "2026-07-12T10:00:00Z",
            "ended_at": "2026-07-12T10:01:00Z",
            "exit_code": 0 if status == "passed" else 1,
            "status": status,
            "failure_class": "none" if status == "passed" else "semantic",
        }
        envelope = {
            "schema_version": self.evidence.ENVELOPE_VERSION,
            "evidence_id": "",
            "gate_id": gate["id"],
            "official": False,
            "candidate": self.candidate,
            "workflow": self.workflow,
            "command": gate["commands"],
            "working_directory": gate.get("working_directory", "."),
            "started_at": attempt["started_at"],
            "ended_at": attempt["ended_at"],
            "exit_code": attempt["exit_code"],
            "attempt_history": [attempt],
            "inputs": [
                {"name": path, "path": path, "sha256": "c" * 64, "byte_size": 1}
                for path in gate.get("inputs", [])
            ],
            "observed_status": status,
            "metrics": {},
            "output": self.evidence.descriptor(
                log,
                name=f"{gate['id']}-log",
                display_path=f"outputs/{gate['id']}.log",
                media_type="text/plain",
            ),
            "valid_until": "2026-07-13T10:01:00Z",
        }
        envelope["evidence_id"] = self.evidence.identity_digest(envelope, "evidence_id")
        self.evidence.write_canonical_json(
            self.evidence_dir / "envelopes" / f"{gate['id']}.json", envelope
        )
        return envelope

    def _extract(self, bundle: Path | None = None) -> Path:
        destination = self.root / f"extract-{len(list(self.root.glob('extract-*')))}"
        destination.mkdir()
        with zipfile.ZipFile(bundle or self.bundle) as archive:
            archive.extractall(destination)
        return next(destination.rglob("bundle-manifest.json")).parent

    def _repack(self, extracted: Path, name: str) -> Path:
        output = self.root / name
        self.evidence.deterministic_zip(extracted, output)
        return output

    def test_valid_bundle_verdict_is_byte_stable_and_diagnostic(self) -> None:
        left = self.verify.verify_bundle(self.bundle, now=self.now)
        right = self.verify.verify_bundle(self.bundle, now=self.now)
        self.assertEqual(self.evidence.canonical_bytes(left), self.evidence.canonical_bytes(right))
        self.assertEqual(left["computed_verdict"], "blocked")
        self.assertIn("local/diagnostic", " ".join(left["blockers"]))

    def test_rejects_latest_stale_sha_tree_and_dirty_candidate(self) -> None:
        latest = self.root / "latest.zip"
        shutil.copy2(self.bundle, latest)
        with self.assertRaisesRegex(ValueError, "latest"):
            self.verify.verify_bundle(latest, now=self.now)
        with self.assertRaisesRegex(ValueError, "commit SHA"):
            self.verify.verify_bundle(self.bundle, expected_commit_sha="d" * 40, now=self.now)
        with self.assertRaisesRegex(ValueError, "tree SHA"):
            self.verify.verify_bundle(self.bundle, expected_tree_sha="e" * 40, now=self.now)
        dirty = dict(self.candidate, dirty=True)
        with self.assertRaisesRegex(ValueError, "dirty"):
            self.verify.verify_candidate(dirty)

    def test_rejects_missing_skipped_and_unknown_status_evidence(self) -> None:
        extracted = self._extract()
        first = next((extracted / "evidence").glob("*.json"))
        first.unlink()
        broken = self._repack(extracted, "missing-evidence.zip")
        with self.assertRaises(ValueError):
            self.verify.verify_bundle(broken, now=self.now)

        gate = self.policy["gates"][0]
        envelope = self._write_envelope(gate, status="skipped")
        with self.assertRaisesRegex(ValueError, "skipped"):
            self.verify.verify_envelope(
                self.evidence_dir,
                envelope,
                gate,
                self.candidate,
                self.workflow,
                False,
                self.policy,
                self.now,
            )
        envelope["observed_status"] = "ci_blocking"
        with self.assertRaisesRegex(ValueError, "unknown observed status"):
            self.verify.verify_envelope(
                self.evidence_dir,
                envelope,
                gate,
                self.candidate,
                self.workflow,
                False,
                self.policy,
                self.now,
            )

    def test_rejects_modified_artifact_and_package_attestation_mismatch(self) -> None:
        extracted = self._extract()
        next((extracted / "outputs").glob("*.log")).write_text("modified", encoding="utf-8")
        modified = self._repack(extracted, "modified.zip")
        with self.assertRaisesRegex(ValueError, "(byte size|digest) mismatch"):
            self.verify.verify_bundle(modified, now=self.now)

        extracted = self._extract()
        manifest_path = extracted / "bundle-manifest.json"
        manifest = self.evidence.load_json(manifest_path)
        manifest["package_attestation_subject_sha256"] = "f" * 64
        manifest["bundle_id"] = self.evidence.identity_digest(manifest, "bundle_id")
        self.evidence.write_canonical_json(manifest_path, manifest)
        mismatched = self._repack(extracted, "package-mismatch.zip")
        with self.assertRaisesRegex(ValueError, "attested subject"):
            self.verify.verify_bundle(mismatched, now=self.now)

    def test_rejects_future_expired_and_hidden_retry_evidence(self) -> None:
        gate = self.policy["gates"][0]
        envelope = self._write_envelope(gate)
        envelope["ended_at"] = "2026-07-13T12:00:00Z"
        with self.assertRaisesRegex(ValueError, "future"):
            self.verify.verify_time_window(envelope, self.now)
        envelope = self._write_envelope(gate)
        envelope["valid_until"] = "2026-07-11T12:00:00Z"
        with self.assertRaisesRegex(ValueError, "expired"):
            self.verify.verify_time_window(envelope, self.now)
        envelope = self._write_envelope(gate)
        failed = dict(envelope["attempt_history"][0], status="failed", failure_class="semantic")
        envelope["attempt_history"] = [failed, dict(envelope["attempt_history"][0], attempt=2)]
        with self.assertRaisesRegex(ValueError, "retried"):
            self.verify.verify_attempts(envelope, gate)

    def test_rejects_authored_policy_and_declared_only_workflow(self) -> None:
        policy = json.loads(json.dumps(self.policy))
        policy["gates"][0]["status"] = "ready"
        with self.assertRaisesRegex(ValueError, "authored"):
            self.evidence.validate_policy(policy)
        workflow = dict(self.workflow)
        workflow.update(
            {
                "workflow_file": self.policy["official_workflow"],
                "job_id": self.policy["official_job"],
                "run_id": "declared-only",
                "runner": "Windows",
            }
        )
        with self.assertRaisesRegex(ValueError, "without a successful run"):
            self.verify.verify_official_workflow(workflow, self.policy)

    def test_rejects_wrong_host_suite_baseline_threshold_capacity_and_pages_sha(self) -> None:
        gate = dict(self.policy["gates"][0])
        gate["expected_metrics"] = {
            "suite": "canonical",
            "baseline": "base-v1",
            "threshold": 1.0,
        }
        envelope = self._write_envelope(gate)
        envelope["metrics"] = {"suite": "wrong", "baseline": "base-v1", "threshold": 1.0}
        envelope["evidence_id"] = self.evidence.identity_digest(envelope, "evidence_id")
        with self.assertRaisesRegex(ValueError, "wrong suite"):
            self.verify.verify_envelope(
                self.evidence_dir,
                envelope,
                gate,
                self.candidate,
                self.workflow,
                False,
                self.policy,
                self.now,
            )
        capacity_gate = dict(gate, id="performance.capacity", expected_metrics={})
        capacity = dict(envelope, gate_id="performance.capacity", metrics={"artifact": {"capacity": {}}})
        capacity["evidence_id"] = self.evidence.identity_digest(capacity, "evidence_id")
        with self.assertRaisesRegex(ValueError, "nesting"):
            self.verify.verify_envelope(
                self.evidence_dir,
                capacity,
                capacity_gate,
                self.candidate,
                self.workflow,
                False,
                self.policy,
                self.now,
            )
        pages_gate = dict(gate, id="delivery.pages_candidate_sha", expected_metrics={})
        pages = dict(envelope, gate_id="delivery.pages_candidate_sha", metrics={"deployed_sha": "d" * 40})
        pages["evidence_id"] = self.evidence.identity_digest(pages, "evidence_id")
        with self.assertRaisesRegex(ValueError, "Pages"):
            self.verify.verify_envelope(
                self.evidence_dir,
                pages,
                pages_gate,
                self.candidate,
                self.workflow,
                False,
                self.policy,
                self.now,
            )

    def test_rejects_invalid_nonwaivable_and_cross_candidate_waivers(self) -> None:
        gate = self.policy["gates"][0]
        waiver = {
            "schema_version": "aether.risk-waiver.v1",
            "waiver_id": "",
            "gate_id": gate["id"],
            "candidate_commit_sha": self.candidate["commit_sha"],
            "candidate_tree_sha": self.candidate["tree_sha"],
            "reason": "test",
            "compensating_controls": ["control"],
            "approved_by": "release-owner",
            "approved_at": "2026-07-12T10:00:00Z",
            "expires_at": "2026-07-13T10:00:00Z",
            "signature": {
                "scheme": "external_attestation",
                "attestation_ref": "https://example.invalid/attestation",
                "subject_digest": "0" * 64,
            },
        }
        waiver["signature"]["subject_digest"] = self.verify.waiver_subject_digest(waiver)
        waiver["waiver_id"] = self.evidence.identity_digest(waiver, "waiver_id")
        with self.assertRaisesRegex(ValueError, "non-waivable"):
            self.verify.verify_waiver(waiver, gate, self.candidate, self.now)
        waivable = dict(gate, id="performance.optional", waivable=True)
        waiver["gate_id"] = waivable["id"]
        waiver["candidate_commit_sha"] = "d" * 40
        waiver["signature"]["subject_digest"] = self.verify.waiver_subject_digest(waiver)
        waiver["waiver_id"] = self.evidence.identity_digest(waiver, "waiver_id")
        with self.assertRaisesRegex(ValueError, "crosses candidate"):
            self.verify.verify_waiver(waiver, waivable, self.candidate, self.now)

    def test_rejects_sbom_missing_lockfile_component(self) -> None:
        sbom = self.root / "bad-sbom.json"
        sbom.write_text(
            json.dumps(
                {
                    "aether_lockfile_components": ["pkg:cargo/serde@1.0.0"],
                    "components": [{"name": "other", "version": "1.0.0"}],
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "missing a lockfile component"):
            self.verify.verify_sbom(sbom)


if __name__ == "__main__":
    unittest.main()
