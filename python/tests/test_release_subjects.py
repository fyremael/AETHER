from __future__ import annotations

import importlib.util
import io
import json
import sys
import unittest
import zipfile
from datetime import datetime, timezone
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
    verify_spec.loader.exec_module(verify)
    return evidence, subjects, verify


class ReleaseSubjectTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.evidence, cls.subjects, cls.verify_module = load_modules()

    def setUp(self) -> None:
        self.now = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
        self.candidate = {
            "repository": "fyremael/AETHER",
            "commit_sha": "a" * 40,
            "tree_sha": "b" * 40,
            "ref": "refs/heads/main",
            "dirty": False,
        }
        self.package_sha = "c" * 64
        self.gate_policy = self.evidence.load_json(
            REPO_ROOT / "fixtures" / "release" / "gate-policy.json"
        )

    def details(self, subject_id: str) -> dict:
        details = {
            "capacity": {
                "gate_passed": True,
                "metrics": {"nodes": 1},
                "policy": {
                    "node_class": "M",
                    "minimum_board_size": 1024,
                    "minimum_mixed_operator_concurrency": 32,
                    "minimum_operator_operations_per_worker": 12,
                    "required_operator_mix": [
                        "/health",
                        "/v1/status",
                        "/v1/history",
                        "/v1/reports/pilot/coordination",
                        "/v1/reports/pilot/coordination-delta",
                        "/v1/explanations/resolve",
                    ],
                    "minimum_durable_replay_size": 10000,
                    "maximum_target_p95_latency_ms": 2000.0,
                    "maximum_target_replay_seconds": 60.0,
                    "maximum_operator_p95_latency_ms": 2000.0,
                    "maximum_operator_error_rate": 0.0,
                    "maximum_operator_saturation_responses": 0,
                },
                "selected_envelope": {
                    "node_class": "M",
                    "maximum_recommended_pilot_board_size": 1024,
                    "maximum_recommended_mixed_operator_concurrency": 32,
                    "maximum_recommended_durable_replay_size": 10000,
                },
                "recommended_hardware": {
                    "node_class": "M",
                    "target_p95_latency_ms": 1500.0,
                    "target_replay_seconds": 45.0,
                },
                "concurrency_pack": {
                    "service_instances_per_point": 1,
                    "operations_per_worker": 12,
                    "operation_mix": [
                        "/health",
                        "/v1/status",
                        "/v1/history",
                        "/v1/reports/pilot/coordination",
                        "/v1/reports/pilot/coordination-delta",
                        "/v1/explanations/resolve",
                    ],
                    "points": [
                        {
                            "concurrency": 32,
                            "total_operations": 384,
                            "successful_operations": 384,
                            "failed_operations": 0,
                            "saturation_responses": 0,
                            "setup_duration_ms": 125.0,
                            "measurement_duration_ms": 900.0,
                            "failure_messages": [],
                            "throughput_per_second": 426.6666666666667,
                            "mean_latency_ms": 100.0,
                            "p95_latency_ms": 225.0,
                            "p99_latency_ms": 300.0,
                        }
                    ],
                },
            },
            "code-scan": {
                "jobs": [
                    {"id": 1, "name": "CodeQL (go)", "conclusion": "success"},
                    {"id": 2, "name": "CodeQL (python)", "conclusion": "success"},
                ]
            },
            "transport-tls": {
                "mode": "verify_full",
                "job_conclusion": "success",
                "job": {"id": 3, "name": "Postgres verified TLS journal", "conclusion": "success"},
            },
            "pages-deployment": {
                "observed_candidate_sha": self.candidate["commit_sha"],
                "url": "https://example.invalid/source-version.json",
            },
            "customer-workflow": {"workflow_passed": True, "steps": 5},
            "vulnerability-scan": {"tools": ["trivy"], "findings": 0},
        }[subject_id]
        if subject_id == "capacity":
            acceptance = self.subjects.recompute_capacity_acceptance(
                details["policy"],
                details["selected_envelope"],
                details["recommended_hardware"],
                details["concurrency_pack"],
            )
            details["capacity_acceptance"] = acceptance
            details["metrics"]["checks"] = acceptance["checks"]
            details["metrics"]["operator_error_rate"] = acceptance[
                "operator_error_rate"
            ]
            details["metrics"]["operator_p95_latency_ms"] = acceptance[
                "policy_point"
            ]["p95_latency_ms"]
            details["metrics"]["service_instances_per_point"] = acceptance[
                "service_instances_per_point"
            ]
        if subject_id in {"capacity", "pages-deployment", "vulnerability-scan"}:
            details["source_files"] = [
                {
                    "artifact_id": 9,
                    "path": "evidence.json",
                    "sha256": "d" * 64,
                    "byte_size": 10,
                }
            ]
        return details

    def envelope(self, subject_id: str = "capacity") -> dict:
        details = self.details(subject_id)
        source_run = {
            "workflow_file": ".github/workflows/capacity-planning.yml",
            "run_id": "7",
            "attempt": 1,
            "head_sha": self.candidate["commit_sha"],
            "status": "passed",
        }
        receipt = {
            "artifact_id": 9,
            "artifact_name": "capacity-report-" + self.candidate["commit_sha"] + "-7-1",
            "workflow_file": source_run["workflow_file"],
            "run_id": source_run["run_id"],
            "attempt": source_run["attempt"],
            "head_sha": self.candidate["commit_sha"],
            "status": "passed",
            "sha256": "d" * 64,
            "byte_size": 10,
        }
        payload = {
            "schema_version": self.subjects.SUBJECT_VERSION,
            "subject_identity": "",
            "subject_id": subject_id,
            "candidate": self.candidate,
            "producer": {
                "workflow_file": ".github/workflows/release-readiness.yml",
                "workflow_name": "Release Readiness",
                "job_name": "Release Readiness (Windows)",
                "run_id": "42",
                "attempt": 1,
                "runner": "Windows",
                "host": "github-windows-latest",
            },
            "observed_status": "passed",
            "package": {"name": "aether.zip", "sha256": self.package_sha},
            "source_runs": [source_run],
            "source_artifacts": [receipt],
            "generated_at": "2026-07-15T11:00:00Z",
            "valid_until": "2026-07-16T11:00:00Z",
            "metrics": {"test": 1},
            "observation": {
                "status": "passed",
                "candidate_commit_sha": self.candidate["commit_sha"],
                "candidate_tree_sha": self.candidate["tree_sha"],
                "package_sha256": self.package_sha,
                "check": subject_id,
                "details": details,
            },
        }
        if subject_id == "capacity":
            payload["metrics"] = json.loads(json.dumps(details["metrics"]))
        payload["subject_identity"] = self.evidence.identity_digest(payload, "subject_identity")
        return payload

    def verify(self, payload: dict, subject_id: str = "capacity") -> None:
        self.subjects.verify_envelope(
            payload,
            expected_subject_id=subject_id,
            candidate=self.candidate,
            package_sha256=self.package_sha,
            now=self.now,
            gate_policy=self.gate_policy,
        )

    def resign(self, payload: dict) -> None:
        payload["subject_identity"] = self.evidence.identity_digest(payload, "subject_identity")

    def test_valid_subject_is_candidate_run_artifact_and_package_bound(self) -> None:
        self.verify(self.envelope())

    def test_rejects_missing_failed_expired_cross_candidate_and_wrong_package(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be an object"):
            self.subjects.verify_envelope(
                None,
                expected_subject_id="capacity",
                candidate=self.candidate,
                package_sha256=self.package_sha,
                now=self.now,
                gate_policy=self.gate_policy,
            )
        failed = self.envelope()
        failed["observed_status"] = "failed"
        self.resign(failed)
        with self.assertRaisesRegex(ValueError, "did not pass"):
            self.verify(failed)
        expired = self.envelope()
        expired["valid_until"] = "2026-07-15T10:00:00Z"
        self.resign(expired)
        with self.assertRaisesRegex(ValueError, "expired"):
            self.verify(expired)
        cross_candidate = self.envelope()
        cross_candidate["candidate"] = dict(self.candidate, commit_sha="e" * 40)
        self.resign(cross_candidate)
        with self.assertRaisesRegex(ValueError, "candidate mismatch"):
            self.verify(cross_candidate)
        wrong_package = self.envelope()
        wrong_package["package"]["sha256"] = "f" * 64
        self.resign(wrong_package)
        with self.assertRaisesRegex(ValueError, "another package"):
            self.verify(wrong_package)

    def test_rejects_forged_duplicated_ambiguous_and_cross_run_receipts(self) -> None:
        forged = self.envelope()
        forged["metrics"]["test"] = 2
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            self.verify(forged)
        duplicated = self.envelope()
        duplicated["source_artifacts"].append(json.loads(json.dumps(duplicated["source_artifacts"][0])))
        self.resign(duplicated)
        with self.assertRaisesRegex(ValueError, "duplicate source artifact"):
            self.verify(duplicated)
        ambiguous = self.envelope()
        ambiguous["source_runs"].append(json.loads(json.dumps(ambiguous["source_runs"][0])))
        self.resign(ambiguous)
        with self.assertRaisesRegex(ValueError, "duplicate source runs"):
            self.verify(ambiguous)
        cross_run = self.envelope()
        cross_run["source_artifacts"][0]["run_id"] = "8"
        cross_run["source_artifacts"][0]["artifact_name"] = (
            "capacity-report-" + self.candidate["commit_sha"] + "-8-1"
        )
        self.resign(cross_run)
        with self.assertRaisesRegex(ValueError, "undeclared source run"):
            self.verify(cross_run)

    def test_nonempty_capacity_report_below_policy_threshold_blocks(self) -> None:
        payload = self.envelope("capacity")
        payload["observation"]["details"]["selected_envelope"][
            "maximum_recommended_pilot_board_size"
        ] = 1023
        self.resign(payload)
        with self.assertRaisesRegex(ValueError, "capacity policy threshold failed"):
            self.verify(payload)

    def test_capacity_raw_shared_service_latency_error_and_saturation_policy_blocks(self) -> None:
        cases = [
            (
                "separate-services",
                lambda details: details["concurrency_pack"].update(
                    service_instances_per_point=32
                ),
            ),
            (
                "raw-p95",
                lambda details: details["concurrency_pack"]["points"][0].update(
                    p95_latency_ms=2000.1,
                    p99_latency_ms=2200.0,
                ),
            ),
            (
                "shallow-sample",
                lambda details: (
                    details["concurrency_pack"].update(operations_per_worker=1),
                    details["concurrency_pack"]["points"][0].update(
                        total_operations=32,
                        successful_operations=32,
                        measurement_duration_ms=75.0,
                        throughput_per_second=426.6666666666667,
                    ),
                ),
            ),
            (
                "incomplete-mix",
                lambda details: details["concurrency_pack"].update(
                    operation_mix=["/health"]
                ),
            ),
            (
                "failed-operation",
                lambda details: details["concurrency_pack"]["points"][0].update(
                    successful_operations=383,
                    failed_operations=1,
                    failure_messages=["request failed"],
                    throughput_per_second=425.55555555555554,
                ),
            ),
            (
                "saturation-response",
                lambda details: details["concurrency_pack"]["points"][0].update(
                    successful_operations=383,
                    failed_operations=1,
                    saturation_responses=1,
                    failure_messages=["returned 503"],
                    throughput_per_second=425.55555555555554,
                ),
            ),
        ]
        for label, mutate in cases:
            with self.subTest(case=label):
                payload = self.envelope("capacity")
                mutate(payload["observation"]["details"])
                self.resign(payload)
                with self.assertRaisesRegex(ValueError, "capacity policy threshold failed"):
                    self.verify(payload)

    def test_capacity_raw_policy_point_must_be_unique_and_accounted(self) -> None:
        duplicated = self.envelope("capacity")
        points = duplicated["observation"]["details"]["concurrency_pack"]["points"]
        points.append(json.loads(json.dumps(points[0])))
        self.resign(duplicated)
        with self.assertRaisesRegex(ValueError, "concurrency points are duplicated"):
            self.verify(duplicated)

        inconsistent = self.envelope("capacity")
        inconsistent["observation"]["details"]["concurrency_pack"]["points"][0][
            "total_operations"
        ] = 383
        self.resign(inconsistent)
        with self.assertRaisesRegex(ValueError, "operation count is inconsistent"):
            self.verify(inconsistent)

        inconsistent_metrics = self.envelope("capacity")
        inconsistent_metrics["metrics"]["checks"]["operator_p95"] = False
        self.resign(inconsistent_metrics)
        with self.assertRaisesRegex(ValueError, "envelope metrics disagree"):
            self.verify(inconsistent_metrics)

        boolean_acceptance = self.envelope("capacity")
        boolean_acceptance["observation"]["details"]["capacity_acceptance"][
            "operator_error_rate"
        ] = False
        self.resign(boolean_acceptance)
        with self.assertRaisesRegex(ValueError, "acceptance summary disagrees"):
            self.verify(boolean_acceptance)

        boolean_metrics = self.envelope("capacity")
        boolean_metrics["observation"]["details"]["metrics"][
            "operator_error_rate"
        ] = False
        boolean_metrics["metrics"]["operator_error_rate"] = False
        self.resign(boolean_metrics)
        with self.assertRaisesRegex(ValueError, "operator_error_rate metric disagrees"):
            self.verify(boolean_metrics)

        weakened_policy = self.envelope("capacity")
        weakened_policy["observation"]["details"]["policy"][
            "minimum_mixed_operator_concurrency"
        ] = 1
        self.resign(weakened_policy)
        with self.assertRaisesRegex(ValueError, "differs from canonical gate policy"):
            self.verify(weakened_policy)

    def test_capacity_subject_raw_pack_is_bound_to_redownloaded_source_report(self) -> None:
        payload = self.envelope("capacity")
        details = payload["observation"]["details"]
        report = {
            "single_node_envelopes": [
                json.loads(json.dumps(details["selected_envelope"]))
            ],
            "recommended_hardware": json.loads(
                json.dumps(details["recommended_hardware"])
            ),
            "concurrency_pack": json.loads(json.dumps(details["concurrency_pack"])),
        }
        self.verify_module.verify_capacity_source_payload(
            payload, report, self.gate_policy["capacity_acceptance"]
        )

        report["concurrency_pack"]["points"][0]["p95_latency_ms"] = 1.0
        with self.assertRaisesRegex(ValueError, "concurrency pack differs"):
            self.verify_module.verify_capacity_source_payload(
                payload, report, self.gate_policy["capacity_acceptance"]
            )

    def test_failed_codeql_tls_pages_capacity_customer_and_scanners_block(self) -> None:
        cases = [
            ("code-scan", lambda p: p["observation"]["details"]["jobs"][0].update(conclusion="failure"), "CodeQL"),
            ("transport-tls", lambda p: p["observation"]["details"].update(job_conclusion="failure"), "TLS"),
            ("pages-deployment", lambda p: p["observation"]["details"].update(observed_candidate_sha="e" * 40), "Pages"),
            ("capacity", lambda p: p["observation"]["details"].update(gate_passed=False), "capacity"),
            ("customer-workflow", lambda p: p["observation"]["details"].update(workflow_passed=False), "customer"),
            ("vulnerability-scan", lambda p: p["observation"]["details"].update(findings=1), "findings"),
        ]
        for subject_id, mutate, message in cases:
            with self.subTest(subject_id=subject_id):
                payload = self.envelope(subject_id)
                mutate(payload)
                self.resign(payload)
                with self.assertRaisesRegex(ValueError, message):
                    self.verify(payload, subject_id)

    def test_live_github_recheck_binds_codeql_tls_and_capacity_source_bytes(self) -> None:
        repository = "fyremael/AETHER"
        package = b"immutable canonical package"
        self.package_sha = self.evidence.sha256_bytes(package)
        package_buffer = io.BytesIO()
        with zipfile.ZipFile(package_buffer, "w", zipfile.ZIP_DEFLATED) as artifact_zip:
            artifact_zip.writestr("aether.zip", package)
        archive = package_buffer.getvalue()
        receipt = {
            "artifact_id": 99,
            "artifact_name": "supply-chain-candidate-package-" + self.candidate["commit_sha"] + "-11-1",
            "workflow_file": ".github/workflows/supply-chain.yml",
            "run_id": "11",
            "attempt": 1,
            "head_sha": self.candidate["commit_sha"],
            "status": "passed",
            "sha256": self.evidence.sha256_bytes(archive),
            "byte_size": len(archive),
        }
        supply_run = {
            "workflow_file": ".github/workflows/supply-chain.yml",
            "run_id": "11",
            "attempt": 1,
            "head_sha": self.candidate["commit_sha"],
            "status": "passed",
        }
        ci_run = {
            "workflow_file": ".github/workflows/ci.yml",
            "run_id": "12",
            "attempt": 1,
            "head_sha": self.candidate["commit_sha"],
            "status": "passed",
        }
        capacity_run = {
            "workflow_file": ".github/workflows/capacity-planning.yml",
            "run_id": "13",
            "attempt": 1,
            "head_sha": self.candidate["commit_sha"],
            "status": "passed",
        }
        capacity_details = self.details("capacity")
        capacity_report = {
            "single_node_envelopes": [
                json.loads(json.dumps(capacity_details["selected_envelope"]))
            ],
            "recommended_hardware": json.loads(
                json.dumps(capacity_details["recommended_hardware"])
            ),
            "concurrency_pack": json.loads(
                json.dumps(capacity_details["concurrency_pack"])
            ),
        }
        capacity_source = self.evidence.canonical_bytes(capacity_report)
        capacity_buffer = io.BytesIO()
        with zipfile.ZipFile(
            capacity_buffer, "w", zipfile.ZIP_DEFLATED
        ) as capacity_zip:
            capacity_zip.writestr("capacity-report.json", capacity_source)
        capacity_archive = capacity_buffer.getvalue()
        capacity_receipt = {
            "artifact_id": 98,
            "artifact_name": (
                "capacity-report-" + self.candidate["commit_sha"] + "-13-1"
            ),
            "workflow_file": capacity_run["workflow_file"],
            "run_id": capacity_run["run_id"],
            "attempt": capacity_run["attempt"],
            "head_sha": self.candidate["commit_sha"],
            "status": "passed",
            "sha256": self.evidence.sha256_bytes(capacity_archive),
            "byte_size": len(capacity_archive),
        }
        capacity_details["source_files"] = [
            {
                "artifact_id": capacity_receipt["artifact_id"],
                "path": "capacity-report.json",
                "sha256": self.evidence.sha256_bytes(capacity_source),
                "byte_size": len(capacity_source),
            }
        ]
        producer = {
            "workflow_file": ".github/workflows/release-readiness.yml",
            "run_id": "42",
            "attempt": 1,
        }
        envelopes = {
            "code-scan": {
                "producer": producer,
                "source_runs": [supply_run],
                "source_artifacts": [receipt],
                "package": {"name": "aether.zip", "sha256": self.package_sha},
                "observation": {
                    "details": {
                        "jobs": [
                            {"id": 1, "name": "CodeQL (go)", "conclusion": "success"},
                            {"id": 2, "name": "CodeQL (python)", "conclusion": "success"},
                        ]
                    }
                },
            },
            "transport-tls": {
                "producer": producer,
                "source_runs": [supply_run, ci_run],
                "source_artifacts": [receipt],
                "package": {"name": "aether.zip", "sha256": self.package_sha},
                "observation": {
                    "details": {
                        "job": {"id": 3, "name": "Postgres verified TLS journal", "conclusion": "success"}
                    }
                },
            },
            "capacity": {
                "producer": producer,
                "source_runs": [supply_run, capacity_run],
                "source_artifacts": [receipt, capacity_receipt],
                "package": {"name": "aether.zip", "sha256": self.package_sha},
                "observation": {"details": capacity_details},
            },
        }
        readiness_outputs = {}
        readiness_files = {}
        for output_name in {
            "commercial_policy",
            "customer_workflow",
            "package_file_manifest",
            "performance_beta",
            "pilot_launch_transcript",
            "readiness_transcript",
            "rollback",
            "security_lifecycle",
            "service_operability",
        }:
            content = (output_name + " immutable evidence").encode("utf-8")
            original_name = output_name + ".json"
            readiness_outputs[output_name] = {
                "path": "artifacts/immutable/" + original_name,
                "sha256": self.evidence.sha256_bytes(content),
                "byte_size": len(content),
            }
            readiness_files[f"qualification-readiness/{output_name}-{original_name}"] = content
        readiness_manifest = {
            "schema_version": "aether.release-readiness-evidence.v1",
            "status": "passed",
            "candidate": {
                "commit_sha": self.candidate["commit_sha"],
                "tree_sha": self.candidate["tree_sha"],
                "ref": self.candidate["ref"],
            },
            "workflow": {"run_id": "42", "attempt": 1},
            "outputs": readiness_outputs,
        }
        def build_qualification_archive() -> bytes:
            qualification_buffer = io.BytesIO()
            with zipfile.ZipFile(
                qualification_buffer, "w", zipfile.ZIP_DEFLATED
            ) as qualification_zip:
                qualification_zip.writestr(
                    "qualification-readiness/release-readiness-evidence-"
                    + self.candidate["commit_sha"]
                    + "-42-1.json",
                    self.evidence.canonical_bytes(readiness_manifest),
                )
                for path, content in readiness_files.items():
                    qualification_zip.writestr(path, content)
                for subject_id, envelope in envelopes.items():
                    qualification_zip.writestr(
                        f"qualification-subjects/{subject_id}.json",
                        self.evidence.canonical_bytes(envelope),
                    )
            return qualification_buffer.getvalue()

        qualification_archive = build_qualification_archive()
        qualification_name = (
            "release-qualification-subjects-"
            + self.candidate["commit_sha"]
            + "-42-1"
        )
        run_payloads = {
            "11": {
                "id": 11,
                "run_attempt": 1,
                "head_sha": self.candidate["commit_sha"],
                "head_branch": "main",
                "status": "completed",
                "conclusion": "success",
                "path": ".github/workflows/supply-chain.yml",
            },
            "12": {
                "id": 12,
                "run_attempt": 1,
                "head_sha": self.candidate["commit_sha"],
                "head_branch": "main",
                "status": "completed",
                "conclusion": "success",
                "path": ".github/workflows/ci.yml",
            },
            "13": {
                "id": 13,
                "run_attempt": 1,
                "head_sha": self.candidate["commit_sha"],
                "head_branch": "main",
                "status": "completed",
                "conclusion": "success",
                "path": ".github/workflows/capacity-planning.yml",
            },
        }
        job_payloads = {
            "11": {
                "jobs": [
                    {"id": 1, "name": "CodeQL (go)", "status": "completed", "conclusion": "success"},
                    {"id": 2, "name": "CodeQL (python)", "status": "completed", "conclusion": "success"},
                ]
            },
            "12": {
                "jobs": [
                    {"id": 3, "name": "Postgres verified TLS journal", "status": "completed", "conclusion": "success"}
                ]
            },
        }

        def api(endpoint: str):
            if endpoint.endswith("/artifacts?per_page=100"):
                if "/runs/42/" in endpoint:
                    return {
                        "artifacts": [
                            {
                                "id": 100,
                                "name": qualification_name,
                                "expired": False,
                                "size_in_bytes": len(qualification_archive),
                                "digest": "sha256:"
                                + self.evidence.sha256_bytes(qualification_archive),
                            }
                        ]
                    }
                if "/runs/13/" in endpoint:
                    return {
                        "artifacts": [
                            {
                                "id": 98,
                                "name": capacity_receipt["artifact_name"],
                                "expired": False,
                                "size_in_bytes": len(capacity_archive),
                                "digest": "sha256:"
                                + self.evidence.sha256_bytes(capacity_archive),
                            }
                        ]
                    }
                return {
                    "artifacts": [
                        {
                            "id": 99,
                            "name": receipt["artifact_name"],
                            "expired": False,
                            "size_in_bytes": len(archive),
                            "digest": f"sha256:{receipt['sha256']}",
                        }
                    ]
                }
            run_id = endpoint.split("/runs/")[1].split("/")[0]
            if endpoint.endswith("/jobs?per_page=100"):
                return job_payloads[run_id]
            return run_payloads[run_id]

        def download(_repository: str, artifact_id: int) -> bytes:
            return {
                98: capacity_archive,
                99: archive,
                100: qualification_archive,
            }[artifact_id]

        policy = self.evidence.load_json(REPO_ROOT / "fixtures" / "release" / "gate-policy.json")
        producer_workflow = {
            "artifact_name": (
                "aether-release-evidence-"
                + self.candidate["commit_sha"]
                + "-42-1"
            ),
            "attempt": 1,
            "host": "github-windows-latest",
            "job_id": "exact-candidate-evidence",
            "repository": "fyremael/AETHER",
            "run_id": "42",
            "runner": "Windows",
            "tool_versions": {"verifier": "aether-release-evidence-verifier-v3"},
            "workflow_file": ".github/workflows/reusable-exact-candidate-evidence.yml",
        }
        self.verify_module.verify_subject_github_outcomes(
            envelopes,
            producer_workflow,
            self.candidate,
            policy,
            api=api,
            download_artifact=download,
        )

        readiness_manifest["workflow"]["run_id"] = "43"
        qualification_archive = build_qualification_archive()
        with self.assertRaisesRegex(ValueError, "readiness workflow binding differs"):
            self.verify_module.verify_subject_github_outcomes(
                envelopes,
                producer_workflow,
                self.candidate,
                policy,
                api=api,
                download_artifact=download,
            )
        readiness_manifest["workflow"]["run_id"] = "42"

        readiness_manifest["workflow"]["attempt"] = 2
        qualification_archive = build_qualification_archive()
        with self.assertRaisesRegex(ValueError, "readiness workflow binding differs"):
            self.verify_module.verify_subject_github_outcomes(
                envelopes,
                producer_workflow,
                self.candidate,
                policy,
                api=api,
                download_artifact=download,
            )
        readiness_manifest["workflow"]["attempt"] = 1
        qualification_archive = build_qualification_archive()

        original_pack = json.loads(
            json.dumps(envelopes["capacity"]["observation"]["details"]["concurrency_pack"])
        )
        envelopes["capacity"]["observation"]["details"]["concurrency_pack"][
            "points"
        ][0]["p95_latency_ms"] = 1.0
        qualification_archive = build_qualification_archive()
        with self.assertRaisesRegex(ValueError, "capacity concurrency pack differs"):
            self.verify_module.verify_subject_github_outcomes(
                envelopes,
                producer_workflow,
                self.candidate,
                policy,
                api=api,
                download_artifact=download,
            )

        envelopes["capacity"]["observation"]["details"][
            "concurrency_pack"
        ] = original_pack
        restored_details = envelopes["capacity"]["observation"]["details"]
        restored_details["capacity_acceptance"] = (
            self.subjects.recompute_capacity_acceptance(
                restored_details["policy"],
                restored_details["selected_envelope"],
                restored_details["recommended_hardware"],
                restored_details["concurrency_pack"],
            )
        )
        qualification_archive = build_qualification_archive()
        job_payloads["12"]["jobs"][0]["conclusion"] = "failure"
        with self.assertRaisesRegex(ValueError, "transport-tls GitHub job did not pass"):
            self.verify_module.verify_subject_github_outcomes(
                envelopes,
                producer_workflow,
                self.candidate,
                policy,
                api=api,
                download_artifact=download,
            )


if __name__ == "__main__":
    unittest.main()
