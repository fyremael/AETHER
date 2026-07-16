from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "service_v2_operability.py"


def load_module():
    spec = importlib.util.spec_from_file_location("service_v2_operability", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ServiceV2OperabilityTests(unittest.TestCase):
    def test_python_ci_installs_evidence_test_dependencies(self) -> None:
        workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )
        python_job = workflow.split("  python-sdk:", 1)[1].split("  pilot-launch-gate:", 1)[0]
        self.assertIn("requirements-release.txt", python_job)
        self.assertIn("python -m unittest discover python/tests -v", python_job)

    def test_container_ci_smoke_has_required_markers(self) -> None:
        module = load_module()
        ok, missing = module.file_contains_all(
            REPO_ROOT / ".github" / "workflows" / "ci.yml",
            [
                "container-smoke",
                "Boot image and verify authenticated status",
                "docker stop",
                "docker start",
                "X-Aether-Namespace",
                "/v1/status",
                "/v1/history",
            ],
        )

        self.assertTrue(ok, missing)

    def test_postgres_ci_has_required_markers(self) -> None:
        module = load_module()
        ok, missing = module.file_contains_all(
            REPO_ROOT / ".github" / "workflows" / "ci.yml",
            [
                "postgres-journal",
                "scripts/ci-postgres-tls.sh",
                "Postgres transport security matrix",
                "cargo test -p aether_storage --lib",
                "cargo test -p aether_storage --test postgres_tls",
                "cargo test -p aether_api --test http_service http_service_postgres_namespaces",
            ],
        )

        self.assertTrue(ok, missing)

    def test_hardening_promotion_status_reads_admin_operator_flags(self) -> None:
        module = load_module()
        status = module.promotion_blocking_status(
            REPO_ROOT / ".github" / "hardening-promotion-state.json"
        )

        self.assertIn("admin", status)
        self.assertIn("operator", status)
        self.assertIsInstance(status["admin"], bool)
        self.assertIsInstance(status["operator"], bool)

    def test_namespace_concurrency_contract_is_bounded(self) -> None:
        http_source = (REPO_ROOT / "crates" / "aether_http" / "src" / "http.rs").read_text(
            encoding="utf-8"
        )
        partition_source = (
            REPO_ROOT / "crates" / "aether_partition" / "src" / "lib.rs"
        ).read_text(encoding="utf-8")

        for marker in (
            "struct BoundedBlockingExecutor",
            "struct NamespaceServiceDirectory",
            "try_acquire_owned",
            '"namespace_busy"',
            "RETRY_AFTER",
            "mpsc::sync_channel::<AuditEntry>",
        ):
            self.assertIn(marker, http_source)
        self.assertNotIn("std::thread::spawn", http_source)
        self.assertIn("Arc<Mutex<ReplicatedPartition>>", partition_source)

    def test_hardening_pack_status_defaults_missing_without_latest_json(self) -> None:
        module = load_module()
        status = module.hardening_latest_status(REPO_ROOT, REPO_ROOT / "does-not-exist.json")

        self.assertEqual(status["admin"], "missing")
        self.assertEqual(status["operator"], "missing")


if __name__ == "__main__":
    unittest.main()
