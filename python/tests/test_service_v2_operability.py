from __future__ import annotations

import ast
import importlib.util
import inspect
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


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
        expected_markers = [
            "postgres-journal",
            "scripts/ci-postgres-tls.sh",
            "Postgres transport security matrix",
            "cargo test -p aether_storage --lib",
            "cargo test -p aether_storage --test postgres_tls",
            "cargo test -p aether_api --test http_service http_service_postgres_namespaces",
        ]

        self.assertEqual(module.POSTGRES_CI_REQUIRED_MARKERS, expected_markers)
        ok, missing = module.file_contains_all(
            REPO_ROOT / ".github" / "workflows" / "ci.yml",
            module.POSTGRES_CI_REQUIRED_MARKERS,
        )

        self.assertTrue(ok, missing)

    def test_collector_accepts_current_blocking_postgres_ci_contract(self) -> None:
        module = load_module()
        args = SimpleNamespace(
            generated_at="2026-07-19T00:00:00+00:00",
            hardening_json=None,
            package_root=None,
            artifact_dir=None,
            postgres_env="AETHER_POSTGRES_TEST_URL",
            accept_ci_postgres=True,
            timeout_seconds=1,
        )

        with mock.patch.object(module.shutil, "which", return_value=None), mock.patch.dict(
            module.os.environ, {}, clear=True
        ):
            payload = module.collect_service_v2_evidence(args)

        postgres_gate = next(
            gate for gate in payload["gates"] if gate["id"] == "postgres_journal_restart_replay"
        )
        self.assertEqual(postgres_gate["status"], "ci_blocking")
        self.assertEqual(postgres_gate["blockers"], [])

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

    def test_package_drill_acknowledges_quiesced_backup_and_restore(self) -> None:
        module = load_module()
        source = inspect.getsource(module.run_package_backup_restore_drill)
        tree = ast.parse(source)
        helper_commands: dict[str, list[str]] = {}

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            if not isinstance(node.func, ast.Name) or node.func.id != "command_result":
                continue
            command = node.args[0]
            if not isinstance(command, ast.List):
                continue
            rendered = [ast.unparse(item) for item in command.elts]
            for helper in ("backup_script", "restore_script"):
                if f"str({helper})" in rendered:
                    helper_commands[helper] = rendered

        self.assertEqual(set(helper_commands), {"backup_script", "restore_script"})
        for helper, command in helper_commands.items():
            self.assertIn(
                "'-ConfirmServiceStopped'",
                command,
                f"{helper} invocation must acknowledge the already-quiesced service",
            )

        self.assertGreaterEqual(source.count("stop_process(service)"), 3)


if __name__ == "__main__":
    unittest.main()
