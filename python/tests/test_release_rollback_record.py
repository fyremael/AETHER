from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "release_rollback_record.py"


def load_module():
    spec = importlib.util.spec_from_file_location("release_rollback_record", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReleaseRollbackRecordTests(unittest.TestCase):
    def test_service_v2_gate_status_reads_named_gate(self) -> None:
        module = load_module()
        payload = {"gates": [{"id": "package_backup_restore_restart", "status": "passed"}]}

        self.assertEqual(
            module.service_v2_gate_status(payload, "package_backup_restore_restart"),
            "passed",
        )
        self.assertEqual(module.service_v2_gate_status(payload, "missing"), "missing")

    def test_playbook_and_postgres_boundary_markers_exist(self) -> None:
        module = load_module()
        playbook_ok, playbook_missing = module.file_contains_all(
            REPO_ROOT / "docs" / "PILOT_OPERATIONS_PLAYBOOK.md",
            ["in-place upgrade", "rollback", "Restore the database snapshot", "Re-run the launch pack"],
        )
        postgres_ok, postgres_missing = module.file_contains_all(
            REPO_ROOT / "docs" / "PILOT_DEPLOYMENT.md",
            ["For Postgres deployments", "export and restore", "normal Postgres tooling", "journal schema"],
        )

        self.assertTrue(playbook_ok, playbook_missing)
        self.assertTrue(postgres_ok, postgres_missing)

    def test_rendered_markdown_contains_candidate_and_policy(self) -> None:
        module = load_module()
        payload = {
            "generated_at": "2026-07-02T00:00:00+00:00",
            "candidate_id": "candidate",
            "commit": "abc123",
            "rollback_ready": True,
            "package_root": "package",
            "package_zip": "package.zip",
            "service_v2_json": "service.json",
            "rollback_policy": {
                "primary": "binary/config first",
                "sqlite": "package helpers",
                "postgres": "operator tooling",
            },
            "gates": [
                {
                    "title": "Versioned rollback record is generated",
                    "status": "passed",
                    "evidence": ["this record"],
                    "blockers": [],
                    "next_actions": [],
                }
            ],
        }

        text = module.render_markdown(payload)
        self.assertIn("candidate", text)
        self.assertIn("binary/config first", text)


if __name__ == "__main__":
    unittest.main()
