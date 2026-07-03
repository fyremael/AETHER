from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "security_key_lifecycle.py"


def load_module():
    spec = importlib.util.spec_from_file_location("security_key_lifecycle", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SecurityKeyLifecycleTests(unittest.TestCase):
    def test_package_required_files_include_token_and_recovery_helpers(self) -> None:
        module = load_module()
        paths = {str(path).replace("\\", "/") for path in module.PACKAGE_REQUIRED_FILES}

        self.assertIn("config/pilot-operator.token", paths)
        self.assertIn("rotate-pilot-token.ps1", paths)
        self.assertIn("backup-pilot-state.ps1", paths)
        self.assertIn("restore-pilot-state.ps1", paths)

    def test_file_sha256_hashes_file_content(self) -> None:
        module = load_module()

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.txt"
            path.write_text("abc", encoding="utf-8")

            self.assertEqual(
                module.file_sha256(path),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            )

    def test_secret_manager_docs_have_required_markers(self) -> None:
        module = load_module()
        playbook_ok, playbook_missing = module.file_contains_all(
            REPO_ROOT / "docs" / "PILOT_OPERATIONS_PLAYBOOK.md",
            ["External Secret-Manager Playbook", "token_command", "cloud secret-manager CLIs", "restart is the reload boundary"],
        )
        deployment_ok, deployment_missing = module.file_contains_all(
            REPO_ROOT / "docs" / "PILOT_DEPLOYMENT.md",
            ["token_file", "token_command", "secret-manager", "revoked token"],
        )

        self.assertTrue(playbook_ok, playbook_missing)
        self.assertTrue(deployment_ok, deployment_missing)


if __name__ == "__main__":
    unittest.main()
