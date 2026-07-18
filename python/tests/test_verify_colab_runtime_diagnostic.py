from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
import warnings
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "scripts" / "verify_colab_runtime_diagnostic.py"
COMMIT = "a" * 40
TREE = "b" * 40


def load_module():
    spec = importlib.util.spec_from_file_location(
        "aether_verify_colab_runtime_diagnostic", MODULE_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load Colab diagnostic verifier module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def summary_bytes(*, commit: str = COMMIT, tree: str = TREE) -> bytes:
    return (
        json.dumps(
            {
                "qualification_status": "diagnostic_only",
                "candidate": {"commit_sha": commit, "tree_sha": tree},
                "policy": {"commercial_beta_authority": False},
            },
            indent=2,
        )
        + "\n"
    ).encode()


class ColabRuntimeDiagnosticVerifierTests(unittest.TestCase):
    def write_fixture(self, root: Path, *, inner_summary: bytes | None = None):
        downloaded_summary = summary_bytes()
        summary_path = root / "summary.json"
        archive_path = root / "diagnostic.zip"
        summary_path.write_bytes(downloaded_summary)
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr(
                "summary.json",
                downloaded_summary if inner_summary is None else inner_summary,
            )
        return summary_path, archive_path

    def build(self, root: Path, *, inner_summary: bytes | None = None):
        module = load_module()
        summary_path, archive_path = self.write_fixture(
            root, inner_summary=inner_summary
        )
        return module.build_receipt(
            summary_path=summary_path,
            expected_commit=COMMIT,
            expected_tree=TREE,
            session="aether-test",
            archive_path=archive_path,
        )

    def test_receipt_binds_exact_inner_summary_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            receipt = self.build(Path(directory))
        self.assertEqual(
            receipt["artifacts"]["archive"]["summary_sha256"],
            receipt["artifacts"]["summary"]["sha256"],
        )

    def test_rejects_mismatched_inner_summary_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "summary bytes differ"):
                self.build(Path(directory), inner_summary=b"{}\n")

    def test_rejects_duplicate_inner_summary(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path, archive_path = self.write_fixture(root)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                with zipfile.ZipFile(archive_path, "a") as archive:
                    archive.writestr("summary.json", summary_bytes())
            with self.assertRaisesRegex(ValueError, "exactly one"):
                module.build_receipt(
                    summary_path=summary_path,
                    expected_commit=COMMIT,
                    expected_tree=TREE,
                    session="aether-test",
                    archive_path=archive_path,
                )


if __name__ == "__main__":
    unittest.main()
