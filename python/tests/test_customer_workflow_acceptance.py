from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "customer_workflow_acceptance.py"


def load_module():
    spec = importlib.util.spec_from_file_location("customer_workflow_acceptance", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CustomerWorkflowAcceptanceTests(unittest.TestCase):
    def test_required_workflow_pack_paths_exist(self) -> None:
        module = load_module()

        self.assertEqual(module.missing_paths(REPO_ROOT, module.REQUIRED_DOCS), [])

    def test_required_demo_markers_cover_support_story(self) -> None:
        module = load_module()
        markers = module.REQUIRED_DEMO_MARKERS

        for expected in [
            "Active support cases on the desk",
            "Retrieved evidence from the support-memory sidecar",
            "Which resolution is actually ready",
            "Who owns the case now",
            "Fenced stale recommendations at Current",
            "Why the current selected resolution is true",
        ]:
            self.assertIn(expected, markers)

    def test_missing_text_markers_reports_only_absent_markers(self) -> None:
        module = load_module()

        self.assertEqual(
            module.missing_text_markers("alpha beta", ["alpha", "gamma"]),
            ["gamma"],
        )


if __name__ == "__main__":
    unittest.main()
