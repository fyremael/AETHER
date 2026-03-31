from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK_ROOT = REPO_ROOT / "python" / "notebooks"


def _load_colab_setup_module():
    module_path = NOTEBOOK_ROOT / "colab_setup.py"
    spec = importlib.util.spec_from_file_location("aether_colab_setup", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module spec from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(spec.name, module)
    spec.loader.exec_module(module)
    return module


class NotebookOnboardingTest(unittest.TestCase):
    def test_notebooks_are_valid_json_with_cells(self) -> None:
        notebooks = sorted(NOTEBOOK_ROOT.glob("*.ipynb"))
        self.assertGreaterEqual(len(notebooks), 6)

        for notebook_path in notebooks:
            with self.subTest(notebook=str(notebook_path.name)):
                data = json.loads(notebook_path.read_text(encoding="utf-8"))
                self.assertGreaterEqual(data.get("nbformat", 0), 4)
                self.assertTrue(data.get("cells"))
                self.assertTrue(any(cell.get("source") for cell in data["cells"]))

    def test_notebook_readme_links_point_at_tracked_series(self) -> None:
        readme = (NOTEBOOK_ROOT / "README.md").read_text(encoding="utf-8")
        for notebook_name in (
            "01_aether_onramp.ipynb",
            "02_time_cuts_and_memory.ipynb",
            "03_recursive_closure_and_explain.ipynb",
            "04_governed_incident_blackboard.ipynb",
            "05_policy_and_sidecars.ipynb",
            "06_ai_support_resolution_desk.ipynb",
        ):
            with self.subTest(notebook=notebook_name):
                self.assertIn(notebook_name, readme)
                self.assertIn(
                    f"https://colab.research.google.com/github/fyremael/AETHER/blob/main/python/notebooks/{notebook_name}",
                    readme,
                )

    def test_colab_helper_reuses_local_checkout_and_exports_bootstrap(self) -> None:
        module = _load_colab_setup_module()

        self.assertTrue(hasattr(module, "bootstrap_repo"))
        self.assertTrue(hasattr(module, "start_http_service"))
        self.assertTrue(hasattr(module, "stop_http_service"))

        repo_root = module.bootstrap_repo(repo_root=REPO_ROOT)
        self.assertEqual(Path(repo_root).resolve(), REPO_ROOT.resolve())


if __name__ == "__main__":
    unittest.main()
