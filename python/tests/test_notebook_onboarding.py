from __future__ import annotations

import ast
import importlib.util
import json
import sys
import tempfile
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

    def test_notebook_code_cells_compile(self) -> None:
        for notebook_path in sorted(NOTEBOOK_ROOT.glob("*.ipynb")):
            data = json.loads(notebook_path.read_text(encoding="utf-8"))
            for index, cell in enumerate(data.get("cells", [])):
                if cell.get("cell_type") != "code":
                    continue
                source = "".join(cell.get("source", []))
                with self.subTest(notebook=notebook_path.name, cell=index):
                    ast.parse(source)

    def test_tutorial_documents_use_explicit_schema_version(self) -> None:
        for notebook_path in sorted(NOTEBOOK_ROOT.glob("*.ipynb")):
            data = json.loads(notebook_path.read_text(encoding="utf-8"))
            for index, cell in enumerate(data.get("cells", [])):
                if cell.get("cell_type") != "code":
                    continue
                source = "".join(cell.get("source", []))
                if '"""' not in source or "schema" not in source:
                    continue
                with self.subTest(notebook=notebook_path.name, cell=index):
                    self.assertNotIn("schema {\n", source)
                    self.assertTrue("rules {\n" in source or "rules {{" in source)

    def test_pretty_json_calls_are_labeled(self) -> None:
        for notebook_path in sorted(NOTEBOOK_ROOT.glob("*.ipynb")):
            data = json.loads(notebook_path.read_text(encoding="utf-8"))
            for index, cell in enumerate(data.get("cells", [])):
                if cell.get("cell_type") != "code":
                    continue
                source = "".join(cell.get("source", []))
                if "pretty_json(" not in source:
                    continue
                with self.subTest(notebook=notebook_path.name, cell=index):
                    self.assertIn("title=", source)

    def test_pretty_json_describes_core_response_shapes(self) -> None:
        module = _load_colab_setup_module()

        status_lines = module.describe_value(
            {
                "status": "ok",
                "service_mode": "single_node",
                "config_version": "pilot-v2-colab",
                "schema_version": "v1",
                "effective_namespace": "notebook",
                "storage": {"backend": "sqlite", "data_root": "/tmp/aether"},
                "principals": [
                    {
                        "principal": "notebook-operator",
                        "token_id": "token:notebook-operator",
                        "scopes": ["append", "query"],
                    }
                ],
                "active_namespace_count": 1,
                "namespaces": ["notebook"],
            }
        )
        self.assertTrue(any("Service status" in line for line in status_lines))

        query_lines = module.describe_value(
            {"rows": [{"values": [{"Entity": 1}], "tuple_id": 7}]}
        )
        self.assertTrue(any("Query result: 1 row" in line for line in query_lines))

    def test_notebook_readme_links_point_at_tracked_series(self) -> None:
        readme = (NOTEBOOK_ROOT / "README.md").read_text(encoding="utf-8")
        for notebook_name in (
            "01_aether_onramp.ipynb",
            "02_time_cuts_and_memory.ipynb",
            "03_recursive_closure_and_explain.ipynb",
            "04_governed_incident_blackboard.ipynb",
            "05_policy_and_sidecars.ipynb",
            "06_ai_support_resolution_desk.ipynb",
            "07_operating_proof_and_trends.ipynb",
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
        self.assertTrue(hasattr(module, "start_pilot_service"))
        self.assertTrue(hasattr(module, "stop_http_service"))
        self.assertTrue(hasattr(module, "require_service_capabilities"))

        repo_root = module.bootstrap_repo(repo_root=REPO_ROOT)
        self.assertEqual(Path(repo_root).resolve(), REPO_ROOT.resolve())

    def test_notebooks_preflight_service_capabilities(self) -> None:
        for notebook_path in sorted(NOTEBOOK_ROOT.glob("*.ipynb")):
            data = json.loads(notebook_path.read_text(encoding="utf-8"))
            source = "\n".join(
                "".join(cell.get("source", [])) for cell in data.get("cells", [])
            )
            with self.subTest(notebook=notebook_path.name):
                self.assertIn("require_service_capabilities(client)", source)

    def test_colab_helper_prefers_built_pilot_binary_when_present(self) -> None:
        module = _load_colab_setup_module()
        suffix = ".exe" if sys.platform.startswith("win") else ""

        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            binary = (
                repo_root
                / "target"
                / "debug"
                / "examples"
                / f"pilot_http_kernel_service{suffix}"
            )
            binary.parent.mkdir(parents=True)
            binary.write_text("", encoding="utf-8")

            self.assertEqual(
                module._pilot_service_command(
                    repo_root,
                    prefer_existing_binary=True,
                ),
                [str(binary)],
            )
            self.assertEqual(
                module._pilot_service_command(
                    repo_root,
                    prefer_existing_binary=False,
                )[:2],
                ["cargo", "run"],
            )


if __name__ == "__main__":
    unittest.main()
