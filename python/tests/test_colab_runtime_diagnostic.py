from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "scripts" / "colab_runtime_diagnostic.py"


def load_module():
    spec = importlib.util.spec_from_file_location("aether_colab_runtime_diagnostic", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load Colab runtime diagnostic module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ColabRuntimeDiagnosticTests(unittest.TestCase):
    def test_lane_is_diagnostic_only(self) -> None:
        module = load_module()
        self.assertEqual(module.QUALIFICATION_STATUS, "diagnostic_only")
        self.assertIs(module.COMMERCIAL_BETA_AUTHORITY, False)

    def test_measurement_requires_one_exact_surface(self) -> None:
        module = load_module()
        bundle = {
            "report": {
                "measurements": [
                    {
                        "workload": "Tuple explanation runtime",
                        "scale": "chain 128",
                        "throughput_per_second": 123.0,
                        "latency": {
                            "mean": {"secs": 1, "nanos": 25},
                            "sample_durations_ns": [1_000_000_025],
                        },
                    }
                ]
            }
        }
        observed = module.measurement(bundle, "Tuple explanation runtime", "chain 128")
        self.assertEqual(observed["mean_ns"], 1_000_000_025)
        self.assertEqual(observed["throughput_per_second"], 123.0)
        with self.assertRaises(RuntimeError):
            module.measurement(bundle, "missing", "chain 128")


if __name__ == "__main__":
    unittest.main()
