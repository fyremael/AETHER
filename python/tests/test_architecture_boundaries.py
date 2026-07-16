from __future__ import annotations

import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def dependencies(crate: str) -> set[str]:
    payload = tomllib.loads((ROOT / "crates" / crate / "Cargo.toml").read_text(encoding="utf-8"))
    return set(payload.get("dependencies", {}))


class ArchitectureBoundaryTests(unittest.TestCase):
    def test_owning_crates_do_not_depend_on_api_or_outward_layers(self) -> None:
        forbidden = {
            "aether_service_core": {"aether_api", "aether_http", "aether_partition", "aether_pilot", "aether_perf"},
            "aether_sidecar": {"aether_api", "aether_service_core", "aether_http", "aether_partition", "aether_pilot", "aether_perf"},
            "aether_partition": {"aether_api", "aether_http", "aether_pilot", "aether_perf"},
            "aether_pilot": {"aether_api", "aether_http", "aether_partition", "aether_perf"},
            "aether_http": {"aether_api", "aether_perf"},
            "aether_perf": {"aether_api"},
        }
        for crate, disallowed in forbidden.items():
            self.assertFalse(
                dependencies(crate) & disallowed,
                f"{crate} has an outward dependency: {dependencies(crate) & disallowed}",
            )

    def test_api_is_a_compatibility_facade_not_an_implementation_owner(self) -> None:
        source = ROOT / "crates" / "aether_api" / "src"
        self.assertEqual(
            {path.relative_to(source).as_posix() for path in source.rglob("*.rs")},
            {"lib.rs", "bin/aether_pilot_service.rs"},
        )
        facade = (source / "lib.rs").read_text(encoding="utf-8")
        for crate in (
            "aether_service_core",
            "aether_http",
            "aether_partition",
            "aether_pilot",
            "aether_perf",
            "aether_sidecar",
        ):
            self.assertIn(crate, facade)

    def test_runtime_consumes_the_versioned_executable_plan(self) -> None:
        plan = (ROOT / "crates" / "aether_plan" / "src" / "lib.rs").read_text(encoding="utf-8")
        runtime = (ROOT / "crates" / "aether_runtime" / "src" / "lib.rs").read_text(encoding="utf-8")
        for marker in (
            "EXECUTABLE_PLAN_FORMAT_VERSION",
            "DeltaAnchorStrategy",
            "ProvenanceRequirement",
        ):
            self.assertIn(marker, plan)
            self.assertIn(marker, runtime)
        for marker in ("ExecutableSchedule", "AggregatePlanNode"):
            self.assertIn(marker, plan)
        self.assertIn(".aggregates", runtime)
        self.assertNotIn("fn build_scc_evaluation_order", runtime)
        self.assertNotIn("fn current_scc_positive_indices", runtime)


if __name__ == "__main__":
    unittest.main()
