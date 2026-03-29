import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "scripts" / "hardening_promotion.py"

spec = importlib.util.spec_from_file_location("hardening_promotion", MODULE_PATH)
hardening_promotion = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(hardening_promotion)


class HardeningPromotionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config_path = REPO_ROOT / ".github" / "hardening-promotion-state.json"
        self.config = hardening_promotion.load_config(self.config_path)

    def test_compute_streaks_stops_at_first_non_green(self) -> None:
        metrics = [
            {"pack_statuses": {"admin": "passed", "operator": "passed"}},
            {"pack_statuses": {"admin": "passed", "operator": "failed"}},
            {"pack_statuses": {"admin": "passed", "operator": "passed"}},
        ]

        streaks = hardening_promotion.compute_streaks(
            ["admin", "operator"], metrics
        )

        self.assertEqual(streaks["admin"], 3)
        self.assertEqual(streaks["operator"], 1)

    def test_build_tracker_markdown_calls_out_next_eligible_group(self) -> None:
        current_metrics = {
            "run_id": "101",
            "ref_name": "main",
            "event_name": "schedule",
            "sha": "deadbeef",
            "pack_statuses": {
                "admin": "passed",
                "operator": "passed",
                "user": "passed",
                "exec": "passed",
            },
        }
        streaks = {"admin": 3, "operator": 0, "user": 0, "exec": 0}

        markdown = hardening_promotion.build_tracker_markdown(
            config=self.config,
            current_metrics=current_metrics,
            streaks=streaks,
            next_group="admin",
        )

        self.assertIn("`admin` is eligible for promotion", markdown)
        self.assertIn("`3/3`", markdown)

    def test_apply_promotion_marks_group_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_config_path = Path(temp_dir) / "promotion.json"
            temp_config_path.write_text(
                json.dumps(self.config, indent=2), encoding="utf-8"
            )

            exit_code = hardening_promotion.cmd_apply_promotion(
                type(
                    "Args",
                    (),
                    {
                        "config": str(temp_config_path),
                        "group": "admin",
                        "promoted_at": "2026-03-29T00:00:00+00:00",
                    },
                )()
            )

            updated = json.loads(temp_config_path.read_text(encoding="utf-8"))
            self.assertEqual(exit_code, 0)
            self.assertTrue(updated["groups"]["admin"]["blocking"])
            self.assertEqual(
                updated["groups"]["admin"]["promoted_at"],
                "2026-03-29T00:00:00+00:00",
            )

    def test_write_run_metrics_defaults_missing_groups(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            out_json = Path(temp_dir) / "run.json"
            out_md = Path(temp_dir) / "run.md"

            exit_code = hardening_promotion.cmd_write_run_metrics(
                type(
                    "Args",
                    (),
                    {
                        "config": str(self.config_path),
                        "out_json": str(out_json),
                        "out_md": str(out_md),
                        "run_id": "42",
                        "run_attempt": "1",
                        "event_name": "workflow_dispatch",
                        "ref_name": "main",
                        "sha": "cafebabe",
                        "generated_at": "2026-03-29T00:00:00+00:00",
                        "pack_status": ["admin=passed", "operator=failed"],
                    },
                )()
            )

            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(exit_code, 0)
            self.assertEqual(payload["pack_statuses"]["admin"], "passed")
            self.assertEqual(payload["pack_statuses"]["operator"], "failed")
            self.assertEqual(payload["pack_statuses"]["user"], "missing")
            self.assertEqual(payload["pack_statuses"]["exec"], "missing")


if __name__ == "__main__":
    unittest.main()
