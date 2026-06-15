from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tcm_prover.fixtures.fixture006 import (
    DUAL_COLS,
    DUAL_ROWS,
    WEIGHTS,
    build_artifacts,
    solve_assignment_max_plus_count,
    validate_dual_certificate,
    validate_witness,
)


class Fixture006Tests(unittest.TestCase):
    def test_max_plus_count_contraction_finds_unique_optimum(self):
        result = solve_assignment_max_plus_count(WEIGHTS)
        self.assertEqual(85, result.optimum)
        self.assertEqual(1, result.optimum_count)
        self.assertEqual([0, 1, 2, 3, 4], result.witness_cols)

    def test_primal_and_dual_certificate_meet_at_optimum(self):
        primal = validate_witness(WEIGHTS, [0, 1, 2, 3, 4])
        upper = validate_dual_certificate(WEIGHTS, DUAL_ROWS, DUAL_COLS)
        self.assertEqual(85, primal)
        self.assertEqual(primal, upper)

    def test_invalid_witness_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_witness(WEIGHTS, [0, 0, 2, 3, 4])

    def test_artifact_roundtrip_emits_contract_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "fixture006"
            result_card = build_artifacts(out)
            self.assertIn(result_card["status"], {"locally_checked_fallback", "externally_checked"})
            expected = [
                "fixture006_report.md",
                "instance.opb",
                "primal_witness.json",
                "pb_dual_certificate.json",
                "checker_transcript.txt",
                "result_card.json",
                "failure_mode_ledger.md",
                "proof_import_stub.lean",
                "visuals/01_weight_matrix_matching.png",
                "visuals/02_dual_slack_certificate.png",
                "visuals/03_roundtrip_flow.png",
            ]
            for relative in expected:
                self.assertTrue((out / relative).exists(), relative)
            card = json.loads((out / "result_card.json").read_text(encoding="utf-8"))
            self.assertEqual(85, card["claim"]["optimum"])
            self.assertEqual("TCM search is untrusted; certificate replay is trusted.", card["trust_boundary"])
            self.assertEqual(b"\x89PNG\r\n\x1a\n", (out / "visuals/01_weight_matrix_matching.png").read_bytes()[:8])


if __name__ == "__main__":
    unittest.main()
