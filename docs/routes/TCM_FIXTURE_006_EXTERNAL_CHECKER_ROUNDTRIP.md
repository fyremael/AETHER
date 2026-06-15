# TCM Fixture 006: External Checker Round-Trip

## Programme Context

`TCM Fixture 006` is the first MATH-PROGRAMME fixture whose success criterion is not a bespoke local checker alone. It defines the certificate-interchange boundary for Tropical Contraction Machines inside the Grand Challenge MATH-PROGRAMME.

Doctrine:

> Search tropical; trust external certificates.

TCM remains a MATHSOLVE route. The proof status is granted only when MATHCERT receives an independently replayable checker transcript.

## Objective

Implement an end-to-end round trip:

1. Compile a pseudo-Boolean optimization instance.
2. Solve it through exact max-plus/count semiring contraction.
3. Emit an OPB-style optimization instance.
4. Emit a certificate artifact containing a primal witness and dual upper-bound proof.
5. Run an external checker when available.
6. Fall back to a deterministic local reference checker when the external binary is absent.
7. Write a machine-readable result card, checker transcript, failure-mode ledger, and proof-import stub.

## Required Artifact Contract

A successful run must produce:

```text
artifacts/fixture006/
├── fixture006_report.md
├── instance.opb
├── primal_witness.json
├── pb_dual_certificate.json
├── checker_transcript.txt
├── result_card.json
├── failure_mode_ledger.md
├── proof_import_stub.lean
└── visuals/
    ├── 01_weight_matrix_matching.png
    ├── 02_dual_slack_certificate.png
    └── 03_roundtrip_flow.png
```

## Result Card Schema

```json
{
  "fixture": "TCM-Prover Fixture 006",
  "route_family": "SEMIRING-CONTRACTION/TCM",
  "status": "externally_checked|locally_checked_fallback|failed",
  "external_checker": {
    "enabled": true,
    "command": "...",
    "exit_code": 0,
    "transcript_path": "artifacts/fixture006/checker_transcript.txt"
  },
  "claim": {
    "problem": "max_weight_assignment_pb",
    "optimum": 85,
    "optimum_count": 1
  },
  "certificate": {
    "opb_path": "artifacts/fixture006/instance.opb",
    "primal_witness_path": "artifacts/fixture006/primal_witness.json",
    "dual_certificate_path": "artifacts/fixture006/pb_dual_certificate.json",
    "proof_import_stub_path": "artifacts/fixture006/proof_import_stub.lean"
  },
  "trust_boundary": "TCM search is untrusted; certificate replay is trusted."
}
```

## Running

```bash
PYTHONPATH=python python -m tcm_prover.fixtures.fixture006 --out artifacts/fixture006
PYTHONPATH=python python -m unittest python/tests/test_fixture006.py -v
```

To use an external checker, set `TCM_FIXTURE006_CHECKER` to a command template. The template may reference `{opb}`, `{witness}`, `{dual}`, and `{out}`.

```bash
TCM_FIXTURE006_CHECKER='my-checker --opb {opb} --witness {witness} --dual {dual}' \
  PYTHONPATH=python python -m tcm_prover.fixtures.fixture006 --out artifacts/fixture006
```

## Acceptance Criteria

- `python -m tcm_prover.fixtures.fixture006 --out artifacts/fixture006` completes.
- `python -m unittest python/tests/test_fixture006.py -v` passes.
- The result card records whether the run used an external checker or the local fallback.
- The checker transcript is always emitted.
- The failure ledger documents missing external checker, malformed certificate, witness infeasibility, dual-bound failure, and optimum mismatch.
- No floating point is part of the trusted path.

## Non-Goals

- Do not trust GPU kernels, learned policies, soft-tropical approximations, or visuals.
- Do not claim Lean has proved the theorem from the stub; the stub is an import target and audit scaffold.
- Do not treat a local fallback as equivalent to a formally verified external checker.
