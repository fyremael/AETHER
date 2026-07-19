# Colab Runtime Lab

The Colab runtime lane provides disposable Linux containment for AETHER tests,
builds, and performance diagnostics. It is deliberately subordinate to the
protected Windows commercial-candidate workflow.

## Claim boundary

- Colab output is always `diagnostic_only`.
- It cannot authorize commercial beta, regenerate a Windows package verdict,
  or substitute for the protected exact-SHA Release Readiness workflow.
- Cross-host comparisons between Colab, GitHub-hosted Windows, and native
  Windows are descriptive only.
- A Colab failure can surface a defect. A Colab pass cannot erase a failed
  protected-candidate verdict.

## Setup

The CLI requires a Unix control host. On Windows, use WSL:

```bash
uv tool install google-colab-cli==0.6.0
colab --auth=oauth2 whoami
```

The first OAuth login requires browser consent. Verify that the resulting token
includes the `colaboratory` scope before allocating a session.

## Run

From PowerShell:

```powershell
./scripts/run-colab-runtime-diagnostic.ps1 -CandidateSha <full-commit-sha>
```

From WSL:

```bash
bash scripts/run-colab-runtime-diagnostic.sh <full-commit-sha> oauth2
```

The launcher creates a uniquely named CPU runtime, executes the exact detached
candidate, retrieves its artifacts, and stops the session even when execution
fails. A successful run performs:

1. exact commit and tree verification;
2. release-mode `aether_explain` and `aether_perf` tests;
3. a release build of the performance reporter;
4. one recorded but discarded warm-up report;
5. three retained core-kernel reports with five samples per workload;
6. byte-exact equality between the downloaded summary and the ZIP's sole
   top-level `summary.json`;
7. candidate-bound summary and receipt generation; and
8. an explicit check that the named runtime is absent after teardown, including
   failure and interruption paths.

Outputs are written under
`artifacts/colab/runs/<commit>/<timestamp>/`. The receipt hashes the downloaded
archive and summary and reasserts that the output has no beta authority.

## Interpretation

Use the lane to investigate order effects, allocation sensitivity, Linux
correctness, and benchmark variance. Promotion-grade performance evidence still
requires a reviewed measurement contract and the protected Windows candidate
sequence.
