# ADR 0017: Operational Evidence Uses Exact Candidates And Predeclared Verdicts

## Status

Accepted

## Context

Capacity artifacts, documentation deployment, and performance checks previously
allowed operational state to drift away from the candidate being discussed. A
nested artifact directory broke Capacity Planning, Pages did not display its
source commit, and launch validation could rerun a failing performance sample
once and keep only the later outcome.

## Decision

- Capacity Planning asserts its downloaded directory layout before report
  construction and always uploads a hashed input inventory, including failure.
- Pages embeds the full source SHA and workspace version visibly and in
  `source-version.json`. The workflow fetches the deployed file, records every
  infrastructure verification attempt, and fails unless it matches the exact
  workflow SHA.
- Performance drift uses one predeclared policy from
  `fixtures/performance/verdict-policy.json`. It records every raw duration in a
  fixed five-sample run and computes one verdict from the arithmetic mean and
  tracked thresholds. A red verdict is never rerun into green.
- Capacity remains diagnostic unless a separately reviewed claim policy names
  it as required evidence.

## Consequences

- Artifact layout regressions leave inspectable evidence instead of only a
  missing-file error.
- A successful Pages deployment proves which source candidate is visible.
- Performance instability remains visible in the retained samples and cannot be
  hidden by selective retry.
- Hosted runs remain necessary; local workflow validation does not qualify an
  exact candidate.
