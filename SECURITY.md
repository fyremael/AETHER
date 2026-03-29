# Security Policy

## Scope

AETHER is currently an internal-first, post-v1 hardening project with a
launch-ready design-partner pilot and a structured QA program.

We want security reports now, but we do not want sensitive findings posted in
public before they are triaged.

This repository is not currently advertising a paid public bug bounty.

## How To Report A Vulnerability

For vulnerabilities, secret-handling issues, auth bypasses, policy leaks, data
exposure, or other security-sensitive defects:

1. Prefer a private disclosure path for this repository.
2. Include the smallest reliable reproduction you have.
3. Include affected commit, configuration shape, and whether secrets or
   operator data are involved.
4. Do not post exploitable details, tokens, private configs, or live secrets in
   a public issue.

If private vulnerability reporting is available for this repository, use it.
If it is not available in your environment, contact the maintainers through a
private channel before opening a public issue.

## What To Include

Please include:

- summary of the issue
- impact
- affected surface or endpoint
- exact reproduction steps or commands
- whether the issue requires authentication, a specific policy context, or a
  packaged deployment
- logs, payloads, or artifacts with secrets removed

## What Belongs In Public Issues Instead

Use the public bug template for:

- non-sensitive regressions
- packaging or operational failures without secret exposure
- documentation gaps
- usability issues

Use the semantics discussion template for:

- ambiguity in specs, ADRs, or documented invariants
- questions about intended semantic behavior

## Response Posture

The current goal is responsible triage, remediation, and documentation
discipline, not fast marketing claims about a bounty program.

That means:

- reproduce first
- classify clearly
- fix truthfully
- update the governing docs when the fix changes expectations
