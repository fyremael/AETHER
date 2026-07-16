# AETHER Comprehensive Audit

- Date: 2026-07-09
- Audited commit: `fd4c68db9f0232a18e930d42f55a30f1d74f6201`
- Branch: `main`
- Scope: Rust semantic kernel, service/API boundary, Go and Python clients,
  tests, CI, documentation, security posture, release evidence, and current
  product/readiness claims

The execution plan responding to this audit is
`docs/REMEDIATION_PROGRAMME.md`.

## Executive Verdict

AETHER is a substantial Rust semantic-kernel implementation, not a speculative
scaffold. The unrestricted single-node path has credible coverage for schema
resolution, deterministic replay, recursive SCC execution, stratified
negation, bounded aggregation, provenance, explanations, SQLite durability,
HTTP boundaries, sidecars, and a narrow partition/replication prototype.

The current repository is not defensibly `commercial_beta` ready, however.
Two policy-isolation failures are reproducible semantic defects, the tuple
explanation endpoint can return a proof for a different run than the caller
intended, and the service accepts semantically invalid journal entries before
any schema validation. The current security and release gates do not detect
those failures and can report `ready` from static statuses and path existence.

The right posture is:

| Claim | Audit verdict |
| --- | --- |
| Rust mainline kernel exists | Supported |
| First unrestricted library acceptance slice exists | Supported |
| Full v1 closure including policy-aware semantics | Not supported until P0 policy defects are fixed |
| Closely controlled design-partner alpha | Supportable with one visibility domain, trusted appenders, and explicit limitations |
| Commercial beta as currently recorded | Not supported |
| General availability | Correctly blocked by existing docs |

The audit found no reason to abandon the Rust/DSL/SCC architecture. The main
problem is that service, policy, evidence, and product claims widened faster
than the semantic and release contracts beneath them.

## Repository State At Audit Start

`main` matched `origin/main`. The worktree already contained documentation
changes from earlier review and positioning work:

- modified `docs/COMMERCIALIZATION/README.md`
- modified `docs/KNOWN_LIMITATIONS.md`
- modified `docs/ROADMAP.md`
- untracked `docs/COMMERCIALIZATION/AGENTIC_POSITIONING_OPTIONS.md`
- untracked `docs/V2_EXTERNAL_REVIEW.md`

Those files were treated as user-owned and were not rewritten by this audit.
This report is the only durable file added by the audit.

## Validation Performed

| Gate | Result |
| --- | --- |
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passed |
| `cargo test --workspace --all-targets` | Passed: 125 tests, 10 intentionally ignored; examples and Criterion benchmark targets also executed |
| `go test ./...` | Passed across 3 packages |
| `python -m pytest -q python/tests` | Passed: 34 tests |
| `cargo doc --workspace --no-deps` | Passed |
| `python scripts/build_pages.py --out-dir <temporary-dir>` | Passed |
| `cargo audit` | Not available locally and not present in CI |
| `govulncheck` | Not available locally and not present in CI |

Additional targeted audit probes were created temporarily, executed, and
removed. They established the following current behavior:

1. A public scalar assertion followed by a protected retract resolves to no
   public state. Expected policy-scoped replay retained one public entity;
   actual result retained zero.
2. A protected `blocked(entity(1))` fact suppresses an otherwise public rule
   derived through `not blocked(t)`. Expected one public row; actual result was
   zero.
3. A tuple ID returned by one document run silently resolves to a different
   tuple after another document run reuses the same process-local ID.
4. The append API accepts attribute `999` without a schema. A subsequent
   ordinary DSL document run then fails with `unknown attribute 999`.

## What Is Strong

### The architectural center follows `AGENTS.md`

- The root is a Rust workspace with the required nine semantic crates.
- Rust remains authoritative; Go and Python are boundary clients.
- The DSL is the semantic authoring surface.
- Recursive evaluation is generic rule/SCC execution, not a hand-coded
  reachability utility.
- Derived tuples carry provenance and explanation metadata.
- No Rust `unsafe` blocks were found.
- All five mandatory early ADR subjects exist, alongside later federation and
  Service v2 ADRs.

### The unrestricted core is real

The repository has focused tests for:

- scalar, set, reference, and sequence merge behavior
- `Current` and `AsOf` replay
- duplicate and unknown journal cuts
- parser and rule safety errors
- SCC compilation and semi-naive closure
- stratified negation
- bounded aggregation
- source and derived provenance
- SQLite restart/replay
- authenticated HTTP and namespace behavior
- sidecar journal anchoring and provenance-bearing projection
- single-host replicated partition fencing and divergent-prefix rejection

The green Rust, Go, Python, documentation, current-main CI, Postgres CI, and
container-smoke results are meaningful positive evidence. They do not cover the
defects below because the current tests encode filtering behavior without the
non-monotonic and hidden-retraction adversarial cases.

## Prioritized Findings

### P0 — Policy is applied after semantic evaluation, so hidden data changes public truth

Evidence:

- `KernelServiceCore::current_state` and `as_of` resolve the full datom set and
  filter the materialized result afterward in
  `crates/aether_api/src/lib.rs:419-440`.
- `run_document` evaluates the full journal and full compiled fact set before
  filtering state, program, derived tuples, and query rows in
  `crates/aether_api/src/lib.rs:500-594`.
- `RuleRuntime::evaluate` has no policy-context argument and builds extensional
  rows from the complete state/program in
  `crates/aether_runtime/src/lib.rs:14-20` and
  `crates/aether_runtime/src/lib.rs:96-103`.
- Policy filtering appears only at query matching time in
  `crates/aether_runtime/src/lib.rs:313-372`. That is too late for negation,
  aggregation, and fixed-point membership.

Reproduced consequences:

- A protected retract erases a public assertion before response filtering.
- A protected fact can satisfy a negative predicate and suppress a public
  derivation.
- Protected input can influence aggregate counts and fixed-point structure
  before filtering by the same mechanism.

There are also direct metadata leaks in the filtered response:

- `filter_resolved_state` preserves the unfiltered `as_of` tail at
  `crates/aether_api/src/lib.rs:264-312`, revealing the element ID of hidden
  activity.
- `filter_derived_set` preserves unfiltered iteration/delta counts at
  `crates/aether_api/src/lib.rs:315-345`, revealing hidden derivation shape and
  counts.

Impact:

- This is an authorization correctness failure, not only an ergonomic gap.
- Absence, counts, and cut metadata can leak protected activity.
- `docs/STATUS.md:52-54`, `docs/V1_CLOSEOUT.md:19`, and the policy-aware
  closure language in `docs/SEMANTIC_COMPLIANCE_MATRIX.md` overstate the
  implemented guarantee.
- Mixed-visibility design-partner or commercial-beta use is unsafe.

Required correction:

1. Define policy-scoped journal replay and policy-scoped program facts as the
   semantic input cut.
2. Pass the effective policy context into resolver/runtime or construct the
   visible journal/program before materialization and evaluation.
3. Recompute negation, aggregation, fixed points, `as_of`, iteration metadata,
   and explanations entirely inside that cut.
4. Add the hidden-retract, hidden-negation, hidden-aggregate, hidden-cut, and
   hidden-iteration-count cases as release-blocking regression tests.

### P1 — Tuple explanation IDs alias across runs and remain stale after append

Evidence:

- `KernelServiceCore` stores only one `last_derived` value at
  `crates/aether_api/src/lib.rs:131-136`.
- Each evaluation overwrites it without recording namespace, journal cut,
  program/document hash, or policy context at
  `crates/aether_api/src/lib.rs:202-204`.
- Append does not invalidate it at `crates/aether_api/src/lib.rs:405-411`.
- The explanation request accepts only `TupleId` plus policy context and
  resolves it against the last cached set at
  `crates/aether_api/src/lib.rs:463-475`.

The audit reproduced a concrete alias: `t1` from a run deriving entity `1`
returned the trace for entity `2` after a second run reused `t1`.

Impact:

- A syntactically successful proof can describe a different query/program/cut
  than the caller intended.
- Audit and operator evidence can be silently wrong.
- Appending new truth leaves an apparently valid proof tied to an older cut.

Required correction:

- Replace process-local tuple IDs at the API boundary with opaque trace handles
  bound to namespace, journal cut, program/document digest, effective policy,
  and tuple ID.
- Invalidate or version caches on append, promotion, auth/policy changes, and
  document changes.
- Prefer returning proof material with the run that produced it.

### P1 — The service accepts unreplayable datoms before schema validation

Evidence:

- `AppendRequest` contains datoms but no schema/schema handle.
- `KernelServiceCore::append` writes directly to the journal at
  `crates/aether_api/src/lib.rs:405-411`.
- Schema/operation validation happens only later during resolver replay.
- The append-only journal has no quarantine or repair operation.

The audit verified that attribute `999` is accepted and only fails later when a
normal DSL document resolves the journal. The same pattern applies to operation
class mismatches and other schema-invalid entries.

Impact:

- One authorized client defect can make ordinary document replay fail for an
  entire namespace.
- SQLite/Postgres durability faithfully preserves the poison across restart.
- This undermines the service promise even though the low-level journal is
  correctly schema-agnostic.

Required correction:

- Keep `aether_storage` schema-agnostic, but give each service namespace a
  versioned canonical schema contract.
- Validate the complete append batch against that schema before committing any
  datom.
- Record schema version/digest with the append and define migration/quarantine
  handling before widening the service boundary.

### P1 — Commercial readiness is self-attested rather than bound to current evidence

Evidence:

- `scripts/commercial_readiness.py:39-124` validates ledger shape and whether
  referenced paths exist, but it does not validate evidence outcomes, commit
  identity, freshness, checksums, or workflow conclusions.
- `scripts/commercial_readiness.py:127-173` calculates stage readiness directly
  from static `status` fields in the ledger.
- Generated release-readiness evidence is under ignored `artifacts/`, so it is
  neither portable nor part of the audited commit.
- The latest local release-readiness report was generated for commit
  `d9280786211fd8f4473f6e7c9b4e8ccaa133bac7`, while current `main` is
  `fd4c68db9f0232a18e930d42f55a30f1d74f6201`.
- The GitHub `Release Readiness` workflow has no recorded runs.
- The static ledger says `commercial_beta=ready` even though the P0 policy
  defect is part of the claimed closure surface.

Impact:

- The renderer can emit a green beta verdict when evidence is stale, missing
  from a fresh checkout, or semantically contradicted by known defects.
- “Ready” currently means a human-authored status plus existing source paths,
  not a reproducible decision over immutable evidence.

Required correction:

- Make each blocking gate consume a machine-readable result bound to the exact
  commit, host/suite where relevant, command, timestamp, and artifact digest.
- Fail closed on missing/stale evidence.
- Publish one immutable release evidence bundle from the GitHub workflow.
- Reclassify commercial beta as blocked until P0/P1 semantic gates pass.

### P1 — The security gate is incomplete and its “SBOM” is a file checksum manifest

Evidence:

- `scripts/security_key_lifecycle.py:98-129` records package file paths, byte
  counts, and SHA-256 hashes. It does not inventory Rust/Go dependencies,
  versions, licenses, or package identifiers.
- The security gate labels that output an SBOM and passes solely on file count
  at `scripts/security_key_lifecycle.py:322-334`.
- GitHub reports Dependabot alerts disabled and no code-scanning analysis.
- Neither `cargo audit` nor `govulncheck` is installed locally or run in CI.
- GitHub Actions use mutable major tags rather than immutable commit SHAs.
- `PostgresJournal::open` always uses `NoTls` at
  `crates/aether_storage/src/lib.rs:164-173`, and the deployment docs do not
  constrain Postgres to a same-host/private transport or document TLS as an
  unsupported boundary.

Positive evidence:

- Secret scanning returned no open alerts.
- The package does generate useful file-integrity hashes.
- Token rotation, secret-command resolution, auth reload, namespace binding,
  and revocation tests exist.

Impact:

- “Security and key lifecycle are beta-ready” is broader than the actual gate.
- Dependency vulnerabilities and license issues are invisible to the release
  decision.
- A remote Postgres deployment cannot establish transport encryption through
  the current journal connector.

Required correction:

- Rename the current output to package file manifest/checksum manifest.
- Generate a standard CycloneDX or SPDX SBOM including Rust and Go dependency
  components.
- Add RustSec, Go vulnerability, license-policy, and code-scanning gates.
- Pin third-party Actions by full commit SHA.
- Add a TLS-capable Postgres connector and certificate policy before remote
  Postgres is a supported beta deployment.

### P2 — The scheduled capacity pipeline is currently broken

The 2026-07-08 Capacity Planning run failed at the capacity-report job. The
matrix job uploads both `artifacts/performance/matrix` and
`artifacts/performance/trends` as one artifact, then the next job downloads that
artifact into `artifacts/performance/matrix` at
`.github/workflows/capacity-planning.yml:132-167`. The report step expects
`artifacts/performance/matrix/latest.json` at lines `169-188`; after artifact
path preservation, that file is nested under another `matrix` directory.

Impact:

- Capacity report and tracker jobs are skipped.
- Current capacity guidance is not receiving its scheduled refresh.
- The failure history is material: only one of the last ten listed capacity
  workflow runs succeeded.

Required correction:

- Download to `artifacts/performance` or upload/download the matrix and trends
  directories as separate artifacts with explicit target paths.
- Add a preflight path assertion and rerun the scheduled workflow.

### P2 — HTTP execution is globally serialized across namespaces

Evidence:

- All namespace services share one `Arc<Mutex<NamespaceServiceStore>>` at
  `crates/aether_api/src/http.rs:32-39`.
- The mutex remains held while a full service operation executes at
  `crates/aether_api/src/http.rs:240-271` and
  `crates/aether_api/src/http.rs:881-896`.
- The Postgres path creates an OS thread and immediately joins it from the async
  request path instead of using `tokio::task::spawn_blocking`.

Impact:

- A long document, report, vector search, or blocked database call delays every
  other namespace.
- HTTP concurrency and tail-latency evidence can look healthy in low-contention
  tests while the design has a deterministic head-of-line bottleneck.

Required correction:

- Store independently lockable per-namespace service handles.
- Use a bounded blocking pool for synchronous storage.
- Add cross-namespace contention and slow-storage tests before multi-tenant
  language widens.

### P2 — Crate names no longer describe ownership boundaries

Evidence:

- `aether_api` now contains approximately 29,000 Rust source lines across API,
  HTTP, deployment, sidecars, federation/replication, reports, pilot product
  logic, performance measurement, capacity planning, and tests.
- `crates/aether_api/src/perf.rs` alone is 4,389 lines and
  `partitioned.rs` is 3,135 lines.
- `aether_plan` is 46 lines of data structures at
  `crates/aether_plan/src/lib.rs:1-46`.
- The compiler builds SCCs, strata, bindings, and delta metadata in
  `crates/aether_rules/src/lib.rs:22-109`, while the runtime reconstructs
  execution scheduling from the compiled bundle.

Impact:

- Service/product concerns are becoming the practical center of the workspace,
  contrary to the intended kernel-centered architecture.
- `aether_plan` is not yet a stable compiler/runtime contract.
- Performance tooling and product demos inflate normal API dependencies and
  review surface.

Required correction:

- Fix the semantic defects first; do not begin with a cosmetic crate split.
- Then extract service core, HTTP/auth/audit, sidecars, partitions, performance,
  and pilot/report surfaces behind explicit Rust contracts.
- Promote `aether_plan` into the executable plan boundary consumed by runtime.

### P2 — Current public documentation is not deployed from current main

The latest Pages run for current `main` built the Rust docs and site artifact
successfully, but the deploy job failed with GitHub's generic “Deployment
failed, try again later” result. Local documentation generation is green, so
this appears operational/transient rather than a site-build defect. It still
means the latest main documentation has no successful Pages deployment.

Required correction:

- Rerun Pages and verify the deployed commit SHA.
- Surface deployed SHA/version in the site footer or status page.

### P3 — Storage and parser interfaces are acceptable v1 implementations but v2 pressure points

- `Journal::history` and `prefix` return cloned `Vec<Datom>` values; full
  histories and prefixes are repeatedly materialized for service operations.
- The DSL parser is line/delimiter oriented and reports limited source context.
- Extensional predicate binding is inferred from names.
- Performance and capacity data are artifact files rather than a durable trend
  store.

These are not blockers for a controlled v1 pilot. They should remain behind the
P0/P1 correctness, evidence, and security work.

## Documentation And Claim Audit

The documentation is unusually extensive and generally honest about
distributed-truth, sidecar, platform, and GA limitations. The strongest defect
is not missing prose; it is inconsistency between semantic evidence and closure
language.

Documents that need correction after fixes are chosen:

- `docs/STATUS.md`: qualify policy-aware execution and current readiness.
- `docs/V1_CLOSEOUT.md`: reopen or explicitly carve policy-aware semantics out
  of the closed claim. The cleaner option is to reopen the affected gate.
- `docs/SEMANTIC_COMPLIANCE_MATRIX.md`: add adversarial policy cases and bind
  evidence to exact tests.
- `docs/KNOWN_LIMITATIONS.md`: retain the already drafted policy and explanation
  defects, then add append validation, dependency scanning, and Postgres TLS.
- `docs/ROADMAP.md`: keep P0/P1 ahead of interface broadening.
- `fixtures/release/commercial-readiness-ledger.json`: mark commercial beta
  blocked until machine-bound evidence is green.

## Recommended Execution Order

1. Pause mixed-policy and commercial-beta claims; keep narrowly controlled
   alpha/demo use explicit.
2. Land failing regression tests for policy-scoped replay, policy-scoped
   negation/aggregation, filtered metadata, tuple-handle identity, and invalid
   append rejection.
3. Implement policy as semantic input and replace tuple IDs with trace handles.
4. Add namespace schema identity and transactional append validation.
5. Make release/security evidence commit-bound, immutable, and fail-closed;
   generate a real SBOM and add vulnerability/license/code scanning.
6. Add Postgres TLS support and document network/security boundaries.
7. Fix and rerun Capacity Planning; rerun Pages; execute Release Readiness on
   GitHub for the repaired commit.
8. Remove global HTTP serialization and add contention evidence.
9. Reorganize `aether_api` and promote `aether_plan` only after semantics are
   stable.

## Final Assessment

AETHER's central technical thesis is worth continuing. The Rust kernel,
canonical DSL, deterministic replay, SCC/semi-naive execution, and provenance
model form a coherent base. The current failure is one of claim and boundary
discipline: access policy is being treated like presentation filtering,
explanation identity is process-local, append validity is deferred until replay,
and readiness automation trusts declarations more than immutable outcomes.

Fix those in that order. Until then, describe AETHER as a strong, controlled
single-node alpha with a real semantic kernel—not as a commercially beta-ready
governed runtime.
