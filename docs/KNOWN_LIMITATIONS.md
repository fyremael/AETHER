# KNOWN_LIMITATIONS

The unrestricted v1 single-node kernel slice remains implemented, but the
policy-aware portion of semantic closure is reopened. The active external claim
is:

> Controlled single-node alpha with a real Rust semantic kernel, limited to one
> visibility domain, trusted appenders, and explicitly supported deployment
> boundaries.

The historical closeout is `docs/V1_CLOSEOUT.md`; the audit and binding repair
sequence are `docs/COMPREHENSIVE_AUDIT_2026-07-09.md` and
`docs/REMEDIATION_PROGRAMME.md`.

These are real boundaries, not hidden footnotes. They mark the edge of the
implemented system.

## External Review Defects

The July 2026 external reviews are recorded in
`docs/V2_EXTERNAL_REVIEW.md` and
`docs/COMPREHENSIVE_AUDIT_2026-07-09.md`. These findings are defects, not
ordinary feature backlog:

- The late-policy-filtering defect is repaired in the current remediation
  branch: policy scope now binds replay, program compilation, runtime,
  federation, reports, and sidecar cuts before semantic evaluation. The
  mixed-policy external claim remains reopened until Postgres parity,
  performance characterization, and exact-candidate evidence are complete.
- Service explanations now resolve opaque execution-scoped handles persisted
  beside the journal and re-check namespace and current policy authorization.
  Retention is bounded to 1,024 executions by default; evicted handles return
  an explicit expired result. Postgres journal mode currently keeps this
  derived execution metadata in local SQLite, so operators must back up both
  stores at one operational cut.
- Public append now validates the complete batch against one active namespace
  schema and commits it with its cut and durable receipt atomically. Existing
  incompatible prefixes are visibly quarantined. The remaining boundary is
  qualification: online mixed-schema history and in-place type/class changes
  are unsupported, and the compatibility release still permits omitted schema
  references to negotiate the active or conservative legacy-inferred schema.
- The authored/path-based readiness defect is repaired locally: the ledger is
  policy-only and the verifier computes from exact candidate/package outcomes.
  The official workflow run and R5 evidence subjects remain outstanding.

## Language And Runtime Scope

- The runtime now executes semi-naive recursion, stratified negation, and bounded aggregation for the current v1 slice, but aggregation is intentionally frozen at non-recursive grouped head-term `count`, `sum`, `min`, and `max` rules rather than richer post-v1 aggregate syntax.
- Extensional predicate binding is inferred by name against schema attributes and is therefore deliberately conservative.
- Explain traces currently reconstruct one merged proof graph per tuple; they do not yet distinguish alternative proof families for the same derived tuple.
- The DSL now covers the current canonical v1 surface, but it still lacks post-v1 ergonomic features such as richer type aliases, broader document modularity, and more generalized explain/query composition.

## Service, Governance, And Operator Surface

- The kernel service now has in-memory, SQLite-backed, and optional Postgres-journal execution paths. Service v2 namespaces isolate HTTP service state and token authorization, but they are not DSL semantics, authority partitions, cross-partition transactions, or a full managed multi-tenant platform.
- Coordination semantics now cover heartbeats and execution outcomes in the pilot slice, but expiry still relies on explicit semantic state rather than clock-driven timeout windows or distributed failure detection.
- HTTP authorization still uses coarse endpoint scopes. Tokens now bind policy before scoped snapshot construction, but the external noninterference claim remains pending exact-candidate evidence and scheduled Postgres parity.
- Audit entries now capture effective policy decisions plus requested, granted, and effective semantic visibility, but they still do not capture full operator intent or semantic diffs between cuts.
- Omitted append schema references and the legacy tuple explanation endpoint are
  compatibility-only paths with explicit audit telemetry. They remain callable
  until the evidence-gated sunset in `docs/API_CLIENT_MIGRATION.md`; first-party
  clients do not automatically fall back to either path.
- Operator reports carry policy context in fixed-format markdown and JSON, compute from policy-projected semantic snapshots, and carry execution-scoped trace handles; their fixed report shape remains narrower than arbitrary investigative views.
- Coordination delta reports now compare explicit cuts and carry trace handles where visible, but they still summarize fixed pilot sections rather than arbitrary user-defined investigative views.
- The Go operator TUI is now implemented as the primary live pilot cockpit, but it is intentionally pilot-focused and read-only in v1 rather than a general workflow IDE or mutation surface.
- The pilot service now has a packaged deployment path with config-backed startup, package-local rotation tooling, backup/restore helpers, auth reload, explicit token/principal identities, and secret-file/env/command token resolution, but it is still a single-node bundle rather than a fully managed deployment story with automated rotation services, distributed revocation, or native cloud secret-manager integrations.

## Performance, Storage, And Release Discipline

- The performance suite now supports host-aware run bundles, suite-specific drift comparison, stress fixtures, matrix summaries, and a lightweight trend index across saved bundles, but it is still artifact-based rather than a persistent benchmark database.
- The new capacity planner now produces concrete node-class guidance and explicit scale-out triggers, but those envelopes are still internal planning outputs from single-host calibration rather than public guarantees or cloud-SKU-specific commitments.
- Capacity artifact layout and Pages source identity are now asserted locally,
  and performance verdicts retain fixed raw samples without retry. None of those
  local automation changes substitutes for green hosted runs on the exact
  candidate; Capacity remains diagnostic unless a claim policy explicitly names it.
- The accepted regression gate is still deliberately narrow: `core_kernel` and `service_in_process` on the canonical native Windows dev host are the tracked release baselines, while HTTP and replicated-partition suites remain observational until their variance is better understood.
- The current measured default `M` envelope is conservative: it presently recommends `1,024` pilot-board tasks even though larger ladders run correctly, because operator/report latency degrades before replay or local storage become the limiting factor.
- The new R4 verifier binds observations to clean commit/tree/ref identity,
  exact commands, workflow attempts, output bytes, and package digest; authored
  statuses, source-path existence, and `latest` inputs are rejected. An official
  GitHub run and independently downloaded bundle are still required before this
  local implementation satisfies the evidence-integrity release gate.
- The commercial release readiness ledger targets controlled design-partner
  alpha. Commercial beta is governed by the non-waivable R1-R6 gate policy and
  additionally requires protected exact-candidate hosting controls, every named
  immutable bundle subject, exact-SHA Pages deployment, protected release
  approval, and independent verification. Local or pull-request checks do not
  satisfy that promotion boundary. GA remains separately blocked.
- Existing Service v2 operability, backup/restore, performance, package, and
  customer-workflow artifacts remain useful partial evidence only until they
  are emitted as verified subjects in the immutable candidate bundle. They do
  not override the remaining blockers or qualify commercial beta.
- The operational file/checksum inventory is now honestly named a file
  manifest. Strict CycloneDX Rust, Go, and assembled-package SBOMs plus pinned
  vulnerability, license, secret, package, and supported-language CodeQL gates
  are implemented. Commercial beta remains blocked until those hosted gates,
  attestations, and repository protection settings pass for the exact candidate.
- The optional Postgres journal now defaults to forced `verify_full`, with explicit
  `verify_ca`, CA-bundle, mTLS, and loopback-only development plaintext modes.
  Non-loopback HTTP requires an explicitly named trusted HTTPS ingress. The local
  contract and negative tests pass, but remote Postgres/non-loopback HTTP remain
  outside the controlled-alpha claim until the hosted real-TLS matrix and ingress
  isolation evidence pass for the exact candidate.
- Namespace and replicated-partition work is independently lockable and bounded
  by global workers plus per-namespace admission. Request, DSL, runtime, page,
  rate, audit, and execution-retention limits now fail closed. The packaged
  pilot uses fixed process-lifetime defaults rather than dynamic/adaptive tenant
  quotas. Timeout cancels only queued work; started synchronous semantic work
  completes atomically and is not cooperatively interrupted.
- The new QA hardening workflow is intentionally non-blocking in phase one. It is a diagnostic program for surfacing admin, operator, user, and exec defects before stable subchecks are promoted into `CI` or release-readiness.
- The repository now has a responsible-disclosure policy, but it is not yet advertising a paid public bug bounty.
- Memory figures in the performance report are structural lower-bound estimates rather than allocator-exact telemetry.
- Telemetry stops at host facts plus kernel/runtime counters. Profiler-grade CPU, allocator, or scheduler tracing is still out of scope for the current phase.
- SQLite remains the default local/package journal backend. The optional Postgres backend is journal-first only: it preserves committed source order per namespace through the `Journal` contract, but it is not a SQL rule engine, not a global `AsOf`, not consensus over derived state, and not a sidecar catalog backend.
- The planner now makes the “do not chase one giant node beyond `XL`” rule explicit, but the partition/federation posture is still operational guidance rather than an automated re-sharding or multi-host placement system.

## Boundary Clients And Scaling

- `aether_api` remains a temporary compatibility facade over the recovered
  responsibility crates. Removing those re-exports is a future breaking change
  and requires client migration evidence.
- The executable plan format is versioned and fail-closed but currently has one
  supported version and no cross-version migration/serialization promise.

- The governed incident blackboard demo pack is a product-facing packaging layer over current proof, not a claim that AETHER is already a general multi-agent control plane.
- The AI support resolution desk app pack is a flagship ML-facing reference application over current proof, not a claim that AETHER is already a finished ML orchestration platform, autonomous support SaaS, or authoritative vector-truth layer.
- The blackboard / TupleSpace language remains a reference pattern and explanation aid. It is not yet a stable top-level product API, facade contract, or replacement public identity for AETHER.
- The Go shell and Python SDK are now capability-negotiated real boundary clients,
  but both remain early surfaces without richer async and administration layers.
- The notebook hardening checks validate structure, bootstrap assumptions, path integrity, and Python code-cell syntax, but they do not yet execute full Colab notebook runs as a release blocker.
- Artifact and vector sidecar federation is now journal-subordinated and temporally exact on the SQLite-backed pilot path, but it is still a single-node backend and does not yet replicate or fail over independently of the kernel process.
- Vector search can now project provenance-bearing semantic facts back into the rule layer, but the current projection is deliberately narrow: a three-field `(query_entity, matched_entity, score)` extensional fact shape.
- The first partition-aware distributed-truth slice now includes imported-fact reasoning, federated explain/report surfaces, a SQLite-backed durable backend, and a single-host leader/follower replicated authority-partition prototype with restart-safe metadata, manual promotion, stale-epoch fencing, lag/degraded status, and divergent-prefix rejection. What it still does not include is follower-read contracts, automatic election, quorum consensus, multi-host replication, or a managed failover plane.
- Imported-fact federation is semantically exact for the current slice, but that slice is intentionally narrow: imported queries must currently be single-goal tuple-producing reads rather than arbitrary joined row shapes.
- Sidecars remain partition-local and journal-subordinated in the replicated prototype. They do not replicate or fail over independently.
- Sidecars also remain local SQLite catalogs in Service v2, including when the authoritative journal backend is Postgres. Postgres sidecar catalogs, remote sidecar failover, and sidecar-first control planes are deferred.
