# STATUS

## Current state

The repository has advanced from a pure specification bundle to a functioning
single-node semantic kernel. Its active claim is deliberately contained while
the July 2026 audit findings are remediated:

> Controlled single-node alpha with a real Rust semantic kernel, limited to one
> visibility domain, trusted appenders, and explicitly supported deployment
> boundaries.

In plain terms: AETHER can now journal facts, replay exact cuts, derive
recursive truths, explain them, and serve them through authenticated boundaries
without asking the user to imagine the kernel into existence.

The repository now also carries a governed incident blackboard demo pack that
repackages those existing proof surfaces in plain product language for
design-partner conversations. That packaging layer is documentation and demo
work, not a new kernel-semantic claim.

The commercialization layer now also carries an AI support resolution desk app
pack as the flagship ML-facing end-user application. That pack reuses the
current pilot, sidecar, replay, and explanation surfaces to make AETHER
legible in a buyer-relevant support workflow before broader platform language.

Post-v1 hardening work is now also underway as an internal-first QA program:
persona-based sweeps, disclosure guidance, stronger defect intake, and
scheduled non-blocking hardening automation now exist to hunt admin,
operator, boundary, and front-door failures before they are promoted into
blocking release gates.

That QA layer now also includes a repeatable perturbation-and-capacity sweep
that composes the persona pass, fresh performance bundles, drift checks, and
larger release-mode stress workloads into one saved planning artifact for
single-node scale discussions.

That scaling lane now also has a first-class live sizing system: typed
capacity curves, concrete `S`/`M`/`L`/`XL` node classes, a recurring capacity
bundle, and a scheduled GitHub tracker issue for headroom and ceiling drift.

The unrestricted kernel acceptance slice remains substantial, but the
policy-aware portion of v1 closure is reopened. The reproduced defects and
binding repair sequence are recorded in
`docs/COMPREHENSIVE_AUDIT_2026-07-09.md` and
`docs/REMEDIATION_PROGRAMME.md`. `docs/V1_CLOSEOUT.md` and the semantic
compliance matrix now carry that reopened boundary explicitly.

Remediation execution has completed the local R1 semantic repair, R2 proof
identity repair, and R3 transactional append-admission repair: normalized
`PolicyScope`, cut-then-project scoped replay, dependency-closure
certification, scoped program compilation, typed scoped runtime bundles, a
central service evaluation path, federated/sidecar projection, and
projection-local evaluation keys are implemented. The compatibility evaluator
now also accepts source datoms and a source rule program and scopes both before
replay, compilation, negation, aggregation, or closure; it no longer filters an
unrestricted result. Service evaluations emit deterministic execution receipts
and persisted opaque trace handles backed by bounded in-memory or SQLite
execution stores. Equivalent executions reuse the same tuple handle instead of
growing trace storage; execution and expired-handle retention are both bounded.
Resolution rechecks namespace
and current policy, supports digest-checked replay, survives restart and
backup/restore, and binds federated source cuts, epochs, prefix digests, and
source execution IDs. Rust, HTTP, reports, Go, Python, the TUI, demos, and
notebooks use handles; bare tuple explanation returns
`409 ambiguous_tuple_reference`. The external mixed-policy and durable-proof
claims remain contained until the later immutable-evidence and operational
qualification phases bind these local results to an exact candidate.

Namespace writes now pass one canonical active schema before journal commit.
In-memory, SQLite, and Postgres compare schema identity and exact journal cut
atomically with batch append and durable receipt creation. Existing prefixes
are sealed as certified baselines or visible quarantines; schema activation
contends with append on the same cut. Partition leaders issue the receipt and
followers preserve and verify its exact identity. Schema discovery/admin,
dry-run, receipt, structured-error, Go, and Python surfaces are implemented.

R4's immutable evidence layer is also implemented locally. Versioned envelope,
bundle, waiver, release-subject, promotion-record, and gate-policy contracts now separate release requirements
from observations. A standard-library runner captures clean commit/tree/ref,
workflow/run/job, exact commands, inputs, attempts, outputs, and expiry; the
verifier re-hashes every byte and recomputes a deterministic verdict.
The commercial ledger is policy-only and rejects authored outcomes. A reusable
exact-candidate workflow now consumes the successful exact-SHA Supply Chain
package after Release Readiness has downloaded, digest-checked, and tested
those exact bytes. Eighteen candidate-bound subjects are assembled only after
operational readiness, including customer-workflow evidence. The workflow
emits a SHA/run/attempt-named bundle and delegates verification to a dependent
job. Official verification now requires signed package provenance plus live
producer and prerequisite run/job outcomes and redownloaded artifact bytes; a
numeric run declaration, declared host, or correctly named file is
insufficient. The qualification and generated promotion-record path are
implemented locally. An independent adversarial review found no remaining
P0/P1 defects after the final capacity and readiness-byte tightening. Local
validation was green before hosted qualification. PR #28 has since merged and
protected candidate `11380eed81d0690717637a6926ae0087547205c2` passed exact-SHA
CI, Supply Chain, Pages, and Capacity Planning. Release Readiness run
`29625522886` failed the unchanged service performance gate because its first
durable coordination restart stalled while subsequent passes remained stable.
No official bundle or passed verdict was emitted. Phase-level, all-pass restart
instrumentation and ten fresh native Windows processes localized `99.67%` of
first-observed restart time to separately committed execution-trace
persistence; replay and recursive execution remained millisecond-scale. The
hash-bound diagnostic record and bounded batch-persistence follow-up are in
`docs/RESTART_LATENCY_INVESTIGATION.md`. Atomic SQLite batch persistence is now
implemented with rollback and restart coverage; ten fresh local processes cut
first-observed mean restart latency from `3,347.816 ms` to `30.579 ms`.
The derived execution catalog now uses the established journal WAL and
`synchronous=NORMAL` posture plus derived-store-specific suppression of
last-connection checkpointing. The latest ten-process run bounded first
persistence to `7.239 ms` and service close to `1.198 ms`; five local exact
baseline/current comparisons passed the unchanged gate. Backup/restore coverage
now proves the database/WAL/SHM snapshot boundary. PR #29 merged, and protected
candidate `083833634c174ce04f4a7329b78bcdcdb241024d` passed exact-SHA CI,
Supply Chain, Pages, and Capacity Planning. Release Readiness run `29642281070`
then exposed an in-repo host-policy mismatch: every latency and drift threshold
passed, but the beta gate rejected its pinned `github-windows-latest` evidence
because the policy named only `dev-chad-windows-native`. No official bundle or
passed verdict was emitted. The policy now explicitly permits those two
Windows evidence hosts while continuing to reject every other host and
preserving same-host drift comparisons. A policy-integrity gate pins every
required drift and latency surface and rejects missing, duplicate, malformed,
non-finite, or status-weakened entries; failed readiness runs also retain their
primary failure in a partial immutable manifest. A new protected candidate must
pass the complete sequence. This does not widen the controlled-alpha claim.

R5.1-R5.6 are now implemented locally. The service has strict dependency and
package gates, verified transport modes, independent namespace admission,
capability-negotiated clients, exact operational verdict automation, and
fail-closed resource controls. Request/document/rule/runtime/result/rate limits,
queue timeout semantics, finite execution retention, audit backpressure, and
typed no-partial-mutation failures are published in
`docs/RESOURCE_CONTROL_CONTRACT.md`. Hosted exact-candidate runs, ingress
isolation evidence, real TLS-Postgres evidence, and independent bundle
verification remain required; the public claim therefore remains controlled
alpha.

R6 responsibility recovery is also implemented locally. Service semantics,
HTTP/deployment, sidecars, partitions/federation, performance, and pilot proof
packs now live in dedicated crates; `aether_api` is a compatibility facade.
`aether_plan` owns a versioned executable schedule with delta, aggregate, and
provenance nodes, and runtime consumes it fail-closed. The ownership contract,
measurements, and dependency gate are in `docs/ARCHITECTURE_BOUNDARIES.md`.

R7 hosting controls are now explicit rather than assumed. CI and Supply Chain
publish stable aggregate check names, Actions is constrained to full-SHA
GitHub/allowlisted actions (including nested composite dependencies), and a
read-only verifier compares the live branch, Actions, security, and environment
settings with `.github/repository-controls.json`. Commercial beta remains
blocked until protected `main`, protected release approval, every required
bundle subject, exact-SHA Pages, and independent bundle verification all pass.

Completed:

- Rust workspace root created
- canonical Rust crates added under `crates/`
- Go and Python boundary directories created
- schema, storage, resolver, compiler, and runtime substrate implemented as an initial vertical slice
- durable SQLite journal implemented behind the `Journal` boundary with restart-safe replay coverage
- source datom provenance threaded through resolution and derivation
- first recursive tuple explainer implemented
- execution-scoped proof manifests, receipts, opaque trace handles, durable
  SQLite resolution, authorization re-checks, and optional replay verification implemented
- canonical namespace schema revisions, cut-bound activation, whole-batch
  admission, durable append receipts, idempotent retries, history baseline
  certification/quarantine, and leader/follower receipt verification implemented
- immutable candidate evidence schemas, deterministic gate capture/bundling,
  fail-closed offline verification, negative tamper/drift coverage, a policy-only
  commercial ledger, and reusable exact-candidate workflow implemented locally
- bounded request bodies, documents, rules, runtime iterations, derived tuples,
  pages, rates, global workers, per-namespace queues, audit writes, and execution
  retention implemented with structured audited failures and explicit
  cancel-before-start/complete-after-start semantics
- responsibility crates for service core, HTTP, sidecars, partitions, pilot
  proof packs, and performance, with compatibility re-exports and a versioned
  executable-plan/runtime boundary
- whole-document DSL parser implemented for the current canonical v1 surface: schema, attribute classes, facts, repeated queries, explain directives, temporal views, and policy annotations
- `Current` and `AsOf` query execution implemented
- policy annotations wired through state, document, explanation, report, federation, and sidecar paths as pre-replay/pre-compilation semantic scope; immutable release qualification remains pending
- authenticated HTTP tokens now bind maximum semantic policy visibility, with request policy contexts only allowed to narrow that bound
- policy context is carried through explain, history, audit, and report surfaces; local projection-equivalence, hidden retract, negation, recursion, aggregation, metadata, sequence, and SQLite parity tests now pass, with Postgres/performance/exact-candidate evidence still pending
- strict v1 operation/class validation implemented across scalar, set, and sequence attributes, with anchored `InsertAfter` semantics and deterministic replay for `SequenceRGA`
- semi-naive delta execution implemented for recursive SCC evaluation
- executable stratified negation implemented for stratified programs
- bounded aggregation implemented for non-recursive grouped head-term `count`, `sum`, `min`, and `max` rules, including multiple aggregate terms per head; this now covers the v1 bounded-aggregation requirement
- first coordination acceptance slice implemented for readiness, claims, leases, lease heartbeats, execution outcomes, and stale-result rejection
- in-memory kernel service implemented in `aether_api`
- minimal HTTP JSON kernel service implemented over `aether_api`
- kernel service generalized over in-memory and durable journal backends
- coordination pilot contract frozen in restart-safe service and HTTP tests
- bearer-token authentication and endpoint scope enforcement implemented on the pilot HTTP path
- auditable request logging implemented on the pilot HTTP path, including semantic cut/query/tuple context and persisted JSONL output
- operator-grade coordination report artifacts implemented in markdown and JSON for the pilot workload
- release-mode performance report example, Criterion benchmarks, and ignored stress workloads added for early performance tracking
- live console dashboard added for real-time and collected instrument views over the performance suite
- machine-readable performance baseline capture and point-in-time drift reporting implemented for the pilot path
- host-aware benchmark catalog implemented with typed host snapshots, tracked host manifests, suite-specific accepted baselines, timestamped run bundles, and matrix summaries across the native dev host, WSL, and GitHub runner surfaces
- release and launch validation now resolve baselines by suite plus host id, with `core_kernel` and `service_in_process` remaining the accepted regression gates on the canonical Windows dev host while HTTP and replicated-partition suites remain measured but observational
- authenticated HTTP restart-cycle drills added to preserve semantic answers and persisted audit context across repeated service restarts
- ignored release-mode soak and misuse drills added for the authenticated pilot HTTP path
- a one-command pilot launch validation pack added to produce the current report, drift, soak, and stress evidence set
- artifact and vector sidecar federation implemented in `aether_api`, including journal-tail-anchored registration, journal-exact `AsOf` visibility, external artifact references, vector search, semantic fact projection with provenance, and SQLite-backed durability for the durable kernel service
- scheduled/manual GitHub Actions automation added for the pilot launch-validation and drift artifact pack
- launch validation and drift promotion completed into a required mainline CI gate
- packaged durable pilot-service bundles implemented with config-backed startup, package-local rotation tooling, restart/replay benchmark coverage, and secret-file/env/command token resolution
- service-status, auth-reload, and config-backed token/principal identity surfaces implemented for the hardened pilot boundary
- packaged backup and restore helpers implemented for the Windows pilot bundle,
  with fail-closed quiescence confirmation and endpoint checks, clean-target
  enforcement, a validated versioned snapshot contract, IPv4/IPv6 wildcard
  probes, snapshot/export of journal, WAL/SHM companions, sidecar catalog,
  execution metadata, audit log, config, and token files, and a restored-handle
  replay check in the hardening drill
- scheduled/manual extended-operability workflow added for soak, package build, and launch-validation evidence beyond the standard release gate
- first real Go operator shell implemented against the HTTP API with typed client coverage
- pilot-focused Go operator TUI implemented as the live cockpit for health, coordination state, audit entries, history, tuple proof traces, service status, and coordination diffs
- broader typed Python SDK surface implemented against the HTTP API with fixture builders and live integration coverage
- Colab notebook lane now boots the authenticated pilot boundary with notebook-local token, namespace, SQLite storage, service status, and audit context instead of the older unauthenticated in-memory HTTP example
- semantic compliance matrix added to map `SPEC.md` sections `1-11` to implementation and acceptance evidence, with policy-aware closure now explicitly reopened
- formal v1 closeout record retained as the historical acceptance record, with its policy-aware and release-claim portions reopened pending the remediation gates
- partition IDs, partition-qualified cuts, and federated-cut types implemented in the semantic model
- single-process partition-aware in-memory service implemented for exact per-partition append/history/state reads plus explicit federated-history reads
- imported-fact federation implemented over explicit partition cuts, including provenance-bearing extensional facts that carry source partition/cut context into derived tuples
- federated document execution, explain traces, and markdown report generation implemented on top of the partition-aware in-memory service
- SQLite-backed partition-aware service implemented for durable per-partition replay and restart-safe federated imported-fact / explain / report execution
- single-host leader/follower replicated authority partitions implemented as an operable manual-failover prototype with leader epochs, follower replay, restart-safe metadata reload, stale-epoch fencing, explicit leader/lag/degraded replica status, divergent-prefix rejection, HTTP status surfaces, and federated HTTP routes
- AETHER Service v2 groundwork implemented as a service-plane widening over the Rust kernel: `X-Aether-Namespace` request isolation, namespace-bound bearer tokens, namespace-bearing audit/status surfaces, legacy pilot config compatibility, tagged v2 SQLite/Postgres storage config, and a Postgres journal backend behind the existing `Journal` contract
- coordination delta reports implemented in JSON and markdown for cut-to-cut operator comparisons, including policy-related trace-metadata redaction instead of hard failure; these are not yet execution-scoped proof handles
- federated run/report HTTP surfaces implemented for the replicated prototype, with imported-cut provenance carried into explain and report artifacts
- lightweight exact-response reuse implemented for repeated replicated federated run/report requests, invalidated on append and promotion
- structured release-readiness QA suite implemented with a dedicated runner, Pages preview build, package build, and uploaded workflow artifacts for manual/tagged release preparation
- documentation portal, architecture guide, developer workflow guide, operator guide, glossary, and documentation standards now exist
- GitHub Pages publishing pipeline added for the documentation portal and generated Rust API reference
- unit tests added across the Rust core crates
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test` verified on Windows and WSL
- GitHub CI added for Ubuntu and Windows
- repository front-door docs, contribution guidance, and worked examples now exist
- a governed incident blackboard commercialization document, runnable demo, and front-door docs/site packaging now exist as the canonical adjacent-next design-partner exemplar over the current pilot proof
- an AI support resolution desk commercialization document, runnable demo, site page, and Colab notebook now exist as the flagship ML-facing working app pack over the current pilot proof
- an M6 operating-proof Colab notebook now exists for status, coordination reports, cut diffs, tuple proof traces, audit intent, and benchmark trend artifacts
- a post-v1 QA hardening program now exists with a governing rubric, responsible-disclosure policy, stronger issue templates, a repeatable persona sweep runner, and a scheduled/manual non-blocking hardening workflow
- the scheduled hardening workflow now also publishes promotion metrics, updates a standing GitHub tracker issue, and can open a promotion PR when the next pack satisfies the documented streak threshold
- release-readiness now emits an explicit hardening gate-state summary that separates blocking packs from diagnostic packs and reports the latest pass/fail/skipped evidence
- admin and operator hardening packs are now promoted into blocking CI/release-readiness gates after five downloaded scheduled QA Hardening metrics artifacts showed consecutive `passed` statuses, exceeding the configured three-run threshold
- a tracked commercial release readiness ledger separates design-partner alpha, commercial beta, and GA; its active target is now controlled design-partner alpha, and authored ledger state is not accepted as proof of beta readiness
- release-readiness now also emits a Service v2 operability proof artifact with a direct SQLite namespace restart/replay drill, Postgres availability status, CI container-smoke coverage, current-run package backup/restore-through-restart proof, and admin/operator promotion status
- release-readiness now emits a versioned rollback record that ties the packaged bundle, package-local backup/restore proof, restart/replay evidence, packaged upgrade/rollback playbook, and Postgres export/restore boundary into one release artifact
- release-readiness now emits a customer workflow acceptance artifact that runs the AI support resolution desk demo and checks the buyer-facing workflow markers
- release-readiness now emits a performance beta gate artifact that enforces
  explicit approved-Windows-host thresholds for gated same-host drift,
  restart/replay, in-process report latency, and pilot HTTP read paths
- release-readiness emits a security/key lifecycle artifact that verifies package token rotation, token-command/auth-reload tests, secret-manager documentation, and an honestly named package file/checksum manifest
- strict CycloneDX 1.5 Rust, Go, and assembled-package SBOM generation now
  covers lockfiles, package URLs, hashes, licenses, dependency graphs, and every
  packaged file; pinned cargo-audit, govulncheck, Trivy, Gitleaks, CodeQL,
  provenance/SBOM attestations, immutable Actions/images, and Dependabot are
  wired into the Supply Chain workflow, pending a successful hosted exact-SHA run
- Postgres now defaults to forced `verify_full` TLS, supports explicit
  `verify_ca`, CA bundles and optional mTLS identity, and permits plaintext only
  through an explicit literal-loopback development mode; the HTTP config rejects
  non-loopback plaintext unless it declares a trusted `https://` ingress boundary
- non-secret service status exposes transport mode, trust-root posture, CA count,
  and client-certificate presence without database URLs, certificate/key paths,
  or key material; the exact-digest local Postgres fixtures pass trusted CA,
  hostname mismatch, explicit `verify_ca`, untrusted/expired certificate, no-
  downgrade, mTLS, and two-CA rotation tests, while the hosted exact-SHA run is
  still pending
- namespace services now live behind independent per-namespace handles and all
  synchronous kernel/storage work uses a bounded blocking executor; saturation
  returns structured `503 namespace_busy` with `Retry-After`, same-namespace
  order remains deterministic, and initialization is single-handle
- replicated authority partitions now lock independently, while audit uses a
  bounded in-memory FIFO plus a bounded single-writer queue with visible
  backpressure instead of holding the in-memory audit lock across slow I/O
- the HTTP boundary now publishes capability-negotiated trace-handle, schema-ref,
  append-receipt, and structured-error contracts; typed Rust helpers plus Go,
  Python, TUI, CLI report commands, and notebook preflights prevent silent
  fallback, while audit telemetry makes legacy tuple endpoints and omitted
  schema refs visible
- Capacity Planning now downloads matrix artifacts into their declared root,
  asserts the complete input layout, and always uploads a hashed inventory;
  Pages embeds and verifies the exact deployed source SHA/version; performance
  drift retains all five predeclared samples and no longer retries red into green
- the existing beta-candidate runner remains available for diagnostics, but it does not qualify a commercial beta until R4 replaces authored/path-based readiness with exact-candidate immutable evidence
- the commercial readiness ledger now targets controlled design-partner alpha; commercial beta is blocked by six non-waivable remediation gates, and GA remains blocked by its separate release, support/security, distribution, and distributed-truth gates
- a repeatable perturbation sweep now exists to run the persona pass, full-stack benchmark snapshot, host-aware drift checks, deeper ignored stress workloads, and single-node capacity projections in one artifact pack
- a typed capacity-planning layer now exists over perturbation and matrix evidence, with measured board/closure/replay/concurrency ladders, concrete hardware-class guidance, explicit scale-out triggers, and a scheduled GitHub tracker workflow
- a lightweight performance trend index now exists over saved run bundles and tracked accepted baselines so latest/prior/baseline context is visible without manually opening every benchmark artifact

Still open:

- official exact-candidate qualification of the locally green policy-scoped semantics
- official exact-candidate qualification of durable, non-aliasing trace identity
- a successful official exact-candidate workflow bundle and independently
  downloaded verification; local tooling no longer trusts authored status,
  path existence, declared hosts, or numeric run IDs without GitHub outcomes
- hosted confirmation that the new dependency/package supply-chain workflow,
  CodeQL, secret scanning, attestations, and protected repository controls pass
  for the exact candidate SHA
- hosted confirmation that the verified-TLS Postgres matrix passes for the exact
  candidate and that the supported ingress prevents direct backend reachability
- a new protected candidate whose exact-SHA CI, Supply Chain, Pages, Capacity
  Planning, and Release Readiness runs all pass; the first selected candidate's
  readiness run failed the service restart-latency gate
- post-v1 DSL ergonomics and document modularity beyond the current canonical surface
- production hardening for the optional Postgres journal deployment path beyond current parity/concurrency coverage
- production-hardened kernel service integrations beyond the current minimal HTTP boundary
- mature Go/Python client ecosystems beyond the current first real boundary clients
- persistent benchmark dashboards and long-lived trend storage beyond the current run bundles, matrix summaries, trend index, perturbation artifacts, and uploaded workflow artifacts

## Immediate focus

The immediate work is to complete hosted validation of the remediated
first-restart latency documented in `docs/RESTART_LATENCY_INVESTIGATION.md`,
then resume R7 exact-candidate qualification from
`docs/REMEDIATION_PROGRAMME.md`. Feature broadening across policy, service
execution, append, proof identity, or release claims stays frozen until the
relevant repaired contract is green:

- keep the temporary controlled-alpha claim identical across status, roadmap, limitations, commercialization, and site source
- preserve the now-green local R1-R3 semantic, proof-identity, and append-admission contracts while qualifying them through immutable exact-candidate evidence
- run and independently verify the new official exact-candidate bundle; keep its
  beta verdict blocked until every R5 subject is present and verified
- replace self-authored readiness with immutable exact-candidate evidence before restoring any beta language
- keep admin and operator hardening gates blocking, and do not promote user or exec checks until their release-blocking criteria are explicitly accepted
- keep commercial beta and GA blocked until their separate programme gates are backed by current evidence
- continue hardening the single-host replicated authority-partition prototype with longer recovery drills, clearer operator presets, and measured promotion/follower-replay evidence without pretending it is a generalized cluster manager
- continue Service v2 hardening around namespace isolation, Postgres journal parity, container smoke, and design-partner deployment docs while keeping SQLite the package default and sidecars local/journal-subordinated
- add longer-duration scheduled operability evidence beyond the current release and launch gates
- decide how far to widen audit context from the current semantic cut/query/tuple/diff fields into fuller operator intent
- decide which post-v1 ergonomic DSL extensions matter beyond the now-implemented canonical surface
- continue runtime optimization now that the current bounded-aggregation requirement is covered and the replicated federated read path has its first cache
- decide how far to widen imported-fact federation beyond the current provenance-exact single-goal query shape
- let the new perturbation sweep accumulate repeated host evidence so scaling projections become a trend rather than a single run
- use the new capacity tracker to watch for meaningful headroom drift and keep single-node guidance current as the benchmark matrix evolves
- continue building compelling working applications on top of the pilot, starting with ML-relevant support operations and then widening only where the live proof remains honest
