# IMPLEMENTATION_DECISION.md

## 1. Questions

1. Should mainline proceed by forking Janus or by writing our own implementation?
2. Should mainline center on Go or Rust?
3. How should Codex agents interpret the repository center of gravity?

## 2. Decisions

### Decision A — Ownership model
Mainline proceeds as a **native AETHER implementation**.

Janus is a reference model, benchmark target, and possible source of selectively adapted ideas. It is not the main dependency backbone.

### Decision B — Mainline language
Mainline semantic kernel is implemented in **Rust**.

### Decision C — Secondary languages
- **Go** is used for operational shell, CLI, and service wrappers.
- **Python** is used for experimentation and SDK ergonomics.

### Decision D — Canonical semantics language
The AETHER DSL is the canonical rule/query/schema surface.

## 3. Why not a Janus fork as mainline?

Janus aligns strongly with the substrate layer:

- append-only datom style,
- temporal views,
- explicit phase contracts,
- typed host-language ergonomics.

But our mainline center of gravity is now:

- recursive closure,
- SCC-aware compilation,
- semi-naive execution,
- derivation provenance,
- sidecar-aware semantic control.

A hard fork risks retrofitting recursion into an architecture whose strongest existing shape is not organized around recursive compilation.

## 4. Why Rust for the mainline kernel?

Rust is the language of record for the recursive and incremental semantic core because it best matches the kernel requirements:

- strongly typed IRs and invariants,
- predictable memory/layout control,
- good fit for embedded rule engines and semi-naive runtimes,
- strong library-first architecture,
- clean path toward more advanced incremental/dataflow techniques later.

## 5. Why not Go as the semantic center?

Go remains valuable, but primarily for:

- operator tooling,
- service shells,
- deployment ergonomics,
- integration adapters.

It should not own the authoritative recursive semantics.

## 6. Why not a single-language monolith?

A single-language monolith would create the wrong tradeoffs.

- If centered on Go, the recursive core is not in the strongest ecosystem posture.
- If centered on Python, performance and authority boundaries degrade.
- If centered only on Rust without a DSL and service shell, operability and research ergonomics suffer.

## 7. Codex-specific policy

Codex agents must interpret these decisions as binding:

1. repository root is a Rust workspace,
2. Go and Python are subordinate layers,
3. DSL semantics must not be replaced by host-language-only APIs,
4. any Janus code reuse requires an ADR and a spike result.

## 8. Reconsideration triggers

Only reconsider these decisions if a disciplined spike shows all of the following:

- a Janus-derived path yields cleaner recursive semantics than the native path,
- provenance/explain remains simpler,
- operational complexity is materially lower,
- maintenance burden is lower even after recursive features are added.

Absent that evidence, the decision stands.
