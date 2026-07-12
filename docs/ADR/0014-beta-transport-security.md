# ADR 0014: Beta Transport Security

- Status: Accepted
- Date: 2026-07-12
- Programme gate: `service.beta_boundary`

## Context

The optional Postgres journal connected with `NoTls`, while the HTTP service
could bind plaintext to a non-loopback address. Authentication and semantic
policy cannot protect credentials, requests, or journal traffic from a network
attacker when the transport boundary itself is unverified. An implicit TLS
preference is also insufficient because it can downgrade to plaintext.

## Decision

The production Postgres default is `verify_full`. AETHER overrides URL
`sslmode` to require TLS, validates the certificate chain and validity period,
and verifies the configured hostname. Operators may explicitly select
`verify_ca`, which retains chain and validity verification but omits hostname
matching. It is never selected automatically.

The TLS configuration accepts zero or more CA certificate paths, an option to
disable platform roots, and an optional PEM client certificate plus PKCS #8
PEM private key. Certificate and key must be configured together. Multiple CA
paths are intentional: they support a bounded two-CA rotation window. Runtime
status reports only the mode, CA count, whether a client certificate exists,
and whether platform roots are enabled. It never returns the database URL,
certificate paths, private-key path, or key bytes.

Plaintext Postgres is named `development_plaintext`. It overrides URL
`sslmode` to disable TLS, accepts only literal loopback hosts, `localhost`, or a
Unix socket, and rejects CA/client/root-store options. A failed TLS connection
never retries in plaintext.

The Rust HTTP listener remains plaintext in this phase. Its default
`loopback_plaintext` mode rejects non-loopback binds. Remote HTTP is supported
only when the config explicitly names `trusted_tls_ingress`, declares an
`https://` external origin, and identifies the ingress boundary. The ingress
must terminate verified TLS and prevent direct access to the backend listener;
rate limiting at that boundary is added under R5.6. Native HTTP TLS remains a
future alternative, not an implied capability.

## Certificate rotation

Postgres CA rotation is a two-CA transition:

1. Add the new CA path beside the old CA and restart/reload AETHER clients.
2. Prove both the old and new TLS endpoints connect with `verify_full`.
3. Rotate the server certificate and, when used, client identity.
4. Confirm hostname, expiry, trust, and mTLS tests against the new endpoint.
5. Remove the old CA only after all supported clients have crossed the cut.

Ingress rotation follows the provider's overlapping-certificate procedure.
The public origin must stay `https://`; direct backend reachability is a failed
deployment, not a temporary downgrade path.

## Consequences

- Existing remote plaintext Postgres and direct non-loopback HTTP
  configurations fail closed and require explicit migration.
- SQLite/package localhost operation remains available without certificates.
- `verify_ca` is supported for constrained deployments but is visibly weaker
  than the default and cannot silently become `verify_full` evidence.
- Hosted CI owns the real TLS Postgres matrix: trusted CA, hostname mismatch,
  untrusted and expired certificates, mTLS, plaintext rejection, and two-CA
  rotation.

## Verification

- `aether_storage::tests::postgres_tls` covers mode validation, explicit
  downgrade rejection, client-pair completeness, rotation configuration, and
  key-path redaction.
- `crates/aether_storage/tests/postgres_tls.rs` is the environment-gated live
  handshake matrix.
- `scripts/ci-postgres-tls.sh` launches exact-digest Postgres fixtures for the
  CI `Postgres verified TLS journal` job.
- deployment tests cover loopback enforcement, trusted HTTPS ingress
  declaration, and non-secret status serialization.
