# API And Client Migration Contract

Status: active R5.4 transition contract

The HTTP boundary publishes its contract through `GET /v1/status`. First-party
clients must read `capabilities` and fail closed before using a contract they do
not understand. Capability absence never authorizes a semantic fallback.

## Required capabilities

| Flag | Contract |
| --- | --- |
| `capability_negotiation_v1` | The status capability set is authoritative for boundary negotiation. |
| `trace_handles_v1` | Explanations resolve opaque, execution-scoped trace handles. Clients must not retry with tuple IDs. |
| `namespace_schema_ref_v1` | Schema catalog and append requests use namespace-scoped content-addressed schema references. |
| `append_receipts_v1` | Successful admitted appends return durable receipts and receipt lookup is available. |
| `structured_errors_v1` | Failures include `error`, `code`, `request_id`, and `details`. |
| `resource_limits_v1` | The service publishes and enforces fail-closed resource bounds. |
| `pagination_v1` | History, document query rows, and trace tuples have bounded page endpoints. |

Rust exposes typed status/error negotiation helpers and service demos print the
capability set. The Go client and TUI, Python SDK, CLI report commands, and
notebooks consume it. Human-readable error text remains for one transition, but
automation must branch on `code`. The
`X-Aether-Request-Id` response header equals the structured error body
`request_id` and is also present on successful responses.

The Python and Go clients expose bounded history, document-run, and trace
resolution page helpers. They require both new flags during preflight. Resource
errors branch on `code`; clients must not retry a limit failure with an
unbounded legacy endpoint.

## Schema transition

New integrations should obtain the active reference from `GET /v1/schema` and
send it in `schema_ref` for dry-run and commit. Go exposes `ActiveSchemaRef`; the
Python SDK exposes `active_schema_ref`. No client automatically retries a schema
mismatch or substitutes a different schema.

Omitted `schema_ref` remains accepted only for the compatibility transition.
Every append and dry-run records `context.schema_ref_omitted=true` in audit
telemetry when it uses that path. Removal requires two independently verified
exact-candidate qualification runs with zero first-party omissions and an
explicit ADR updating this contract.

## Explanation transition

`POST /v1/explanations/resolve` with a trace handle is the supported explanation
contract. `POST /v1/explain/tuple` is legacy and every call records
`context.legacy_endpoint=true`. First-party clients contain no tuple-ID fallback.
Removal requires two independently verified exact-candidate qualification runs
with zero legacy calls and an explicit ADR. An unknown, expired, or unauthorized
handle remains a typed failure; it is not permission to search for a tuple.

## Structured failure shape

```json
{
  "error": "schema reference does not match the active namespace schema",
  "code": "schema_mismatch",
  "request_id": "7a2f9a9e89f14c78a731a6c16a37f830",
  "details": {
    "expected_schema_ref": {},
    "provided_schema_ref": {}
  }
}
```

`details` is always a JSON object, including when empty. Request IDs are
correlation identifiers, not authority or proof identities. They must not be
used in place of execution IDs, trace handles, append receipts, or schema refs.

## Operator verification

1. Read `/v1/status` and confirm all required flags.
2. Inspect `/v1/audit` for `legacy_endpoint` and `schema_ref_omitted`.
3. Treat any first-party occurrence as migration debt for that candidate.
4. Confirm client errors retain code, request ID, details, and human text.
5. Confirm explanation UI and report paths expose trace handles only.
