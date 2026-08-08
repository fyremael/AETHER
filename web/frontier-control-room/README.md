# AETHER Orbital

A full-screen temporal operations console for AETHER's frontier engineering
fabric. The console now has a read-only observation adapter as well as its
deterministic Demo 07 fallback.

The console is a deterministic live simulation of the Demo 07 semantic
contract. It visualizes:

- semantic topology and authority flow
- an advancing causal journal
- exact `AsOf` replay and return-to-Current controls
- recursive work readiness
- product and tooling identity separation
- evidence freshness and runner-epoch fencing
- competing candidate decisions
- explainable promotion provenance

The `/api/fabric` adapter can observe three source classes without exposing a
write path:

- AETHER `GET /v1/status`, `GET /v1/history/page`, and `GET /v1/audit`
- the latest GitHub Actions runs for CI, Supply Chain, Pages, Capacity Planning,
  and Release Readiness
- an optional immutable release-evidence record that binds product and tooling
  identities

GitHub workflow success alone is displayed as an observation, never as
promotion authorization. Product/tooling binding remains `unverified` unless
an `aether.release-evidence.v1` record explicitly verifies it. External agents,
runners, and the console remain outside the authoritative kernel.

## Local development

```bash
npm install
npm run dev
```

### Adapter configuration

All configuration is server-side. None of these values are serialized into the
client bundle.

| Variable | Purpose | Default |
| --- | --- | --- |
| `AETHER_SERVICE_URL` | Fixed AETHER service origin; HTTPS is required except for loopback qualification | unconfigured |
| `AETHER_READ_TOKEN` | Bearer credential with the minimum status/history/audit read scopes | unconfigured |
| `AETHER_GITHUB_REPOSITORY` | Repository whose workflow observations are read; an empty value disables the source | `fyremael/AETHER` |
| `AETHER_GITHUB_TOKEN` | Optional read-only GitHub token for a higher API rate limit | unauthenticated public reads |
| `AETHER_RELEASE_EVIDENCE_URL` | Optional HTTPS URL for an immutable `aether.release-evidence.v1` identity-binding record | unconfigured |
| `AETHER_ADAPTER_TIMEOUT_MS` | Per-request fail-closed timeout, bounded to 250-15000 ms | `3500` |
| `AETHER_JOURNAL_TAIL` | Maximum projected datoms and audit entries, bounded to 1-100 | `48` |

The adapter never returns bearer tokens, principals, token IDs, bind addresses,
database or audit paths, source URIs, or audit details. It keeps only the source
digest from provenance. Responses are `no-store`; GitHub observations are
opportunistically cached for at most 30 seconds inside a Worker isolate to avoid
turning an operations display into CI/API pressure.

Source state is explicit:

- `live`: service status, journal, and release observations are all current
- `hybrid`: at least one real source is current and another is unavailable
- `disconnected`: no real source is current; the UI uses its labeled demo fallback

Click the source chip to switch between observed data and the deterministic
demo when a real source is available. There are no write-capable controls.

## Qualification

```bash
npm run build
node --test tests/rendered-html.test.mjs
npm run lint
```

The rendered regression suite exercises disconnected mode and a complete
mocked live path, including negative assertions against secret and filesystem
location leakage.
