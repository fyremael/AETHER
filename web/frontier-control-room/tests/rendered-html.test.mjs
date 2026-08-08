import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";

async function render() {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(
    new Request("http://localhost/", { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
}

async function loadWorker() {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("api-test", `${process.pid}-${Date.now()}-${Math.random()}`);
  return (await import(workerUrl.href)).default;
}

test("server-renders the AETHER control-room shell", async () => {
  const response = await render();
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^text\/html\b/i);
  const html = await response.text();
  assert.match(html, /AETHER \/ ORBITAL/);
  assert.match(html, /Frontier Engineering Fabric/);
  assert.match(html, /Operational truth, in motion\./);
  assert.match(html, /TEMPORAL CONTROL/);
  assert.match(html, /CAUSAL JOURNAL/);
  assert.match(html, /CONTROLLED ALPHA/);
  assert.doesNotMatch(html, /codex-preview|Your site is taking shape|react-loading-skeleton/i);
});

test("removes the disposable starter and keeps the claim boundary", async () => {
  const [page, layout, packageJson] = await Promise.all([
    readFile(new URL("../app/page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/layout.tsx", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);
  assert.match(page, /Deterministic control-room simulation over Demo 07 semantics/);
  assert.match(page, /External agents and CI remain outside the authoritative kernel/);
  assert.match(layout, /AETHER Orbital/);
  assert.doesNotMatch(packageJson, /react-loading-skeleton/);
  await assert.rejects(access(new URL("../app/_sites-preview", import.meta.url)));
});

test("returns an explicit disconnected read-only envelope when all sources are disabled", async () => {
  const worker = await loadWorker();
  const response = await worker.fetch(
    new Request("http://localhost/api/fabric"),
    { AETHER_GITHUB_REPOSITORY: "" },
    { waitUntil() {}, passThroughOnException() {} },
  );
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("cache-control"), "no-store, max-age=0");
  const payload = await response.json();
  assert.equal(payload.contract_version, "aether.orbital.fabric.v1");
  assert.equal(payload.read_only, true);
  assert.equal(payload.mode, "disconnected");
  assert.deepEqual(payload.sources.map((source) => source.state), ["unconfigured", "unconfigured", "unconfigured"]);
  assert.equal(payload.release.promotion_authorized, false);
});

test("normalizes live sources without leaking credentials, principals, or filesystem locations", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input) => {
    const url = new URL(typeof input === "string" ? input : input.url ?? input.toString());
    if (url.hostname === "service.example.test" && url.pathname === "/v1/status") {
      return Response.json({
        status: "ok", build_version: "1.2.3", config_version: "pilot-v1", schema_version: "v1",
        service_mode: "single_node", active_namespace_count: 1,
        storage: { backend: "sqlite", database_path: "C:/secret/aether.db", sidecar_mode: "sqlite_local", audit_log_path: "C:/secret/audit.log" },
        capabilities: ["pagination_v1", "resource_limits_v1"],
        principals: [{ principal: "secret-operator", token_id: "secret-token" }],
        replicas: [{ partition: "root", replica_id: 7, role: "leader", leader_epoch: 3, applied_element: 42, replication_lag: 0, healthy: true }],
        resource_controls: { operation_timeout_ms: 30000, max_page_size: 500, global_worker_limit: 8, per_namespace_queue_limit: 64 },
      });
    }
    if (url.hostname === "service.example.test" && url.pathname === "/v1/history/page") {
      const full = url.searchParams.get("limit") !== "1";
      return Response.json({
        page: { offset: 0, limit: full ? 48 : 1, total: 2, next_offset: null },
        datoms: full ? [{
          entity: 1, attribute: 2, value: { String: "ready" }, op: "Assert", element: 42, replica: 7,
          causal_context: { frontier: [41] },
          provenance: { agent_id: "agent-1", tool_id: "tool-1", trust_domain: "pilot", confidence: 1, source_ref: { uri: "file://C:/secret/input", digest: "sha256:abc" } },
        }] : [],
      });
    }
    if (url.hostname === "service.example.test" && url.pathname === "/v1/audit") {
      return Response.json({ entries: [{
        timestamp_ms: 1000, principal: "secret-operator", token_id: "secret-token", method: "GET",
        path: "/v1/history/page?token=secret-token", status: 200, scope: "read", outcome: "allowed",
        context: { temporal_view: "current", selected_cut: "Current", last_element: 42, policy_decision: "allow" },
      }] });
    }
    if (url.hostname === "api.github.com") {
      const workflows = ["ci.yml", "supply-chain.yml", "pages.yml", "capacity-planning.yml", "release-readiness.yml"];
      return Response.json({ workflow_runs: workflows.map((workflow, index) => ({
        id: 9001 + index, run_attempt: 1, name: workflow, path: `.github/workflows/${workflow}`,
        status: "completed", conclusion: "success", head_sha: "0123456789abcdef0123456789abcdef01234567",
        event: "workflow_dispatch", updated_at: "2026-07-31T12:00:00Z",
        html_url: `https://github.com/fyremael/AETHER/actions/runs/${9001 + index}`,
      })) });
    }
    return new Response("not found", { status: 404 });
  };

  try {
    const worker = await loadWorker();
    const response = await worker.fetch(
      new Request("http://localhost/api/fabric"),
      { AETHER_SERVICE_URL: "https://service.example.test", AETHER_READ_TOKEN: "secret-token", AETHER_GITHUB_REPOSITORY: "fyremael/AETHER" },
      { waitUntil() {}, passThroughOnException() {} },
    );
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.mode, "live", JSON.stringify(payload));
    assert.equal(payload.service.status, "ok");
    assert.equal(payload.journal.latest_element, 42);
    assert.equal(payload.release.gates.length, 5);
    assert.equal(payload.release.identity_binding, "unverified");
    assert.equal(payload.release.promotion_authorized, false);
    const serialized = JSON.stringify(payload);
    assert.doesNotMatch(serialized, /secret-token|secret-operator|C:\/secret|file:\/\//);
    assert.match(serialized, /sha256:abc/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
