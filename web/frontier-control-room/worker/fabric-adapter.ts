import type {
  FabricAuditEvent,
  FabricDatom,
  FabricEnvelope,
  ReleaseRun,
  SourceObservation,
} from "../lib/fabric-contract";

export interface FabricAdapterEnv {
  AETHER_SERVICE_URL?: string;
  AETHER_READ_TOKEN?: string;
  AETHER_GITHUB_REPOSITORY?: string;
  AETHER_GITHUB_TOKEN?: string;
  AETHER_RELEASE_EVIDENCE_URL?: string;
  AETHER_ADAPTER_TIMEOUT_MS?: string;
  AETHER_JOURNAL_TAIL?: string;
}

type JsonRecord = Record<string, unknown>;
type Fetcher = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

const RELEASE_WORKFLOWS = [
  ["ci", "CI", "ci.yml"],
  ["supply_chain", "Supply Chain", "supply-chain.yml"],
  ["pages", "Pages", "pages.yml"],
  ["capacity", "Capacity Planning", "capacity-planning.yml"],
  ["release_readiness", "Release Readiness", "release-readiness.yml"],
] as const;

function record(value: unknown): JsonRecord {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {};
}

function textValue(value: unknown, fallback = "unknown"): string {
  return typeof value === "string" && value.length > 0 ? value : fallback;
}

function numberValue(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

function boundedInteger(value: string | undefined, fallback: number, min: number, max: number): number {
  const candidate = Number.parseInt(value ?? "", 10);
  return Number.isFinite(candidate) ? Math.min(max, Math.max(min, candidate)) : fallback;
}

function safeServiceBase(raw: string | undefined): URL | null {
  if (!raw) return null;
  const url = new URL(raw);
  const local = url.hostname === "localhost" || url.hostname === "127.0.0.1" || url.hostname === "[::1]";
  if (url.protocol !== "https:" && !(url.protocol === "http:" && local)) {
    throw new Error("service URL must use HTTPS (loopback HTTP is allowed for local qualification)");
  }
  url.username = "";
  url.password = "";
  url.search = "";
  url.hash = "";
  url.pathname = `${url.pathname.replace(/\/+$/, "")}/`;
  return url;
}

async function fetchJson(fetcher: Fetcher, url: URL, init: RequestInit, timeoutMs: number): Promise<unknown> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    // Workers supports manual redirect handling, not redirect="error". A 3xx
    // response remains non-ok below, so source-controlled URLs still fail closed.
    const response = await fetcher(url, { ...init, redirect: "manual", signal: controller.signal });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

function displayValue(value: unknown): string {
  if (typeof value === "string") return value.slice(0, 160);
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  const tagged = record(value);
  const entries = Object.entries(tagged);
  if (entries.length === 1) return `${entries[0][0]}:${String(entries[0][1]).slice(0, 140)}`;
  return JSON.stringify(value).slice(0, 160);
}

function normalizeDatom(value: unknown): FabricDatom | null {
  const datom = record(value);
  const element = numberValue(datom.element);
  if (element === null) return null;
  const causal = record(datom.causal_context);
  const provenance = record(datom.provenance);
  const sourceRef = record(provenance.source_ref);
  return {
    element,
    entity: displayValue(datom.entity),
    attribute: displayValue(datom.attribute),
    value: displayValue(datom.value),
    op: textValue(datom.op),
    replica: numberValue(datom.replica),
    causal_frontier: (Array.isArray(causal.frontier) ? causal.frontier : []).filter((item): item is number => typeof item === "number"),
    provenance: {
      agent_id: typeof provenance.agent_id === "string" ? provenance.agent_id : null,
      tool_id: typeof provenance.tool_id === "string" ? provenance.tool_id : null,
      trust_domain: typeof provenance.trust_domain === "string" ? provenance.trust_domain : null,
      confidence: numberValue(provenance.confidence),
      source_digest: typeof sourceRef.digest === "string" ? sourceRef.digest : null,
    },
  };
}

function normalizeAudit(value: unknown): FabricAuditEvent | null {
  const entry = record(value);
  const timestamp = numberValue(entry.timestamp_ms);
  const status = numberValue(entry.status);
  if (timestamp === null || status === null) return null;
  const context = record(entry.context);
  return {
    timestamp_ms: timestamp,
    method: textValue(entry.method),
    path: textValue(entry.path).split("?")[0].slice(0, 100),
    status,
    scope: textValue(entry.scope),
    outcome: textValue(entry.outcome),
    temporal_view: typeof context.temporal_view === "string" ? context.temporal_view : null,
    selected_cut: typeof context.selected_cut === "string" ? context.selected_cut : null,
    query_goal: typeof context.query_goal === "string" ? context.query_goal.slice(0, 160) : null,
    last_element: numberValue(context.last_element),
    policy_decision: typeof context.policy_decision === "string" ? context.policy_decision : null,
  };
}

function normalizeService(value: unknown): NonNullable<FabricEnvelope["service"]> {
  const status = record(value);
  const storage = record(status.storage);
  const controls = record(status.resource_controls);
  return {
    status: textValue(status.status),
    build_version: textValue(status.build_version),
    config_version: textValue(status.config_version),
    schema_version: textValue(status.schema_version),
    service_mode: textValue(status.service_mode),
    storage_backend: textValue(storage.backend),
    sidecar_mode: textValue(storage.sidecar_mode),
    active_namespace_count: numberValue(status.active_namespace_count) ?? 0,
    capabilities: stringArray(status.capabilities).slice(0, 64),
    replicas: (Array.isArray(status.replicas) ? status.replicas : []).slice(0, 64).map((item) => {
      const replica = record(item);
      return {
        partition: textValue(replica.partition),
        replica_id: numberValue(replica.replica_id) ?? 0,
        role: textValue(replica.role),
        leader_epoch: numberValue(replica.leader_epoch) ?? 0,
        applied_element: numberValue(replica.applied_element),
        replication_lag: numberValue(replica.replication_lag) ?? 0,
        healthy: replica.healthy === true,
      };
    }),
    resource_controls: {
      operation_timeout_ms: numberValue(controls.operation_timeout_ms),
      max_page_size: numberValue(controls.max_page_size),
      global_worker_limit: numberValue(controls.global_worker_limit),
      per_namespace_queue_limit: numberValue(controls.per_namespace_queue_limit),
    },
  };
}

async function readService(env: FabricAdapterEnv, fetcher: Fetcher, observedAt: string, timeoutMs: number, tail: number) {
  const started = Date.now();
  let base: URL | null;
  try {
    base = safeServiceBase(env.AETHER_SERVICE_URL);
  } catch (error) {
    const detail = error instanceof Error ? error.message : "invalid service configuration";
    return {
      source: { id: "service", kind: "aether-http", state: "degraded", observed_at: observedAt, latency_ms: 0, detail } as SourceObservation,
      journalSource: { id: "journal", kind: "aether-journal", state: "degraded", observed_at: observedAt, latency_ms: 0, detail: "journal disabled by invalid service configuration" } as SourceObservation,
      service: null, total: 0, datoms: [] as FabricDatom[], audit: [] as FabricAuditEvent[], warning: `service adapter: ${detail}`,
    };
  }
  if (!base) {
    const source: SourceObservation = { id: "service", kind: "aether-http", state: "unconfigured", observed_at: observedAt, latency_ms: null, detail: "AETHER_SERVICE_URL is not configured" };
    const journalSource: SourceObservation = { id: "journal", kind: "aether-journal", state: "unconfigured", observed_at: observedAt, latency_ms: null, detail: "service source is not configured" };
    return { source, journalSource, service: null, total: 0, datoms: [] as FabricDatom[], audit: [] as FabricAuditEvent[], warning: null as string | null };
  }

  const headers = new Headers({ accept: "application/json" });
  if (env.AETHER_READ_TOKEN) headers.set("authorization", `Bearer ${env.AETHER_READ_TOKEN}`);
  const get = (path: string) => fetchJson(fetcher, new URL(path, base), { headers, cache: "no-store" }, timeoutMs);
  try {
    const [statusRaw, firstRaw, auditRaw] = await Promise.all([
      get("v1/status"),
      get("v1/history/page?offset=0&limit=1"),
      get("v1/audit"),
    ]);
    const first = record(firstRaw);
    const total = numberValue(record(first.page).total) ?? 0;
    const offset = Math.max(0, total - tail);
    const pageRaw = offset === 0 && total <= 1 ? firstRaw : await get(`v1/history/page?offset=${offset}&limit=${tail}`);
    const datoms = (Array.isArray(record(pageRaw).datoms) ? record(pageRaw).datoms as unknown[] : []).map(normalizeDatom).filter((item): item is FabricDatom => item !== null);
    const auditEntries = (Array.isArray(record(auditRaw).entries) ? record(auditRaw).entries as unknown[] : []).slice(-tail).map(normalizeAudit).filter((item): item is FabricAuditEvent => item !== null);
    const latency = Date.now() - started;
    return {
      source: { id: "service", kind: "aether-http", state: "current", observed_at: observedAt, latency_ms: latency, detail: "status projection current" } as SourceObservation,
      journalSource: { id: "journal", kind: "aether-journal", state: "current", observed_at: observedAt, latency_ms: latency, detail: `${datoms.length} redacted tail datoms; ${auditEntries.length} audit events` } as SourceObservation,
      service: normalizeService(statusRaw), total, datoms, audit: auditEntries, warning: null,
    };
  } catch (error) {
    const detail = error instanceof Error ? error.message : "unknown service error";
    return {
      source: { id: "service", kind: "aether-http", state: "degraded", observed_at: observedAt, latency_ms: Date.now() - started, detail } as SourceObservation,
      journalSource: { id: "journal", kind: "aether-journal", state: "degraded", observed_at: observedAt, latency_ms: Date.now() - started, detail: "journal unavailable because the service read failed" } as SourceObservation,
      service: null, total: 0, datoms: [] as FabricDatom[], audit: [] as FabricAuditEvent[], warning: `service adapter: ${detail}`,
    };
  }
}

function normalizeRun(lane: ReleaseRun["lane"], fallbackName: string, value: unknown): ReleaseRun | null {
  const run = record(value);
  const id = numberValue(run.id);
  if (id === null) return null;
  return {
    lane,
    name: textValue(run.name, fallbackName),
    run_id: id,
    run_attempt: numberValue(run.run_attempt) ?? 1,
    status: textValue(run.status),
    conclusion: typeof run.conclusion === "string" ? run.conclusion : null,
    tooling_sha: textValue(run.head_sha),
    event: textValue(run.event),
    updated_at: textValue(run.updated_at),
    url: textValue(run.html_url),
  };
}

async function readReleaseUncached(env: FabricAdapterEnv, fetcher: Fetcher, observedAt: string, timeoutMs: number) {
  const started = Date.now();
  const repository = env.AETHER_GITHUB_REPOSITORY === undefined ? "fyremael/AETHER" : env.AETHER_GITHUB_REPOSITORY.trim();
  if (!repository) {
    const source: SourceObservation = { id: "release", kind: "github-actions", state: "unconfigured", observed_at: observedAt, latency_ms: null, detail: "release evidence source is disabled" };
    return { source, repository: null, gates: [] as ReleaseRun[], productSha: null as string | null, toolingSha: null as string | null, binding: "unverified" as const, authorized: false, warning: null as string | null };
  }
  const headers = new Headers({ accept: "application/vnd.github+json", "user-agent": "aether-orbital-read-adapter", "x-github-api-version": "2022-11-28" });
  if (env.AETHER_GITHUB_TOKEN) headers.set("authorization", `Bearer ${env.AETHER_GITHUB_TOKEN}`);
  try {
    if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repository)) throw new Error("invalid AETHER_GITHUB_REPOSITORY");
    const url = new URL(`https://api.github.com/repos/${repository}/actions/runs?per_page=100`);
    const payload = record(await fetchJson(fetcher, url, { headers, cache: "no-store" }, timeoutMs));
    const runs = Array.isArray(payload.workflow_runs) ? payload.workflow_runs : [];
    const gates = RELEASE_WORKFLOWS.map(([lane, name, file]) => {
      const first = runs.find((value) => textValue(record(value).path, "").endsWith(`/${file}`));
      return first ? normalizeRun(lane, name, first) : null;
    });
    const present = gates.filter((item): item is ReleaseRun => item !== null);
    const readiness = present.find((item) => item.lane === "release_readiness") ?? null;
    let productSha: string | null = null;
    let toolingSha = readiness?.tooling_sha ?? present[0]?.tooling_sha ?? null;
    let binding: "verified" | "unverified" = "unverified";
    let authorized = false;

    if (env.AETHER_RELEASE_EVIDENCE_URL) {
      const evidenceUrl = new URL(env.AETHER_RELEASE_EVIDENCE_URL);
      if (evidenceUrl.protocol !== "https:") throw new Error("release evidence URL must use HTTPS");
      const evidence = record(await fetchJson(fetcher, evidenceUrl, { headers: new Headers({ accept: "application/json" }), cache: "no-store" }, timeoutMs));
      if (evidence.contract_version !== "aether.release-evidence.v1") throw new Error("unsupported release evidence contract");
      productSha = textValue(evidence.product_sha, "") || null;
      toolingSha = textValue(evidence.tooling_sha, "") || toolingSha;
      binding = productSha && toolingSha && evidence.identity_binding === "verified" ? "verified" : "unverified";
      authorized = binding === "verified" && evidence.promotion_authorized === true;
    }

    return {
      source: { id: "release", kind: "github-actions", state: "current", observed_at: observedAt, latency_ms: Date.now() - started, detail: `${present.length} workflow lanes observed; identity binding ${binding}` } as SourceObservation,
      repository, gates: present, productSha, toolingSha, binding, authorized, warning: binding === "verified" ? null : "workflow status is live, but product/tooling identity binding is unverified",
    };
  } catch (error) {
    const detail = error instanceof Error ? error.message : "unknown release evidence error";
    return {
      source: { id: "release", kind: "github-actions", state: "degraded", observed_at: observedAt, latency_ms: Date.now() - started, detail } as SourceObservation,
      repository, gates: [] as ReleaseRun[], productSha: null, toolingSha: null, binding: "unverified" as const, authorized: false, warning: `release adapter: ${detail}`,
    };
  }
}

type ReleaseResult = Awaited<ReturnType<typeof readReleaseUncached>>;
let releaseCache: { key: string; expiresAt: number; value: ReleaseResult } | null = null;
let releasePending: Promise<ReleaseResult> | null = null;

async function readRelease(env: FabricAdapterEnv, fetcher: Fetcher, observedAt: string, timeoutMs: number): Promise<ReleaseResult> {
  const key = `${env.AETHER_GITHUB_REPOSITORY ?? "fyremael/AETHER"}|${env.AETHER_RELEASE_EVIDENCE_URL ?? ""}`;
  if (releaseCache?.key === key && releaseCache.expiresAt > Date.now()) {
    return {
      ...releaseCache.value,
      source: {
        ...releaseCache.value.source,
        observed_at: observedAt,
        latency_ms: 0,
        detail: `${releaseCache.value.source.detail}; cached <=30s`,
      },
    };
  }
  if (!releasePending) {
    releasePending = readReleaseUncached(env, fetcher, observedAt, timeoutMs).finally(() => { releasePending = null; });
  }
  const value = await releasePending;
  releaseCache = { key, expiresAt: Date.now() + 30_000, value };
  return value;
}

export async function readFabricEnvelope(env: FabricAdapterEnv, fetcher: Fetcher = fetch): Promise<FabricEnvelope> {
  const generatedAt = new Date().toISOString();
  const timeoutMs = boundedInteger(env.AETHER_ADAPTER_TIMEOUT_MS, 3500, 250, 15000);
  const tail = boundedInteger(env.AETHER_JOURNAL_TAIL, 48, 1, 100);
  const [service, release] = await Promise.all([
    readService(env, fetcher, generatedAt, timeoutMs, tail),
    readRelease(env, fetcher, generatedAt, timeoutMs),
  ]);
  const current = [service.source, service.journalSource, release.source].filter((source) => source.state === "current").length;
  const mode = current === 3 ? "live" : current > 0 ? "hybrid" : "disconnected";
  const latestElement = service.datoms.reduce<number | null>((latest, datom) => latest === null || datom.element > latest ? datom.element : latest, null);
  return {
    contract_version: "aether.orbital.fabric.v1",
    generated_at: generatedAt,
    mode,
    read_only: true,
    sources: [service.source, service.journalSource, release.source],
    service: service.service,
    journal: { total: service.total, latest_element: latestElement, datoms: service.datoms, audit_events: service.audit },
    release: {
      repository: release.repository,
      product_sha: release.productSha,
      tooling_sha: release.toolingSha,
      identity_binding: release.binding,
      promotion_authorized: release.authorized,
      gates: release.gates,
    },
    warnings: [service.warning, release.warning].filter((item): item is string => item !== null),
  };
}
