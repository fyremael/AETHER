"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { FabricEnvelope, ReleaseRun } from "@/lib/fabric-contract";

type FabricEvent = {
  cut: number;
  time: string;
  kind: "fabric" | "evidence" | "authority" | "decision" | "learning";
  title: string;
  detail: string;
};

type Candidate = {
  id: string;
  label: string;
  owner: string;
  product: string;
  tooling: string;
  status: "forming" | "fenced" | "rejected" | "promoted";
  reason: string;
};

const MIN_CUT = 181;
const MAX_CUT = 202;

const EVENTS: FabricEvent[] = [
  { cut: 181, time: "14:04:02.118", kind: "fabric", title: "Campaign opened", detail: "24 work packages admitted to the engineering fabric." },
  { cut: 182, time: "14:04:02.441", kind: "fabric", title: "Dependency closure", detail: "Recursive planner resolved 31 dependency edges." },
  { cut: 183, time: "14:04:03.090", kind: "authority", title: "Runner authority", detail: "windows-runner-07 accepted at epoch 1." },
  { cut: 184, time: "14:04:04.277", kind: "decision", title: "Candidate A", detail: "Release-control candidate entered qualification." },
  { cut: 185, time: "14:04:05.006", kind: "evidence", title: "CI receipt", detail: "Exact prerequisite passed for candidate A." },
  { cut: 186, time: "14:04:05.884", kind: "evidence", title: "Supply chain", detail: "Package receipt anchored to candidate A." },
  { cut: 187, time: "14:04:06.219", kind: "evidence", title: "Pages receipt", detail: "Presentation surface passed at product identity." },
  { cut: 188, time: "14:04:07.610", kind: "evidence", title: "Capacity receipt", detail: "Four of four epoch-1 gates now successful." },
  { cut: 189, time: "14:04:08.004", kind: "decision", title: "Candidate B", detail: "Product-changing alternative submitted for review." },
  { cut: 190, time: "14:04:08.792", kind: "fabric", title: "Identity divergence", detail: "Candidate B product SHA differs from selected product." },
  { cut: 191, time: "14:04:09.334", kind: "decision", title: "Policy rejection", detail: "Product drift candidate excluded from evidence reuse." },
  { cut: 192, time: "14:04:10.116", kind: "fabric", title: "Runner latency", detail: "Alternate-runner readiness deadline exceeded." },
  { cut: 193, time: "14:04:10.922", kind: "fabric", title: "Downstream hold", detail: "Six work packages recursively blocked." },
  { cut: 194, time: "14:04:11.370", kind: "decision", title: "Repair selected", detail: "Release-control-only repair enters implementation." },
  { cut: 195, time: "14:04:12.005", kind: "fabric", title: "Package check", detail: "Canonical package bytes re-hashed before repair." },
  { cut: 196, time: "14:04:13.126", kind: "authority", title: "Authority degraded", detail: "Epoch-1 runner no longer satisfies live deadline." },
  { cut: 197, time: "14:04:14.088", kind: "fabric", title: "Replay window", detail: "Prior decision surface retained for exact AsOf replay." },
  { cut: 198, time: "14:04:14.841", kind: "decision", title: "Candidate C", detail: "Shared naming-contract repair submitted." },
  { cut: 199, time: "14:04:15.390", kind: "fabric", title: "Qualification cut", detail: "No candidate promotable at saved cut e199." },
  { cut: 200, time: "14:04:16.207", kind: "fabric", title: "Repair committed", detail: "Alternate-runner lifecycle repair closes the work root." },
  { cut: 201, time: "14:04:17.004", kind: "authority", title: "Epoch advanced", detail: "windows-runner-09 becomes authoritative at epoch 2." },
  { cut: 202, time: "14:04:17.661", kind: "decision", title: "Promotion derived", detail: "Candidate C satisfies identity, bytes, authority, and 12 receipts." },
];

const GATE_NAMES = ["CI", "SUPPLY CHAIN", "PAGES", "CAPACITY"];

function shortHash(value: string) {
  return `${value.slice(0, 7)}…${value.slice(-5)}`;
}

function snapshotAt(cut: number) {
  const gateReceipts =
    cut < 185 ? 0 :
    cut < 188 ? cut - 184 :
    cut < 200 ? 4 :
    cut === 200 ? 6 :
    cut === 201 ? 8 : 12;
  return {
    authorityEpoch: cut >= 201 ? 2 : 1,
    blocked: cut < 200 ? 6 : cut === 200 ? 2 : 0,
    staleReceipts: cut >= 201 ? 4 : 0,
    gateReceipts,
    promotion: cut >= 202 ? "candidate-c" : "none",
    posture: cut >= 202 ? "NOMINAL" : cut >= 196 ? "DEGRADED" : cut >= 192 ? "WATCH" : "NOMINAL",
    convergence: cut >= 202 ? "100.0" : cut >= 200 ? "96.4" : cut >= 193 ? "74.8" : "91.2",
    proofTuples: cut >= 202 ? 21 : cut >= 199 ? 13 : Math.max(3, cut - 178),
  };
}

function candidatesAt(cut: number): Candidate[] {
  return [
    {
      id: "A",
      label: "stale runner",
      owner: "agent-delta",
      product: "6227264…f618",
      tooling: "8449486…183a",
      status: cut >= 201 ? "fenced" : cut >= 184 ? "forming" : "forming",
      reason: cut >= 201 ? "epoch 1 < current epoch 2" : "epoch-1 evidence collecting",
    },
    {
      id: "B",
      label: "product drift",
      owner: "agent-epsilon",
      product: "7338375…0729",
      tooling: "9550597…294b",
      status: cut >= 191 ? "rejected" : "forming",
      reason: cut >= 191 ? "product SHA mismatch" : "identity review pending",
    },
    {
      id: "C",
      label: "control repair",
      owner: "agent-zeta",
      product: "6227264…f618",
      tooling: "9550597…294b",
      status: cut >= 202 ? "promoted" : "forming",
      reason: cut >= 202 ? "all semantic gates satisfied" : cut >= 200 ? "current evidence converging" : "repair in progress",
    },
  ];
}

function SignalCanvas({ cut, running }: { cut: number; running: boolean }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const context = canvas.getContext("2d");
    if (!context) return;

    let frame = 0;
    let animation = 0;
    const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const nodes = [
      { id: "journal", label: "JOURNAL", x: 0.10, y: 0.50, active: true },
      { id: "planner", label: "PLAN", x: 0.28, y: 0.20, active: true },
      { id: "runner", label: `RUNNER E${cut >= 201 ? 2 : 1}`, x: 0.28, y: 0.79, active: true },
      { id: "core", label: "CORE", x: 0.47, y: 0.48, active: true },
      { id: "a", label: "CAND A", x: 0.63, y: 0.15, active: cut < 201 },
      { id: "b", label: "CAND B", x: 0.76, y: 0.35, active: cut < 191 },
      { id: "c", label: "CAND C", x: 0.62, y: 0.72, active: cut >= 198 },
      { id: "evidence", label: "EVIDENCE", x: 0.85, y: 0.58, active: cut >= 185 },
      { id: "promote", label: "PROMOTE", x: 0.89, y: 0.86, active: cut >= 202 },
      { id: "learn", label: "LEARN", x: 0.44, y: 0.91, active: cut >= 202 },
    ];
    const edges = [
      ["journal", "planner"], ["journal", "runner"], ["planner", "core"], ["runner", "core"],
      ["core", "a"], ["core", "b"], ["core", "c"], ["a", "evidence"], ["b", "evidence"],
      ["c", "evidence"], ["evidence", "promote"], ["promote", "learn"], ["learn", "journal"],
    ];

    const resize = () => {
      const bounds = canvas.getBoundingClientRect();
      const ratio = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.max(1, Math.floor(bounds.width * ratio));
      canvas.height = Math.max(1, Math.floor(bounds.height * ratio));
      context.setTransform(ratio, 0, 0, ratio, 0, 0);
    };

    const draw = () => {
      const width = canvas.clientWidth;
      const height = canvas.clientHeight;
      context.clearRect(0, 0, width, height);

      context.strokeStyle = "rgba(91, 225, 203, 0.055)";
      context.lineWidth = 1;
      for (let x = 0; x <= width; x += 34) {
        context.beginPath();
        context.moveTo(x, 0);
        context.lineTo(x, height);
        context.stroke();
      }
      for (let y = 0; y <= height; y += 34) {
        context.beginPath();
        context.moveTo(0, y);
        context.lineTo(width, y);
        context.stroke();
      }

      const nodeMap = new Map(nodes.map((node) => [node.id, node]));
      edges.forEach(([fromId, toId], index) => {
        const from = nodeMap.get(fromId)!;
        const to = nodeMap.get(toId)!;
        const active = from.active && to.active;
        const x1 = from.x * width;
        const y1 = from.y * height;
        const x2 = to.x * width;
        const y2 = to.y * height;
        context.beginPath();
        context.moveTo(x1, y1);
        context.lineTo(x2, y2);
        context.strokeStyle = active ? "rgba(83, 214, 188, .34)" : "rgba(111, 128, 140, .16)";
        context.lineWidth = active ? 1.4 : 1;
        context.stroke();

        if (active && running && !reducedMotion) {
          const progress = ((frame * 0.004 + index * 0.137) % 1);
          const px = x1 + (x2 - x1) * progress;
          const py = y1 + (y2 - y1) * progress;
          const glow = context.createRadialGradient(px, py, 0, px, py, 12);
          glow.addColorStop(0, "rgba(142, 255, 219, .95)");
          glow.addColorStop(1, "rgba(142, 255, 219, 0)");
          context.fillStyle = glow;
          context.beginPath();
          context.arc(px, py, 12, 0, Math.PI * 2);
          context.fill();
        }
      });

      nodes.forEach((node, index) => {
        const x = node.x * width;
        const y = node.y * height;
        const radius = node.id === "core" ? 27 : 18;
        if (node.active) {
          const pulse = reducedMotion ? 0 : Math.sin(frame * 0.025 + index) * 2;
          context.beginPath();
          context.arc(x, y, radius + 9 + pulse, 0, Math.PI * 2);
          context.strokeStyle = node.id === "promote" ? "rgba(255, 194, 96, .32)" : "rgba(86, 230, 196, .18)";
          context.stroke();
        }
        context.beginPath();
        context.arc(x, y, radius, 0, Math.PI * 2);
        context.fillStyle = node.active
          ? node.id === "promote" ? "rgba(255, 183, 74, .22)" : "rgba(52, 194, 164, .17)"
          : "rgba(86, 102, 113, .12)";
        context.fill();
        context.strokeStyle = node.active
          ? node.id === "promote" ? "#ffc263" : "#58d9bd"
          : "rgba(134, 149, 158, .34)";
        context.lineWidth = 1.4;
        context.stroke();
        context.fillStyle = node.active ? "#dffcf5" : "#687985";
        context.font = `${node.id === "core" ? 700 : 600} 9px "Cascadia Mono", Consolas, monospace`;
        context.textAlign = "center";
        context.fillText(node.label, x, y + radius + 15);
      });

      frame += 1;
      animation = window.requestAnimationFrame(draw);
    };

    resize();
    const observer = new ResizeObserver(resize);
    observer.observe(canvas);
    draw();
    return () => {
      observer.disconnect();
      window.cancelAnimationFrame(animation);
    };
  }, [cut, running]);

  return (
    <canvas
      ref={canvasRef}
      className="signal-canvas"
      aria-label="Animated topology of the AETHER semantic fabric"
      role="img"
    />
  );
}

function StatusDot({ tone = "good" }: { tone?: "good" | "warn" | "bad" | "quiet" }) {
  return <span className={`status-dot ${tone}`} aria-hidden="true" />;
}

function runTone(run: ReleaseRun): "good" | "warn" | "bad" | "quiet" {
  if (run.status !== "completed") return "warn";
  if (run.conclusion === "success") return "good";
  if (run.conclusion === "failure" || run.conclusion === "cancelled") return "bad";
  return "quiet";
}

function compactSha(value: string | null | undefined) {
  return value ? `${value.slice(0, 7)}…${value.slice(-5)}` : "UNAVAILABLE";
}

export default function Home() {
  const [cut, setCut] = useState(MIN_CUT);
  const [running, setRunning] = useState(true);
  const [speed, setSpeed] = useState(2);
  const [cycle, setCycle] = useState(47);
  const [clock, setClock] = useState<Date | null>(null);
  const [selectedCandidate, setSelectedCandidate] = useState("C");
  const [traceOpen, setTraceOpen] = useState(false);
  const [fabric, setFabric] = useState<FabricEnvelope | null>(null);
  const [adapterError, setAdapterError] = useState<string | null>(null);
  const [forceDemo, setForceDemo] = useState(false);

  useEffect(() => {
    const timer = window.setInterval(() => setClock(new Date()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    let active = true;
    const refresh = async () => {
      try {
        const response = await fetch("/api/fabric", { cache: "no-store", headers: { accept: "application/json" } });
        if (!response.ok) throw new Error(`adapter HTTP ${response.status}`);
        const payload = await response.json() as FabricEnvelope;
        if (payload.contract_version !== "aether.orbital.fabric.v1" || payload.read_only !== true) {
          throw new Error("unsupported adapter contract");
        }
        if (active) {
          setFabric(payload);
          setAdapterError(null);
        }
      } catch (error) {
        if (active) setAdapterError(error instanceof Error ? error.message : "adapter unavailable");
      }
    };
    void refresh();
    const timer = window.setInterval(refresh, 8000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    if (!running) return;
    const timer = window.setInterval(() => {
      setCut((current) => {
        if (current >= MAX_CUT) {
          setCycle((value) => value + 1);
          return MIN_CUT;
        }
        return current + 1;
      });
    }, 2200 / speed);
    return () => window.clearInterval(timer);
  }, [running, speed]);

  const snapshot = useMemo(() => snapshotAt(cut), [cut]);
  const candidates = useMemo(() => candidatesAt(cut), [cut]);
  const visibleEvents = useMemo(
    () => EVENTS.filter((event) => event.cut <= cut).slice(-7).reverse(),
    [cut],
  );
  const currentEvent = EVENTS.find((event) => event.cut === cut) ?? EVENTS[0];
  const isReplay = !running || cut < MAX_CUT;
  const hasObservedSource = fabric !== null && fabric.mode !== "disconnected";
  const useLive = hasObservedSource && !forceDemo;
  const liveCut = fabric?.journal.latest_element ?? null;
  const displayCut = useLive && liveCut !== null ? liveCut : cut;
  const healthyReplicas = fabric?.service?.replicas.filter((replica) => replica.healthy).length ?? 0;
  const releaseRuns = fabric?.release.gates ?? [];
  const successfulRuns = releaseRuns.filter((run) => run.status === "completed" && run.conclusion === "success").length;
  const livePosture = fabric?.service?.status === "ok" && fabric.sources.every((source) => source.state === "current") ? "NOMINAL" : "DEGRADED";
  const sourceLabel = useLive ? `${fabric?.mode.toUpperCase()} ADAPTER` : hasObservedSource ? "DEMO OVERRIDE" : "DEMO FALLBACK";
  const sourceTone = useLive ? (fabric?.mode === "live" ? "good" : "warn") : "quiet";
  const liveDatoms = useMemo(() => [...(fabric?.journal.datoms ?? [])].slice(-7).reverse(), [fabric]);

  const jumpTo = useCallback((nextCut: number) => {
    setCut(nextCut);
    setRunning(false);
  }, []);

  return (
    <main className="control-room">
      <div className="ambient-grid" aria-hidden="true" />
      <header className="command-bar">
        <div className="brand-lockup">
          <div className="brand-mark"><span>A</span></div>
          <div>
            <p className="brand-name">AETHER / ORBITAL</p>
            <p className="brand-subtitle">Frontier Engineering Fabric</p>
          </div>
        </div>
        <div className="command-center">
          <button className="environment-chip source-switch" type="button" onClick={() => hasObservedSource && setForceDemo((value) => !value)}>
            <StatusDot tone={sourceTone} /> {sourceLabel}
          </button>
          <span className="operation-id">{useLive ? `READ-ONLY / OBSERVED ${fabric?.generated_at.slice(11, 19)}Z` : `OPERATION RADIANT THREAD / CYCLE ${cycle}`}</span>
        </div>
        <div className="clock-block">
          <p>{clock ? clock.toISOString().slice(11, 19) : "--:--:--"} <span>UTC</span></p>
          <small>CONTROL SURFACE 07</small>
        </div>
      </header>

      <section className="posture-strip" aria-label="Operational posture">
        <div className="posture-primary">
          <StatusDot tone={(useLive ? livePosture : snapshot.posture) === "DEGRADED" ? "warn" : "good"} />
          <span>FABRIC POSTURE</span>
          <strong>{useLive ? livePosture : snapshot.posture}</strong>
        </div>
        <div className="posture-metric">
          <span>JOURNAL CUT</span>
          <strong>{liveCut === null && useLive ? "EMPTY" : `e${displayCut}`}</strong>
          <small>{useLive ? "OBSERVED CURRENT" : isReplay ? "TEMPORAL VIEW" : "CURRENT"}</small>
        </div>
        <div className="posture-metric">
          <span>CONVERGENCE</span>
          <strong>{useLive ? `${fabric?.journal.datoms.length ?? 0}` : `${snapshot.convergence}%`}</strong>
          <small>{useLive ? "TAIL DATOMS" : "2 TEMPORAL EVALUATIONS"}</small>
        </div>
        <div className="posture-metric">
          <span>EVIDENCE</span>
          <strong>{useLive ? `${successfulRuns}/${releaseRuns.length}` : `${snapshot.gateReceipts}/12`}</strong>
          <small>{useLive ? "WORKFLOW OBSERVATIONS" : `${snapshot.staleReceipts} FENCED`}</small>
        </div>
        <div className="posture-metric">
          <span>WORK GRAPH</span>
          <strong>{useLive ? `${fabric?.service?.active_namespace_count ?? 0} NS` : snapshot.blocked === 0 ? "CLEAR" : `${snapshot.blocked} HOLD`}</strong>
          <small>{useLive ? `${healthyReplicas}/${fabric?.service?.replicas.length ?? 0} REPLICAS HEALTHY` : "24 PACKAGES / 31 EDGES"}</small>
        </div>
        <div className="posture-metric promotion-metric">
          <span>PROMOTION</span>
          <strong>{useLive ? fabric?.release.promotion_authorized ? "AUTHORIZED" : "UNVERIFIED" : snapshot.promotion === "none" ? "HELD" : "AUTHORIZED"}</strong>
          <small>{useLive ? `IDENTITY ${fabric?.release.identity_binding.toUpperCase()}` : snapshot.promotion === "none" ? "NO CURRENT CANDIDATE" : "CANDIDATE C"}</small>
        </div>
      </section>

      <section className="console-grid">
        <article className="panel topology-panel">
          <div className="panel-heading">
            <div>
              <p className="panel-kicker">SEMANTIC TOPOLOGY</p>
              <h1>Operational truth, in motion.</h1>
            </div>
            <div className="panel-meta">
              <span><StatusDot tone={sourceTone} /> {useLive ? `${fabric?.journal.total ?? 0} DATOMS` : "202 DATOMS"}</span>
              <span>{useLive ? `${fabric?.service?.active_namespace_count ?? 0} NAMESPACES` : "1 VISIBILITY DOMAIN"}</span>
            </div>
          </div>
          <div className="topology-stage">
            <SignalCanvas cut={cut} running={running} />
            <div className="topology-callout top-left">
              <span>ACTIVE AUTHORITY</span>
              <strong>{useLive ? fabric?.service?.service_mode ?? "unavailable" : `windows-runner-0${snapshot.authorityEpoch === 2 ? 9 : 7}`}</strong>
              <small>{useLive ? `${healthyReplicas} healthy replicas` : `epoch ${snapshot.authorityEpoch}`}</small>
            </div>
            <div className="topology-callout bottom-right">
              <span>DERIVATION FRONT</span>
              <strong>{useLive ? `${fabric?.journal.audit_events.length ?? 0} audit events` : `${snapshot.proofTuples} tuples`}</strong>
              <small>{useLive ? "redacted projection" : `root ${cut >= 202 ? "t380" : "forming"}`}</small>
            </div>
            <div className="scan-line" aria-hidden="true" />
          </div>
          <div className="topology-legend">
            <span><StatusDot /> active semantic path</span>
            <span><StatusDot tone="warn" /> degraded authority</span>
            <span><StatusDot tone="quiet" /> inactive or fenced</span>
            <span className="live-sweep">{useLive ? `READ-ONLY POLL / ${fabric?.mode.toUpperCase()}` : running ? "DEMO SWEEP ACTIVE" : `DEMO REPLAY HELD AT e${cut}`}</span>
          </div>
        </article>

        <aside className="panel executive-panel">
          <div className="panel-heading compact">
            <div>
              <p className="panel-kicker">SHIFT BRIEF</p>
              <h2>Command posture</h2>
            </div>
            <span className="classification">CONTROLLED ALPHA</span>
          </div>
          <div className={`posture-orb ${(useLive ? livePosture : snapshot.posture).toLowerCase()}`}>
            <div className="orb-rings" aria-hidden="true" />
            <div>
              <span>CONFIDENCE</span>
              <strong>{useLive ? `${fabric?.sources.filter((source) => source.state === "current").length ?? 0}/3` : cut >= 202 ? "0.997" : cut >= 196 ? "0.742" : "0.914"}</strong>
              <small>{useLive ? "sources current" : "semantic decision surface"}</small>
            </div>
          </div>
          <div className="brief-stack">
            <div className="brief-row">
              <span>Product identity</span>
              <strong>{useLive ? compactSha(fabric?.release.product_sha) : "6227264…f618"}</strong>
              <em>{useLive ? fabric?.release.product_sha ? "observed" : "not exposed by workflow API" : "frozen"}</em>
            </div>
            <div className="brief-row">
              <span>Tooling identity</span>
              <strong>{useLive ? compactSha(fabric?.release.tooling_sha) : cut >= 198 ? "9550597…294b" : "8449486…183a"}</strong>
              <em>{useLive ? "workflow head" : cut >= 198 ? "protected" : "superseded"}</em>
            </div>
            <div className="brief-row">
              <span>Identity binding</span>
              <strong>{useLive ? fabric?.release.identity_binding : "sha256:86e85…902e4"}</strong>
              <em>{useLive ? "requires immutable evidence" : "anchored"}</em>
            </div>
            <div className="brief-row">
              <span>Adapter contract</span>
              <strong>{useLive ? fabric?.contract_version : cut >= 200 ? "approved" : "pending"}</strong>
              <em>{useLive ? "read only" : cut >= 200 ? "current" : "required"}</em>
            </div>
          </div>
          <div className="brief-verdict">
            <p>{useLive ? "SOURCE VERDICT" : currentEvent.title}</p>
            <strong>{useLive ? fabric?.warnings[0] ?? "All configured observations are current." : currentEvent.detail}</strong>
          </div>
        </aside>

        <article className="panel timeline-panel">
          <div className="panel-heading compact">
            <div>
              <p className="panel-kicker">TEMPORAL CONTROL</p>
              <h2>{useLive ? "Following Observed Current" : running ? "Following Demo Current" : `Inspecting Demo AsOf(e${cut})`}</h2>
            </div>
            <div className="transport-controls" aria-label="Playback controls" aria-disabled={useLive}>
              <button type="button" disabled={useLive} onClick={() => setRunning((value) => !value)} aria-label={running ? "Pause fabric" : "Play fabric"}>
                {running ? "Ⅱ" : "▶"}
              </button>
              {[1, 2, 4].map((value) => (
                <button
                  type="button"
                  disabled={useLive}
                  key={value}
                  className={speed === value ? "active" : ""}
                  onClick={() => setSpeed(value)}
                  aria-label={`Set playback speed to ${value} times`}
                >
                  {value}×
                </button>
              ))}
            </div>
          </div>
          <div className="timeline-track">
            <input
              aria-label="Journal cut"
              type="range"
              min={MIN_CUT}
              max={MAX_CUT}
              value={cut}
              disabled={useLive}
              onChange={(event) => jumpTo(Number(event.target.value))}
              style={{ "--timeline-progress": `${((cut - MIN_CUT) / (MAX_CUT - MIN_CUT)) * 100}%` } as React.CSSProperties}
            />
            <div className="timeline-labels">
              <button type="button" onClick={() => jumpTo(181)}>e181 / OPEN</button>
              <button type="button" onClick={() => jumpTo(199)}>e199 / SAVED CUT</button>
              <button type="button" onClick={() => jumpTo(201)}>e201 / EPOCH 2</button>
              <button type="button" onClick={() => jumpTo(202)}>e202 / PROMOTE</button>
            </div>
          </div>
          <div className="timeline-now">
            <span>{currentEvent.time}</span>
            <strong>{useLive ? liveCut === null ? "EMPTY JOURNAL" : `e${liveCut} / OBSERVED CURRENT` : `e${cut} / ${currentEvent.title}`}</strong>
            <p>{useLive ? `Adapter refreshed ${fabric?.generated_at}; writes are structurally unavailable.` : currentEvent.detail}</p>
            {!running && <button type="button" onClick={() => { setCut(MAX_CUT); setRunning(true); }}>RETURN TO CURRENT →</button>}
          </div>
        </article>

        <article className="panel gates-panel">
          <div className="panel-heading compact">
            <div>
              <p className="panel-kicker">PREREQUISITE LANES</p>
              <h2>{useLive ? "Workflow observations" : "Evidence freshness"}</h2>
            </div>
            <span className="receipt-count">{useLive ? `${successfulRuns} / ${releaseRuns.length}` : `${snapshot.gateReceipts} / 12`}</span>
          </div>
          <div className="gate-grid">
            {(useLive ? releaseRuns : GATE_NAMES).map((gate, index) => {
              if (typeof gate !== "string") {
                const tone = runTone(gate);
                return (
                  <a className="gate-lane live-gate" key={gate.lane} href={gate.url} target="_blank" rel="noreferrer">
                    <div className="gate-name"><span>{gate.name.toUpperCase()}</span><strong><StatusDot tone={tone} /> {gate.conclusion ?? gate.status}</strong></div>
                    <div className="gate-segments"><span className={tone === "good" ? "filled" : ""} /><span className={gate.status === "completed" ? "filled" : ""} /><span className={gate.run_attempt > 0 ? "filled" : ""} /></div>
                    <small>{compactSha(gate.tooling_sha)} / RUN {gate.run_id}.{gate.run_attempt}</small>
                  </a>
                );
              }
              const currentCount = Math.max(0, Math.min(3, snapshot.gateReceipts - index * 3));
              return (
                <div className="gate-lane" key={gate}>
                  <div className="gate-name"><span>{gate}</span><strong>{currentCount}/3</strong></div>
                  <div className="gate-segments">
                    {[0, 1, 2].map((segment) => <span className={segment < currentCount ? "filled" : ""} key={segment} />)}
                  </div>
                  <small>{cut >= 202 ? "exact / current" : currentCount ? "converging" : "awaiting"}</small>
                </div>
              );
            })}
          </div>
        </article>

        <article className="panel candidate-panel">
          <div className="panel-heading compact">
            <div>
              <p className="panel-kicker">DECISION SURFACE</p>
              <h2>Candidate authority</h2>
            </div>
            <span className="candidate-total">03 TRACKED</span>
          </div>
          {useLive ? <div className="candidate-list release-list">
            {releaseRuns.map((release) => (
              <a className="candidate-card selected" href={release.url} target="_blank" rel="noreferrer" key={release.lane}>
                <span className="candidate-id"><StatusDot tone={runTone(release)} /></span>
                <span className="candidate-copy"><strong>{release.name}</strong><small>{compactSha(release.tooling_sha)}</small></span>
                <span className="candidate-state">{release.conclusion ?? release.status}</span>
              </a>
            ))}
          </div> : <div className="candidate-list">
            {candidates.map((candidate) => (
              <button
                type="button"
                className={`candidate-card ${candidate.status} ${selectedCandidate === candidate.id ? "selected" : ""}`}
                key={candidate.id}
                onClick={() => setSelectedCandidate(candidate.id)}
              >
                <span className="candidate-id">{candidate.id}</span>
                <span className="candidate-copy">
                  <strong>{candidate.label}</strong>
                  <small>{candidate.owner}</small>
                </span>
                <span className="candidate-state">{candidate.status}</span>
              </button>
            ))}
          </div>}
          {!useLive && candidates.filter((candidate) => candidate.id === selectedCandidate).map((candidate) => (
            <div className="candidate-detail" key={candidate.id}>
              <div><span>PRODUCT</span><strong>{candidate.product}</strong></div>
              <div><span>TOOLING</span><strong>{candidate.tooling}</strong></div>
              <p><StatusDot tone={candidate.status === "promoted" ? "good" : candidate.status === "forming" ? "warn" : "bad"} /> {candidate.reason}</p>
            </div>
          ))}
        </article>

        <article className="panel journal-panel">
          <div className="panel-heading compact">
            <div>
              <p className="panel-kicker">CAUSAL JOURNAL</p>
              <h2>{useLive ? "Observed semantic feed" : "Simulated semantic feed"}</h2>
            </div>
            <span className="stream-state"><StatusDot tone={sourceTone} /> {useLive ? "POLLING" : "SIMULATING"}</span>
          </div>
          <div className="journal-feed" aria-live="polite">
            {useLive ? liveDatoms.map((datom, index) => (
              <div className="journal-row kind-fabric" key={`${datom.element}-${index}`}>
                <span className="journal-time">OBSERVED</span>
                <span className="journal-cut">e{datom.element}</span>
                <span className="journal-kind">{datom.op}</span>
                <div><strong>{datom.entity} / {datom.attribute}</strong><p>{datom.value} · replica {datom.replica ?? "n/a"} · {datom.provenance.trust_domain ?? "unscoped"}</p></div>
                {index === 0 && <span className="new-event">LATEST</span>}
              </div>
            )) : visibleEvents.map((event, index) => (
              <div className={`journal-row kind-${event.kind}`} key={event.cut}>
                <span className="journal-time">{event.time}</span>
                <span className="journal-cut">e{event.cut}</span>
                <span className="journal-kind">{event.kind}</span>
                <div>
                  <strong>{event.title}</strong>
                  <p>{event.detail}</p>
                </div>
                {index === 0 && <span className="new-event">LATEST</span>}
              </div>
            ))}
            {useLive && liveDatoms.length === 0 && <div className="empty-feed">No journal datoms are currently visible to the adapter.</div>}
          </div>
        </article>

        <article className="panel proof-panel">
          <div className="panel-heading compact">
            <div>
              <p className="panel-kicker">EXPLAIN / PROVENANCE</p>
              <h2>{useLive ? "Source accountability" : "Why this is true"}</h2>
            </div>
            <button type="button" className="trace-toggle" onClick={() => setTraceOpen((value) => !value)}>
              {traceOpen ? "COLLAPSE" : "INSPECT TRACE"}
            </button>
          </div>
          <div className="proof-root">
            <span className="tuple-node">{useLive ? "RO" : "t380"}</span>
            <div>
              <strong>{useLive ? `identity_binding(${fabric?.release.identity_binding})` : cut >= 202 ? "promotion_allowed(candidate-c)" : "promotion_allowed(?)"}</strong>
              <p>{useLive ? `${fabric?.sources.length ?? 0} sources / ${fabric?.journal.audit_events.length ?? 0} audit observations` : cut >= 202 ? "21 tuples / 40 iterations / 37 source datoms" : `${snapshot.proofTuples} tuples currently assembled`}</p>
            </div>
            <span className={`proof-verdict ${useLive && fabric?.release.identity_binding === "verified" || !useLive && cut >= 202 ? "proved" : ""}`}>{useLive ? fabric?.release.identity_binding === "verified" ? "VERIFIED" : "UNVERIFIED" : cut >= 202 ? "PROVED" : "FORMING"}</span>
          </div>
          <div className={`trace-tree ${traceOpen ? "open" : ""}`}>
            <div><span>r22</span><strong>promotion gate</strong><em>{cut >= 202 ? "satisfied" : "waiting"}</em></div>
            <div><span>r15</span><strong>all evidence current</strong><em>{snapshot.gateReceipts}/12</em></div>
            <div><span>r11</span><strong>work graph ready</strong><em>{snapshot.blocked === 0 ? "true" : "false"}</em></div>
            <div><span>r09</span><strong>package exact</strong><em>true</em></div>
            <div><span>r08</span><strong>tooling admissible</strong><em>{cut >= 198 ? "true" : "false"}</em></div>
            <div><span>r07</span><strong>product exact</strong><em>true</em></div>
          </div>
        </article>
      </section>

      <footer className="room-footer">
        <div>
          <StatusDot />
          <span>AETHER OPERATIONAL TRUTH LAYER</span>
          <small>Core · Coordinate · Memory · Learn · Explain</small>
        </div>
        <p>
          {useLive
            ? "Read-only projection of service, journal, audit, and workflow observations. External systems remain outside the authoritative kernel."
            : `Deterministic control-room simulation over Demo 07 semantics. External agents and CI remain outside the authoritative kernel.${adapterError ? ` Adapter unavailable: ${adapterError}.` : ""}`}
        </p>
        <div className="footer-hash">
          <span>PACKAGE ANCHOR</span>
          <strong>{shortHash("86e85f3f1bc1d977a351ac02f70d7d6cc98eeec5d13f5b7272c902e4")}</strong>
        </div>
      </footer>
    </main>
  );
}
