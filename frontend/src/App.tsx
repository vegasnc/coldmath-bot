import { useCallback, useEffect, useMemo, useRef, useState } from "react";

type MonitorEvent = Record<string, unknown> & {
  id?: number;
  ts?: string;
  type?: string;
};

type Win = Window & { __CM_BACKEND_ORIGIN__?: string };

/**
 * REST + WebSocket base (no trailing slash).
 * Injected when the bot serves index.html; falls back to same-origin or dev default.
 */
function apiBase(): string {
  if (typeof window === "undefined") return "http://127.0.0.1:8765";
  const inj = ((window as Win).__CM_BACKEND_ORIGIN__ || "").trim();
  if (inj.startsWith("http") && !inj.includes("__INJECT") && !inj.includes("INJECT_BACKEND")) {
    return inj.replace(/\/$/, "");
  }
  const q = new URLSearchParams(window.location.search).get("backend");
  if (q?.trim().startsWith("http")) return q.trim().replace(/\/$/, "");
  const env = (import.meta.env.VITE_BACKEND_ORIGIN as string | undefined)?.trim();
  if (env) return env.replace(/\/$/, "");
  // Vite dev: HTTP /api/* is proxied to the bot; see vite.config.ts.
  if (import.meta.env.DEV) return window.location.origin.replace(/\/$/, "");
  return window.location.origin.replace(/\/$/, "");
}

/** Bot HTTP origin for WebSocket in dev (never use Vite ws proxy — it errors on close). */
function devWsHttpBase(): string {
  const env = (import.meta.env.VITE_BACKEND_ORIGIN as string | undefined)?.trim();
  if (env) return env.replace(/\/$/, "");
  return "http://127.0.0.1:8765";
}

function wsUrl(): string {
  if (import.meta.env.DEV) {
    const u = new URL(devWsHttpBase());
    const wsProto = u.protocol === "https:" ? "wss:" : "ws:";
    return `${wsProto}//${u.host}/ws/events`;
  }
  const base = apiBase();
  const u = new URL(base.startsWith("http") ? base : `http://${base}`);
  const wsProto = u.protocol === "https:" ? "wss:" : "ws:";
  return `${wsProto}//${u.host}/ws/events`;
}

function mergeById(prev: MonitorEvent[], incoming: MonitorEvent[], cap: number): MonitorEvent[] {
  const byId = new Map<number, MonitorEvent>();
  for (const e of prev) {
    const id = typeof e.id === "number" ? e.id : undefined;
    if (id !== undefined) byId.set(id, e);
  }
  for (const e of incoming) {
    const id = typeof e.id === "number" ? e.id : undefined;
    if (id !== undefined) byId.set(id, e);
  }
  const sorted = Array.from(byId.values()).sort((a, b) => (Number(a.id) || 0) - (Number(b.id) || 0));
  return sorted.length > cap ? sorted.slice(-cap) : sorted;
}

function fmtPrice(x: unknown): string {
  if (x === null || x === undefined) return "—";
  const n = Number(x);
  if (Number.isNaN(n)) return String(x);
  return `${(n * 100).toFixed(2)}¢`;
}

function fmtProb01(x: unknown): string {
  if (x === null || x === undefined) return "—";
  const n = Number(x);
  if (Number.isNaN(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function ticketStatusLabel(e: MonitorEvent): string {
  const st = String(e.ticket_status || "").trim();
  if (st === "selected") return "Selected";
  if (st === "rejected") return "Rejected";
  return st ? st : "—";
}

function ticketReasonCode(e: MonitorEvent): string {
  return String(e.reject_reason || "").trim() || "—";
}

function eventTitle(e: MonitorEvent): string {
  const t = (e.type as string) || "";
  switch (t) {
    case "price":
      return `${e.title || e.slug || "quote"} · ${e.outcome || ""}`;
    case "buy":
      return `BUY ${e.leg} · ${e.title || e.slug}`;
    case "sell":
      return `SELL · ${e.title || e.slug}`;
    case "merge":
      return `Merge · ${e.title || e.slug || ""}`;
    case "execute":
      return `Execute ${e.success ? "ok" : "fail"} · ${e.slug}${
        !e.success && e.reject_reason ? ` · ${e.reject_reason}` : ""
      }`;
    case "virtual_plan":
      return `Plan · ${e.title || e.slug}${
        e.ticket_detail ? ` · ${String(e.ticket_detail).slice(0, 56)}` : ""
      }`;
    case "opportunity":
      return `Opp · ${e.title || e.slug}${
        e.can_execute === false && e.reject_reason ? ` · ${e.reject_reason}` : ""
      }`;
    case "domain_scan":
      return `Scan ${e.phase} · ${e.domain}`;
    case "session":
      return `Sessions · ${(e.active as string[])?.join(", ") || ""}`;
    case "engine":
      return `${e.phase} · ${e.utc || ""}`;
    case "early_sell":
      return `Early sell ×${e.sold_count}`;
    case "error":
      return `Error · ${e.where || ""}`;
    case "weather_discovery":
      return `Weather Gamma · ${e.event_count} event(s)`;
    case "weather_prices":
      return `WS book snapshot · ${e.row_count} row(s)`;
    default:
      return t || "event";
  }
}

export default function App() {
  const [connected, setConnected] = useState(false);
  const [events, setEvents] = useState<MonitorEvent[]>([]);
  const [filter, setFilter] = useState("");
  const bottomRef = useRef<HTMLDivElement>(null);
  const maxEvents = 800;

  const push = useCallback((ev: MonitorEvent) => {
    setEvents((prev) => {
      const id = typeof ev.id === "number" ? ev.id : undefined;
      if (id !== undefined) {
        const idx = prev.findIndex((x) => x.id === id);
        if (idx >= 0) {
          const next = [...prev];
          next[idx] = ev;
          return next.length > maxEvents ? next.slice(-maxEvents) : next;
        }
      }
      const next = [...prev, ev];
      return next.length > maxEvents ? next.slice(-maxEvents) : next;
    });
  }, []);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let alive = true;

    const connect = () => {
      if (!alive) return;
      try {
        ws = new WebSocket(wsUrl());
      } catch {
        setConnected(false);
        if (alive) setTimeout(connect, 2000);
        return;
      }
      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        if (alive) setTimeout(connect, 2000);
      };
      ws.onerror = () => {
        setConnected(false);
      };
      ws.onmessage = (m) => {
        try {
          const ev = JSON.parse(m.data) as MonitorEvent;
          push(ev);
        } catch {
          /* ignore */
        }
      };
    };

    connect();
    return () => {
      alive = false;
      ws?.close();
    };
  }, [push]);

  useEffect(() => {
    if (connected) return;
    let cancelled = false;
    const poll = async () => {
      if (cancelled) return;
      try {
        const r = await fetch(`${apiBase()}/api/events?limit=400`);
        if (!r.ok) return;
        const data = (await r.json()) as { events?: MonitorEvent[] };
        const arr = data.events;
        if (!arr?.length) return;
        setEvents((prev) => mergeById(prev, arr, maxEvents));
      } catch {
        /* backend not reachable */
      }
    };
    void poll();
    const id = window.setInterval(poll, 2500);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [connected, maxEvents]);

  const lastEngine = useMemo(
    () => [...events].reverse().find((e) => e.type === "engine"),
    [events],
  );
  const lastSession = useMemo(
    () => [...events].reverse().find((e) => e.type === "session"),
    [events],
  );

  const lastWeatherDiscovery = useMemo(
    () => [...events].reverse().find((e) => e.type === "weather_discovery"),
    [events],
  );

  const prices = useMemo(() => {
    const map = new Map<string, MonitorEvent>();
    for (const e of events) {
      if (e.type === "price") {
        const slug = String(e.slug || "");
        const oc = String(e.outcome || "");
        const ak = String(e.asset_key || "");
        const key = `${slug}|${oc}|${ak}`;
        map.set(key, e);
        continue;
      }
      if (e.type === "weather_prices" && Array.isArray(e.rows)) {
        const ts = String(e.ts || "");
        for (const row of e.rows as MonitorEvent[]) {
          if (!row || typeof row !== "object") continue;
          const slug = String(row.slug || "");
          const oc = String(row.outcome || "");
          const ak = String(row.asset_key || "");
          const key = `${slug}|${oc}|${ak}`;
          map.set(key, {
            ...row,
            type: "price",
            ts,
            source: String(row.source || "snapshot"),
            id: typeof row.id === "number" ? row.id : undefined,
          });
        }
      }
    }
    return Array.from(map.values()).slice(-120);
  }, [events]);

  const actions = useMemo(() => {
    const types = new Set([
      "buy",
      "sell",
      "merge",
      "execute",
      "virtual_plan",
      "early_sell",
    ]);
    return events.filter((e) => types.has(String(e.type || "")));
  }, [events]);

  const states = useMemo(() => {
    const types = new Set([
      "engine",
      "session",
      "domain_scan",
      "opportunity",
      "error",
      "weather_discovery",
      "weather_prices",
    ]);
    return events.filter((e) => types.has(String(e.type || "")));
  }, [events]);

  const filteredFeed = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return events.slice(-200);
    return events
      .filter((e) => JSON.stringify(e).toLowerCase().includes(q))
      .slice(-200);
  }, [events, filter]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [filteredFeed.length]);

  return (
    <div style={{ padding: "1rem 1.25rem 2rem", maxWidth: 1400, margin: "0 auto" }}>
      <header style={{ marginBottom: "1.25rem", borderBottom: "1px solid var(--border)", paddingBottom: "0.75rem" }}>
        <h1 style={{ margin: 0, fontSize: "1.35rem", fontWeight: 600 }}>ColdMath Bot Monitor</h1>
        <p style={{ margin: "0.35rem 0 0", color: "var(--muted)", fontSize: "0.9rem" }}>
          Live engine state, scans, orders, and Polymarket quotes (labeled tokens).
        </p>
        <p style={{ margin: "0.5rem 0 0", fontSize: "0.9rem" }}>
          <span
            style={{
              display: "inline-block",
              width: 8,
              height: 8,
              borderRadius: "50%",
              marginRight: 6,
              background: connected ? "var(--buy)" : "var(--err)",
            }}
          />
          {connected
            ? "WebSocket connected"
            : `HTTP poll to ${apiBase()} (WS reconnecting…)`}
          {lastEngine?.paper_trade !== undefined && (
            <span style={{ marginLeft: 12, color: "var(--muted)" }}>
              Paper: {String(lastEngine.paper_trade)}
            </span>
          )}
        </p>
        {import.meta.env.DEV && (
          <p
            style={{
              margin: "0.75rem 0 0",
              padding: "0.5rem 0.65rem",
              background: "#2a2210",
              border: "1px solid #5c4818",
              borderRadius: 6,
              fontSize: "0.85rem",
              color: "#f5e6c8",
            }}
          >
            Vite dev: <code>/api</code> is proxied to the bot. WebSocket connects directly to{" "}
            <strong>{devWsHttpBase().replace(/^http/, "ws")}/ws/events</strong> (no Vite WS proxy). Run{" "}
            <code>python main.py --web</code> first. Set <code>VITE_BACKEND_ORIGIN</code> if the bot
            is not on 8765.
          </p>
        )}
      </header>

      {lastWeatherDiscovery && (
        <section
          style={{
            background: "var(--panel)",
            border: "1px solid var(--border)",
            borderRadius: 8,
            padding: "0.75rem 1rem",
            marginBottom: "1rem",
          }}
        >
          <h2 style={{ margin: "0 0 0.5rem", fontSize: "1rem", color: "#a78bfa" }}>
            Polymarket weather events (Gamma / scan)
          </h2>
          <p style={{ margin: "0 0 0.5rem", color: "var(--muted)", fontSize: "0.85rem" }}>
            Count: <strong style={{ color: "var(--text)" }}>{String(lastWeatherDiscovery.event_count ?? "—")}</strong>
            {" · "}
            <span style={{ color: "var(--muted)" }}>{lastWeatherDiscovery.ts}</span>
          </p>
          <div style={{ maxHeight: 200, overflow: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.8rem" }}>
              <thead>
                <tr style={{ textAlign: "left", color: "var(--muted)" }}>
                  <th style={{ padding: 4 }}>Slug</th>
                  <th style={{ padding: 4 }}>Title</th>
                  <th style={{ padding: 4 }}>Mkts</th>
                </tr>
              </thead>
              <tbody>
                {((lastWeatherDiscovery.events as MonitorEvent[]) || []).map((row, i) => (
                  <tr key={i} style={{ borderTop: "1px solid var(--border)" }}>
                    <td style={{ padding: "4px 6px", maxWidth: 160, wordBreak: "break-all" }}>
                      {String(row.slug || "—")}
                    </td>
                    <td style={{ padding: "4px 6px" }}>{String(row.title || "—")}</td>
                    <td style={{ padding: "4px 6px" }}>{String(row.market_count ?? "—")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: "1rem",
          marginBottom: "1rem",
        }}
      >
        <section
          style={{
            background: "var(--panel)",
            border: "1px solid var(--border)",
            borderRadius: 8,
            padding: "0.75rem 1rem",
          }}
        >
          <h2 style={{ margin: "0 0 0.5rem", fontSize: "1rem", color: "var(--accent)" }}>Detected states</h2>
          <div style={{ color: "var(--muted)", fontSize: "0.85rem", marginBottom: 6 }}>
            Last session:{" "}
            <strong style={{ color: "var(--text)" }}>
              {(lastSession?.active as string[])?.join(", ") || "—"}
            </strong>
          </div>
          <div style={{ maxHeight: 220, overflowY: "auto", fontFamily: "ui-monospace, monospace", fontSize: "0.8rem" }}>
            {states.slice(-25).map((e, i) => (
              <div key={`${e.id}-${i}`} style={{ padding: "3px 0", borderBottom: "1px solid #0003" }}>
                <span style={{ color: "var(--muted)" }}>{e.ts}</span>{" "}
                <span style={{ color: "#7dd3fc" }}>{e.type}</span> {eventTitle(e)}
              </div>
            ))}
          </div>
        </section>

        <section
          style={{
            background: "var(--panel)",
            border: "1px solid var(--border)",
            borderRadius: 8,
            padding: "0.75rem 1rem",
          }}
        >
          <h2 style={{ margin: "0 0 0.5rem", fontSize: "1rem", color: "var(--sell)" }}>Buy / sell / execute</h2>
          <div style={{ maxHeight: 220, overflowY: "auto", fontFamily: "ui-monospace, monospace", fontSize: "0.8rem" }}>
            {actions.slice(-25).map((e, i) => (
              <div key={`${e.id}-${i}`} style={{ padding: "3px 0", borderBottom: "1px solid #0003" }}>
                <span style={{ color: "var(--muted)" }}>{e.ts}</span>{" "}
                <span style={{ color: e.type === "sell" ? "var(--sell)" : "var(--buy)" }}>{e.type}</span>{" "}
                {eventTitle(e)}
                {e.type === "buy" && (
                  <span style={{ color: "var(--muted)" }}>
                    {" "}
                    @{fmtPrice(e.price)} ×{String(e.shares ?? "")}
                  </span>
                )}
              </div>
            ))}
          </div>
        </section>
      </div>

      <section
        style={{
          background: "var(--panel)",
          border: "1px solid var(--border)",
          borderRadius: 8,
          padding: "0.75rem 1rem",
          marginBottom: "1rem",
        }}
      >
        <h2 style={{ margin: "0 0 0.5rem", fontSize: "1rem", color: "var(--price)" }}>Real-time prices (WS)</h2>
        <p style={{ margin: "0 0 0.65rem", fontSize: "0.8rem", color: "var(--muted)", maxWidth: 900 }}>
          After each weather scan, rows include <strong>ticket_status</strong> and an evidence string explaining parser
          mismatches, missing quotes, model confidence/edge gates, or why a bracket was not the event winner. Session
          execute skips and order failures are also labeled in the <strong>opportunity</strong> / <strong>execute</strong>{" "}
          feed with <code>reject_reason</code> / <code>ticket_detail</code>.
        </p>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}>
            <thead>
              <tr style={{ textAlign: "left", color: "var(--muted)" }}>
                <th style={{ padding: "4px 8px" }}>Time</th>
                <th style={{ padding: "4px 8px" }}>Question</th>
                <th style={{ padding: "4px 8px" }}>Slug</th>
                <th style={{ padding: "4px 8px" }}>Leg</th>
                <th style={{ padding: "4px 8px" }}>Bid</th>
                <th style={{ padding: "4px 8px" }}>Ask</th>
                <th style={{ padding: "4px 8px" }}>Ticket</th>
                <th style={{ padding: "4px 8px" }}>Reason</th>
                <th style={{ padding: "4px 8px" }}>P(NO)</th>
                <th style={{ padding: "4px 8px" }}>Edge</th>
                <th style={{ padding: "4px 8px" }}>Evidence</th>
                <th style={{ padding: "4px 8px" }}>Src</th>
              </tr>
            </thead>
            <tbody>
              {prices.length === 0 ? (
                <tr>
                  <td colSpan={12} style={{ padding: 8, color: "var(--muted)" }}>
                    No CLOB rows yet. Run <code>python main.py --web</code> and wait for a <strong>weather</strong> domain
                    scan (session window). After subscribe you should see a <code>snapshot</code> row per YES/NO token when
                    the book has loaded.
                  </td>
                </tr>
              ) : (
                prices.map((e, i) => (
                  <tr key={`${e.asset_key || e.slug}-${e.outcome}-${i}`} style={{ borderTop: "1px solid var(--border)" }}>
                    <td style={{ padding: "6px 8px", color: "var(--muted)", whiteSpace: "nowrap" }}>{e.ts}</td>
                    <td style={{ padding: "6px 8px", maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis" }}>
                      {String(e.title || "—")}
                    </td>
                    <td style={{ padding: "6px 8px", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis" }}>
                      {String(e.slug || "—")}
                    </td>
                    <td style={{ padding: "6px 8px" }}>{String(e.outcome || "—")}</td>
                    <td style={{ padding: "6px 8px" }}>{fmtPrice(e.best_bid)}</td>
                    <td style={{ padding: "6px 8px" }}>{fmtPrice(e.best_ask)}</td>
                    <td
                      style={{
                        padding: "6px 8px",
                        color:
                          e.ticket_status === "selected"
                            ? "var(--buy)"
                            : e.ticket_status === "rejected"
                              ? "var(--err)"
                              : "var(--muted)",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {ticketStatusLabel(e)}
                    </td>
                    <td style={{ padding: "6px 8px", fontSize: "0.78rem", maxWidth: 120 }}>{ticketReasonCode(e)}</td>
                    <td style={{ padding: "6px 8px" }}>{fmtProb01(e.our_prob_no)}</td>
                    <td style={{ padding: "6px 8px" }}>
                      {e.edge !== undefined && e.edge !== null && !Number.isNaN(Number(e.edge))
                        ? `${(Number(e.edge) * 100).toFixed(2)}pt`
                        : "—"}
                    </td>
                    <td
                      style={{ padding: "6px 8px", maxWidth: 320, fontSize: "0.78rem", color: "var(--muted)" }}
                      title={String(e.ticket_detail || "")}
                    >
                      {String(e.ticket_detail || "").trim()
                        ? String(e.ticket_detail).length > 140
                          ? `${String(e.ticket_detail).slice(0, 137)}…`
                          : String(e.ticket_detail)
                        : "—"}
                    </td>
                    <td style={{ padding: "6px 8px", color: "var(--muted)" }}>{String(e.source || "")}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section
        style={{
          background: "var(--panel)",
          border: "1px solid var(--border)",
          borderRadius: 8,
          padding: "0.75rem 1rem",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 8, flexWrap: "wrap" }}>
          <h2 style={{ margin: 0, fontSize: "1rem" }}>Full event feed</h2>
          <input
            type="search"
            placeholder="Filter (slug, type, …)"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            style={{
              flex: 1,
              minWidth: 200,
              padding: "6px 10px",
              borderRadius: 6,
              border: "1px solid var(--border)",
              background: "#0c1014",
              color: "var(--text)",
            }}
          />
        </div>
        <div
          style={{
            maxHeight: 420,
            overflowY: "auto",
            fontFamily: "ui-monospace, monospace",
            fontSize: "0.78rem",
            lineHeight: 1.4,
          }}
        >
          {filteredFeed.map((e, i) => (
            <pre
              key={`${e.id}-${i}`}
              style={{
                margin: 0,
                padding: "6px 8px",
                borderBottom: "1px solid #0004",
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
              }}
            >
              <span style={{ color: "var(--muted)" }}>{e.ts}</span>{" "}
              <span style={{ color: "#7dd3fc" }}>{e.type}</span> {JSON.stringify(e)}
            </pre>
          ))}
          <div ref={bottomRef} />
        </div>
      </section>

      <footer style={{ marginTop: "1rem", color: "var(--muted)", fontSize: "0.8rem" }}>
        One backend port: bot + API + this UI. Run <code>python main.py --web</code> (or set{" "}
        <code>web_enabled</code> in config). Build once:{" "}
        <code>cd frontend && npm install && npm run build</code>. Then open{" "}
        <code>http://127.0.0.1:8765/</code> (see <code>web_port</code> in config).
      </footer>
    </div>
  );
}
