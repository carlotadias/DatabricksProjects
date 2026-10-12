// Architecture page for the demo: the data flow, live facts read through the Data API, links into the
// workspace, and the nightly refresh timeline.
import type { ReactNode } from "react";
import gold from "../../../sql/lakebase_gold.sql?raw";
import { api, useLoad } from "../api";
import { DataApiPanel, ErrorNote } from "../components/Bits";
import { CodeBlock, CopyButton } from "../components/CodeBlock";
import { IconExternal } from "../components/Icons";
import { DEFAULT_SCOPE, EXPECTED_ROWS } from "../lib/catalog";
import { addDays } from "../lib/dates";
import { fmtFullDay } from "../lib/format";
import { fullSql, parseGold } from "../lib/goldSql";
import { buildLinks, lakebaseUrl, type ArchConfig, type LinkCard } from "../lib/links";
import { dataApiSnippets, postgresDatabase } from "../lib/snippets";

const GOLD = parseGold(gold); // single source of truth: sql/lakebase_gold.sql, bundled at build time

const stages = (cfg: ArchConfig | null) => [
  { tag: "Unity Catalog", title: "Silver", items: [`${cfg?.catalog ?? "<catalog>"}.${cfg?.data_schema ?? "<data_schema>"}`,
    ".silver_hh_readings", "13 months, Change Data Feed on"],
    caption: "Governed Delta tables. The daily job appends the next simulated day." },
  { tag: "Lakeflow", title: "Synced-table pipeline", items: ["TRIGGERED", "refreshed daily"],
    caption: "Incremental: only the changed rows (from the Change Data Feed) are applied." },
  { tag: "Lakebase Postgres", title: "Silver replica + gold", items: ["databricks_postgres.portal.silver_hh_readings",
    "(synced, read-only) + 2 indexes", "portal_work.hh_account_totals (internal)",
    "portal_gold.*: 6 views, 9 materialized views"],
    caption: "Gold built inside Lakebase, next to the synced silver and dimension tables." },
  { tag: "Data API", title: "PostgREST over HTTPS", items: ["GET /portal_gold/<name>?filters", "exposes schema portal_gold only",
    "OAuth token, SELECT-only role"],
    caption: "No Postgres client code in the app: filters and ordering are URL parameters." },
  { tag: "This app", title: "Energy portal (demo)", items: ["customer and advisor views", "reads portal_gold only"],
    caption: "A thin allow-listed proxy; every screen shows the Data API calls it made." },
];

const TIMELINE = [
  { at: "Scheduled", title: "Append yesterday", detail: "New half-hourly readings land in UC silver (oldest day rolls off)." },
  { at: "+ minutes", title: "Incremental sync", detail: "The synced-table pipeline applies the changes to the Lakebase replica." },
  { at: "+ 5-10 min", title: "Refresh gold", detail: "REFRESH MATERIALIZED VIEW CONCURRENTLY: the portal keeps serving meanwhile." },
  { at: "Done", title: "Portal shows the new day", detail: "Half-hourly views read the replica directly; totals come from gold." },
];

function Fact({ label, value }: { label: string; value: string }) {
  return <div className="fact"><span className="fact-value">{value}</span><span className="fact-label">{label}</span></div>;
}

function LinkTile({ l }: { l: LinkCard }) {
  if (!l.url) {
    return <div className="link-tile disabled" aria-disabled="true"><span className="link-title">{l.title}</span>
      <span className="subtle">Not configured for this deployment</span></div>;
  }
  return (
    <a className="link-tile" href={l.url} target="_blank" rel="noopener noreferrer">
      <span className="link-title">{l.title} <IconExternal /><span className="sr-only">(opens in a new tab)</span></span>
      <span className="subtle">{l.detail}</span>
    </a>
  );
}

function Locations({ cfg }: { cfg: ArchConfig | null }) {
  const lb = cfg ? lakebaseUrl(cfg) : null;
  const uc = (schema: string | null) => cfg?.host && cfg.catalog && schema
    ? `${cfg.host}/explore/data/${cfg.catalog}/${schema}/silver_hh_readings` : null;
  const db = postgresDatabase(cfg?.data_api_url ?? null);
  const cell = (url: string | null, text: string) => url
    ? <a href={url} target="_blank" rel="noopener noreferrer"><code>{text}</code> <IconExternal /><span className="sr-only">(opens in a new tab)</span></a>
    : <code>{text}</code>;
  const rows: { layer: string; where: ReactNode; type: string; rows: string; refresh: string; api: string }[] = [
    { layer: "UC silver", where: cell(uc(cfg?.data_schema ?? null), `${cfg?.catalog}.${cfg?.data_schema}.silver_hh_readings`),
      type: "Delta table, Change Data Feed on", rows: "about 19 M", refresh: "next day appended by the daily job", api: "-" },
    { layer: "Synced table (UC entry)", where: cell(uc(cfg?.synced_schema ?? null), `${cfg?.catalog}.${cfg?.synced_schema}.silver_hh_readings`),
      type: "UC online view of the Postgres table", rows: "about 19 M", refresh: "TRIGGERED pipeline, nightly", api: "-" },
    { layer: "Lakebase silver replica", where: cell(lb, `${db}.portal.silver_hh_readings`),
      type: "synced table (read-only); indexes: primary key (mpan, reading_ts), (mpan, reading_date), (reading_date)",
      rows: "about 19 M", refresh: "incremental sync, nightly", api: "not exposed" },
    { layer: "Lakebase dimensions", where: cell(lb, `${db}.portal.{organisations, accounts, sites, mpans, tariffs}`),
      type: "synced tables (SNAPSHOT, read-only)", rows: "1,000 MPANs and parents", refresh: "once (static demo data)",
      api: "not exposed" },
    ...GOLD.map((o) => ({
      layer: o.exposed ? "Lakebase gold" : "Lakebase internal helper",
      where: cell(lb, `${db}.${o.schema}.${o.name}`),
      type: o.kind === "VIEW" ? "VIEW (computed on read)" : "MATERIALIZED VIEW",
      rows: EXPECTED_ROWS[`${o.schema}.${o.name}`]?.toLocaleString("en-GB") ?? (o.kind === "VIEW" ? "on read" : "-"),
      refresh: o.kind === "VIEW" ? "always current (reads the replica)"
        : o.exposed ? "REFRESH CONCURRENTLY nightly" : "REFRESH nightly (before the gold MVs)",
      api: o.exposed ? `/portal_gold/${o.name}` : "not exposed",
    })),
  ];
  return (
    <section className="card arch-card" aria-labelledby="where-title">
      <h2 id="where-title" className="card-title">Where everything lives</h2>
      <p className="subtle">Expected row counts for the default scope ({DEFAULT_SCOPE}); each night adds one day and drops
        the oldest. Lakebase rows open the project's Tables page.</p>
      <div className="table-scroll wide" tabIndex={0} aria-label="Data locations">
        <table className="where-table">
          <thead><tr><th scope="col">Layer</th><th scope="col">Location</th><th scope="col">Type</th>
            <th scope="col" className="num">Rows</th><th scope="col">Refresh</th><th scope="col">Data API path</th></tr></thead>
          <tbody>{rows.map((r, i) => (
            <tr key={i}><td>{r.layer}</td><td>{r.where}</td><td>{r.type}</td><td className="num">{r.rows}</td>
              <td>{r.refresh}</td><td>{r.api === "-" || r.api === "not exposed" ? <span className="subtle">{r.api}</span>
                : <code>{r.api}</code>}</td></tr>))}</tbody>
        </table>
      </div>
    </section>
  );
}

function GoldSql() {
  return (
    <section className="card arch-card">
      <details className="gold-sql">
        <summary><h2 className="card-title">How the gold views are built</h2>
          <span className="subtle">From the versioned file sql/lakebase_gold.sql (the same file the provisioning
            script applies)</span></summary>
        <div className="gold-tools"><CopyButton text={gold} label="Copy the whole file" /></div>
        {GOLD.map((o) => (
          <details key={`${o.schema}.${o.name}`} className="gold-obj">
            <summary><code>{o.schema}.{o.name}</code> <span className="subtle">{o.kind}{o.exposed ? "" : " · internal"}</span></summary>
            <CodeBlock sql code={fullSql(o)} label={`${o.schema}.${o.name}`} />
          </details>
        ))}
      </details>
    </section>
  );
}

function ApiSnippets({ cfg, mpan, last }: { cfg: ArchConfig | null; mpan: string | null; last: string | null }) {
  const d2 = last ?? "2026-10-08";
  const s = dataApiSnippets(cfg?.data_api_url ?? null, mpan ?? "<mpan>", addDays(d2, -6), d2);
  return (
    <section className="card arch-card" aria-labelledby="api-title">
      <h2 id="api-title" className="card-title">How the app calls the Data API</h2>
      <div className="base-url">
        <span className="subtle">Data API base URL (called by the app server with an OAuth token; append /portal_gold/&lt;view&gt;)</span>
        <div className="base-url-row"><code>{cfg?.data_api_url ?? "not configured"}</code>
          {cfg?.data_api_url && <CopyButton text={cfg.data_api_url} />}</div>
      </div>
      <p className="subtle">The Data API runs each request as the caller's Postgres role: here the app service principal's role,
        created with <code>databricks_create_role</code>, granted to <code>authenticator</code>, with USAGE and SELECT on
        schema <code>portal_gold</code> only.</p>
      <CodeBlock code={s.http} label="HTTP request (one real view)" />
      <CodeBlock code={s.python} label="Python (httpx), like app/hhportal/dataapi.py" />
      <CodeBlock code={s.curl} label="curl" />
    </section>
  );
}

export function ArchitecturePage() {
  const cfg = useLoad("architecture", api.architecture);
  const facts = useLoad("facts", api.facts);
  const links = cfg.data ? buildLinks(cfg.data) : [];
  const groups = ["Unity Catalog", "Lakeflow", "Lakebase", "Source"] as const;
  const f = facts.data;

  return (
    <>
      <div className="page-head"><h1>Architecture</h1></div>

      <section className="card arch-card" aria-labelledby="flow-title">
        <h2 id="flow-title" className="card-title">How the data gets to this portal</h2>
        <ol className="flow">
          {stages(cfg.data).map((s, i) => (
            <li key={s.tag} className="flow-stage">
              <div className={`stage-box stage-${i}`}>
                <span className="stage-tag">{s.tag}</span>
                <span className="stage-title">{s.title}</span>
                <ul>{s.items.map((x) => <li key={x}><code>{x}</code></li>)}</ul>
              </div>
              <p className="stage-caption">{s.caption}</p>
            </li>
          ))}
        </ol>
      </section>

      <div className="arch-grid">
        <section className="card arch-card" aria-labelledby="facts-title">
          <h2 id="facts-title" className="card-title">Live facts (read through the Data API)</h2>
          {facts.error ? <ErrorNote error={facts.error} /> : (
            <div className="facts" aria-busy={facts.loading}>
              <Fact label="Data up to" value={f?.last_date ? fmtFullDay(f.last_date) : "..."} />
              <Fact label="MPANs" value={f ? f.mpans.toLocaleString("en-GB") : "..."} />
              <Fact label="Accounts" value={f ? f.accounts.toLocaleString("en-GB") : "..."} />
              <Fact label="Organisations" value={f ? String(f.orgs) : "..."} />
              <Fact label="Data API time for these calls" value={f ? `${f.latency_ms} ms` : "..."} />
            </div>
          )}
          {f && <DataApiPanel calls={f.data_api} />}
        </section>

        <section className="card arch-card" aria-labelledby="timeline-title">
          <h2 id="timeline-title" className="card-title">How it refreshes (daily job{cfg.data?.daily_schedule
            ? `, ${cfg.data.daily_schedule}` : ""})</h2>
          <ol className="timeline">
            {TIMELINE.map((t) => (
              <li key={t.title}><span className="tl-at">{t.at}</span><span className="tl-title">{t.title}</span>
                <span className="subtle">{t.detail}</span></li>
            ))}
          </ol>
        </section>
      </div>

      <Locations cfg={cfg.data} />
      <GoldSql />
      <ApiSnippets cfg={cfg.data} mpan={f?.sample_mpan ?? null} last={f?.last_date ?? null} />

      <section className="card arch-card" aria-labelledby="links-title">
        <h2 id="links-title" className="card-title">Open in Databricks</h2>
        {cfg.error ? <ErrorNote error={cfg.error} /> : groups.map((g) => (
          <div key={g} className="link-group">
            <h3>{g}</h3>
            <div className="link-grid">{links.filter((l) => l.group === g).map((l) => <LinkTile key={l.id} l={l} />)}</div>
          </div>
        ))}
      </section>
    </>
  );
}
