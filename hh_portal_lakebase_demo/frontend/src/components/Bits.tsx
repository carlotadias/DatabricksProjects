// Small shared pieces: segmented tabs, unit toggle, change chip, states, Data API panel, total usage.
import type { ReactNode } from "react";
import { ApiError, type Loadable } from "../api";
import { changeChip, pctChange } from "../lib/compare";
import { fmtGbp, fmtKwh } from "../lib/format";
import type { DataApiCall, Summary } from "../types";
import { IconDown, IconUp } from "./Icons";
import type { Unit } from "./UsageChart";

export function Segmented<T extends string>({ options, value, onChange, label, small }: {
  options: { id: T; label: string }[]; value: T; onChange: (v: T) => void; label: string; small?: boolean }) {
  return (
    <div className={`segmented${small ? " segmented-small" : ""}`} role="tablist" aria-label={label}>
      {options.map((o) => (
        <button key={o.id} type="button" role="tab" aria-selected={value === o.id}
          className={value === o.id ? "active" : ""} onClick={() => onChange(o.id)}>{o.label}</button>
      ))}
    </div>
  );
}

export function UnitToggle({ unit, onChange }: { unit: Unit; onChange: (u: Unit) => void }) {
  return (
    <div className="unit-toggle" role="group" aria-label="Show values in">
      <button type="button" aria-pressed={unit === "kwh"} className={unit === "kwh" ? "active" : ""}
        onClick={() => onChange("kwh")}>kWh</button>
      <button type="button" aria-pressed={unit === "gbp"} className={unit === "gbp" ? "active" : ""}
        onClick={() => onChange("gbp")} aria-label="Pounds sterling">£</button>
    </div>
  );
}

export function ChangeChip({ current, previous, noun, unavailable }: { current: number; previous: number | null;
  noun: string; unavailable?: string | null }) {
  if (unavailable) return <span className="chip chip-muted">{unavailable}</span>;
  if (previous === null) return null;
  const chip = changeChip(pctChange(current, previous), noun);
  if (!chip) return <span className="chip chip-muted">No data for the {noun}</span>;
  return (
    <span className={`chip chip-${chip.direction}`}>
      {chip.direction === "up" ? <IconUp /> : chip.direction === "down" ? <IconDown /> : null}
      <span>{chip.text}</span>
    </span>
  );
}

export function TotalUsage({ summary, previous, unit, noun, unavailable }: { summary: Summary | null;
  previous: Summary | null; unit: Unit; noun: string; unavailable?: string | null }) {
  const cur = summary ? (unit === "kwh" ? summary.kwh : summary.total_gbp) : null;
  const prev = previous ? (unit === "kwh" ? previous.kwh : previous.total_gbp) : null;
  return (
    <div className="total-usage">
      <div className="total-label">Total usage</div>
      <div className="total-row">
        <span className="total-value">
          {cur === null ? <span className="skeleton skeleton-text" /> : unit === "kwh" ? fmtKwh(cur) : fmtGbp(cur)}
        </span>
        <span className="total-unit">{unit === "kwh" ? "kWh" : "total incl. standing charge"}</span>
        {cur !== null && <ChangeChip current={cur} previous={prev} noun={noun} unavailable={unavailable} />}
      </div>
    </div>
  );
}

export function ErrorNote({ error, onRetry }: { error: Error; onRetry?: () => void }) {
  return (
    <div className="state state-error" role="alert">
      <strong>We could not load this data.</strong> <span>{error.message}</span>
      {onRetry && <button type="button" className="link-button" onClick={onRetry}>Try again</button>}
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="state state-empty">{children}</div>;
}

export function ChartSkeleton() {
  return <div className="skeleton skeleton-chart" aria-label="Loading chart" role="status" />;
}

/** Data API calls behind a response, or behind the error (the failed path is shown too). */
export function callsOf(l: Loadable<{ data_api: DataApiCall[] }>): DataApiCall[] {
  if (l.data) return l.data.data_api;
  return l.error instanceof ApiError ? l.error.dataApi : [];
}

export function DataApiPanel({ calls }: { calls: DataApiCall[] }) {
  if (!calls.length) return null;
  const ms = calls.reduce((s, c) => s + (c.ms || 0), 0);
  return (
    <details className="dev-panel">
      <summary>How this was fetched: {calls.length} Lakebase Data API GET{calls.length > 1 ? "s" : ""}, {ms} ms</summary>
      <p>
        Each call is an HTTPS GET to the Data API URL plus the path below, with the app's OAuth token. The app sends no
        SQL: filters (<code>eq</code>, <code>in</code>, <code>gte</code>, <code>lte</code>) and ordering are URL
        parameters on views in schema <code>portal_gold</code>.
      </p>
      <ul>
        {calls.map((c, i) => (
          <li key={i}><code>GET {c.path}</code> <span className="subtle">{c.status ?? "no answer"} · {c.rows ?? "-"} rows ·
            {" "}{c.ms} ms</span></li>
        ))}
      </ul>
    </details>
  );
}
