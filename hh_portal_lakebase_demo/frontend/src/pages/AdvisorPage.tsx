// Advisor view: choose what to mirror, find a customer by 88/87 number, then the customer's data with a summary,
// granularity tabs, From/To dates, the chart and the readings table. Sessions are for authorised use.
import { useState, type FormEvent } from "react";
import { api, ApiError, useLoad } from "../api";
import { callsOf, ChartSkeleton, DataApiPanel, Empty, ErrorNote, Segmented, TotalUsage, UnitToggle } from "../components/Bits";
import { IconArrowCircle, IconChart, IconInfo, IconSummary, IconUser } from "../components/Icons";
import { MpanDrawer } from "../components/MpanDrawer";
import { download, ReadingsTable } from "../components/ReadingsTable";
import { UsageChart, type Unit } from "../components/UsageChart";
import { addDays, daysBetween } from "../lib/dates";
import { fmtDayTime, fmtFullDay, fmtGbp, fmtKwh } from "../lib/format";
import { clampRange, covered, previousRange, type Bounds, type Granularity, type Range } from "../lib/periods";
import { useUsage } from "../lib/useUsage";
import type { Customer, Lookup, Summary } from "../types";
import { FilterPill, useSelection } from "./CustomerPage";

const GRANULARITIES: { id: Granularity; label: string }[] = [
  { id: "halfhour", label: "Half-hourly" }, { id: "hour", label: "Hourly" }, { id: "day", label: "Daily" },
  { id: "month", label: "Monthly" },
];
// Same caps as the backend (app/hhportal/queries.py MAX_DAYS) so a request is never rejected there.
const MAX_DAYS: Record<Granularity, { org: number; other: number }> = {
  halfhour: { org: 7, other: 31 }, hour: { org: 31, other: 92 }, day: { org: 400, other: 400 },
  month: { org: 400, other: 400 },
};

type Step = "landing" | "find" | "session";

export function AdvisorPage({ bounds, identity }: { bounds: Bounds; identity: string | null }) {
  const [step, setStep] = useState<Step>("landing");
  const [found, setFound] = useState<Lookup | null>(null);
  if (step === "landing") return <Landing onData={() => setStep("find")} />;
  if (step === "find" || !found) {
    return <FindCustomer identity={identity} onFound={(l) => { setFound(l); setStep("session"); }} />;
  }
  return <Session lookup={found} bounds={bounds} identity={identity} onEnd={() => { setFound(null); setStep("find"); }} />;
}

function Landing({ onData }: { onData: () => void }) {
  return (
    <div className="landing">
      <h1 className="landing-title">What would you like to mirror?</h1>
      <div className="option-grid">
        <div className="option-card disabled" aria-disabled="true">
          <span className="option-icon"><IconUser size={20} /></span>
          <h2>Customer account</h2>
          <p>Billing, balance, invoices and account details for this customer.</p>
          <span className="subtle">Not part of this demo</span>
        </div>
        <button type="button" className="option-card" onClick={onData}>
          <span className="option-icon"><IconChart /></span>
          <h2>Half-hourly data</h2>
          <p>Consumption and standing charge data by site and meter.</p>
          <span className="text-link">View data <IconArrowCircle /></span>
        </button>
      </div>
    </div>
  );
}

function Notice({ identity }: { identity: string | null }) {
  return (
    <p className="notice"><IconInfo /><span><strong>Authorised use only.</strong> All mirroring sessions are logged and
      audited. Only access accounts when assisting the customer. Signed in as <strong>{identity ?? "unknown user"}</strong>.
    </span></p>
  );
}

function FindCustomer({ identity, onFound }: { identity: string | null; onFound: (l: Lookup) => void }) {
  const [value, setValue] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    const n = value.trim();
    if (!/^(88|87)\d{6}$/.test(n)) {
      setError("Enter an organisation number (88 + 6 digits) or an account number (87 + 6 digits).");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      onFound(await api.lookup(n));
    } catch (err) {
      setError(err instanceof ApiError && err.status === 404 ? `No customer found for ${n}.` : (err as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="find">
      <p className="crumb">Home ›</p>
      <h1 className="find-title">Half-hourly data</h1>
      <p className="muted">Look up a customer's half-hourly consumption data.</p>
      <form className="find-card" onSubmit={submit} noValidate>
        <h2>Find a customer</h2>
        <p className="muted">Enter an 88 or 87 number.</p>
        <label className="sr-only" htmlFor="customer-number">Customer number</label>
        <input id="customer-number" className="text-input" inputMode="numeric" autoComplete="off" placeholder="Customer number"
          value={value} onChange={(e) => setValue(e.target.value)} aria-invalid={!!error}
          aria-describedby={error ? "find-error" : undefined} />
        {error && <p id="find-error" className="error-text" role="alert">{error}</p>}
        <button type="submit" className="btn btn-navy" disabled={busy}>{busy ? "Starting..." : "Start session"}</button>
      </form>
      <Notice identity={identity} />
    </div>
  );
}

function Session({ lookup, bounds, identity, onEnd }: { lookup: Lookup; bounds: Bounds; identity: string | null;
  onEnd: () => void }) {
  const customer = useLoad(`customer:${lookup.org_id}`, () => api.customer(lookup.org_id));
  return (
    <>
      <div className="session-bar">
        <span>Mirroring <strong>{lookup.kind === "account" ? `${lookup.account_name} (${lookup.account_id})`
          : `${lookup.org_name} (${lookup.org_id})`}</strong></span>
        <button type="button" className="btn btn-outline" onClick={onEnd}>End session</button>
      </div>
      {customer.error ? <ErrorNote error={customer.error} />
        : !customer.data ? <ChartSkeleton />
        : <SessionData customer={customer.data} lookup={lookup} bounds={bounds} />}
      <Notice identity={identity} />
    </>
  );
}

function SummaryPanel({ summary, previous, granularity, unavailable }: { summary: Summary | null;
  previous: Summary | null; granularity: Granularity; unavailable: string | null }) {
  const pct = summary && previous && previous.kwh > 0 ? ((summary.kwh - previous.kwh) / previous.kwh) * 100 : null;
  const days = summary?.days ?? 0;
  return (
    <section className="summary-panel" aria-label="Summary">
      <h2><IconSummary /> Summary</h2>
      <div className="summary-cards">
        <div className="summary-card">
          {summary ? <>Total usage for {days === 1 ? "this day" : "this period"} is <strong>{fmtKwh(summary.kwh)} kWh</strong>
            {unavailable ? "." : pct === null ? ", with no data for the previous period." : <>, {pct >= 0 ? "up" : "down"}
              {" "}{Math.abs(pct).toFixed(1)}% versus the previous {days === 1 ? "day" : "period"}.</>}</>
            : <span className="skeleton skeleton-text" />}
        </div>
        <div className="summary-card">
          {summary?.peak ? <>{summary.peak.kind === "meter" ? "The highest single meter reading was " : "The peak reading was "}
            <strong>{fmtKwh(summary.peak.kwh)} kWh at {summary.peak.ts ? fmtDayTime(summary.peak.ts) : "-"}</strong>
            {summary.peak.kind === "meter" && summary.peak.mpan ? <span className="subtle"> (MPAN {summary.peak.mpan})</span> : null}.</>
            : summary ? "No readings in this period." : <span className="skeleton skeleton-text" />}
        </div>
        <div className="summary-card">
          {summary && summary.avg_kwh_per_reading !== null ? <>Average consumption is <strong>{fmtKwh(summary.avg_kwh_per_reading)}
            {" "}kWh</strong> per reading, with an estimated cost of <strong>{fmtGbp(summary.total_gbp)}</strong> for this period
            {granularity === "month" ? " (whole months)" : ""}.</>
            : summary ? "No readings in this period." : <span className="skeleton skeleton-text" />}
        </div>
      </div>
    </section>
  );
}

function SessionData({ customer, lookup, bounds }: { customer: Customer; lookup: Lookup; bounds: Bounds }) {
  const { groups, mpans, setMpans, selection, setSelection } = useSelection(customer, true, lookup.account_id);
  const [drawer, setDrawer] = useState(false);
  const [granularity, setGranularity] = useState<Granularity>("halfhour");
  const [dates, setDates] = useState<Range>({ start: bounds.last, end: bounds.last });
  const [unit, setUnit] = useState<Unit>("kwh");
  const cap = MAX_DAYS[granularity][selection?.scope === "org" ? "org" : "other"];
  const span = daysBetween(dates.start, dates.end);
  const dateError = dates.start > dates.end ? "From must be on or before To."
    : span > cap ? `${GRANULARITIES.find((g) => g.id === granularity)!.label} data covers at most ${cap} days here.` : null;
  const range = dateError ? null : clampRange(dates, bounds);
  const prev = range ? previousRange("custom", range) : null;
  const { current, previous } = useUsage(selection, range, granularity, prev);
  const unavailable = prev && !covered(prev, bounds) ? "Previous period is outside the 13-month history" : null;
  const rows = current.data?.rows ?? [];
  const caption = range ? (range.start === range.end ? fmtFullDay(range.start) : `${fmtFullDay(range.start)} - ${fmtFullDay(range.end)}`) : "";
  const save = () => download(rows, selection?.label ?? "", granularity, range?.start ?? "", range?.end ?? "");

  return (
    <>
      <div className="page-head">
        <h1>Half-hourly data</h1>
        <FilterPill label={selection?.label ?? "Select MPANs"} onClick={() => setDrawer(true)} />
      </div>
      <SummaryPanel summary={current.data?.summary ?? null} previous={unavailable ? null : previous.data?.summary ?? null}
        granularity={granularity} unavailable={unavailable} />
      <section className="card usage-card" aria-label="Consumption">
        <div className="card-top">
          <Segmented label="Granularity" options={GRANULARITIES} value={granularity} onChange={(g) => {
            setGranularity(g);
            const max = MAX_DAYS[g][selection?.scope === "org" ? "org" : "other"];
            if (daysBetween(dates.start, dates.end) > max) setDates({ start: addDays(dates.end, -(max - 1)), end: dates.end });
          }} />
          <div className="date-fields">
            <label>From<input type="date" value={dates.start} min={bounds.first} max={bounds.last}
              onChange={(e) => e.target.value && setDates({ ...dates, start: e.target.value })} /></label>
            <label>To<input type="date" value={dates.end} min={bounds.first} max={bounds.last}
              onChange={(e) => e.target.value && setDates({ ...dates, end: e.target.value })} /></label>
          </div>
        </div>
        <div className="card-body">
          <div className="usage-head">
            <TotalUsage summary={current.data?.summary ?? null} previous={previous.data?.summary ?? null} unit={unit}
              noun={range && range.start === range.end ? "previous day" : "previous period"} unavailable={unavailable} />
            <UnitToggle unit={unit} onChange={setUnit} />
          </div>
          {dateError ? <Empty>{dateError}</Empty>
            : !selection ? <Empty>Choose MPANs with the filter above.</Empty>
            : current.error ? <ErrorNote error={current.error} />
            : current.loading && !current.data ? <ChartSkeleton />
            : rows.length === 0 ? <Empty>No readings between these dates.</Empty>
            : <UsageChart rows={rows} granularity={granularity} unit={unit}
                label={`${unit === "kwh" ? "Consumption in kWh" : "Cost in GBP"} for ${selection.label}, ${caption}`} />}
        </div>
      </section>
      {rows.length > 0 && <ReadingsTable rows={rows} granularity={granularity} caption={caption} onDownload={save} />}
      <DataApiPanel calls={[...callsOf(current), ...callsOf(previous)]} />
      {drawer && (
        <MpanDrawer groups={groups} org={customer.org} allowOrg initial={mpans} onClose={() => setDrawer(false)}
          onApply={(sel, ids) => { setSelection(sel); setMpans(ids); setDrawer(false); }} />
      )}
    </>
  );
}
