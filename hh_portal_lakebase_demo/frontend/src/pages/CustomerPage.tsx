// Customer view: period tabs, total usage vs previous, kWh/GBP chart, table and CSV; MPAN selector panel.
import { useMemo, useState } from "react";
import { callsOf, ChartSkeleton, DataApiPanel, Empty, ErrorNote, Segmented, TotalUsage, UnitToggle } from "../components/Bits";
import { IconArrowCircle, IconChevronDown, IconChevronLeft, IconChevronRight, IconFilter } from "../components/Icons";
import { MpanDrawer } from "../components/MpanDrawer";
import { download, ReadingsTable } from "../components/ReadingsTable";
import { UsageChart, type Unit } from "../components/UsageChart";
import { addDays, daysBetween } from "../lib/dates";
import { defaultSelection, groupMeters, resolveSelection, type Selection } from "../lib/mpans";
import {
  canShift, clampRange, comparisonNoun, covered, granularityFor, padRows, periodLabel, periodRange, PERIOD_KINDS,
  previousRange,
  shiftAnchor, type Bounds, type PeriodKind, type Range,
} from "../lib/periods";
import { useUsage } from "../lib/useUsage";
import type { Customer } from "../types";

const CUSTOM_MAX_DAYS = 400;

export function useSelection(customer: Customer, allowOrg: boolean, accountId?: string | null) {
  const groups = useMemo(() => groupMeters(customer.accounts, customer.mpans), [customer]);
  const [mpans, setMpans] = useState<Set<string>>(() => defaultSelection(groups, allowOrg, accountId));
  const resolved = resolveSelection(mpans, groups, customer.org, allowOrg);
  const [selection, setSelection] = useState<Selection | null>("error" in resolved ? null : resolved);
  return { groups, mpans, setMpans, selection, setSelection };
}

export function FilterPill({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <button type="button" className="pill-button" onClick={onClick} aria-haspopup="dialog">
      <IconFilter /><span className="pill-label">{label}</span><IconChevronDown />
    </button>
  );
}

export function CustomerPage({ customer, bounds }: { customer: Customer; bounds: Bounds }) {
  const allowOrg = customer.org.is_tpi; // organisation-level totals are for TPIs only
  const { groups, mpans, setMpans, selection, setSelection } = useSelection(customer, allowOrg);
  const [drawer, setDrawer] = useState(false);
  const [kind, setKind] = useState<PeriodKind>("day");
  const [anchor, setAnchor] = useState(bounds.last);
  const [custom, setCustom] = useState<Range>({ start: addDays(bounds.last, -13), end: bounds.last });
  const [unit, setUnit] = useState<Unit>("kwh");
  const [table, setTable] = useState(false);

  const customError = custom.start > custom.end ? "The start date must be before the end date."
    : daysBetween(custom.start, custom.end) > CUSTOM_MAX_DAYS ? `Choose at most ${CUSTOM_MAX_DAYS} days.` : null;
  const range = kind === "custom" ? (customError ? null : clampRange(custom, bounds))
    : clampRange(periodRange(kind, anchor), bounds);
  const prev = range ? previousRange(kind, range) : null;
  const granularity = range ? granularityFor(kind, range) : "day";
  const { current, previous } = useUsage(selection, range, granularity, prev);
  const noun = comparisonNoun(kind);
  const unavailable = prev && !covered(prev, bounds) ? `${noun[0].toUpperCase()}${noun.slice(1)} is outside the 13-month history` : null;
  const full = kind === "custom" ? range : periodRange(kind, anchor); // the whole calendar period on the axis
  const label = full ? periodLabel(kind === "custom" ? "week" : kind, full) : "";
  const rows = current.data?.rows ?? [];
  const bars = full ? padRows(rows, full, granularity) : rows;

  return (
    <>
      <div className="page-head">
        <h1>Half-hourly data</h1>
        <FilterPill label={selection?.label ?? "Select MPANs"} onClick={() => setDrawer(true)} />
      </div>

      <section className="card usage-card" aria-label="Consumption">
        <div className="card-top">
          <Segmented label="Period" options={PERIOD_KINDS} value={kind} onChange={(k) => { setKind(k); setTable(false); }} />
          {kind === "custom" && (
            <div className="date-fields">
              <label>From<input type="date" value={custom.start} min={bounds.first} max={bounds.last}
                onChange={(e) => e.target.value && setCustom({ ...custom, start: e.target.value })} /></label>
              <label>To<input type="date" value={custom.end} min={bounds.first} max={bounds.last}
                onChange={(e) => e.target.value && setCustom({ ...custom, end: e.target.value })} /></label>
            </div>
          )}
        </div>
        <div className="card-body">
          <div className="usage-head">
            <TotalUsage summary={current.data?.summary ?? null} previous={previous.data?.summary ?? null} unit={unit}
              noun={noun} unavailable={unavailable} />
            <UnitToggle unit={unit} onChange={setUnit} />
          </div>
          {kind !== "custom" ? (
            <div className="date-nav">
              <button type="button" className="icon-button" aria-label={`Previous ${kind}`}
                disabled={!canShift(kind, anchor, -1, bounds)} onClick={() => setAnchor(shiftAnchor(kind, anchor, -1))}>
                <IconChevronLeft /></button>
              <span aria-live="polite">{label}</span>
              <button type="button" className="icon-button" aria-label={`Next ${kind}`}
                disabled={!canShift(kind, anchor, 1, bounds)} onClick={() => setAnchor(shiftAnchor(kind, anchor, 1))}>
                <IconChevronRight /></button>
            </div>
          ) : (
            <div className="date-nav"><span>{customError ?? label}</span></div>
          )}
          {!selection ? <Empty>Choose MPANs with the filter above to see their consumption.</Empty>
            : current.error ? <ErrorNote error={current.error} />
            : current.loading && !current.data ? <ChartSkeleton />
            : rows.length === 0 ? <Empty>No readings for {label}. Try another period.</Empty>
            : <UsageChart rows={bars} granularity={granularity} unit={unit}
                label={`${unit === "kwh" ? "Consumption in kWh" : "Cost in GBP"} for ${selection.label}, ${label}`} />}
        </div>
      </section>
      <div className="card-links">
        <button type="button" className="text-link" onClick={() => setTable((t) => !t)} aria-expanded={table}
          disabled={!rows.length}>{table ? "Hide the table" : "Show as a table"} <IconArrowCircle /></button>
        <button type="button" className="text-link" disabled={!rows.length}
          onClick={() => download(rows, selection?.label ?? "", range?.start ?? "", range?.end ?? "")}>
          Download .CSV <IconArrowCircle /></button>
      </div>
      {table && rows.length > 0 && (
        <ReadingsTable rows={rows} granularity={granularity} caption={label}
          onDownload={() => download(rows, selection?.label ?? "", range?.start ?? "", range?.end ?? "")} />
      )}
      <DataApiPanel calls={[...callsOf(current), ...callsOf(previous)]} />

      {drawer && (
        <MpanDrawer groups={groups} org={customer.org} allowOrg={allowOrg} initial={mpans} onClose={() => setDrawer(false)}
          onApply={(sel, ids) => { setSelection(sel); setMpans(ids); setDrawer(false); }} />
      )}
    </>
  );
}
