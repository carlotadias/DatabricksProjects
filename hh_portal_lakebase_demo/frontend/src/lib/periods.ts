// Period tabs (Day / Week / Month / Year / Custom), prev/next navigation and the comparison period.
import { addDays, addMonths, daysBetween, daysInMonth, maxISO, minISO, parseISO } from "./dates";

export type PeriodKind = "day" | "week" | "month" | "year" | "custom";
export type Granularity = "halfhour" | "hour" | "day" | "month";
export interface Range { start: string; end: string }
export interface Bounds { first: string; last: string }

export const PERIOD_KINDS: { id: PeriodKind; label: string }[] = [
  { id: "day", label: "Day" }, { id: "week", label: "Week" }, { id: "month", label: "Month" },
  { id: "year", label: "Year" }, { id: "custom", label: "Custom" },
];
export const CUSTOM_DAILY_MAX_DAYS = 92;

/** The full calendar period containing the anchor date (weeks start on Monday). */
export function periodRange(kind: Exclude<PeriodKind, "custom">, anchor: string): Range {
  const d = parseISO(anchor);
  if (kind === "day") return { start: anchor, end: anchor };
  if (kind === "week") {
    const offset = (d.getUTCDay() + 6) % 7; // Monday = 0
    const start = addDays(anchor, -offset);
    return { start, end: addDays(start, 6) };
  }
  if (kind === "month") {
    const start = anchor.slice(0, 8) + "01";
    const last = daysInMonth(d.getUTCFullYear(), d.getUTCMonth());
    return { start, end: anchor.slice(0, 8) + String(last).padStart(2, "0") };
  }
  const y = anchor.slice(0, 4);
  return { start: `${y}-01-01`, end: `${y}-12-31` };
}

/** Clamp a range to the data window; null when they do not overlap. */
export function clampRange(r: Range, b: Bounds): Range | null {
  const start = maxISO(r.start, b.first);
  const end = minISO(r.end, b.last);
  return start <= end ? { start, end } : null;
}

export function shiftAnchor(kind: PeriodKind, anchor: string, dir: -1 | 1): string {
  if (kind === "day") return addDays(anchor, dir);
  if (kind === "week") return addDays(anchor, 7 * dir);
  if (kind === "month") return addMonths(anchor, dir);
  if (kind === "year") return addMonths(anchor, 12 * dir);
  return anchor;
}

export function canShift(kind: Exclude<PeriodKind, "custom">, anchor: string, dir: -1 | 1, b: Bounds): boolean {
  return clampRange(periodRange(kind, shiftAnchor(kind, anchor, dir)), b) !== null;
}

/** The comparison period: the same span one period earlier (a partial month compares with the same days of
 *  the previous month); for Custom, the same number of days immediately before. */
export function previousRange(kind: PeriodKind, r: Range): Range {
  if (kind === "custom") {
    const n = daysBetween(r.start, r.end);
    return { start: addDays(r.start, -n), end: addDays(r.start, -1) };
  }
  if (kind === "day") return { start: addDays(r.start, -1), end: addDays(r.end, -1) };
  if (kind === "week") return { start: addDays(r.start, -7), end: addDays(r.end, -7) };
  const months = kind === "month" ? 1 : 12;
  return { start: addMonths(r.start, -months), end: addMonths(r.end, -months) };
}

export function granularityFor(kind: PeriodKind, r: Range): Granularity {
  if (kind === "day") return "halfhour";
  if (kind === "week" || kind === "month") return "day";
  if (kind === "year") return "month";
  return daysBetween(r.start, r.end) <= CUSTOM_DAILY_MAX_DAYS ? "day" : "month";
}

export function comparisonNoun(kind: PeriodKind): string {
  return kind === "custom" ? "previous period" : `previous ${kind}`;
}

const LONG = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" });
const MONTH = new Intl.DateTimeFormat("en-GB", { month: "long", year: "numeric", timeZone: "UTC" });

export function periodLabel(kind: PeriodKind, r: Range): string {
  if (kind === "day") return LONG.format(parseISO(r.start));
  if (kind === "month") return MONTH.format(parseISO(r.start));
  if (kind === "year") return r.start.slice(0, 4);
  return `${LONG.format(parseISO(r.start))} - ${LONG.format(parseISO(r.end))}`;
}

/** Whole period inside the data window (else "vs previous" would compare with missing days). */
export function covered(r: Range, b: Bounds): boolean {
  return r.start >= b.first && r.end <= b.last;
}

export interface Bar { t: string; kwh: number; consumption_gbp: number; standing_gbp: number; total_gbp: number;
  missing?: boolean }

/** Keep the whole calendar period on the axis (daily or monthly bars): days or months without data yet are
 *  empty slots (missing), as on a bill-style chart. Half-hourly and hourly rows are returned as they are. */
export function padRows(rows: Bar[], full: Range, granularity: Granularity): Bar[] {
  if (granularity !== "day" && granularity !== "month") return rows;
  const have = new Map(rows.map((r) => [r.t.slice(0, 10), r]));
  const keys: string[] = [];
  if (granularity === "day") {
    for (let d = full.start; d <= full.end; d = addDays(d, 1)) keys.push(d);
  } else {
    for (let m = full.start.slice(0, 8) + "01"; m <= full.end; m = addMonths(m, 1)) keys.push(m);
  }
  return keys.map((k) => have.get(k) ?? { t: k, kwh: 0, consumption_gbp: 0, standing_gbp: 0, total_gbp: 0, missing: true });
}
