// Display formats. Reading timestamps are clock times of the half-hour (UTC from the Data API).

const GBP = new Intl.NumberFormat("en-GB", { style: "currency", currency: "GBP", minimumFractionDigits: 2,
  maximumFractionDigits: 2 });
const NUM2 = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 2, minimumFractionDigits: 2 });
const NUM0 = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 0 });
const DAY_MONTH = new Intl.DateTimeFormat("en-GB", { day: "2-digit", month: "short", timeZone: "UTC" });
const MONTH_SHORT = new Intl.DateTimeFormat("en-GB", { month: "short", timeZone: "UTC" });
const MONTH_YEAR = new Intl.DateTimeFormat("en-GB", { month: "short", year: "numeric", timeZone: "UTC" });
const FULL_DAY = new Intl.DateTimeFormat("en-GB", { weekday: "long", day: "numeric", month: "long",
  year: "numeric", timeZone: "UTC" });

export function fmtKwh(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "-";
  return Math.abs(v) >= 1000 ? NUM0.format(v) : NUM2.format(v);
}

export function fmtGbp(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "-";
  return GBP.format(v);
}

function asDate(ts: string): Date {
  return new Date(ts.length === 10 ? `${ts}T00:00:00Z` : ts);
}

export function fmtTime(ts: string): string {
  return asDate(ts).toISOString().slice(11, 16);
}

export function fmtDayMonth(ts: string): string {
  return DAY_MONTH.format(asDate(ts));
}

export function fmtDayTime(ts: string): string {
  return `${fmtDayMonth(ts)} ${fmtTime(ts)}`;
}

export function fmtMonth(ts: string): string {
  return MONTH_SHORT.format(asDate(ts));
}

export function fmtMonthYear(ts: string): string {
  return MONTH_YEAR.format(asDate(ts));
}

export function fmtFullDay(ts: string): string {
  return FULL_DAY.format(asDate(ts));
}

/** Table timestamp, e.g. "01 AUG 23:30" (half-hourly), "01 AUG" (daily), "AUG 2026" (monthly). */
export function fmtReadingStamp(ts: string, granularity: "halfhour" | "hour" | "day" | "month"): string {
  if (granularity === "month") return fmtMonthYear(ts).toUpperCase();
  if (granularity === "day") return fmtDayMonth(ts).toUpperCase();
  return fmtDayTime(ts).toUpperCase();
}
