// Calendar arithmetic on ISO dates (YYYY-MM-DD), always in UTC so no local time zone shifts a day.

export function parseISO(s: string): Date {
  const [y, m, d] = s.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d));
}

export function toISO(d: Date): string {
  return d.toISOString().slice(0, 10);
}

export function addDays(s: string, n: number): string {
  const d = parseISO(s);
  d.setUTCDate(d.getUTCDate() + n);
  return toISO(d);
}

export function daysInMonth(year: number, month0: number): number {
  return new Date(Date.UTC(year, month0 + 1, 0)).getUTCDate();
}

/** Same day of the target month, clamped to its last day. */
export function addMonths(s: string, n: number): string {
  const d = parseISO(s);
  const total = d.getUTCFullYear() * 12 + d.getUTCMonth() + n;
  const year = Math.floor(total / 12);
  const month0 = total - year * 12;
  const day = Math.min(d.getUTCDate(), daysInMonth(year, month0));
  return toISO(new Date(Date.UTC(year, month0, day)));
}

export function daysBetween(start: string, end: string): number {
  return Math.round((parseISO(end).getTime() - parseISO(start).getTime()) / 86_400_000) + 1;
}

export function minISO(a: string, b: string): string {
  return a < b ? a : b;
}

export function maxISO(a: string, b: string): string {
  return a > b ? a : b;
}
