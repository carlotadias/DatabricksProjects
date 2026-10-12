// "x% vs previous ..." from two Data API calls (current and previous period).

export type Direction = "up" | "down" | "flat";

export function pctChange(current: number, previous: number): number | null {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) return null;
  return ((current - previous) / previous) * 100;
}

export function changeChip(pct: number | null, noun: string): { text: string; direction: Direction } | null {
  if (pct === null) return null;
  const rounded = Math.round(Math.abs(pct));
  const direction: Direction = rounded === 0 ? "flat" : pct > 0 ? "up" : "down";
  return { text: `${rounded}% vs ${noun}`, direction };
}
