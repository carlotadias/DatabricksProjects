// Current and previous period from the backend (two calls; the previous one only needs the daily summary).
import { api, useLoad } from "../api";
import type { Selection } from "./mpans";
import type { Granularity, Range } from "./periods";

export function useUsage(sel: Selection | null, range: Range | null, granularity: Granularity, prev: Range | null) {
  const base = sel && range ? `${sel.scope}:${sel.ids.join(",")}` : null;
  const current = useLoad(base && range ? `${base}|${granularity}|${range.start}|${range.end}` : null,
    () => api.usage({ scope: sel!.scope, ids: sel!.ids, granularity, start: range!.start, end: range!.end }));
  const prevGranularity = granularity === "month" ? "month" : "day";
  const previous = useLoad(base && prev ? `${base}|${prevGranularity}|${prev.start}|${prev.end}` : null,
    () => api.usage({ scope: sel!.scope, ids: sel!.ids, granularity: prevGranularity, start: prev!.start,
      end: prev!.end }));
  return { current, previous };
}
