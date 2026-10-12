// Portal backend calls. The backend proxies allow-listed Lakebase Data API GETs and returns the paths it used.
import { useEffect, useState } from "react";
import type { ArchConfig } from "./lib/links";
import type { Customer, DataApiCall, Facts, Freshness, Lookup, Me, Org, Scope, Usage } from "./types";

export class ApiError extends Error {
  status: number;
  category: string;
  dataApi: DataApiCall[];
  constructor(message: string, status: number, category: string, dataApi: DataApiCall[] = []) {
    super(message);
    this.status = status;
    this.category = category;
    this.dataApi = dataApi;
  }
}

async function get<T>(path: string, params: Record<string, string> = {}): Promise<T> {
  const qs = new URLSearchParams(params).toString();
  const res = await fetch(qs ? `${path}?${qs}` : path, { headers: { Accept: "application/json" } });
  let body: unknown = null;
  try {
    body = await res.json();
  } catch {
    body = null;
  }
  if (!res.ok) {
    const b = (body ?? {}) as Record<string, unknown>;
    const d = (b.detail && typeof b.detail === "object" ? b.detail : b) as Record<string, unknown>;
    const errors = Array.isArray(b.errors) ? (b.errors as string[]).join(" ") : null;
    const message = errors ?? (typeof d.message === "string" ? d.message : `Request failed (${res.status})`);
    throw new ApiError(message, res.status, typeof d.error === "string" ? d.error : errors ? "invalid" : "http",
      Array.isArray(d.data_api) ? (d.data_api as DataApiCall[]) : []);
  }
  return body as T;
}

export interface UsageQuery { scope: Scope; ids: string[]; granularity: string; start: string; end: string }

export const api = {
  me: () => get<Me>("/api/me"),
  architecture: () => get<ArchConfig>("/api/architecture"),
  facts: () => get<Facts>("/api/facts"),
  freshness: () => get<Freshness>("/api/freshness"),
  customers: () => get<{ customers: Org[]; data_api: DataApiCall[] }>("/api/customers"),
  customer: (orgId: string) => get<Customer>("/api/customer", { org_id: orgId }),
  lookup: (n: string) => get<Lookup>("/api/lookup", { number: n }),
  usage: (q: UsageQuery) => get<Usage>("/api/usage", { scope: q.scope, ids: q.ids.join(","),
    granularity: q.granularity, start: q.start, end: q.end }),
};

export interface Loadable<T> { data: T | null; error: ApiError | Error | null; loading: boolean }

/** Run `fn` whenever `key` changes (null = idle); stale responses are ignored. */
export function useLoad<T>(key: string | null, fn: () => Promise<T>): Loadable<T> {
  const [state, setState] = useState<Loadable<T>>({ data: null, error: null, loading: key !== null });
  useEffect(() => {
    if (key === null) {
      setState({ data: null, error: null, loading: false });
      return;
    }
    let live = true;
    setState((s) => ({ data: s.data, error: null, loading: true }));
    fn().then((data) => live && setState({ data, error: null, loading: false }),
      (error: Error) => live && setState({ data: null, error, loading: false }));
    return () => { live = false; };
  }, [key]);
  return state;
}
