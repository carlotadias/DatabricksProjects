// MPAN selector: meters grouped by account, search, and the selection -> Data API scope.
import type { Account, Meter, Org, Scope } from "../types";

export const MAX_MPANS = 20;

export interface AccountGroup { account: Account; address: string; meters: Meter[] }
export interface Selection { scope: Scope; ids: string[]; label: string }

export function meterAddress(m: Pick<Meter, "address_line" | "postcode">): string {
  return [m.address_line, m.postcode].filter(Boolean).join(", ");
}

export function groupMeters(accounts: Account[], meters: Meter[]): AccountGroup[] {
  const byAccount = new Map<string, Meter[]>();
  for (const m of meters) {
    const list = byAccount.get(m.account_id) ?? [];
    list.push(m);
    byAccount.set(m.account_id, list);
  }
  return accounts.map((a) => {
    const ms = byAccount.get(a.account_id) ?? [];
    return { account: a, address: ms.length ? meterAddress(ms[0]) : "", meters: ms };
  }).filter((g) => g.meters.length > 0);
}

function norm(s: string): string {
  return s.toLowerCase().replace(/\s+/g, "");
}

/** Matching accounts keep all their meters; otherwise only the meters whose address, postcode or MPAN match. */
export function filterGroups(groups: AccountGroup[], query: string): AccountGroup[] {
  const q = norm(query);
  if (!q) return groups;
  const out: AccountGroup[] = [];
  for (const g of groups) {
    if (norm(`${g.account.account_name}${g.account.account_id}`).includes(q)) {
      out.push(g);
      continue;
    }
    const meters = g.meters.filter((m) => norm(`${m.mpan}${m.address_line}${m.postcode}${m.site_id}`).includes(q));
    if (meters.length) out.push({ ...g, meters });
  }
  return out;
}

/** Organisation totals for TPIs (and advisors), one whole account, or up to MAX_MPANS meters. */
export function resolveSelection(selected: ReadonlySet<string>, groups: AccountGroup[], org: Org,
  allowOrg: boolean): Selection | { error: string } {
  if (selected.size === 0) return { error: "Select at least one MPAN." };
  const all = groups.flatMap((g) => g.meters.map((m) => m.mpan));
  if (allowOrg && all.length > 0 && all.every((m) => selected.has(m)) && selected.size === all.length) {
    return { scope: "org", ids: [org.org_id], label: `${org.org_name} - all sites` };
  }
  for (const g of groups) {
    if (g.meters.length === selected.size && g.meters.every((m) => selected.has(m.mpan))) {
      return { scope: "account", ids: [g.account.account_id], label: g.account.account_name };
    }
  }
  if (selected.size > MAX_MPANS) {
    return { error: `Select one account or up to ${MAX_MPANS} MPANs${allowOrg ? " (or all MPANs)" : ""}.` };
  }
  const ids = all.filter((m) => selected.has(m));
  if (ids.length === 1) {
    const m = groups.flatMap((g) => g.meters).find((x) => x.mpan === ids[0])!;
    return { scope: "mpan", ids, label: meterAddress(m) || `MPAN ${m.mpan}` };
  }
  return { scope: "mpan", ids, label: `${ids.length} MPANs` };
}

export function defaultSelection(groups: AccountGroup[], allowOrg: boolean, accountId?: string | null): Set<string> {
  if (accountId) {
    const g = groups.find((x) => x.account.account_id === accountId);
    if (g) return new Set(g.meters.map((m) => m.mpan));
  }
  if (allowOrg) return new Set(groups.flatMap((g) => g.meters.map((m) => m.mpan)));
  return new Set(groups[0]?.meters.map((m) => m.mpan) ?? []);
}
