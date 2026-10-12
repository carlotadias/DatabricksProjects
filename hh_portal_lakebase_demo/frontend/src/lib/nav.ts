// Sidebar navigation state from the URL hash (#/half-hourly, #/overview, ...). Only half-hourly data is built.

export const PAGES = [
  { id: "overview", label: "Overview" },
  { id: "meter-readings", label: "Meter readings" },
  { id: "invoices", label: "Invoices" },
  { id: "energy-reports", label: "Energy reports" },
  { id: "contract-tariff", label: "Contract & tariff" },
  { id: "half-hourly", label: "Half-hourly data" },
  { id: "architecture", label: "Architecture" },
] as const;

export type PageId = (typeof PAGES)[number]["id"];
export const HOME: PageId = "half-hourly";

export function pageFromHash(hash: string): PageId {
  const id = hash.replace(/^#\/?/, "").split(/[/?]/)[0];
  return (PAGES.find((p) => p.id === id)?.id ?? HOME) as PageId;
}

export function hrefFor(id: PageId): string {
  return `#/${id}`;
}

export function pageLabel(id: PageId): string {
  return PAGES.find((p) => p.id === id)!.label;
}

/** Organisation switcher filter: name or number, case-insensitive; TPIs first, then by name. */
export function filterOrgs<T extends { org_id: string; org_name: string; is_tpi: boolean }>(orgs: T[], q: string): T[] {
  const s = q.trim().toLowerCase();
  return orgs.filter((o) => !s || o.org_name.toLowerCase().includes(s) || o.org_id.includes(s))
    .sort((a, b) => Number(b.is_tpi) - Number(a.is_tpi) || a.org_name.localeCompare(b.org_name));
}
