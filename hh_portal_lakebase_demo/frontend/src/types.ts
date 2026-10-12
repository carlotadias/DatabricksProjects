// Shapes returned by the portal backend (/api/*), which proxies the Lakebase Data API.

export interface DataApiCall { path: string; rows: number | null; status: number | null; ms: number }

export interface Org { org_id: string; org_name: string; is_tpi: boolean; accounts: number; sites: number; mpans: number }

export interface Account {
  account_id: string; account_name: string; org_id: string; org_name: string; is_tpi: boolean; sites: number;
  mpans: number;
}

export interface Meter {
  mpan: string; site_id: string; address_line: string; postcode: string; region: string; account_id: string;
  account_name: string; org_id: string; org_name: string; meter_type: string;
}

export interface Customer { org: Org; accounts: Account[]; mpans: Meter[]; data_api: DataApiCall[] }

export interface UsageRow { t: string; kwh: number; consumption_gbp: number; standing_gbp: number; total_gbp: number }

/** summed = highest half-hour of the scope's total; meter = highest single meter reading (several MPANs, not
 *  half-hourly bars). */
export interface Peak { kwh: number; ts: string | null; mpan: string | null; kind?: "summed" | "meter" }

export interface Summary {
  kwh: number; consumption_gbp: number; standing_gbp: number; total_gbp: number; readings: number;
  avg_kwh_per_reading: number | null; peak: Peak | null; days: number; first: string | null; last: string | null;
}

export type Scope = "org" | "account" | "mpan";

export interface Usage {
  request: { scope: Scope; ids: string[]; granularity: string; start: string; end: string };
  rows: UsageRow[]; summary: Summary; data_api: DataApiCall[];
}

export interface Me { identity: string | null; environment: string; schema: string; data_api_configured: boolean;
  synthetic: boolean }

export interface Freshness { first_date: string | null; last_date: string | null; data_api: DataApiCall[] }

export interface Lookup { kind: "org" | "account"; org_id: string; org_name: string; account_id: string | null;
  account_name: string | null; data_api: DataApiCall[] }

export interface Facts { last_date: string | null; orgs: number; accounts: number; mpans: number; latency_ms: number;
  sample_mpan: string | null;
  data_api: DataApiCall[] }
