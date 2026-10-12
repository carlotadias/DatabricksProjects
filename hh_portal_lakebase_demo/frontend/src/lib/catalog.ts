// Expected row counts of the Lakebase objects for the default scope (1,000 MPANs in 200 accounts and 20
// organisations, 13 months, about 396 days): directory sizes are exact, daily and monthly counts approximate.
// The live counts of each nightly refresh are in the UC table portal_refresh_log.
export const DEFAULT_SCOPE = "1,000 MPANs, 200 accounts, 20 organisations, 13 months";
export const EXPECTED_ROWS: Record<string, number> = {
  "portal_gold.directory": 1000, "portal_gold.account_directory": 200, "portal_gold.org_directory": 20,
  "portal_gold.daily_mpan": 396000, "portal_gold.daily_account": 79200, "portal_gold.daily_org": 7920,
  "portal_gold.monthly_mpan": 14000, "portal_gold.monthly_account": 2800, "portal_gold.monthly_org": 280,
  "portal_work.hh_account_totals": 3801600,
};
