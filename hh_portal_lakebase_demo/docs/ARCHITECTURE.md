# HH portal on Lakebase (demo) - Architecture

## Overview

A half-hourly energy consumption portal whose gold layer lives inside Lakebase Postgres and is read only through the
Lakebase Data API. Silver stays governed in Unity Catalog; Lakebase holds a synced, read-only replica of it plus the
gold views and materialized views that the portal reads. All data is synthetic.

## Component and data flow

```
Unity Catalog                          Lakebase Postgres (project <lakebase_project_id>, branch production)
<catalog>.<data_schema>                database databricks_postgres
  organisations, accounts, sites,  --SNAPSHOT sync (once)-->  portal.organisations ... portal.tariffs
  mpans, tariffs                                              |
  silver_hh_readings (CDF on)     --TRIGGERED sync (daily)--> portal.silver_hh_readings (+ indexes)
        ^                                                     |
        | append_day (daily job)                              v
                                                    portal_work.hh_account_totals (internal MV)
                                                              |
                                                              v
                                       portal_gold: 6 views + 9 materialized views (unique indexes)
                                                              |
                                              Lakebase Data API (exposes portal_gold only)
                                                              |
                                        HTTPS GET + OAuth token of the app service principal
                                                              v
                                         Databricks App "Energy portal (demo)" (FastAPI + React)
```

## Components

| Component | Purpose | Technology |
|---|---|---|
| `hh_portal_setup` job | Generates the synthetic hierarchy (about 1,000 MPANs in whole organisations) and 13 months of half-hourly readings into UC silver | Serverless job, Spark SQL, dbldatagen |
| UC silver `silver_hh_readings` | Governed source of the readings: PK `(mpan, reading_ts)`, `CLUSTER BY (mpan, reading_date)`, Change Data Feed on | Delta table |
| Synced tables (`<catalog>.portal.*`) | Managed UC to Postgres sync: dimensions SNAPSHOT (once), silver TRIGGERED (incremental, via CDF) | Lakebase synced tables, Lakeflow pipelines |
| `sql/lakebase_gold.sql` | Gold: plain views (half-hourly, hourly) and materialized views (directory, daily, monthly) with unique indexes | Postgres views and materialized views |
| `hh_portal_daily` job | append_day, then sync_silver, then refresh_gold; logs to `portal_refresh_log` | Serverless job, Databricks SDK, psycopg |
| `scripts/provision.py` | Idempotent provisioning of the Lakebase side and teardown | Databricks CLI, psycopg |
| Lakebase Data API | PostgREST-compatible REST over `portal_gold`; each request runs as the caller's Postgres role | Lakebase Data API |
| App backend (`app/hhportal`) | Validates input, maps each UI request to one or two allow-listed GETs, sums multi-meter selections, reports the exact Data API paths used | FastAPI, httpx |
| App UI (`frontend/`) | Customer view (Day / Week / Month / Year / Custom, kWh / GBP, comparison, CSV), advisor view (lookup, summary, readings), Architecture page | React, TypeScript, recharts, Vite |

## Runtime environment

- **Workspace / profile:** yours; the bundle has no host and no identifiers (variables only).
- **Catalog / schemas:** `<catalog>.<data_schema>` (bundle-managed), `<catalog>.portal` (synced tables; the name is
  also the Postgres schema that `sql/lakebase_gold.sql` reads, so it is fixed).
- **Compute:** serverless jobs; Lakebase autoscaling 1-4 CU with scale to zero (8 CU only during the initial
  snapshot and first gold build); one Databricks App.
- **Security:** see the README "Security model" and [ADR 0003](ADR.md#adr-0003-app-role-created-with-databricks_create_role-select-on-portal_gold-only).

## Synced tables, materialized views, and who refreshes what

| Layer | Object | What it is | Kept up to date by |
|---|---|---|---|
| Silver in UC | `<catalog>.<data_schema>.silver_hh_readings` | Delta table, 13 months, Change Data Feed on | Your ingestion. In the demo, the daily job's `append_day` task simulates the next day's readings |
| Silver in Lakebase | `portal.silver_hh_readings` (UC entry `<catalog>.portal.silver_hh_readings`) | A standard Lakebase **synced table** (managed UC to Postgres sync) in TRIGGERED mode: incremental via the Change Data Feed | The synced table's own managed pipeline. A TRIGGERED synced table only updates when triggered, so the daily job triggers it |
| Dimensions in Lakebase | `portal.organisations`, `accounts`, `sites`, `mpans`, `tariffs` | Synced tables in SNAPSHOT mode | Synced once (the demo dimensions are static); re-sync if you change them |
| Gold in Lakebase | `portal_gold.*` (+ internal `portal_work.hh_account_totals`) | **Postgres views and materialized views** created inside Lakebase on top of the synced tables. Not synced tables | Postgres, when told to: the daily job runs `REFRESH MATERIALIZED VIEW [CONCURRENTLY]` |

Why gold is not a synced table: a synced table copies a table that already exists in Unity Catalog. This demo shows
the pattern where the gold layer is built inside Lakebase instead (for example when the serving team owns gold in
Postgres). Postgres materialized views do not refresh themselves, so the job calls the refresh, and only after the
sync has succeeded; if the sync fails, gold keeps yesterday's complete numbers. Plain views (`hh_*`, `hourly_*`)
need no refresh: they read the replica directly and are as fresh as it is.

If gold can be built in UC in your case, sync it as standard synced tables instead and the `refresh_gold` step
disappears ([ADR 0001](ADR.md#adr-0001-gold-built-inside-lakebase-as-views-and-materialized-views)).

## Data model

- Identifiers: organisation `88` + 6 digits, account `87` + 6 digits, MPAN 13 digits, site `ST` + 7 digits; all are
  bijections of a row index, so they are unique by construction and reproducible.
- Every fifth organisation is a TPI (third-party intermediary): TPI customers (and advisors) can see organisation
  totals.
- Load shape: per-MPAN scale and base load, business-hours peak, weekend dip, winter uplift, multiplicative noise;
  2.5 % of readings flagged as estimated. Costs use the tariff version valid on the day (two versions per tariff);
  standing charges are per MPAN per day.
- Peaks: for an MPAN its highest half-hourly reading; for an account or organisation the highest **sum** over its
  MPANs at one half-hour ([ADR 0007](ADR.md#adr-0007-peaks-of-summed-half-hours-for-accounts-and-organisations)).
