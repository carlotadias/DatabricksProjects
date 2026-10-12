# Architecture Decision Records

Every non-obvious technical choice of this demo, MADR-lite: Context / Decision / Consequences / Alternatives /
References.

## Index

- [ADR 0001: Gold built inside Lakebase as views and materialized views](#adr-0001-gold-built-inside-lakebase-as-views-and-materialized-views)
- [ADR 0002: The app reads only through the Lakebase Data API](#adr-0002-the-app-reads-only-through-the-lakebase-data-api)
- [ADR 0003: App role created with databricks_create_role, SELECT on portal_gold only](#adr-0003-app-role-created-with-databricks_create_role-select-on-portal_gold-only)
- [ADR 0004: One Postgres schema `portal` for the synced silver and dimensions](#adr-0004-one-postgres-schema-portal-for-the-synced-silver-and-dimensions)
- [ADR 0005: The daily refresh is a job: append, sync, refresh](#adr-0005-the-daily-refresh-is-a-job-append-sync-refresh)
- [ADR 0006: Synthetic data sized from mpan_target, whole organisations, 13 months](#adr-0006-synthetic-data-sized-from-mpan_target-whole-organisations-13-months)
- [ADR 0007: Peaks of summed half-hours for accounts and organisations](#adr-0007-peaks-of-summed-half-hours-for-accounts-and-organisations)
- [ADR 0008: Data API schema-cache reload](#adr-0008-data-api-schema-cache-reload)
- [ADR 0009: Lakebase provisioned by a script, not by bundle resources](#adr-0009-lakebase-provisioned-by-a-script-not-by-bundle-resources)
- [ADR 0010: Bundle variables without environment defaults](#adr-0010-bundle-variables-without-environment-defaults)

---

## ADR 0001: Gold built inside Lakebase as views and materialized views

**Status:** Accepted

### Context
The portal is served from Lakebase through the Data API, and the scenario requires that the gold layer is built in
Lakebase rather than in Unity Catalog (for example because the serving team owns gold in Postgres). Silver remains a
governed Delta table in UC.

### Decision
- UC silver -> one TRIGGERED synced table -> Lakebase replica + indexes -> gold in schema `portal_gold`, defined in a
  single reviewable file, `sql/lakebase_gold.sql` (idempotent, DDL only, checked by `hhportal_data.gold.sanity_issues`:
  no grants or DML, every MV has a UNIQUE index, plain views only in `portal_gold`).
- Plain views for the half-hourly and hourly data (`hh_*`, `hourly_*`): filters on their grouping columns are pushed
  below the GROUP BY, so they read only the requested rows through the `(mpan, reading_date)` index and are always
  fresh.
- Materialized views for the directory (`directory`, `account_directory`, `org_directory`) and the daily and monthly
  aggregates (`daily_*`, `monthly_*`). `portal_work.hh_account_totals` (account x half-hour sums, about 3.8 M rows by
  default) is an internal MV, neither granted nor exposed, refreshed without CONCURRENTLY (only the gold refresh reads
  it).
- `cost_gbp` comes from silver (priced per reading); standing charges come from the tariff valid on each day.
- The directory only lists MPANs that have readings in the replica (`WHERE EXISTS` on its index).

### Consequences
- The nightly refresh recomputes `daily_mpan` and `portal_work.hh_account_totals` in full (each scans about 19 M
  rows), plus the CONCURRENTLY diffs: about 5-10 min per night at up to 4 CU.
- MV data is at most one day old; the plain views are as fresh as the replica.
- Changing an MV definition needs `scripts/provision.py gold --rebuild` (drop and recreate; the portal shows errors
  while it runs).

### Alternatives considered
- **Gold in UC, synced as standard synced tables**: no refresh step at all; preferred when gold may be built in UC.
- **Incremental gold tables** (delete and re-insert the last days in plain tables): cheaper nightly, more Postgres
  code to own; the first optimisation if refresh time matters.
- **pg_ivm (incremental MVs)**: check `pg_available_extensions` on your project; not assumed here.

### References
- [Serve lakehouse data with synced tables](https://docs.databricks.com/aws/en/oltp/projects/sync-tables)

---

## ADR 0002: The app reads only through the Lakebase Data API

**Status:** Accepted

### Context
The goal is to minimise the Postgres knowledge needed in the application: no driver, no connection pool, no SQL.

### Decision
- `app/hhportal` is a FastAPI proxy. Each route validates input (MPAN 13 digits, account `87` + 6, organisation
  `88` + 6, ISO dates, granularity enum, range caps per granularity and scope) and maps to one allow-listed view with
  `select`, `eq` / `in` / `gte` / `lte` / `is` and `order` only.
- Paging at `max_rows` (2000, the Data API `db_max_rows`), at most 10 pages per request.
- The app service principal's OAuth headers come from the Databricks SDK in a worker thread, are cached and are
  refetched once after a 401. The token never appears in logs, errors or responses.
- Every response carries the exact Data API paths it used, shown in the UI ("How this was fetched").
- Multi-MPAN selections (max 20) are summed in the proxy; half-hourly and hourly bars get the day's standing charge
  spread over 48 or 24 slots.

### Consequences
- Every screen is one or two HTTPS GETs; the UI can show exactly what was called.
- Anything the Data API cannot express (for example ad-hoc multi-meter sums) is done in the proxy or in gold.

### Alternatives considered
- **Postgres driver in the app**: more flexible, but brings SQL, pooling and credentials into the app.
- **PostgREST aggregates** for multi-meter sums: would remove the proxy-side sum; not relied on here.

### References
- [Lakebase Data API](https://docs.databricks.com/aws/en/oltp/projects/data-api)

---

## ADR 0003: App role created with databricks_create_role, SELECT on portal_gold only

**Status:** Accepted

### Context
The Data API runs each request as the caller's Postgres role, which must be a member of `authenticator`.

### Decision
- `scripts/provision.py grants` runs, as the project owner: `CREATE EXTENSION IF NOT EXISTS databricks_auth`,
  `databricks_create_role('<app SP client id>', 'SERVICE_PRINCIPAL')` (if the role is absent),
  `GRANT <role> TO authenticator`, `GRANT USAGE ON SCHEMA portal_gold`, `GRANT SELECT ON ALL TABLES IN SCHEMA
  portal_gold`, and default privileges for future gold objects.
- No grants on `portal` or `portal_work`: views and MVs are owned by the deployer and run with the owner's rights.
- The app has **no `postgres` app resource**: a role created that way (or by the role API or UI) cannot be granted to
  `authenticator` (`permission denied to grant role` / `42501 permission denied to set role`).

### Consequences
- The app can read gold and nothing else. Run `grants` after the app exists (it needs the service principal) and
  after the Data API is enabled (it creates `authenticator`).
- If a role was created another way, delete it (`databricks postgres delete-role`) and re-run `grants`.

### Alternatives considered
- **Row-level security and on-behalf-of-user access**: the next step for a real portal (per-customer visibility);
  out of scope for this demo, where the Customer / Advisor switch is a demo control only.

### References
- [Lakebase Data API](https://docs.databricks.com/aws/en/oltp/projects/data-api), [Manage roles](https://docs.databricks.com/aws/en/oltp/projects/manage-roles)

---

## ADR 0004: One Postgres schema `portal` for the synced silver and dimensions

**Status:** Accepted

### Context
Gold joins the readings to the dimensions (MPAN -> site -> account -> organisation, tariffs). The demo must be
self-contained.

### Decision
- Synced tables are created in a regular UC catalog, schema `<catalog>.portal`, which maps to Postgres schema
  `portal`: `silver_hh_readings` (TRIGGERED, PK `(mpan, reading_ts)`) and the dimensions (SNAPSHOT; PKs `org_id`,
  `account_id`, `site_id`, `(tariff_id, valid_from)`, `mpan`).
- Pipeline storage is `<catalog>.<data_schema>`. Lookup indexes on the replica: `(mpan, reading_date)`,
  `(reading_date)`, `mpans(account_id)`, `mpans(org_id)`, `accounts(org_id)`.
- The schema name `portal` is fixed (the gold SQL reads it); the UC data schema is a variable.

### Consequences
- No separate Lakebase UC catalog is needed (that would need metastore `CREATE CATALOG`).
- Dimension changes need a re-sync of the SNAPSHOT tables (the demo dimensions are static).

### Alternatives considered
- **A registered Lakebase database catalog**: also valid; needs more privileges.

---

## ADR 0005: The daily refresh is a job: append, sync, refresh

**Status:** Accepted

### Context
Postgres materialized views do not refresh themselves, and the refresh must only run after the sync has succeeded.

### Decision
- `hh_portal_daily` (PAUSED by default) has three dependent tasks: `append_day` (simulated arrivals, see ADR 0006),
  `sync_silver` (reads `status.pipeline_id` from `GET /api/2.0/postgres/synced_tables/<name>`, runs
  `pipelines.start_update(full_refresh=False)` and waits for `COMPLETED`, or for the update already running),
  `refresh_gold` (psycopg as the job identity, the MV owner, with a token from
  `postgres.generate_database_credential`; `REFRESH ... CONCURRENTLY` per gold MV in dependency order, `count(*)`
  each, `ANALYZE`; stops at the first failure).
- Every step logs `(run_id, step, started, ended, rows, status, detail)` to UC `portal_refresh_log` (no data rows).
- The job runs as the deployer, who owns the gold objects (REFRESH needs the owner).

### Consequences
- If the sync fails, gold is not refreshed and keeps the previous day's complete numbers.
- A same-day re-run appends nothing (data never runs ahead of yesterday).

### Alternatives considered
- **pg_cron** (in-database schedule): cannot wait for the sync; not assumed to be available.

---

## ADR 0006: Synthetic data sized from mpan_target, whole organisations, 13 months

**Status:** Accepted

### Context
The demo needs realistic-looking data quickly and cheaply, with every customer complete (all of its meters).

### Decision
- `mpan_target` (default 1,000) sets the hierarchy: about 50 MPANs per organisation, 5 per account and 1.67 per site
  (default: 20 organisations, 200 accounts, 600 sites, 1,000 MPANs, 40 tariffs with two versions). Children map to
  skewed parents, and every parent gets at least one child, so all MPANs belong to whole organisations.
- Dimensions are Spark SQL over `range()`; the readings (`history_months`, default 13, up to yesterday: about 19 M
  rows) are generated with [dbldatagen](https://github.com/databrickslabs/dbldatagen) straight into silver.
- The daily `append_day` reuses the generator's load-shape expression and recovers each MPAN's index from the MPAN
  bijection, so every meter keeps its scale; its noise and estimated flag are deterministic hashes (dbldatagen randoms
  are not reproducible per row). Idempotent: a present day is never appended again (date check plus `NOT EXISTS`).
- The setup job skips when silver already holds rows; the job parameter `rebuild=true` regenerates everything (then re-create the
  synced tables, see the runbook).

### Consequences
- No scope selection step is needed: the whole generated population is the demo.

### Alternatives considered
- **A larger population with a selected subset**: closer to a benchmark set-up; more data to generate and store.

---

## ADR 0007: Peaks of summed half-hours for accounts and organisations

**Status:** Accepted

### Context
"Peak" must mean the same thing at every level of the hierarchy.

### Decision
- Peak = the highest half-hourly consumption of the scope with its timestamp: for an MPAN its own reading; for an
  account or organisation the highest SUM over its MPANs at one half-hour, from `portal_work.hh_account_totals`
  (`DISTINCT ON (key, reading_date) ... ORDER BY kwh DESC, reading_ts`; ties: the earliest). `peak_mpan` is NULL there.
- A month's peak is the highest of its daily peaks (exact, as every half-hour belongs to one day).
- For a multi-MPAN selection the proxy takes the peak of the summed half-hourly bars when the bars are half-hourly;
  otherwise it shows the highest single meter reading and labels it so (`peak.kind = "meter"`).

### Consequences
- One extra internal MV (`hh_account_totals`) is refreshed nightly.

---

## ADR 0008: Data API schema-cache reload

**Status:** Accepted (UI fallback documented)

### Context
After new views are created, the Data API must reload its schema cache. The documentation describes a UI button
(Lakebase UI > Data API > Refresh schema cache).

### Decision
- `gold` ends with `NOTIFY pgrst, 'reload schema'` (the PostgREST reload channel; harmless if not listened to).
- `data-api` always re-applies the configuration with `databricks postgres update-data-api <db>/data-api
  spec.db_schemas`, which also refreshes the cache.
- If the portal still gets 404 for a `portal_gold` view, press the UI button once.

### Consequences
- Daily refreshes change no schema, so no reload is needed after them.

---

## ADR 0009: Lakebase provisioned by a script, not by bundle resources

**Status:** Accepted

### Context
Recent CLI versions offer bundle resources for Lakebase (`postgres_projects`, `postgres_synced_tables`, ...). The
demo also needs ordering (silver must exist and hold data before the TRIGGERED synced table is created), a temporary
8 CU boost for the initial snapshot, SQL (indexes, gold, grants via `databricks_create_role`) and Data API
configuration.

### Decision
`scripts/provision.py` drives the CLI and psycopg: idempotent steps, `--dry-run`, an `outputs` step that prints the
bundle variable values, and a `teardown` step that asks for confirmation. The bundle owns the jobs, the app and the UC
data schema.

### Consequences
- Two tools to run (bundle and script), in the order of the runbook.
- `bundle destroy` never deletes the Lakebase project by accident.

### Alternatives considered
- **Bundle Lakebase resources**: attractive for the project and synced tables once the ordering and SQL steps are
  handled elsewhere; a possible next step.

---

## ADR 0010: Bundle variables without environment defaults

**Status:** Accepted

### Context
The project must deploy into any workspace and contain no workspace hosts, IDs or personal names.

### Decision
- Every environment value is a variable with no real default (`catalog`, `data_schema`, `app_name_prefix`,
  `lakebase_project_id`, `lakebase_data_api_url`; optional uids and pipeline id for links default to empty). Generic
  defaults only: branch `production`, endpoint `primary`, schedule, `mpan_target` 1000, `history_months` 13.
- Values go in the gitignored `.databricks/bundle/<target>/variable-overrides.json` (example shipped) or `--var`.
- The workspace URL in the app comes from `DATABRICKS_HOST`, set by Databricks Apps at runtime.
- No `mode: development`: it would prefix the UC schema resource name that the jobs and the app reference.

### References
- [Bundle variables](https://docs.databricks.com/aws/en/dev-tools/bundles/variables)
