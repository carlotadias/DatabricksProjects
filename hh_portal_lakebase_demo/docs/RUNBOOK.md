# HH portal on Lakebase (demo) - Runbook

Ordered steps to deploy the demo into your workspace, with durations (estimates) and expected output. Steps 3 onward
create billable resources.

```bash
cd hh_portal_lakebase_demo
P=<PROFILE>                       # Databricks CLI profile of your workspace
CAT=<CATALOG>                     # existing UC catalog
DS=hh_portal_demo                 # new UC schema for the data (bundle var data_schema)
PROJ=hh-portal-demo               # Lakebase project id (bundle var lakebase_project_id)
APP=hh-portal-dev                 # <app_name_prefix>-<target>
PROV="python scripts/provision.py --profile $P --catalog $CAT --data-schema $DS --project $PROJ"
```

Configuration: copy `variable-overrides.example.json` to `.databricks/bundle/dev/variable-overrides.json`
(gitignored) and set `catalog`, `data_schema`, `app_name_prefix`, `lakebase_project_id`. Leave
`lakebase_data_api_url` as the placeholder until step 5. Alternatively pass `--var name=value` to every `bundle`
command.

| # | Step | Command | Duration | Expected |
|---|------|---------|----------|----------|
| 0 | Dry run of the Lakebase side | `$PROV all --dry-run` | 1 min | `[dry-run] databricks postgres create-project ...`, `create-synced-table ... SNAPSHOT/TRIGGERED`, the gold SQL, `create-data-api`; nothing is created |
| 1 | Clean build and checks | see 1 below | 3 min | vitest and pytest green, `npm run build` writes `app/static`, `Validation OK!` |
| 2 | Deploy (UC data schema, `hh_portal_setup`, `hh_portal_daily` (PAUSED), the app) | see 2 below | 2-4 min | uploads `app/**` (with `app/static`), `src/**`, `jobs/**`, `sql/**`, `scripts/**`, bundle files |
| 3 | Generate the synthetic data and silver | `databricks bundle run hh_portal_setup -p $P` | 10-15 min | run SUCCESS; `portal_refresh_log` row `generate ok rows=~19000000 ... 20 organisations (4 TPI), 200 accounts, 600 sites, 1000 MPANs, duplicate keys 0, Change Data Feed on` |
| 4 | Lakebase: project, scale-up, sync, wait, indexes, gold, Data API, scale-down, outputs | `$PROV all` | 40-60 min | see 4 below |
| 5 | Set the printed outputs and redeploy | edit `variable-overrides.json`, then `databricks bundle deploy -p $P` | 2-3 min | the app now has `HH_PORTAL_DATA_API_URL` set |
| 6 | Postgres role and grants for the app | `$PROV grants --app $APP` | < 1 min | `databricks_create_role(<sp>, 'SERVICE_PRINCIPAL')`, `GRANT <sp> TO authenticator`, USAGE/SELECT on `portal_gold` only |
| 7 | Start the app | `databricks bundle run hh_portal_app -p $P` | 2-4 min | app RUNNING; URL printed |
| 8 | Smoke test | open the app URL (`databricks apps get $APP -p $P -o json \| jq -r .url`) | seconds | the Day chart loads; "How this was fetched" lists `GET /portal_gold/...` with 200s; the Architecture page shows the live facts |
| 9 | (optional) Enable the daily schedule | set `"daily_pause_status": "UNPAUSED"` and redeploy | 2 min | job `hh_portal_daily (dev)` scheduled (`daily_cron`, `daily_timezone`) |

The Data API only accepts roles granted to `authenticator`, so test it through the app (or with your own identity
after creating a role for it the same way).

## 1. Clean build and checks

```bash
rm -rf app/static frontend/dist
(cd frontend && npm ci && npm test && npm run build)
python -m pytest -q
databricks bundle validate -p $P
```

## 2. Deploy

```bash
databricks bundle deploy -p $P
```

When you redeploy after deleting the workspace copy of the files, also delete the local sync snapshot
(`rm -rf .databricks/bundle/dev/sync-snapshots`), otherwise only changed files are uploaded.

## 4. Lakebase provisioning (what `all` does)

| Sub-step | Duration | Expected output |
|---|---|---|
| `project` | 1-3 min when created | `project <id> not found: creating it` or `exists: reusing it`; endpoint `-> 1-4 CU, suspend 300s` |
| `scale-up` | < 1 min | `initial snapshot ahead: endpoint to 8.0 CU` (skipped if the silver synced table exists) |
| `sync` | 1-2 min | `creating UC schema <cat>.portal`; 5 x `creating SNAPSHOT synced table`, 1 x `creating TRIGGERED synced table <cat>.portal.silver_hh_readings` |
| `wait` | about 20 min at 8 CU (19 M rows / (2,000 rows/s/CU x 8 CU)) | `waiting ...: SYNCED_TABLE_PROVISIONING_INITIAL_SNAPSHOT n/19000000 rows`, then `ONLINE ...` for all 6 |
| `indexes` | 2-5 min | 5 `CREATE INDEX IF NOT EXISTS`, `ANALYZE`, row counts per synced table |
| `gold` | 5-10 min at 8 CU | the statements, then row counts (below) |
| `data-api` | 1-2 min | `enabling the Data API ... (schemas: portal_gold)` and `Data API URL: https://...` |
| `scale-down` | < 1 min | `-> 1-4 CU` |
| `outputs` | seconds | a JSON block with `lakebase_data_api_url`, `lakebase_project_uid`, `lakebase_branch_uid`, `sync_pipeline_id` |

Expected row counts after `gold` with the defaults (1,000 MPANs, about 396 days): `directory` 1,000;
`account_directory` 200; `org_directory` 20; `daily_mpan` about 396 k; `portal_work.hh_account_totals` about 3.8 M;
`daily_account` about 79 k; `daily_org` about 7.9 k; `monthly_mpan` about 14 k; `monthly_account` about 2.8 k;
`monthly_org` about 280.

If `wait` is interrupted, re-run `$PROV wait` and continue with the next sub-steps one by one (each is idempotent).
Run `$PROV scale-down` if you stop before the end, so the endpoint does not stay at 8 CU.

## Daily job `hh_portal_daily` (PAUSED by default)

Run by hand: `databricks bundle run hh_portal_daily -p $P`.

| Task | What it does | Duration (estimate) |
|------|--------------|---------------------|
| `append_day` | appends the next day (about 48,000 rows) if it is not later than yesterday; deletes the day that leaves the 13-month window (about 48,000 rows) | 2-4 min serverless |
| `sync_silver` | starts the synced table's pipeline update (incremental, about 96 k changes) and waits | 3-5 min at 4 CU, plus pipeline start-up |
| `refresh_gold` | `REFRESH MATERIALIZED VIEW CONCURRENTLY` for 9 gold MVs (plain `REFRESH` for the internal `portal_work.hh_account_totals`) in dependency order, counts, `ANALYZE` | 5-10 min at up to 4 CU |

Check: `SELECT * FROM <catalog>.<data_schema>.portal_refresh_log ORDER BY started DESC LIMIT 20`. A same-day re-run
appends nothing (`append_day: skipped`).

## Measure the real cost

Lakebase usage is in the billing system tables
([docs](https://docs.databricks.com/aws/en/admin/system-tables/billing)); filter by your project uid (printed by
`outputs`):

```sql
SELECT u.usage_date, u.sku_name, sum(u.usage_quantity) AS quantity, any_value(u.usage_unit) AS unit,
       sum(u.usage_quantity * p.pricing.default) AS list_cost
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON p.sku_name = u.sku_name AND p.cloud = u.cloud
 AND u.usage_end_time >= p.price_start_time AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.billing_origin_product = 'LAKEBASE' AND u.usage_metadata.project_id = '<LAKEBASE_PROJECT_UID>'
GROUP BY ALL ORDER BY u.usage_date DESC
```

Jobs and the synced-table pipelines appear under their own `billing_origin_product` values with `usage_metadata.job_id`
and `usage_metadata.dlt_pipeline_id`. The figures in the README are estimates from the timings above; list prices
differ by cloud, region and tier, and your contract may differ from list prices.

## Notes and troubleshooting

- **Data API 404 for a `portal_gold` view.** Re-run `$PROV data-api` (re-applies the config and reloads the schema
  cache). If it persists, press Lakebase UI > Data API > Refresh schema cache once.
- **Data API 42501 `permission denied to set role`.** The app's role was not created by `databricks_create_role` (for
  example by the UI). Delete it (`databricks postgres delete-role ...`) and re-run `$PROV grants --app $APP`.
- **App shows "not configured".** `lakebase_data_api_url` is still the placeholder: set it from `outputs` and redeploy.
- **Changing a view or MV.** Edit `sql/lakebase_gold.sql`, then `$PROV gold --rebuild` (drops and recreates the gold
  views and MVs; the portal errors meanwhile), then `$PROV data-api`. Rebuild the UI so the Architecture page shows
  the new SQL.
- **Regenerating the data.** `databricks bundle run hh_portal_setup -p $P --params rebuild=true` (otherwise the job
  skips when silver holds rows). Replacing silver resets its Change Data Feed: first `$PROV teardown` (keeps the
  project), then regenerate, then re-run step 4 and `grants`.
- **Name collisions.** The bundle creates `<catalog>.<data_schema>`: choose a schema name that does not exist yet.
  `<catalog>.portal` must not hold other tables.
- **Local DNS.** If psycopg cannot resolve the endpoint host, the script falls back to `dig` for the address.

## Teardown

```bash
$PROV teardown --dry-run          # lists the deletions
$PROV teardown                    # asks you to type the project id; deletes the 6 synced tables, DROP SCHEMA
                                  # portal_gold / portal_work / portal CASCADE, removes portal_gold from the
                                  # Data API, deletes UC schema <catalog>.portal; keeps the project
$PROV teardown --delete-project   # same, but deletes the whole Lakebase project instead of its schemas
databricks bundle destroy -p $P   # jobs, app and the UC data schema <catalog>.<data_schema>
```
