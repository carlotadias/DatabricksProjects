# HH portal on Lakebase (demo) - Technical Specification

Tool-by-tool reference: the CLIs, APIs and configuration this project depends on, and how they are used.

## 1. Declarative Automation Bundles (`databricks.yml`, `resources/`)

### What it does
Deploys the UC data schema, the two jobs and the app, and uploads `app/`, `src/`, `jobs/`, `sql/` and `scripts/`.

### Technical details
- Bundle `hh_portal_lakebase_demo`; targets `dev` (default) and `demo`; no `mode: development`.
- Required variables: `catalog`, `data_schema`, `app_name_prefix`, `lakebase_project_id`, `lakebase_data_api_url`.
  Optional: `lakebase_branch_id` (`production`), `lakebase_endpoint_id` (`primary`), `lakebase_project_uid`,
  `lakebase_branch_uid`, `sync_pipeline_id`, `lakebase_data_api_tab_path`, `daily_cron`, `daily_timezone`,
  `daily_pause_status` (`PAUSED`), `mpan_target` (1000), `history_months` (13), `max_rows` (2000), `repo_url`.
- `sync.include: app/static/**` (the built UI is gitignored); tests, docs and the UI sources are excluded.

### Commands
```bash
databricks bundle validate -p <PROFILE>
databricks bundle deploy -p <PROFILE>
databricks bundle run hh_portal_setup -p <PROFILE> [--params rebuild=true]
databricks bundle run hh_portal_daily -p <PROFILE>
databricks bundle run hh_portal_app -p <PROFILE>
databricks bundle destroy -p <PROFILE>
```

## 2. Jobs (`jobs/setup.py`, `jobs/daily.py`)

### Technical details
- Serverless, environment version 2. Setup depends on `dbldatagen`; daily on `databricks-sdk>=0.81` and
  `psycopg[binary]>=3.2`. Both add `<bundle root>/src` to `sys.path` and import `hhportal_data`.
- Setup task `generate`: dimension tables via `CREATE OR REPLACE TABLE ... AS SELECT ... FROM range(n)`, readings via
  a dbldatagen spec (row id -> MPAN index and half-hour slot), inserted into the silver table created with PK,
  liquid clustering and `delta.enableChangeDataFeed = true`; checks row count, MPAN count and duplicate keys.
- Daily tasks: `append_day` -> `sync_silver` -> `refresh_gold` (see ADR 0005). The endpoint path passed to
  `refresh_gold` is `projects/<project>/branches/<branch>/endpoints/<endpoint>`.

## 3. Lakebase synced tables

### Technical details
- Created with `databricks postgres create-synced-table <catalog>.portal.<table> --no-wait --json <spec>` where the spec
  sets `source_table_full_name`, `primary_key_columns`, `scheduling_policy` (`SNAPSHOT` for dimensions, `TRIGGERED`
  for silver), `branch`, `postgres_database` (`databricks_postgres`), `create_database_objects_if_missing: true` and
  `new_pipeline_spec` (`storage_catalog`, `storage_schema`).
- TRIGGERED needs the Change Data Feed on the source. Status: `databricks postgres get-synced-table
  synced_tables/<name>` (`status.detailed_state`, `status.pipeline_id`, `status.ongoing_sync_progress`).
- The daily trigger is `pipelines.start_update(pipeline_id, full_refresh=False)` on that pipeline.
- Throughput used for estimates: about 2,000 rows/s/CU for snapshots, about 150 rows/s/CU incremental.

### Limitations
- Replacing the source table (regenerating silver) resets its Change Data Feed: delete and re-create the synced table.

## 4. Lakebase project and Postgres (`scripts/provision.py`, psycopg)

### Commands / API calls
```bash
databricks postgres get-project projects/<id>
databricks postgres create-project <id> --json '{"spec": {"display_name": "HH portal demo", "pg_version": 17}}'
databricks postgres update-endpoint projects/<id>/branches/production/endpoints/primary \
  spec.autoscaling_limit_min_cu,spec.autoscaling_limit_max_cu,spec.suspend_timeout_duration --json '{...}'
databricks postgres generate-database-credential projects/<id>/branches/production/endpoints/primary
databricks postgres get-branch projects/<id>/branches/production
databricks postgres delete-synced-table synced_tables/<catalog>.portal.<table>
databricks postgres delete-project projects/<id>
```
- SQL runs as the CLI user over TLS with a short-lived OAuth token as the password (`sslmode=require`), autocommit.
- Gold file rules: one statement per line end (`;` at the end of a line), full-line `--` comments only, DDL only,
  every MV with a UNIQUE index on plain columns (required by `REFRESH ... CONCURRENTLY`).

## 5. Lakebase Data API

### Commands / API calls
```bash
databricks postgres create-data-api projects/<id>/branches/production/databases/databricks-postgres \
  --json '{"spec": {"db_schemas": ["portal_gold"], "db_max_rows": 2000}}'
databricks postgres get-data-api projects/<id>/branches/production/databases/databricks-postgres/data-api
databricks postgres update-data-api <same name> spec.db_schemas --json '{"spec": {"db_schemas": [...], "db_max_rows": 2000}}'
```
- Requests: `GET <DATA_API_URL>/portal_gold/<view>?select=...&<col>=eq.<v>&order=<col>.asc&limit=..&offset=..` with
  `Authorization: Bearer <OAuth token>`; the response is a JSON array of at most `db_max_rows` rows.
- The caller's role must be created with `databricks_create_role` and granted to `authenticator`.

### References
- [Lakebase Data API](https://docs.databricks.com/aws/en/oltp/projects/data-api)
- [Manage roles](https://docs.databricks.com/aws/en/oltp/projects/manage-roles)
- [Autoscaling](https://docs.databricks.com/aws/en/oltp/projects/autoscaling)

## 6. Databricks App (`app/`, `frontend/`)

### Technical details
- Command `uvicorn app:app --host 0.0.0.0 --port 8000`; dependencies in `app/requirements.txt` (fastapi, uvicorn,
  httpx, databricks-sdk). The service principal's credentials come from the Apps runtime via the SDK.
- Environment: `HH_PORTAL_DATA_API_URL`, `HH_PORTAL_SCHEMA` (`portal_gold`), `HH_PORTAL_ENVIRONMENT`,
  `HH_PORTAL_MAX_ROWS`, and identifiers for the Architecture page links (`HH_PORTAL_CATALOG`, `HH_PORTAL_DATA_SCHEMA`,
  `HH_PORTAL_SYNCED_SCHEMA`, `HH_PORTAL_SYNC_PIPELINE_ID`, `HH_PORTAL_DAILY_JOB_ID`, `HH_PORTAL_SETUP_JOB_ID`,
  `HH_PORTAL_DAILY_SCHEDULE`, `HH_PORTAL_LAKEBASE_PROJECT`, `HH_PORTAL_LAKEBASE_BRANCH`,
  `HH_PORTAL_LAKEBASE_PROJECT_UID`, `HH_PORTAL_LAKEBASE_BRANCH_UID`, `HH_PORTAL_LAKEBASE_DATA_API_TAB`,
  `HH_PORTAL_REPO_URL`); unsafe values are dropped. The workspace URL comes from `DATABRICKS_HOST`.
- API routes: `/api/me`, `/api/health`, `/api/architecture`, `/api/facts`, `/api/freshness`, `/api/customers`,
  `/api/customer`, `/api/lookup`, `/api/usage`; everything else serves the single-page UI from `app/static`.
- UI: React 19, recharts, Vite (build output `../app/static`), vitest. The gold SQL viewer imports
  `sql/lakebase_gold.sql` at build time (rebuild the UI after changing it).

### References
- [Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/)

## 7. Tests

```bash
python -m pytest -q                 # tests/: SQL builders, gold file checks, job steps with fakes, provisioning with
                                    # a fake CLI and connection, the app against a mocked Data API
(cd frontend && npm test)           # vitest: periods, links, snippets, gold SQL parsing, helpers
```
No test needs a workspace.
