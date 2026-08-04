#!/usr/bin/env python3
"""OPTION 3 — synced tables on ONE shared pipeline + a refresh schedule, in one pass.

WHY A SCRIPT AND NOT PURE DABs
------------------------------
Ideally you'd create a pipeline, schedule it, then attach tables to it. The platform
doesn't allow that: a "Database Table Sync" pipeline cannot be created empty (the
pipelines API rejects it — `libraries must contain at least one element`), so the
pipeline can only come into existence as a side effect of creating the FIRST synced
table with `new_pipeline_spec`.

That leaves a chicken-and-egg for DABs: to share a pipeline you need its ID, but DABs
resolves references at plan time and the synced-table resource exposes no output field
carrying it. So DABs needs two deploys (Option 2).

The REST API *does* return the ID on create, so imperatively it's one pass:

    1. first table  + new_pipeline_spec       -> response carries status.pipeline_id
    2. other tables + existing_pipeline_id    -> attached to that same pipeline
    3. job with a pipeline_task               -> the scheduled refresh
    4. one run now                            -> brings the newly attached tables online

Trade-off: imperative, so you lose DABs' drift detection and `bundle destroy` for these
resources. This demo already provisions Lakebase with shell scripts, so it fits:
scripts for provisioning, DABs for jobs and apps.

Re-runnable: tables that already exist are skipped, and the pipeline ID is read back
from the first table, so nothing is duplicated.

Usage
-----
    export DATABRICKS_CONFIG_PROFILE=<profile>       # or DATABRICKS_HOST/TOKEN
    python scripts/04_create_synced_tables.py

Verified end-to-end against a live workspace: two tables on one pipeline, and the
scheduled job's single task brought both online. The synced-table API is **Beta** —
re-verify field names before production.
"""

from databricks.sdk import WorkspaceClient

# ─── EDIT ─────────────────────────────────────────────────────────────────────
CATALOG, GOLD_SCHEMA, PG_SCHEMA = "sse_airtricity", "esi_gold", "esi"
BRANCH = "projects/sse-esi/branches/production"     # Lakebase project/branch
PG_DATABASE = "databricks_postgres"
POLICY = "SNAPSHOT"        # SNAPSHOT | TRIGGERED | CONTINUOUS
                           #   SNAPSHOT/TRIGGERED never self-refresh -> need the job below
                           #   CONTINUOUS manages itself (and needs CDF on the source)
CRON = "0 0 2 * * ?"       # quartz: 02:00 daily
TIMEZONE = "Europe/Dublin"

TABLES = {                 # table -> primary key columns (these become the PG indexes)
    "customers":           ["customer_id"],
    "consumption_daily":   ["customer_id", "usage_date"],
    "consumption_monthly": ["customer_id", "month_start"],
    "insights":            ["customer_id", "insight_id"],
}
# ──────────────────────────────────────────────────────────────────────────────

API = "/api/2.0/postgres/synced_tables"
w = WorkspaceClient()


def get(table):
    """The synced table, or None if it doesn't exist yet."""
    try:
        return w.api_client.do("GET", f"{API}/{CATALOG}.{PG_SCHEMA}.{table}")
    except Exception:
        return None


def create(table, pks, pipeline_id=None):
    """Create a synced table.

    pipeline_id=None -> `new_pipeline_spec` creates a pipeline (first table only).
    pipeline_id set  -> `existing_pipeline_id` attaches to that pipeline.
    """
    spec = {
        "source_table_full_name": f"{CATALOG}.{GOLD_SCHEMA}.{table}",
        "primary_key_columns": pks,
        "scheduling_policy": POLICY,
        "create_database_objects_if_missing": True,   # make the PG schema/table if absent
        "branch": BRANCH,
        "postgres_database": PG_DATABASE,
    }
    if pipeline_id:
        spec["existing_pipeline_id"] = pipeline_id
    else:
        # Where the pipeline keeps its own files (checkpoints, event log). Must be a
        # standard UC catalog/schema you can create Delta tables in.
        spec["new_pipeline_spec"] = {"storage_catalog": CATALOG,
                                     "storage_schema": GOLD_SCHEMA}

    # NOTE the body shape: {"spec": ...} — NOT {"synced_table": {"spec": ...}}.
    # The wrapper form is rejected with "Field 'synced_table' is required...".
    return w.api_client.do("POST", API,
                           query={"synced_table_id": f"{CATALOG}.{PG_SCHEMA}.{table}"},
                           body={"spec": spec})


def pipeline_of(resp):
    """status.pipeline_id — the value DABs can't give us. May be absent on a fresh
    create response (the record is still being written); we only need it from the
    first table, which does return it."""
    return ((resp or {}).get("status") or {}).get("pipeline_id")


# ── 1-2. Create the tables. The first makes the pipeline; the rest join it. ────
pipeline_id = None
for table, pks in TABLES.items():
    existing = get(table)
    if existing:
        # Already there — reuse its pipeline so a re-run attaches to the same one.
        pipeline_id = pipeline_id or pipeline_of(existing)
        print(f"  {table}: exists")
        continue
    resp = create(table, pks, pipeline_id)
    pipeline_id = pipeline_id or pipeline_of(resp)
    print(f"  {table}: created")

print(f"\nShared pipeline: {pipeline_id}")

# ── 3. Scheduled refresh — ONE task covers every table on the pipeline ────────
# There is no schedule field on the synced table itself, so a job is the mechanism.
# Because the tables share a pipeline they refresh in the SAME run, so the app never
# sees a half-refreshed set. (With per-table pipelines you'd need one task each, and
# they'd finish at different times.)
job = w.api_client.do("POST", "/api/2.2/jobs/create", body={
    "name": "refresh_synced_tables",
    "schedule": {"quartz_cron_expression": CRON, "timezone_id": TIMEZONE,
                 "pause_status": "UNPAUSED"},
    "tasks": [{"task_key": "refresh",
               "pipeline_task": {"pipeline_id": pipeline_id,
                                 "full_refresh": True}}],   # SNAPSHOT = full replace
    # Recommended once this runs unattended:
    # "email_notifications": {"on_failure": ["data-eng@example.com"]},
    # "timeout_seconds": 7200,
})
print(f"Schedule job:    {job.get('job_id')}  ({CRON} {TIMEZONE})")

# ── 4. One run now, so tables attached after the first snapshot come online ───
# A table attached later sits at SYNCED_TABLE_OFFLINE until a run happens.
# If this errors with "Pipeline update already in progress", the attach already
# kicked one off — that's fine, it's doing the same work.
try:
    upd = w.api_client.do("POST", f"/api/2.0/pipelines/{pipeline_id}/updates",
                          body={"full_refresh": True})
    print(f"Initial refresh: {upd.get('update_id')}")
except Exception as e:
    print(f"Initial refresh: skipped ({str(e)[:80]})")
