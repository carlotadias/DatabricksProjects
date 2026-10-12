#!/usr/bin/env python3
"""Provision the Lakebase side of the HH portal demo. Idempotent; prints every command it runs.

Steps (run in this order; see docs/RUNBOOK.md for durations and expected output):
  project     create the Lakebase project (Postgres 17) if absent, or reuse it; primary endpoint autoscaling
              MIN-MAX CU (default 1-4) with scale to zero
  scale-up    raise the endpoint to INITIAL CU (default 8) for the initial snapshot; skipped when the silver
              synced table already exists (later syncs are small and run at 1-4 CU)
  sync        UC schema <catalog>.portal, then synced tables into Postgres schema `portal`: the dimensions
              (SNAPSHOT) and silver_hh_readings (TRIGGERED, needs Change Data Feed on the UC table)
  wait        poll until every synced table is ONLINE; fails on a failed pipeline
  indexes     lookup indexes on the replica (as you, the project owner), ANALYZE, row counts
  gold        apply sql/lakebase_gold.sql (schemas portal_gold and portal_work, views, materialized views, unique
              indexes; --rebuild drops the views first), ANALYZE, row counts, schema-cache reload notification
  data-api    enable the Lakebase Data API on the database if needed and expose portal_gold (re-applying the
              config also reloads the schema cache); prints the URL
  scale-down  back to MIN-MAX CU (same as `project` for an existing project)
  grants      Postgres role for the app service principal (databricks_create_role + GRANT TO authenticator)
              and USAGE/SELECT on portal_gold only; needs the deployed app (--app)
  outputs     print the bundle variable values to put in .databricks/bundle/<target>/variable-overrides.json
  teardown    delete the synced tables and the demo's Postgres schemas (or the whole project with
              --delete-project) and the UC schema <catalog>.portal; asks for confirmation
  all         project scale-up sync wait indexes gold data-api scale-down outputs (grants: after the deploy)

Uses the `databricks` CLI (`postgres create-synced-table`, `create-data-api`, ...) and psycopg 3 for SQL.
--dry-run prints mutating commands and SQL without running them (reads still run). No secret is printed:
the Postgres OAuth token is passed to psycopg in memory only.
"""

from __future__ import annotations

import argparse
import json
import shlex
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hhportal_data import gold as gold_sql  # noqa: E402
from hhportal_data.silver import (  # noqa: E402
    DIMENSION_KEYS, GOLD_SCHEMA, PRIMARY_KEY, REPLICA_INDEXES, SILVER_TABLE, SYNCED_SCHEMA,
)

ONLINE_PREFIX = "SYNCED_TABLE_ONLINE"
FAILED_STATES = {"SYNCED_TABLE_OFFLINE_FAILED", "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"}
SCHEMA_CACHE_RELOAD = "NOTIFY pgrst, 'reload schema'"


@dataclass
class Settings:
    profile: str
    catalog: str  # regular UC catalog: the Delta sources, the synced-table schema and the pipeline storage
    data_schema: str  # UC schema of the synthetic dimensions, silver and the refresh log
    project: str  # Lakebase project id (projects/<id>)
    branch: str = "production"
    endpoint: str = "primary"
    database_id: str = "databricks-postgres"
    postgres_database: str = "databricks_postgres"
    synced_schema: str = SYNCED_SCHEMA
    gold_schema: str = GOLD_SCHEMA
    min_cu: float = 1.0
    max_cu: float = 4.0
    initial_cu: float = 8.0
    suspend_s: int = 300
    app: str = ""
    db_max_rows: int = 2000
    dry_run: bool = False
    wait_timeout_s: int = 6 * 3600
    poll_s: int = 60
    rebuild: bool = False
    gold_sql: Path = gold_sql.GOLD_SQL_PATH
    yes: bool = False
    delete_project: bool = False

    @property
    def project_path(self) -> str:
        return f"projects/{self.project}"

    @property
    def branch_path(self) -> str:
        return f"{self.project_path}/branches/{self.branch}"

    @property
    def endpoint_path(self) -> str:
        return f"{self.branch_path}/endpoints/{self.endpoint}"

    @property
    def database_path(self) -> str:
        return f"{self.branch_path}/databases/{self.database_id}"

    @property
    def data_api_name(self) -> str:
        return f"{self.database_path}/data-api"

    def synced_table_id(self, table: str) -> str:
        return f"{self.catalog}.{self.synced_schema}.{table}"

    def source(self, table: str) -> str:
        return f"{self.catalog}.{self.data_schema}.{table}"

    def tables(self) -> list[tuple[str, tuple[str, ...], str]]:
        """(table, primary key, scheduling policy): dimensions once (SNAPSHOT), silver incrementally (TRIGGERED)."""
        return [*[(t, pk, "SNAPSHOT") for t, pk in DIMENSION_KEYS.items()], (SILVER_TABLE, PRIMARY_KEY, "TRIGGERED")]


# -- request bodies and SQL (pure, unit-tested) ------------------------------------------------------

def project_body(s: Settings) -> dict:
    return {"spec": {"display_name": "HH portal demo", "pg_version": 17}}


def endpoint_update(s: Settings, min_cu: float | None = None, max_cu: float | None = None) -> tuple[str, dict]:
    lo, hi = (s.min_cu if min_cu is None else min_cu), (s.max_cu if max_cu is None else max_cu)
    if hi - lo > 16 or lo < 0.5 or hi > 32 or lo > hi:
        raise ValueError(f"invalid autoscaling range {lo}-{hi} CU (0.5-32, max - min <= 16)")
    mask = "spec.autoscaling_limit_min_cu,spec.autoscaling_limit_max_cu,spec.suspend_timeout_duration"
    return mask, {"spec": {"autoscaling_limit_min_cu": lo, "autoscaling_limit_max_cu": hi,
                           "suspend_timeout_duration": f"{int(s.suspend_s)}s"}}


def synced_table_body(s: Settings, table: str, primary_key: tuple[str, ...], policy: str) -> dict:
    return {"spec": {
        "source_table_full_name": s.source(table),
        "primary_key_columns": list(primary_key),
        "scheduling_policy": policy,
        "branch": s.branch_path,
        "postgres_database": s.postgres_database,
        "create_database_objects_if_missing": True,
        "new_pipeline_spec": {"storage_catalog": s.catalog, "storage_schema": s.data_schema},
    }}


def data_api_body(s: Settings, current: list[str] | None = None) -> dict:
    schemas = list(current or [])
    if s.gold_schema not in schemas:
        schemas.append(s.gold_schema)
    return {"spec": {"db_schemas": schemas, "db_max_rows": s.db_max_rows}}


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def index_statements(s: Settings) -> list[str]:
    out = []
    for table, cols in REPLICA_INDEXES:
        name = f"{table}_{'_'.join(cols)}_idx"[:63]
        out.append(f"CREATE INDEX IF NOT EXISTS {_ident(name)} ON {_ident(s.synced_schema)}.{_ident(table)} "
                   f"({', '.join(_ident(c) for c in cols)})")
    out += [f"ANALYZE {_ident(s.synced_schema)}.{_ident(t)}" for t, _, _ in s.tables()]
    return out


def gold_statements(s: Settings) -> list[str]:
    """The reviewed gold file (optionally preceded by drops for --rebuild), then ANALYZE and a PostgREST
    schema-cache reload notification (the Data API is PostgREST-compatible; harmless if nobody listens)."""
    text = Path(s.gold_sql).read_text()
    issues = gold_sql.sanity_issues(text)
    if issues:
        raise ValueError("gold SQL failed its checks: " + "; ".join(issues))
    stmts = gold_sql.split_statements(text)
    return ([*gold_sql.drop_statements(stmts)] if s.rebuild else []) + [
        "SET statement_timeout = 0", *stmts, *gold_sql.analyze_statements(stmts), SCHEMA_CACHE_RELOAD]


def grant_statements(sp_client_id: str, schema: str = GOLD_SCHEMA) -> list[str]:
    """Role + read-only grants for the app service principal. The role must be created with
    databricks_create_role (not the role API, the UI or an app resource): only then can it be granted to the
    Data API's `authenticator` role."""
    role, sch = _ident(sp_client_id), _ident(schema)
    lit = "'" + sp_client_id.replace("'", "''") + "'"
    return ["CREATE EXTENSION IF NOT EXISTS databricks_auth",
            f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {lit}) THEN "
            f"PERFORM databricks_create_role({lit}, 'SERVICE_PRINCIPAL'); END IF; END $$",
            f"GRANT {role} TO authenticator",
            f"GRANT USAGE ON SCHEMA {sch} TO {role}",
            f"GRANT SELECT ON ALL TABLES IN SCHEMA {sch} TO {role}",
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {sch} GRANT SELECT ON TABLES TO {role}"]


def teardown_statements(s: Settings) -> list[str]:
    """Postgres objects of the demo, dependents first (used when the project itself is kept)."""
    return [f"DROP SCHEMA IF EXISTS {_ident(s.gold_schema)} CASCADE", f"DROP SCHEMA IF EXISTS {_ident('portal_work')} CASCADE",
            f"DROP SCHEMA IF EXISTS {_ident(s.synced_schema)} CASCADE"]


def sync_state(table: dict | None) -> tuple[str, str]:
    st = (table or {}).get("status") or {}
    progress = st.get("ongoing_sync_progress") or {}
    detail = ""
    if progress:
        detail = f"{progress.get('synced_row_count', 0)}/{progress.get('total_row_count', '?')} rows"
    return str(st.get("detailed_state") or "UNKNOWN"), (detail or st.get("message") or "")[:200]


# -- execution --------------------------------------------------------------------------------------

class CliError(RuntimeError):
    def __init__(self, args, rc, stderr):
        super().__init__(f"{' '.join(args[:4])} failed ({rc}): {stderr.strip()[:500]}")
        self.stderr = stderr


def _is_not_found(e: CliError) -> bool:
    text = e.stderr.lower()
    return "not found" in text or "not_found" in text or "does not exist" in text or "not enabled" in text


@dataclass
class Provisioner:
    s: Settings
    run_cli: object = None  # (args: list[str]) -> (rc, stdout, stderr); default subprocess
    connect: object = None  # (host, user, password, dbname) -> DB-API connection; default psycopg
    sleep: object = time.sleep
    ask: object = input
    log: list[str] = field(default_factory=list)

    def say(self, text: str) -> None:
        self.log.append(text)
        print(text, flush=True)

    def cli(self, *args: str, mutating: bool = False) -> dict | list | None:
        full = ["databricks", *args, "--profile", self.s.profile, "-o", "json"]
        shown = " ".join(shlex.quote(a) for a in full)
        if mutating and self.s.dry_run:
            self.say(f"[dry-run] {shown}")
            return None
        self.say(f"$ {shown}")
        rc, out, err = (self.run_cli or _subprocess_cli)(full)
        if rc != 0:
            raise CliError(full, rc, err)
        return json.loads(out) if out.strip() else None

    def get_or_none(self, *args: str):
        try:
            return self.cli(*args)
        except CliError as e:
            if _is_not_found(e):
                return None
            raise

    def _synced(self, table: str):
        return self.get_or_none("postgres", "get-synced-table", f"synced_tables/{self.s.synced_table_id(table)}")

    # steps ---------------------------------------------------------------------------------------

    def project(self, min_cu: float | None = None, max_cu: float | None = None) -> None:
        if self.get_or_none("postgres", "get-project", self.s.project_path) is None:
            self.say(f"project {self.s.project} not found: creating it (Postgres 17; takes 1-3 min)")
            self.cli("postgres", "create-project", self.s.project, "--json", json.dumps(project_body(self.s)),
                     mutating=True)
        else:
            self.say(f"project {self.s.project} exists: reusing it")
        self._scale(min_cu, max_cu)

    def _scale(self, min_cu: float | None = None, max_cu: float | None = None) -> None:
        mask, body = endpoint_update(self.s, min_cu, max_cu)
        ep = self.get_or_none("postgres", "get-endpoint", self.s.endpoint_path) or {}
        st = ep.get("status") or {}
        want = body["spec"]
        have = (st.get("autoscaling_limit_min_cu"), st.get("autoscaling_limit_max_cu"), st.get("suspend_timeout_duration"))
        if have == (want["autoscaling_limit_min_cu"], want["autoscaling_limit_max_cu"], want["suspend_timeout_duration"]):
            self.say(f"endpoint {self.s.endpoint_path} already {have[0]}-{have[1]} CU, suspend {have[2]}")
            return
        self.say(f"endpoint {self.s.endpoint_path}: {have} -> {want}")
        self.cli("postgres", "update-endpoint", self.s.endpoint_path, mask, "--json", json.dumps(body), mutating=True)

    def scale_up(self) -> None:
        if self._synced(SILVER_TABLE) is not None:
            self.say(f"{self.s.synced_table_id(SILVER_TABLE)} exists (initial snapshot done): staying at "
                     f"{self.s.min_cu}-{self.s.max_cu} CU")
            return
        self.say(f"initial snapshot ahead: endpoint to {self.s.initial_cu} CU (run scale-down afterwards)")
        self._scale(self.s.initial_cu, self.s.initial_cu)

    def scale_down(self) -> None:
        self._scale()

    def _ensure_uc_schema(self) -> None:
        fqn = f"{self.s.catalog}.{self.s.synced_schema}"
        if self.get_or_none("schemas", "get", fqn) is not None:
            self.say(f"UC schema {fqn} exists")
            return
        self.say(f"creating UC schema {fqn} for the synced tables")
        self.cli("schemas", "create", self.s.synced_schema, self.s.catalog, "--comment",
                 "Lakebase synced tables of the HH portal demo (SYNTHETIC)", mutating=True)

    def sync(self) -> None:
        silver = self.get_or_none("tables", "get", self.s.source(SILVER_TABLE))
        if silver is None and self.s.dry_run:
            self.say(f"[dry-run] {self.s.source(SILVER_TABLE)} does not exist yet (the setup job builds it)")
            silver = {"properties": {"delta.enableChangeDataFeed": "true"}}
        if silver is None:
            raise RuntimeError(f"{self.s.source(SILVER_TABLE)} not found: run the setup job first "
                               "(databricks bundle run hh_portal_setup)")
        if str((silver.get("properties") or {}).get("delta.enableChangeDataFeed", "")).lower() != "true":
            raise RuntimeError(f"{self.s.source(SILVER_TABLE)} has no Change Data Feed (a TRIGGERED sync needs it): "
                               "rebuild it with the setup job (job parameter rebuild=true)")
        self._ensure_uc_schema()
        for table, pk, policy in self.s.tables():
            sid = self.s.synced_table_id(table)
            existing = self._synced(table)
            if existing is not None:
                state, detail = sync_state(existing)
                self.say(f"synced table {sid} exists ({state} {detail})")
                continue
            self.say(f"creating {policy} synced table {sid} (primary key {', '.join(pk)})")
            self.cli("postgres", "create-synced-table", sid, "--no-wait", "--json",
                     json.dumps(synced_table_body(self.s, table, pk, policy)), mutating=True)

    def wait(self) -> None:
        if self.s.dry_run:
            self.say("[dry-run] would poll get-synced-table until every table is ONLINE")
            return
        deadline = time.monotonic() + self.s.wait_timeout_s
        pending = [t for t, _, _ in self.s.tables()]
        while pending:
            still = []
            for table in pending:
                sid = self.s.synced_table_id(table)
                state, detail = sync_state(self._synced(table))
                if state in FAILED_STATES:
                    raise RuntimeError(f"synced table {sid} failed: {state} {detail} (see its pipeline in Catalog "
                                       "Explorer)")
                if state.startswith(ONLINE_PREFIX):
                    self.say(f"ONLINE {sid} ({state})")
                else:
                    self.say(f"waiting {sid}: {state} {detail}")
                    still.append(table)
            pending = still
            if pending:
                if time.monotonic() > deadline:
                    raise RuntimeError(f"not ONLINE after {self.s.wait_timeout_s} s: {', '.join(pending)}")
                self.sleep(self.s.poll_s)

    def _sql(self, statements: list[str], counts: list[str] | None = None) -> None:
        if self.s.dry_run:
            if counts:
                self.say("[dry-run] would print Postgres row counts")
            for st in statements:
                self.say(f"[dry-run] SQL {st}")
            return
        ep = self.cli("postgres", "get-endpoint", self.s.endpoint_path)
        host = ep["status"]["hosts"]["host"]
        token = self.cli("postgres", "generate-database-credential", self.s.endpoint_path)["token"]
        user = self.cli("current-user", "me")["userName"]
        conn = (self.connect or _psycopg_connect)(host, user, token, self.s.postgres_database)
        try:
            for st in statements:
                self.say(f"SQL {st}")
                conn.execute(st)
            for name in counts or []:
                sch, tbl = name.split(".")
                n = conn.execute(f"SELECT count(*) FROM {_ident(sch)}.{_ident(tbl)}").fetchone()[0]
                self.say(f"{name}: {n} rows")
        finally:
            conn.close()

    def indexes(self) -> None:
        self._sql(index_statements(self.s), counts=[f"{self.s.synced_schema}.{t}" for t, _, _ in self.s.tables()])

    def gold(self) -> None:
        stmts = gold_statements(self.s)
        mvs = gold_sql.mv_objects(gold_sql.split_statements(Path(self.s.gold_sql).read_text()))
        self.say(f"applying {Path(self.s.gold_sql).name}: {len(mvs)} materialized views (daily_mpan and "
                 "portal_work.hh_account_totals scan the 13-month silver: about 5-10 min at 8 CU on the first build)")
        self._sql(stmts, counts=[f"{sch}.{mv}" for sch, mv in mvs])

    def data_api(self) -> str | None:
        """Enable the Data API (exposing portal_gold) or add portal_gold to it. Re-applying the config with
        update-data-api is also the programmatic schema-cache reload (the docs describe a UI button)."""
        current = self.get_or_none("postgres", "get-data-api", self.s.data_api_name)
        if current is None:
            self.say(f"enabling the Data API on {self.s.database_path} (schemas: {self.s.gold_schema})")
            current = self.cli("postgres", "create-data-api", self.s.database_path, "--json",
                               json.dumps(data_api_body(self.s)), mutating=True) or {}
        else:
            st = current.get("status") or {}
            schemas = st.get("db_schemas") or []
            available = st.get("available_schemas")
            if available is not None and self.s.gold_schema not in available and not self.s.dry_run:
                raise RuntimeError(f"schema {self.s.gold_schema} does not exist yet (available {available}): run "
                                   "the gold step first")
            body = data_api_body(self.s, schemas)
            self.say(f"Data API schemas {schemas} -> {body['spec']['db_schemas']} (also reloads the schema cache)")
            current = self.cli("postgres", "update-data-api", self.s.data_api_name, "spec.db_schemas", "--json",
                               json.dumps(body), mutating=True) or current
        self.say("if the portal gets 404 for a portal_gold view, use Lakebase UI > Data API > Refresh schema cache")
        url = ((current or {}).get("status") or {}).get("url")
        self.say(f"Data API URL: {url or 'unknown yet (run outputs after the operation finishes)'}")
        return url

    def grants(self) -> None:
        if not self.s.app:
            raise RuntimeError("--app is required (the deployed app's name, e.g. hh-portal-dev)")
        app = self.cli("apps", "get", self.s.app)
        sp = (app or {}).get("service_principal_client_id")
        if not sp:
            raise RuntimeError(f"app {self.s.app} has no service principal yet (deploy it first)")
        # Run after the Data API is enabled (it creates `authenticator`). A role made by the role API, the UI or
        # an app `postgres` resource cannot be granted to authenticator ("permission denied to grant role"):
        # delete it with `databricks postgres delete-role` and re-run this step.
        self.say(f"Postgres role + SELECT on {self.s.gold_schema} only for app {self.s.app} (service principal {sp})")
        self._sql(grant_statements(sp, self.s.gold_schema))

    def outputs(self) -> dict:
        proj = self.get_or_none("postgres", "get-project", self.s.project_path) or {}
        branch = self.get_or_none("postgres", "get-branch", self.s.branch_path) or {}
        api = self.get_or_none("postgres", "get-data-api", self.s.data_api_name) or {}
        silver = self._synced(SILVER_TABLE) or {}
        values = {
            "lakebase_data_api_url": (api.get("status") or {}).get("url") or "",
            "lakebase_project_uid": proj.get("uid") or "",
            "lakebase_branch_uid": branch.get("uid") or "",
            "sync_pipeline_id": (silver.get("status") or {}).get("pipeline_id") or "",
        }
        self.say("add these to .databricks/bundle/<target>/variable-overrides.json (identifiers, not secrets):")
        self.say(json.dumps(values, indent=2))
        return values

    def teardown(self) -> None:
        what = (f"the whole Lakebase project {self.s.project}" if self.s.delete_project else
                f"schemas {self.s.gold_schema}, portal_work and {self.s.synced_schema} in project {self.s.project}")
        self.say(f"teardown: deletes the demo's synced tables, {what} and UC schema "
                 f"{self.s.catalog}.{self.s.synced_schema}. The jobs, the app and UC schema "
                 f"{self.s.catalog}.{self.s.data_schema} are removed by `databricks bundle destroy`.")
        if not self.s.dry_run and not self.s.yes:
            if str(self.ask(f"type the project id ({self.s.project}) to confirm: ")).strip() != self.s.project:
                raise SystemExit("not confirmed: nothing was deleted")
        for table, _, _ in self.s.tables():
            sid = self.s.synced_table_id(table)
            if self._synced(table) is None:
                self.say(f"synced table {sid} does not exist")
                continue
            self.cli("postgres", "delete-synced-table", f"synced_tables/{sid}", mutating=True)
        if self.s.delete_project:
            if self.get_or_none("postgres", "get-project", self.s.project_path) is not None:
                self.cli("postgres", "delete-project", self.s.project_path, mutating=True)
        elif self.get_or_none("postgres", "get-project", self.s.project_path) is not None:
            self._sql(teardown_statements(self.s))
            api = self.get_or_none("postgres", "get-data-api", self.s.data_api_name)
            schemas = ((api or {}).get("status") or {}).get("db_schemas") or []
            rest = [x for x in schemas if x != self.s.gold_schema]
            if api is not None and self.s.gold_schema in schemas:
                if rest:
                    self.cli("postgres", "update-data-api", self.s.data_api_name, "spec.db_schemas", "--json",
                             json.dumps({"spec": {"db_schemas": rest, "db_max_rows": self.s.db_max_rows}}),
                             mutating=True)
                else:
                    self.cli("postgres", "delete-data-api", self.s.data_api_name, mutating=True)
        fqn = f"{self.s.catalog}.{self.s.synced_schema}"
        if self.get_or_none("schemas", "get", fqn) is not None:
            self.cli("schemas", "delete", fqn, mutating=True)
        self.say("teardown finished")


def _subprocess_cli(args: list[str]) -> tuple[int, str, str]:
    p = subprocess.run(args, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


def resolve_hostaddr(host: str) -> str | None:
    """Some local resolvers fail on long Lakebase hostnames: fall back to `dig` when getaddrinfo fails."""
    try:
        socket.getaddrinfo(host, 5432)
        return None
    except OSError:
        try:
            out = subprocess.run(["dig", "+short", host], capture_output=True, text=True).stdout.split()
        except OSError:
            return None
        ips = [x for x in out if x.count(".") == 3 and x.replace(".", "").isdigit()]
        return ips[-1] if ips else None


def _psycopg_connect(host: str, user: str, password: str, dbname: str):
    import psycopg

    kw = {"host": host, "user": user, "password": password, "dbname": dbname, "sslmode": "require",
          "autocommit": True, "connect_timeout": 60}
    addr = resolve_hostaddr(host)
    if addr:
        kw["hostaddr"] = addr
    return psycopg.connect(**kw)


STEPS = ("project", "scale-up", "sync", "wait", "indexes", "gold", "data-api", "scale-down", "grants", "outputs",
         "teardown", "all")
ALL = ["project", "scale-up", "sync", "wait", "indexes", "gold", "data-api", "scale-down", "outputs"]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("step", choices=STEPS)
    ap.add_argument("--profile", required=True, help="Databricks CLI profile of the target workspace")
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--data-schema", required=True, help="UC schema of the synthetic data (bundle var data_schema)")
    ap.add_argument("--project", required=True, help="Lakebase project id (bundle var lakebase_project_id)")
    ap.add_argument("--branch", default="production")
    ap.add_argument("--endpoint", default="primary")
    ap.add_argument("--app", default="", help="grants: the deployed app name (bundle var app_name)")
    ap.add_argument("--rebuild", action="store_true", help="gold: drop the gold views and MVs first")
    ap.add_argument("--min-cu", type=float, default=1.0)
    ap.add_argument("--max-cu", type=float, default=4.0)
    ap.add_argument("--initial-cu", type=float, default=8.0, help="scale-up: CU for the initial snapshot")
    ap.add_argument("--wait-timeout-s", type=int, default=6 * 3600)
    ap.add_argument("--delete-project", action="store_true", help="teardown: delete the whole Lakebase project")
    ap.add_argument("--yes", action="store_true", help="teardown: do not ask for confirmation")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    s = Settings(profile=a.profile, catalog=a.catalog, data_schema=a.data_schema, project=a.project, branch=a.branch,
                 endpoint=a.endpoint, app=a.app, min_cu=a.min_cu, max_cu=a.max_cu, initial_cu=a.initial_cu,
                 dry_run=a.dry_run, wait_timeout_s=a.wait_timeout_s, rebuild=a.rebuild, yes=a.yes,
                 delete_project=a.delete_project)
    p = Provisioner(s)
    for step in ALL if a.step == "all" else [a.step]:
        p.say(f"== {step}")
        getattr(p, step.replace("-", "_"))()
    if a.step == "all":
        p.say("next: put the outputs in variable-overrides.json, `databricks bundle deploy`, then the `grants` step")
    return 0


if __name__ == "__main__":
    sys.exit(main())
