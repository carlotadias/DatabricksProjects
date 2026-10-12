"""scripts/provision.py with a fake CLI and a fake Postgres connection (nothing is called)."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location("provision", Path(__file__).resolve().parents[1] / "scripts" / "provision.py")
pl = importlib.util.module_from_spec(_spec)
sys.modules["provision"] = pl  # dataclasses resolve annotations through sys.modules
_spec.loader.exec_module(pl)

HOST = "lakebase-endpoint.example.com"
EP_JSON = {"uid": "ep-uid-1", "status": {"autoscaling_limit_min_cu": 1, "autoscaling_limit_max_cu": 4,
                                          "suspend_timeout_duration": "300s", "hosts": {"host": HOST}}}
CRED = {("postgres", "get-endpoint"): EP_JSON, ("postgres", "generate-database-credential"): {"token": "TOKEN"},
        ("current-user", "me"): {"userName": "owner"}}
API = {"status": {"url": "https://u", "db_schemas": ["public"], "available_schemas": ["portal", "portal_gold", "public"]}}
CDF_ON = {("tables", "get"): {"properties": {"delta.enableChangeDataFeed": "true"}}}
BASE = {"catalog": "cat", "data_schema": "demo", "project": "hh-demo"}


class FakeCli:
    def __init__(self, responses):
        self.responses = responses  # (subcommand, first arg) -> dict | callable | error text
        self.calls = []

    def __call__(self, args):
        self.calls.append(args)
        key = (args[1], args[2]) if args[1] == "postgres" else (args[1], args[2] if len(args) > 2 else "")
        r = self.responses.get(key)
        if callable(r):
            r = r(args)
        if isinstance(r, str):
            return 1, "", r
        return 0, json.dumps(r) if r is not None else "", ""

    def ran(self, sub):
        return [c for c in self.calls if len(c) > 2 and c[2] == sub]


class FakeConn:
    def __init__(self):
        self.statements, self.closed = [], False

    def execute(self, sql):
        self.statements.append(sql)

        class R:
            def fetchone(self):
                return (42,)
        return R()

    def close(self):
        self.closed = True


def prov(responses, answer="", **settings):
    cli, conn, connects = FakeCli(responses), FakeConn(), []

    def connect(host, user, password, dbname):
        connects.append((host, user, password, dbname))
        return conn

    s = pl.Settings(profile="prof", **{**BASE, **settings})
    p = pl.Provisioner(s, run_cli=cli, connect=connect, sleep=lambda _: None, ask=lambda _: answer)
    return p, cli, conn, connects


def settings(**kw):
    return pl.Settings(profile="p", **{**BASE, **kw})


def test_bodies_follow_the_cli_shapes():
    s = settings()
    assert pl.project_body(s) == {"spec": {"display_name": "HH portal demo", "pg_version": 17}}
    mask, body = pl.endpoint_update(s)
    assert mask == "spec.autoscaling_limit_min_cu,spec.autoscaling_limit_max_cu,spec.suspend_timeout_duration"
    assert body == {"spec": {"autoscaling_limit_min_cu": 1.0, "autoscaling_limit_max_cu": 4.0,
                             "suspend_timeout_duration": "300s"}}
    with pytest.raises(ValueError):
        pl.endpoint_update(s, 0.5, 32)  # spread above 16 CU
    assert pl.data_api_body(s, ["public"]) == {"spec": {"db_schemas": ["public", "portal_gold"], "db_max_rows": 2000}}
    assert pl.data_api_body(s) == {"spec": {"db_schemas": ["portal_gold"], "db_max_rows": 2000}}


def test_synced_tables_dimensions_snapshot_and_silver_triggered_in_one_schema():
    s = settings()
    tables = s.tables()
    assert [t for t, _, _ in tables] == ["organisations", "accounts", "sites", "tariffs", "mpans", "silver_hh_readings"]
    assert {p for t, _, p in tables if t != "silver_hh_readings"} == {"SNAPSHOT"}
    t, pk, policy = tables[-1]
    spec = pl.synced_table_body(s, t, pk, policy)["spec"]
    assert spec["source_table_full_name"] == "cat.demo.silver_hh_readings"
    assert spec["scheduling_policy"] == "TRIGGERED" and spec["primary_key_columns"] == ["mpan", "reading_ts"]
    assert spec["branch"] == "projects/hh-demo/branches/production"
    assert spec["new_pipeline_spec"] == {"storage_catalog": "cat", "storage_schema": "demo"}
    assert s.synced_table_id("mpans") == "cat.portal.mpans"
    assert dict((t, pk) for t, pk, _ in tables)["tariffs"] == ("tariff_id", "valid_from")


def test_project_step_creates_when_absent_and_sets_autoscaling():
    p, cli, _, _ = prov({("postgres", "get-project"): "Error: Project with name 'projects/hh-demo' not found",
                         ("postgres", "create-project"): {"name": "projects/hh-demo"},
                         ("postgres", "get-endpoint"): {"status": {"autoscaling_limit_min_cu": 1,
                                                                   "autoscaling_limit_max_cu": 1}},
                         ("postgres", "update-endpoint"): {}})
    p.project()
    [create] = cli.ran("create-project")
    assert create[3] == "hh-demo" and json.loads(create[5])["spec"]["pg_version"] == 17
    [update] = cli.ran("update-endpoint")
    assert update[3] == "projects/hh-demo/branches/production/endpoints/primary"
    assert json.loads(update[6])["spec"]["autoscaling_limit_max_cu"] == 4.0
    assert all(c[-4:] == ["--profile", "prof", "-o", "json"] for c in cli.calls)


def test_project_step_reuses_an_existing_project():
    p, cli, _, _ = prov({("postgres", "get-project"): {"uid": "u"}, ("postgres", "get-endpoint"): EP_JSON})
    p.project()
    assert not cli.ran("create-project") and not cli.ran("update-endpoint")
    assert "project hh-demo exists: reusing it" in p.log


def test_scale_up_only_before_the_initial_snapshot():
    p, cli, _, _ = prov({("postgres", "get-synced-table"): "does not exist", ("postgres", "get-endpoint"): EP_JSON,
                         ("postgres", "update-endpoint"): {}})
    p.scale_up()
    [u] = cli.ran("update-endpoint")
    spec = json.loads(u[6])["spec"]
    assert (spec["autoscaling_limit_min_cu"], spec["autoscaling_limit_max_cu"]) == (8.0, 8.0)
    p, cli, _, _ = prov({("postgres", "get-synced-table"): {"status": {"detailed_state": "SYNCED_TABLE_ONLINE"}},
                         ("postgres", "get-endpoint"): EP_JSON})
    p.scale_up()
    assert not cli.ran("update-endpoint")
    p, cli, _, _ = prov({("postgres", "get-endpoint"): {**EP_JSON, "status": {**EP_JSON["status"],
                                                                              "autoscaling_limit_min_cu": 8,
                                                                              "autoscaling_limit_max_cu": 8}},
                         ("postgres", "update-endpoint"): {}})
    p.scale_down()
    assert json.loads(cli.ran("update-endpoint")[0][6])["spec"]["autoscaling_limit_max_cu"] == 4.0


def test_dry_run_never_mutates():
    p, cli, conn, _ = prov({("postgres", "get-project"): "not found", ("postgres", "get-endpoint"): {},
                            ("tables", "get"): "Table 'cat.demo.silver_hh_readings' does not exist.",
                            ("schemas", "get"): "Schema 'cat.portal' does not exist.",
                            ("postgres", "get-synced-table"): "does not exist",
                            ("postgres", "get-data-api"): "Data API is not enabled"}, dry_run=True)
    p.project(), p.scale_up(), p.sync(), p.wait(), p.indexes(), p.gold(), p.data_api()
    assert not any(c[2].startswith(("create", "update", "delete")) for c in cli.calls)
    assert conn.statements == []
    assert any(line.startswith("[dry-run] databricks postgres create-synced-table cat.portal.silver_hh_readings")
               for line in p.log)
    assert any(line.startswith("[dry-run] SQL CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold.daily_mpan")
               for line in p.log)


def test_sync_requires_cdf_and_creates_schema_and_missing_tables_once():
    p, _, _, _ = prov({("tables", "get"): {"properties": {"delta.enableChangeDataFeed": "false"}}})
    with pytest.raises(RuntimeError, match="Change Data Feed"):
        p.sync()
    p, _, _, _ = prov({("tables", "get"): "Table does not exist."})
    with pytest.raises(RuntimeError, match="setup job"):
        p.sync()
    existing = {"synced_tables/cat.portal.mpans"}

    def get(args):
        return {"status": {"detailed_state": "SYNCED_TABLE_ONLINE"}} if args[3] in existing else "does not exist"

    p, cli, _, _ = prov({**CDF_ON, ("schemas", "get"): "Schema 'cat.portal' does not exist.", ("schemas", "create"): {},
                         ("postgres", "get-synced-table"): get, ("postgres", "create-synced-table"): {}})
    p.sync()
    [schema] = cli.ran("create")
    assert schema[3:5] == ["portal", "cat"]
    created = {c[3]: json.loads(c[c.index("--json") + 1])["spec"] for c in cli.ran("create-synced-table")}
    assert "cat.portal.mpans" not in created and len(created) == 5
    assert created["cat.portal.silver_hh_readings"]["scheduling_policy"] == "TRIGGERED"
    assert created["cat.portal.organisations"]["scheduling_policy"] == "SNAPSHOT"
    assert all("--no-wait" in c for c in cli.ran("create-synced-table"))


def test_wait_polls_until_online_and_fails_on_pipeline_failure():
    states = iter(["SYNCED_TABLE_PROVISIONING_INITIAL_SNAPSHOT"] + ["SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"] * 20)
    p, _, _, _ = prov({("postgres", "get-synced-table"): lambda a: {"status": {
        "detailed_state": next(states), "ongoing_sync_progress": {"synced_row_count": 5, "total_row_count": 9}}}})
    p.wait()
    assert any("waiting cat.portal.organisations" in line and "5/9 rows" in line for line in p.log)
    assert any(line.startswith("ONLINE cat.portal.silver_hh_readings") for line in p.log)
    p, _, _, _ = prov({("postgres", "get-synced-table"): {"status": {
        "detailed_state": "SYNCED_TABLE_OFFLINE_FAILED", "message": "boom"}}})
    with pytest.raises(RuntimeError, match="failed"):
        p.wait()


def test_indexes_connect_as_the_owner_with_an_in_memory_token():
    p, _, conn, connects = prov(dict(CRED))
    p.indexes()
    assert connects == [(HOST, "owner", "TOKEN", "databricks_postgres")] and conn.closed
    idx = pl.index_statements(p.s)
    assert conn.statements[:len(idx)] == idx
    assert 'CREATE INDEX IF NOT EXISTS "silver_hh_readings_mpan_reading_date_idx" ON "portal"."silver_hh_readings" ' \
           '("mpan", "reading_date")' in idx
    assert 'ANALYZE "portal"."silver_hh_readings"' in idx and 'ANALYZE "portal"."mpans"' in idx
    assert not any("TOKEN" in line for line in p.log)
    assert "portal.silver_hh_readings: 42 rows" in p.log


def test_gold_applies_the_file_then_analyzes_and_reloads():
    p, cli, conn, _ = prov(dict(CRED))
    p.gold()
    st = conn.statements
    assert st[0] == "SET statement_timeout = 0" and st[1] == "CREATE SCHEMA IF NOT EXISTS portal_gold"
    ddl = [x for x in st if not x.startswith("SELECT count(*)")]
    assert "ANALYZE portal_gold.daily_mpan" in ddl and ddl[-1] == "NOTIFY pgrst, 'reload schema'"
    assert not any(x.startswith("DROP") for x in st)
    assert not any("bench." in x for x in st)
    assert "portal_gold.daily_mpan: 42 rows" in p.log
    assert not cli.ran("update-data-api")  # exposure is the data-api step


def test_gold_rebuild_drops_first_and_bad_sql_is_refused(tmp_path):
    stmts = pl.gold_statements(settings(rebuild=True))
    assert stmts[0] == "DROP VIEW IF EXISTS portal_gold.hourly_org"
    bad = tmp_path / "gold.sql"
    bad.write_text("CREATE SCHEMA IF NOT EXISTS portal_gold;\nGRANT SELECT ON ALL TABLES IN SCHEMA portal_gold TO x;\n")
    with pytest.raises(ValueError, match="checks"):
        pl.gold_statements(settings(gold_sql=bad))


def test_data_api_enables_or_extends_and_reloads():
    p, cli, _, _ = prov({("postgres", "get-data-api"): "Error: Data API is not enabled for this database",
                         ("postgres", "create-data-api"): {"status": {"url": "https://u", "db_schemas": ["portal_gold"]}}})
    assert p.data_api() == "https://u"
    [c] = cli.ran("create-data-api")
    assert c[3] == "projects/hh-demo/branches/production/databases/databricks-postgres"
    assert json.loads(c[5]) == {"spec": {"db_schemas": ["portal_gold"], "db_max_rows": 2000}}
    p, cli, _, _ = prov({("postgres", "get-data-api"): API, ("postgres", "update-data-api"): API})
    p.data_api()
    [u] = cli.ran("update-data-api")
    assert u[3].endswith("/databases/databricks-postgres/data-api") and u[4] == "spec.db_schemas"
    assert json.loads(u[6])["spec"]["db_schemas"] == ["public", "portal_gold"]
    p, _, _, _ = prov({("postgres", "get-data-api"): {"status": {"db_schemas": [], "available_schemas": ["public"]}}})
    with pytest.raises(RuntimeError, match="gold step first"):
        p.data_api()


def test_grants_cover_portal_gold_only():
    p, _, _, _ = prov({})
    with pytest.raises(RuntimeError, match="--app"):
        p.grants()
    p, cli, conn, _ = prov({**CRED, ("apps", "get"): {"service_principal_client_id": "sp-portal"}}, app="hh-portal-dev")
    p.grants()
    assert cli.ran("get")[0][3] == "hh-portal-dev"
    assert conn.statements == pl.grant_statements("sp-portal", "portal_gold")
    joined = " ".join(conn.statements)
    assert 'GRANT "sp-portal" TO authenticator' in joined and "databricks_create_role('sp-portal'" in joined
    assert '"portal_gold"' in joined and 'SCHEMA "portal"' not in joined
    grants = [x for x in conn.statements if x.startswith(("GRANT", "ALTER DEFAULT"))]
    assert all(("USAGE" in x or "SELECT" in x or "TO authenticator" in x) for x in grants)
    assert not any(w in joined for w in ("INSERT", "UPDATE", "DELETE", "ALL PRIVILEGES"))
    assert not cli.ran("create-role")  # the role API's roles cannot be granted to authenticator


def test_grant_statements_quote_identifiers():
    g = pl.grant_statements('a1b2-c3"d')
    assert g[0] == "CREATE EXTENSION IF NOT EXISTS databricks_auth"
    assert "databricks_create_role('a1b2-c3\"d', 'SERVICE_PRINCIPAL')" in g[1]
    assert g[2] == 'GRANT "a1b2-c3""d" TO authenticator'
    assert g[3] == 'GRANT USAGE ON SCHEMA "portal_gold" TO "a1b2-c3""d"'


def test_outputs_print_the_bundle_variables():
    p, _, _, _ = prov({("postgres", "get-project"): {"uid": "proj-uid"}, ("postgres", "get-branch"): {"uid": "br-uid"},
                       ("postgres", "get-data-api"): {"status": {"url": "https://u"}},
                       ("postgres", "get-synced-table"): {"status": {"pipeline_id": "pipe-1"}}})
    assert p.outputs() == {"lakebase_data_api_url": "https://u", "lakebase_project_uid": "proj-uid",
                           "lakebase_branch_uid": "br-uid", "sync_pipeline_id": "pipe-1"}


def test_teardown_asks_for_confirmation_and_keeps_the_project_by_default():
    online = {"status": {"detailed_state": "SYNCED_TABLE_ONLINE"}}
    responses = {**CRED, ("postgres", "get-synced-table"): online, ("postgres", "delete-synced-table"): {},
                 ("postgres", "get-project"): {"uid": "u"},
                 ("postgres", "get-data-api"): {"status": {"db_schemas": ["public", "portal_gold"]}},
                 ("postgres", "update-data-api"): {}, ("schemas", "get"): {}, ("schemas", "delete"): {}}
    p, cli, conn, _ = prov(responses, answer="nope")
    with pytest.raises(SystemExit, match="not confirmed"):
        p.teardown()
    assert not cli.ran("delete-synced-table")
    p, cli, conn, _ = prov(responses, answer="hh-demo")
    p.teardown()
    assert len(cli.ran("delete-synced-table")) == 6 and not cli.ran("delete-project")
    assert conn.statements == pl.teardown_statements(p.s)
    assert conn.statements[0] == 'DROP SCHEMA IF EXISTS "portal_gold" CASCADE'
    assert json.loads(cli.ran("update-data-api")[0][6])["spec"]["db_schemas"] == ["public"]
    assert cli.ran("delete")[0][3] == "cat.portal"


def test_teardown_can_delete_the_whole_project_and_dry_run_deletes_nothing():
    responses = {("postgres", "get-synced-table"): "does not exist", ("postgres", "get-project"): {"uid": "u"},
                 ("postgres", "delete-project"): {}, ("schemas", "get"): "does not exist"}
    p, cli, conn, _ = prov(responses, delete_project=True, yes=True)
    p.teardown()
    assert cli.ran("delete-project")[0][3] == "projects/hh-demo" and conn.statements == []
    p, cli, _, _ = prov({**responses, ("postgres", "get-synced-table"): {"status": {}}}, delete_project=True,
                        dry_run=True)
    p.teardown()
    assert not any(c[2].startswith("delete") for c in cli.calls)


def test_unexpected_cli_errors_propagate():
    p, _, _, _ = prov({("postgres", "get-project"): "Error: PERMISSION_DENIED"})
    with pytest.raises(pl.CliError):
        p.project()


def test_all_order():
    assert pl.ALL == ["project", "scale-up", "sync", "wait", "indexes", "gold", "data-api", "scale-down", "outputs"]
