"""hhportal.server routes against a mocked Data API (httpx.MockTransport): mapping, validation, errors."""

import httpx
from fastapi.testclient import TestClient

from hhportal.dataapi import DataApiClient, TokenSource
from hhportal.server import Settings, create_app

BASE = "https://lakebase.example.com/api/2.0/workspace/1/rest/databricks_postgres"
DAILY = [{"reading_date": "2026-10-01", "kwh": 48.0, "consumption_gbp": 12.0, "standing_charge_gbp": 0.96,
          "total_gbp": 12.96, "peak_kwh": 2.0, "peak_ts": "2026-10-01T12:00:00+00:00", "peak_mpan": "1000000271828",
          "avg_kwh_per_reading": 0.2, "readings": 240}]
HH = [{"reading_ts": f"2026-10-01T{h:02d}:{m:02d}:00+00:00", "reading_date": "2026-10-01", "kwh": 1.0,
       "cost_gbp": 0.25} for h in range(24) for m in (0, 30)]


def make(routes, settings=None):
    seen = []

    def handler(req):
        seen.append(req)
        view = req.url.path.rsplit("/", 1)[-1]
        r = routes.get(view)
        if callable(r):
            r = r(req)
        if isinstance(r, httpx.Response):
            return r
        return httpx.Response(200, json=r if r is not None else [])

    client = DataApiClient(BASE, "portal_gold", TokenSource(lambda: {"Authorization": "Bearer SECRET"}),
                           httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    app = create_app(settings or Settings(BASE), client, static_dir=__import__("pathlib").Path("/nonexistent"))
    return TestClient(app), seen


def test_me_shows_identity_and_synthetic_flag():
    tc, _ = make({})
    r = tc.get("/api/me", headers={"X-Forwarded-Email": "demo-advisor"}).json()
    assert r == {"identity": "demo-advisor", "environment": "dev", "schema": "portal_gold",
                 "data_api_configured": True, "synthetic": True}


def test_usage_half_hourly_account_day_uses_two_data_api_calls():
    tc, seen = make({"daily_account": DAILY, "hh_account": HH})
    r = tc.get("/api/usage", params={"scope": "account", "ids": "87007919", "granularity": "halfhour",
                                     "start": "2026-10-01", "end": "2026-10-01"})
    assert r.status_code == 200
    body = r.json()
    assert len(body["rows"]) == 48 and body["rows"][0]["standing_gbp"] == 0.96 / 48
    assert body["summary"]["kwh"] == 48.0 and body["summary"]["peak"]["kwh"] == 2.0
    paths = [c["path"] for c in body["data_api"]]
    assert paths[0].startswith("/portal_gold/daily_account?select=reading_date,kwh,consumption_gbp")
    assert paths[1].startswith("/portal_gold/hh_account?select=reading_ts,reading_date,kwh,cost_gbp"
                               "&account_id=eq.87007919&reading_date=gte.2026-10-01&reading_date=lte.2026-10-01")
    assert all(req.method == "GET" for req in seen)
    assert "SECRET" not in r.text


def test_usage_daily_is_a_single_call():
    tc, seen = make({"daily_mpan": DAILY})
    r = tc.get("/api/usage", params={"scope": "mpan", "ids": "1000000271828,1007919271828", "granularity": "day",
                                     "start": "2026-10-01", "end": "2026-10-07"}).json()
    assert len(seen) == 1 and "mpan=in.(1000000271828,1007919271828)" in r["data_api"][0]["path"]
    assert r["rows"][0]["total_gbp"] == 12.96


def test_validation_errors_never_reach_the_data_api():
    tc, seen = make({})
    for params in [{"scope": "account", "ids": "88007919", "granularity": "day", "start": "2026-10-01",
                    "end": "2026-10-01"},
                   {"scope": "account", "ids": "87007919", "granularity": "day", "start": "2026-10-01",
                    "end": "2026-10-01T00:00"},
                   {"scope": "org", "ids": "88104729", "granularity": "halfhour", "start": "2026-09-01",
                    "end": "2026-09-30"}]:
        r = tc.get("/api/usage", params=params)
        assert r.status_code == 400 and r.json()["errors"]
    assert tc.get("/api/lookup", params={"number": "12345"}).status_code == 400
    assert seen == []


def test_lookup_by_org_and_account_numbers():
    org = [{"org_id": "88104729", "org_name": "Alder Foods Ltd", "is_tpi": False, "accounts": 3, "sites": 9,
            "mpans": 20}]
    acc = [{"account_id": "87007919", "account_name": "Alder Foods Ltd - Offices 0001", "org_id": "88104729",
            "org_name": "Alder Foods Ltd", "is_tpi": False, "sites": 3, "mpans": 5}]
    tc, _ = make({"org_directory": org, "account_directory": acc})
    r = tc.get("/api/lookup", params={"number": "88104729"}).json()
    assert r["kind"] == "org" and r["org_id"] == "88104729" and r["account_id"] is None
    r = tc.get("/api/lookup", params={"number": "87007919"}).json()
    assert r["kind"] == "account" and r["account_id"] == "87007919" and r["org_id"] == "88104729"
    tc, _ = make({"org_directory": []})
    assert tc.get("/api/lookup", params={"number": "88000000"}).status_code == 404


def test_customer_loads_org_accounts_and_meters():
    tc, seen = make({"org_directory": [{"org_id": "88104729", "org_name": "X", "is_tpi": True}],
                     "account_directory": [{"account_id": "87007919"}],
                     "directory": [{"mpan": "1000000271828", "account_id": "87007919", "postcode": "AB1 2CD"}]})
    r = tc.get("/api/customer", params={"org_id": "88104729"}).json()
    assert r["org"]["is_tpi"] and r["accounts"] and r["mpans"][0]["postcode"] == "AB1 2CD"
    assert [c["path"].split("?")[0] for c in r["data_api"]] == [
        "/portal_gold/org_directory", "/portal_gold/account_directory", "/portal_gold/directory"]
    assert tc.get("/api/customer", params={"org_id": "87007919"}).status_code == 400


def test_data_api_errors_are_reported_with_the_path_and_no_token():
    tc, _ = make({"daily_org": httpx.Response(401, json={"code": "42501",
                                                          "message": "permission denied to set role"})})
    r = tc.get("/api/usage", params={"scope": "org", "ids": "88104729", "granularity": "day", "start": "2026-10-01",
                                     "end": "2026-10-01"})
    assert r.status_code == 502
    body = r.json()
    assert body["error"] == "permission" and "42501" in body["message"]
    assert body["data_api"][0]["path"].startswith("/portal_gold/daily_org?") and "SECRET" not in r.text


def test_not_configured_is_a_clear_503():
    tc, _ = make({}, settings=Settings("auto"))
    r = tc.get("/api/freshness")
    assert r.status_code == 503 and r.json()["detail"]["error"] == "not_configured"
    assert tc.get("/api/me").json()["data_api_configured"] is False


def test_freshness_reports_the_data_window():
    def daily_org(req):
        desc = "desc" in req.url.params["order"]
        return [{"reading_date": "2026-10-08" if desc else "2025-09-09"}]

    tc, _ = make({"daily_org": daily_org})
    r = tc.get("/api/freshness").json()
    assert r["last_date"] == "2026-10-08" and r["first_date"] == "2025-09-09" and len(r["data_api"]) == 2


def test_architecture_config_comes_from_env_only(monkeypatch):
    env = {"HH_PORTAL_DATA_API_URL": BASE, "HH_PORTAL_WORKSPACE_HOST": "https://workspace.example.com/",
           "HH_PORTAL_CATALOG": "cat", "HH_PORTAL_DATA_SCHEMA": "demo", "HH_PORTAL_SYNC_PIPELINE_ID": "pipe-1",
           "HH_PORTAL_DAILY_JOB_ID": "11", "HH_PORTAL_SETUP_JOB_ID": "22", "HH_PORTAL_LAKEBASE_PROJECT": "proj",
           "HH_PORTAL_LAKEBASE_BRANCH": "production", "HH_PORTAL_REPO_URL": "https://github.com/org/repo/tree/main/x",
           "HH_PORTAL_DAILY_SCHEDULE": "0 0 2 * * ? (UTC)",
           "HH_PORTAL_LAKEBASE_PROJECT_UID": "proj-uid-1",
           "HH_PORTAL_LAKEBASE_BRANCH_UID": "br-example-1"}
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    tc, seen = make({}, settings=Settings.from_env())
    cfg = tc.get("/api/architecture").json()
    assert cfg["host"] == "https://workspace.example.com" and cfg["catalog"] == "cat"
    assert cfg["data_schema"] == "demo" and cfg["synced_schema"] == "portal"
    assert cfg["daily_schedule"] == "0 0 2 * * ? (UTC)"
    assert cfg["sync_pipeline_id"] == "pipe-1" and cfg["daily_job_id"] == "11" and cfg["setup_job_id"] == "22"
    assert cfg["gold_schema"] == "portal_gold" and cfg["repo_url"].startswith("https://github.com/")
    assert cfg["lakebase_project_uid"] == "proj-uid-1"
    assert cfg["lakebase_branch_uid"] == "br-example-1" and cfg["data_api_url"] == BASE
    assert cfg["lakebase_data_api_tab"] == "data-api"  # Lakebase UI Data API tab
    assert seen == []  # no Data API call for the static config


def test_architecture_config_drops_unsafe_values(monkeypatch):
    monkeypatch.setenv("HH_PORTAL_WORKSPACE_HOST", "javascript:alert(1)")
    monkeypatch.setenv("DATABRICKS_HOST", "javascript:alert(1)")
    monkeypatch.setenv("HH_PORTAL_SYNC_PIPELINE_ID", "../../x")
    monkeypatch.setenv("HH_PORTAL_REPO_URL", "http://insecure.example")
    monkeypatch.setenv("HH_PORTAL_LAKEBASE_BRANCH_UID", "auto")
    monkeypatch.setenv("HH_PORTAL_LAKEBASE_DATA_API_TAB", "../evil?x=1")
    tc, _ = make({}, settings=Settings.from_env())
    cfg = tc.get("/api/architecture").json()
    assert cfg["host"] is None and cfg["sync_pipeline_id"] is None and cfg["repo_url"] is None
    assert cfg["lakebase_branch_uid"] is None and cfg["lakebase_data_api_tab"] == "data-api"


def test_facts_are_read_through_the_data_api():
    orgs = [{"org_id": "88000001", "accounts": 3, "mpans": 10}, {"org_id": "88000002", "accounts": 2, "mpans": 5}]
    tc, seen = make({"daily_mpan": [{"reading_date": "2026-10-08"}], "org_directory": orgs,
                     "directory": [{"mpan": "1000000271828"}]})
    r = tc.get("/api/facts").json()
    assert r["last_date"] == "2026-10-08" and r["orgs"] == 2 and r["accounts"] == 5 and r["mpans"] == 15
    assert r["sample_mpan"] == "1000000271828" and len(r["data_api"]) == 3
    paths = [c["path"] for c in r["data_api"]]
    assert paths[0] == "/portal_gold/daily_mpan?select=reading_date&order=reading_date.desc&limit=1"
    assert paths[1].startswith("/portal_gold/org_directory?select=org_id,accounts,mpans")
    assert r["latency_ms"] == sum(c["ms"] for c in r["data_api"]) and all(q.method == "GET" for q in seen)


def test_architecture_host_falls_back_to_the_apps_databricks_host(monkeypatch):
    monkeypatch.delenv("HH_PORTAL_WORKSPACE_HOST", raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", "workspace-2.example.com")
    tc, _ = make({}, settings=Settings.from_env())
    assert tc.get("/api/architecture").json()["host"] == "https://workspace-2.example.com"
