"""hhportal.dataapi: allow-listed PostgREST GETs with httpx.MockTransport (no network)."""

import asyncio
import json

import httpx
import pytest

from hhportal.dataapi import DataApiClient, DataApiError, TokenSource

BASE = "https://lakebase.example.com/api/2.0/workspace/1/rest/databricks_postgres"


def run(coro):
    return asyncio.run(coro)


def client(handler, *, page_size=2000, max_pages=10, fetch=None):
    tokens = TokenSource(fetch or (lambda: {"Authorization": "Bearer T1"}))
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return DataApiClient(BASE, "portal_gold", tokens, http, page_size=page_size, max_pages=max_pages), tokens


def test_get_builds_a_readable_postgrest_path_with_auth():
    seen = []

    def handler(req):
        seen.append(req)
        return httpx.Response(200, json=[{"mpan": "1"}])

    c, _ = client(handler)
    res = run(c.get("daily_account", [("select", "reading_date,kwh"), ("account_id", "eq.87007919"),
                                      ("reading_date", "gte.2026-09-01"), ("reading_date", "lte.2026-09-30")]))
    assert res.rows == [{"mpan": "1"}]
    [call] = res.calls
    assert call["path"] == ("/portal_gold/daily_account?select=reading_date,kwh&account_id=eq.87007919"
                            "&reading_date=gte.2026-09-01&reading_date=lte.2026-09-30&limit=2000&offset=0")
    assert call["rows"] == 1 and call["status"] == 200 and "ms" in call
    assert str(seen[0].url) == BASE + call["path"]
    assert seen[0].method == "GET" and seen[0].headers["authorization"] == "Bearer T1"
    assert "T1" not in json.dumps(res.calls)  # the token never appears in what the UI is shown


def test_in_filters_keep_parentheses_and_commas():
    c, _ = client(lambda req: httpx.Response(200, json=[]))
    res = run(c.get("daily_mpan", [("mpan", "in.(1000000271828,1007919271828)")]))
    assert "mpan=in.(1000000271828,1007919271828)" in res.calls[0]["path"]


def test_pages_until_a_short_page():
    pages = [[{"i": i} for i in range(3)], [{"i": 3}, {"i": 4}, {"i": 5}], [{"i": 6}]]

    def handler(req):
        off = int(req.url.params["offset"])
        return httpx.Response(200, json=pages[off // 3])

    c, _ = client(handler, page_size=3)
    res = run(c.get("hh_account", [("account_id", "eq.87007919")]))
    assert [r["i"] for r in res.rows] == list(range(7)) and len(res.calls) == 3


def test_too_many_pages_is_an_error():
    c, _ = client(lambda req: httpx.Response(200, json=[{"i": 1}, {"i": 2}]), page_size=2, max_pages=2)
    with pytest.raises(DataApiError) as e:
        run(c.get("hh_org", [("org_id", "eq.88104729")]))
    assert e.value.category == "too_many_rows"


def test_explicit_limit_is_a_single_request():
    c, _ = client(lambda req: httpx.Response(200, json=[{"reading_date": "2026-10-05"}]))
    res = run(c.get("daily_org", [("select", "reading_date"), ("order", "reading_date.desc"), ("limit", "1")]))
    assert res.calls[0]["path"].endswith("limit=1") and len(res.calls) == 1


def test_views_and_parameters_are_allow_listed():
    c, _ = client(lambda req: httpx.Response(200, json=[]))
    for view, params in [("silver_hh_readings", []), ("../portal/mpans", []), ("daily_mpan", [("or", "(x)")]),
                         ("daily_mpan", [("select", "*,mpans(*)")]), ("daily_mpan", [("mpan", "like.*")]),
                         ("daily_mpan", [("mpan;drop", "eq.1")])]:
        with pytest.raises(DataApiError) as e:
            run(c.get(view, params))
        assert e.value.category == "not_allowed"


def test_401_refreshes_the_token_once_and_retries():
    tokens_issued = iter(["Bearer OLD", "Bearer NEW"])
    seen = []

    def handler(req):
        seen.append(req.headers["authorization"])
        return httpx.Response(401 if len(seen) == 1 else 200, json=[] if len(seen) > 1 else {"message": "expired"})

    c, tokens = client(handler, fetch=lambda: {"Authorization": next(tokens_issued)})
    run(c.get("directory", [("select", "mpan"), ("limit", "1")]))
    assert seen == ["Bearer OLD", "Bearer NEW"] and tokens.fetches == 2


@pytest.mark.parametrize("status,category", [(403, "permission"), (404, "not_found"), (400, "bad_request"),
                                             (503, "server")])
def test_http_errors_are_categorised_and_sanitised(status, category):
    body = {"code": "42501", "message": "permission denied to set role; Bearer abc.def.ghi"}
    c, _ = client(lambda req: httpx.Response(status, json=body))
    with pytest.raises(DataApiError) as e:
        run(c.get("directory", [("limit", "1")]))
    assert e.value.category == category and e.value.status == status
    assert "abc.def" not in e.value.message and e.value.calls[0]["status"] == status


def test_timeouts_and_network_errors():
    def boom(req):
        raise httpx.ConnectTimeout("slow")

    c, _ = client(boom)
    with pytest.raises(DataApiError) as e:
        run(c.get("directory", [("limit", "1")]))
    assert e.value.category == "timeout"


def test_token_source_caches_and_refreshes_off_loop():
    calls = []

    def fetch():
        calls.append(1)
        return {"Authorization": f"Bearer {len(calls)}"}

    now = [0.0]
    ts = TokenSource(fetch, max_age_s=100, clock=lambda: now[0])

    async def go():
        a = await ts.headers()
        b = await ts.headers()
        now[0] = 150
        c = await ts.headers()
        ts.invalidate()
        d = await ts.headers()
        return a, b, c, d

    a, b, c, d = run(go())
    assert a == b == {"Authorization": "Bearer 1"} and c == {"Authorization": "Bearer 2"} and len(calls) == 3
    assert d == {"Authorization": "Bearer 3"}
