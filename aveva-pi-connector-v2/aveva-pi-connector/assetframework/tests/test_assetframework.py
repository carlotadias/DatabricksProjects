"""Unit tests for the aveva-pi-assetframework thin client — offline, no Spark/PI.

Each library function is a single PI Web API call; these assert it hits the right
endpoint/params and returns the API JSON verbatim (no transformation).
"""

import sys
import types


def _install_pyspark_stubs():
    # not strictly needed (the client has no pyspark import) but harmless + future-proof
    ds = types.ModuleType("pyspark.sql.datasource")
    for cls in ["DataSource", "DataSourceReader", "DataSourceStreamReader", "InputPartition"]:
        setattr(ds, cls, type(cls, (), {}))
    tp = types.ModuleType("pyspark.sql.types")
    for cls in ["StructType", "StructField", "StringType", "DoubleType",
                "BooleanType", "TimestampType"]:
        setattr(tp, cls, type(cls, (), {"__init__": lambda self, *a, **k: None}))
    sys.modules.update({"pyspark": types.ModuleType("pyspark"),
                        "pyspark.sql": types.ModuleType("pyspark.sql"),
                        "pyspark.sql.datasource": ds, "pyspark.sql.types": tp})


_install_pyspark_stubs()

import aveva_pi_assetframework as af          # noqa: E402
from aveva_pi_assetframework import client as c  # noqa: E402
from aveva_pi_assetframework import _http as h   # noqa: E402


class _FakeSession:
    def __init__(self, router):
        self._router = router; self.headers = {}; self.calls = []
    def request(self, method, url, params=None, json=None, timeout=None):
        self.calls.append((method, url, params, json))
        return _FakeResp(self._router(method, url, params or {}, json))


class _FakeResp:
    def __init__(self, payload, status_code=200):
        self._p = payload; self.status_code = status_code; self.headers = {}
    def raise_for_status(self): pass
    def json(self): return self._p


def _patch(monkeypatch, router):
    sess = _FakeSession(router)
    def fake_request_json(session, method, url, *, params=None, json_body=None, timeout=60, retries=4):
        return sess.request(method, url, params=params, json=json_body, timeout=timeout).json()
    for mod in (h, c):
        if hasattr(mod, "session"):
            monkeypatch.setattr(mod, "session", lambda *a, **k: sess)
        if hasattr(mod, "request_json"):
            monkeypatch.setattr(mod, "request_json", fake_request_json)
    return sess


BASE = "http://pi/piwebapi"


def test_get_point_calls_points_by_path(monkeypatch):
    sess = _patch(monkeypatch, lambda m, u, p, b: {"WebId": "W1", "Path": p["path"], "EngineeringUnits": "degC"})
    out = af.get_point(BASE, "PISRV", "Plant.A.U1.Temp", bearer="tok")
    assert out == {"WebId": "W1", "Path": r"\\PISRV\Plant.A.U1.Temp", "EngineeringUnits": "degC"}
    m, u, p, b = sess.calls[0]
    assert m == "GET" and u == f"{BASE}/points" and p["path"] == r"\\PISRV\Plant.A.U1.Temp"


def test_batch_posts_body_verbatim(monkeypatch):
    captured = {}
    def router(m, u, p, b):
        captured["m"], captured["u"], captured["b"] = m, u, b
        return {"1": {"Status": 200, "Content": {"WebId": "W1"}}}
    _patch(monkeypatch, router)
    reqs = {"1": {"Method": "GET", "Resource": f"{BASE}/points?path=x"}}
    out = af.batch(BASE, reqs, bearer="tok")
    assert captured["m"] == "POST" and captured["u"] == f"{BASE}/batch"
    assert captured["b"] == reqs                      # body passed through untouched
    assert out["1"]["Content"]["WebId"] == "W1"       # response returned verbatim


def test_get_asset_database(monkeypatch):
    sess = _patch(monkeypatch, lambda m, u, p, b: {"WebId": "DB1", "Path": p["path"]})
    out = af.get_asset_database(BASE, "PISRV", "MyDB", bearer="tok")
    assert out["WebId"] == "DB1"
    _, u, p, _ = sess.calls[0]
    assert u == f"{BASE}/assetdatabases" and p["path"] == r"\\PISRV\MyDB"


def test_get_database_elements_pages(monkeypatch):
    sess = _patch(monkeypatch, lambda m, u, p, b: {"Items": [{"WebId": "E1"}]})
    out = af.get_database_elements(BASE, "DB1", bearer="tok", start_index=10, max_count=50)
    assert out["Items"][0]["WebId"] == "E1"
    _, u, p, _ = sess.calls[0]
    assert u == f"{BASE}/assetdatabases/DB1/elements"
    assert p["startIndex"] == 10 and p["maxCount"] == 50


def test_get_child_elements(monkeypatch):
    sess = _patch(monkeypatch, lambda m, u, p, b: {"Items": []})
    af.get_child_elements(BASE, "E1", bearer="tok")
    assert sess.calls[0][1] == f"{BASE}/elements/E1/elements"


def test_get_element_attributes(monkeypatch):
    sess = _patch(monkeypatch, lambda m, u, p, b: {"Items": [{"WebId": "A1", "Name": "Temp"}]})
    out = af.get_element_attributes(BASE, "E1", bearer="tok")
    assert out["Items"][0]["Name"] == "Temp"
    assert sess.calls[0][1] == f"{BASE}/elements/E1/attributes"


def test_public_surface_is_primitives_only():
    # the removed orchestration helpers must NOT be re-exported
    for gone in ("resolve_webids", "discover_af", "PIPointsSource", "POINTS_SCHEMA"):
        assert not hasattr(af, gone), f"{gone} should have been removed"
    assert af.__version__ == "3.0.2"


def test_get_point_threads_basic_auth(monkeypatch):
    # basic_user/basic_password on a client call reach session() as a Basic session
    captured = {}
    real_session = h.session
    def spy(*a, **k):
        captured.update(k)
        return real_session(*a, **k)
    monkeypatch.setattr(h, "session", spy)
    monkeypatch.setattr(c, "session", spy)
    monkeypatch.setattr(c, "request_json", lambda *a, **k: {"WebId": "W1"})
    af.get_point(BASE, "PISRV", "Plant.A.U1.Temp", basic_user="DOMAIN\\svc-pi", basic_password="pw")
    assert captured.get("basic_user") == "DOMAIN\\svc-pi" and captured.get("basic_password") == "pw"


def test_get_point_threads_verify_tls(monkeypatch):
    # verify_tls=False on a client call reaches session()
    captured = {}
    real_session = h.session
    def spy(*a, **k):
        captured.update(k)
        return real_session(*a, **k)
    monkeypatch.setattr(h, "session", spy)
    monkeypatch.setattr(c, "session", spy)
    monkeypatch.setattr(c, "request_json", lambda *a, **k: {"WebId": "W1"})
    af.get_point(BASE, "PISRV", "Plant.A.U1.Temp", basic_user="u", basic_password="pw", verify_tls=False)
    assert captured.get("verify_tls") is False
