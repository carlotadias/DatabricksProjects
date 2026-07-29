"""Unit tests for the aveva-pi-timeseries connector — offline, no Spark/PI."""

import sys
import types
from datetime import datetime, timezone


def _install_pyspark_stubs():
    ds = types.ModuleType("pyspark.sql.datasource")
    for cls in ["DataSource", "DataSourceReader", "DataSourceStreamReader", "InputPartition"]:
        setattr(ds, cls, type(cls, (), {}))
    tp = types.ModuleType("pyspark.sql.types")
    for cls in ["StructType", "StructField", "StringType", "DoubleType",
                "BooleanType", "TimestampType"]:
        setattr(tp, cls, type(cls, (), {"__init__": lambda self, *a, **k: None}))
    pkg = types.ModuleType("pyspark"); sqlpkg = types.ModuleType("pyspark.sql")
    sys.modules.update({"pyspark": pkg, "pyspark.sql": sqlpkg,
                        "pyspark.sql.datasource": ds, "pyspark.sql.types": tp})


_install_pyspark_stubs()

import aveva_pi_timeseries as tsp           # noqa: E402
from aveva_pi_timeseries import reader as r  # noqa: E402
from aveva_pi_timeseries import _http as h   # noqa: E402


class _FakeSession:
    def __init__(self, router):
        self._router = router; self.headers = {}; self.calls = []
    def request(self, method, url, params=None, json=None, timeout=None):
        self.calls.append((method, url, params, json))
        return _FakeResp(self._router(method, url, params or (), json))


class _FakeResp:
    def __init__(self, payload, status_code=200):
        self._p = payload; self.status_code = status_code; self.headers = {}
    def raise_for_status(self): pass
    def json(self): return self._p


def _patch(monkeypatch, router):
    sess = _FakeSession(router)
    def fake_request_json(session, method, url, *, params=None, json_body=None, timeout=60, retries=4):
        return sess.request(method, url, params=params, json=json_body, timeout=timeout).json()
    for mod in (h, r):
        if hasattr(mod, "session"):
            monkeypatch.setattr(mod, "session", lambda *a, **k: sess)
        if hasattr(mod, "request_json"):
            monkeypatch.setattr(mod, "request_json", fake_request_json)
    return sess


OPTS = {"endpoint_url": "http://pi/piwebapi", "web_ids": "W-1,W-2"}


def _streamset_router(payload):
    def router(method, url, params, body):
        assert "/streamsets/" in url
        return payload
    return router


def test_helpers():
    assert h.chunk([1, 2, 3], 2) == [[1, 2], [3]]
    assert h.iso(int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())) == "2026-01-01T00:00:00Z"
    assert h.parse_watermark_epoch("2026-01-01T00:00:00") == int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())


def test_web_exception_raises():
    import pytest
    with pytest.raises(RuntimeError):
        h.raise_on_web_exception({"WebException": {"StatusCode": 503, "Errors": ["boom"]}})


def test_coerce_value():
    assert r._coerce_value(3) == 3.0
    assert r._coerce_value({"Name": "On", "Value": 1}) == 1.0
    assert r._coerce_value("Running") is None
    assert r._coerce_value(True) is None


def test_requires_web_ids(monkeypatch):
    _patch(monkeypatch, _streamset_router({"Items": []}))
    import pytest
    with pytest.raises(ValueError):
        r.PITimeSeriesBatchReader({"endpoint_url": "http://pi/piwebapi"})


def test_lean_output(monkeypatch):
    payload = {"Items": [
        {"WebId": "W-1", "Items": [
            {"Timestamp": "2026-01-01T00:00:00Z", "Value": 540.5},
            {"Timestamp": "2026-01-01T00:01:00Z", "Value": None}]},
        {"WebId": "W-2", "Items": [{"Timestamp": "2026-01-01T00:00:00Z", "Value": 300.0}]}]}
    _patch(monkeypatch, _streamset_router(payload))
    rdr = r.PITimeSeriesBatchReader({**OPTS, "read_mode": "recorded",
                                     "initial_watermark": "2026-01-01T00:00:00", "lookback_seconds": "600"})
    rows = [row for p in rdr.partitions() for row in rdr.read(p)]
    assert all(len(row) == 3 for row in rows)
    vals = {(row[0], row[2]) for row in rows}
    assert ("W-1", 540.5) in vals and ("W-2", 300.0) in vals and ("W-1", None) in vals


def test_per_stream_maxcount_under_ceiling(monkeypatch):
    captured = {}
    def router(method, url, params, body):
        captured["params"] = params; return {"Items": []}
    _patch(monkeypatch, router)
    rdr = r.PITimeSeriesBatchReader({"endpoint_url": "http://pi/piwebapi",
                                     "web_ids": ",".join(f"W{i}" for i in range(50)),
                                     "read_mode": "recorded", "max_count": "10000",
                                     "webids_per_call": "50", "initial_watermark": "2026-01-01T00:00:00",
                                     "lookback_seconds": "60"})
    list(rdr.read(rdr.partitions()[0]))
    assert int(dict(captured["params"])["maxCount"]) <= h.PI_MAX_RETURNED_ITEMS // 50


def test_missing_timestamp_skipped(monkeypatch):
    payload = {"Items": [
        {"WebId": "W-1", "Value": {"Value": None, "Errors": ["bad"]}},
        {"WebId": "W-2", "Value": {"Timestamp": "2026-01-01T00:00:00Z", "Value": 5.0}}]}
    _patch(monkeypatch, _streamset_router(payload))
    rdr = r.PITimeSeriesBatchReader({**OPTS, "read_mode": "value"})
    rows = [row for p in rdr.partitions() for row in rdr.read(p)]
    assert len(rows) == 1 and rows[0][0] == "W-2"


def test_recordedattime_streaming_rejected(monkeypatch):
    _patch(monkeypatch, _streamset_router({"Items": []}))
    import pytest
    with pytest.raises(ValueError):
        r.PITimeSeriesStreamReader({**OPTS, "read_mode": "recordedattime", "as_of": "2026-01-01T00:00:00"})


def test_streaming_latest_offset_is_no_arg_and_now(monkeypatch):
    # latestOffset must take NO args (a `limit` param triggers admission control,
    # which needs getDefaultReadLimit and crashes on Databricks). It returns "now"
    # so ONE micro-batch covers [committed, now] — a wide window is chunked inside
    # _fetch, NOT paced into tens of thousands of tiny batches.
    import inspect
    _patch(monkeypatch, _streamset_router({"Items": []}))
    rdr = r.PITimeSeriesStreamReader({**OPTS, "read_mode": "interpolated",
                                      "initial_watermark": "2020-01-01T00:00:00"})
    assert list(inspect.signature(rdr.latestOffset).parameters) == []   # no start/limit
    before = int(datetime.now(timezone.utc).timestamp())
    ts = rdr.latestOffset()["ts"]
    assert before <= ts <= before + 5        # ~now, not initial_watermark + max_advance
    assert not hasattr(rdr, "getDefaultReadLimit")   # basic (non-admission-control) contract


def test_streaming_value_suppresses_late_tags(monkeypatch):
    start = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
    payload = {"Items": [
        {"WebId": "W-1", "Value": {"Timestamp": "2026-01-01T00:05:00Z", "Value": 10.0}},
        {"WebId": "W-2", "Value": {"Timestamp": "2025-12-31T23:00:00Z", "Value": 20.0}}]}
    _patch(monkeypatch, _streamset_router(payload))
    rdr = r.PITimeSeriesStreamReader({**OPTS, "read_mode": "value",
                                      "initial_watermark": "2026-01-01T00:00:00"})
    parts = rdr.partitions({"ts": start}, {"ts": start + 300})
    rows = [row for p in parts for row in rdr.read(p)]
    assert len(rows) == 1 and rows[0][0] == "W-1"


def test_source_name():
    assert tsp.PITimeSeriesSource.name() == "aveva_pi_timeseries"


def test_session_basic_auth():
    # basic_user/basic_password -> a pre-emptive HTTP Basic session (no Bearer header)
    import requests
    s = h.session(None, None, basic_user="DOMAIN\\svc-pi", basic_password="pw")
    assert isinstance(s.auth, requests.auth.HTTPBasicAuth)
    assert s.auth.username == "DOMAIN\\svc-pi" and s.auth.password == "pw"
    assert "Authorization" not in s.headers            # Basic goes via s.auth, not a header
    # bearer path still works and is separate
    s2 = h.session("tok", None)
    assert s2.headers["Authorization"] == "Bearer tok" and s2.auth is None


def test_reader_threads_basic_auth(monkeypatch):
    # options basic_user/basic_password reach session() through the reader
    captured = {}
    real_session = h.session
    def spy(*a, **k):
        captured.update(k)
        return real_session(*a, **k)
    monkeypatch.setattr(h, "session", spy)
    monkeypatch.setattr(r, "session", spy)
    monkeypatch.setattr(r, "request_json", lambda *a, **k: {"Items": []})
    rdr = r.PITimeSeriesBatchReader({**OPTS, "read_mode": "value",
                                     "basic_user": "DOMAIN\\svc-pi", "basic_password": "pw"})
    list(rdr.read(rdr.partitions()[0]))
    assert captured.get("basic_user") == "DOMAIN\\svc-pi" and captured.get("basic_password") == "pw"


def test_session_verify_tls():
    # default verifies; verify_tls=False turns off session.verify
    assert h.session(None, None).verify is True
    assert h.session(None, None, verify_tls=False).verify is False


def test_reader_threads_verify_tls(monkeypatch):
    # option verify_tls=false reaches session() through the reader (default is true)
    captured = {}
    real_session = h.session
    def spy(*a, **k):
        captured.update(k)
        return real_session(*a, **k)
    monkeypatch.setattr(h, "session", spy)
    monkeypatch.setattr(r, "session", spy)
    monkeypatch.setattr(r, "request_json", lambda *a, **k: {"Items": []})
    rdr = r.PITimeSeriesBatchReader({**OPTS, "read_mode": "value", "verify_tls": "false"})
    list(rdr.read(rdr.partitions()[0]))
    assert captured.get("verify_tls") is False
    # default: absent option -> verifies
    captured.clear()
    rdr2 = r.PITimeSeriesBatchReader({**OPTS, "read_mode": "value"})
    list(rdr2.read(rdr2.partitions()[0]))
    assert captured.get("verify_tls") is True
