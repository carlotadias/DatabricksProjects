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



# --------------------------------------------------------------------------- #
# Truncation detection (KNOWN_ISSUES #1/#2) — PI applies maxCount SILENTLY, so a
# full response is indistinguishable from a complete one except by counting.
#
# These drive _WebIdChunkPartition directly rather than via partitions(), so the
# window span is EXACT: initial_watermark would run to now(), i.e. months.
# --------------------------------------------------------------------------- #

def _recorded_reader(**over):
    """A `recorded` reader; 50 tags in one call."""
    return r.PITimeSeriesBatchReader({
        "endpoint_url": "http://pi/piwebapi",
        "web_ids": ",".join(f"W{i}" for i in range(50)),
        "read_mode": "recorded", "webids_per_call": "50",
        "initial_watermark": "2026-01-01T00:00:00", **over})


def _part(rdr, start, end):
    return r._WebIdChunkPartition(tuple(rdr._web_ids), start, end)


def _values(n):
    return [{"Timestamp": "2026-01-01T00:00:%02dZ" % (i % 60), "Value": float(i)}
            for i in range(n)]


def test_truncation_detected_and_window_split(monkeypatch):
    """A response at exactly maxCount must trigger a re-read as two halves."""
    spans = []

    def router(method, url, params, body):
        p = dict(params)
        spans.append((p["startTime"], p["endTime"]))
        cap = int(p["maxCount"])
        # The full window comes back FULL (= truncated); the halves do not.
        return {"Items": [{"WebId": "W0",
                           "Items": _values(cap if len(spans) == 1 else 3)}]}

    _patch(monkeypatch, router)
    rdr = _recorded_reader(max_count="10")
    rows = list(rdr._fetch(_part(rdr, 0, 10)))       # 10s: one window, then split
    assert len(spans) == 3, spans                     # 1 truncated + 2 halves
    assert spans[1][0] == spans[0][0]                 # first half keeps the start
    assert spans[2][1] == spans[0][1]                 # second half keeps the end
    assert spans[1][1] == spans[2][0]                 # contiguous — no gap, no overlap
    assert len(rows) == 6                             # 3 + 3 from the halves


def test_truncation_raises_when_unsplittable(monkeypatch):
    """Always-truncated reads must RAISE, never silently drop the remainder."""
    def router(method, url, params, body):
        cap = int(dict(params)["maxCount"])
        return {"Items": [{"WebId": "W0", "Items": _values(cap)}]}   # always full

    _patch(monkeypatch, router)
    import pytest
    rdr = _recorded_reader(max_count="10")
    with pytest.raises(RuntimeError, match="truncated"):
        list(rdr._fetch(_part(rdr, 0, 64)))


def test_no_truncation_no_extra_calls(monkeypatch):
    """Under the cap, behaviour is unchanged — one call, no splitting."""
    calls = []

    def router(method, url, params, body):
        calls.append(params)
        return {"Items": [{"WebId": "W0", "Items": _values(2)}]}

    _patch(monkeypatch, router)
    rdr = _recorded_reader(max_count="1000")
    rows = list(rdr._fetch(_part(rdr, 0, 10)))
    assert len(calls) == 1 and len(rows) == 2


def test_value_mode_never_treated_as_truncated(monkeypatch):
    """`value` returns one snapshot per stream; maxCount does not apply."""
    calls = []

    def router(method, url, params, body):
        calls.append(params)
        return {"Items": [{"WebId": "W-1",
                           "Value": {"Timestamp": "2026-01-01T00:00:00Z", "Value": 1.0}}]}

    _patch(monkeypatch, router)
    rdr = r.PITimeSeriesBatchReader({**OPTS, "read_mode": "value"})
    rows = [row for p in rdr.partitions() for row in rdr.read(p)]
    assert len(rows) == 1 and len(calls) == 1          # no split attempted


# --------------------------------------------------------------------------- #
# Window sizing (KNOWN_ISSUES #2) — the item count must be converted to seconds
# via an assumed rate, not used directly AS seconds.
# --------------------------------------------------------------------------- #

def test_recorded_span_scales_with_assumed_rate(monkeypatch):
    _patch(monkeypatch, _streamset_router({"Items": []}))
    slow = _recorded_reader(assumed_values_per_second="1", max_count="3000")
    fast = _recorded_reader(assumed_values_per_second="100", max_count="3000")
    day = 86_400
    n_slow = len(slow._time_windows(0, day, 50))
    n_fast = len(fast._time_windows(0, day, 50))
    # A 100x denser archive must be split into ~100x more windows. Previously the item
    # count was used AS seconds, so the rate had no effect at all and these were equal.
    assert n_fast >= n_slow * 50, (n_slow, n_fast)


def test_interpolated_span_uses_interval_exactly(monkeypatch):
    """interpolated CAN be exact: items x interval = duration. Unchanged behaviour."""
    _patch(monkeypatch, _streamset_router({"Items": []}))
    rdr = _recorded_reader(read_mode="interpolated", interval="1m", max_count="100")
    # cap=100 items x 60s = 6000s per window, so 12000s needs 2 windows.
    assert len(rdr._time_windows(0, 12_000, 50)) == 2


# --------------------------------------------------------------------------- #
# partition_concurrency (KNOWN_ISSUES #3) — the pool must now see the WINDOW axis.
# Before the fix the pool sat INSIDE the window loop and got a 1-item list, so it
# was never constructed for bulk reads.
# --------------------------------------------------------------------------- #

def test_concurrency_parallelises_windows(monkeypatch):
    """Every sub-window must be read exactly once, concurrently."""
    seen = []

    def router(method, url, params, body):
        p = dict(params)
        seen.append((p["startTime"], p["endTime"]))
        return {"Items": [{"WebId": "W0", "Items": _values(1)}]}

    _patch(monkeypatch, router)
    # cap 10 items at 1/s -> 10s windows; a 100s span is 10 windows in ONE task.
    rdr = _recorded_reader(max_count="10", assumed_values_per_second="1",
                           partition_concurrency="4")
    rows = list(rdr._fetch(_part(rdr, 0, 100)))
    assert len(seen) == 10, seen                       # all windows read, none skipped
    assert len(set(seen)) == 10                        # none read twice
    assert len(rows) == 10


def test_windows_serial_when_concurrency_one(monkeypatch):
    """concurrency=1 keeps the sequential path and the same coverage."""
    seen = []

    def router(method, url, params, body):
        seen.append((dict(params)["startTime"], dict(params)["endTime"]))
        return {"Items": [{"WebId": "W0", "Items": _values(1)}]}

    _patch(monkeypatch, router)
    rdr = _recorded_reader(max_count="10", assumed_values_per_second="1",
                           partition_concurrency="1")
    list(rdr._fetch(_part(rdr, 0, 100)))
    assert len(seen) == 10 and len(set(seen)) == 10
