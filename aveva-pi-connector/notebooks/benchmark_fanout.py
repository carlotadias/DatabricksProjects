# Databricks notebook source
# MAGIC %md
# MAGIC # AVEVA PI connector v2.0 — fan-out timing benchmark
# MAGIC
# MAGIC Measures **how fast** the connector reads and helps tune the knobs, on a
# MAGIC cluster (DBR 15.x+ / Spark 4.0+) with network to PI. Speed only — for
# MAGIC correctness use `test_assetframework.py` + `test_timeseries.py`.
# MAGIC
# MAGIC | # | Experiment | Compares | Knob |
# MAGIC |---|-----------|----------|------|
# MAGIC | A | WebID resolution | `serial` vs `batch` | Batch controller |
# MAGIC | B | Read | `bulk_read=false` vs `true` | StreamSet bulk |
# MAGIC | C | Chunk sizing | `webids_per_call` sweep | Spark partitions |
# MAGIC | D | Concurrency | `partition_concurrency` sweep | in-partition threads |
# MAGIC
# MAGIC Reports wall-clock seconds, WebIDs/sec, points/sec.

# COMMAND ----------

# MAGIC %pip install /Volumes/<catalog>/<schema>/<volume>/aveva_pi_assetframework-3.0.1-py3-none-any.whl /Volumes/<catalog>/<schema>/<volume>/aveva_pi_timeseries-2.0.3-py3-none-any.whl
# MAGIC # ^ EDIT the Volume path to where you published the wheels (see HOW_TO_USE.md Step 1)
dbutils.library.restartPython()

# COMMAND ----------

ENDPOINT_URL = "https://<host>/piwebapi"   # EDIT
PI_SERVER    = "PISRV"                       # EDIT
SCOPE        = "pi"                           # EDIT

# Auth — HTTP Basic against PI (secret-scope keys, never literals):
BASIC_USER_KEY = "pi_user"       # EDIT
BASIC_PW_KEY   = "pi_password"   # EDIT

TAGS = [
    # Paste a realistic list — 100s+ shows the fan-out benefit best.
]
WINDOW_START = "2026-01-01T00:00:00Z"
INTERVAL     = "1m"
READ_MODE    = "interpolated"     # interpolated | recorded

WEBIDS_PER_CALL_SWEEP       = [10, 50, 100]
PARTITION_CONCURRENCY_SWEEP = [1, 4, 8, 16]

RESULT_TABLE = "<catalog>.<schema>.pi_connector_benchmark"   # EDIT (or None)
USE_MOCK     = False

# COMMAND ----------

import time
from urllib.parse import quote
from aveva_pi_assetframework import batch, get_point
from aveva_pi_timeseries import PITimeSeriesSource
import aveva_pi_assetframework._http as _af_http
import aveva_pi_assetframework.client as _afc
spark.dataSource.register(PITimeSeriesSource)

# One HTTP Basic credential dict — same keys for connector options and library calls.
AUTH = {}
try:
    AUTH = {"basic_user": dbutils.secrets.get(SCOPE, BASIC_USER_KEY),
            "basic_password": dbutils.secrets.get(SCOPE, BASIC_PW_KEY)}
except Exception as _e:
    print(f"⚠️  no credentials resolved ({_e}) — fine only if USE_MOCK=True")

def timed(fn):
    t0 = time.perf_counter(); out = fn(); return out, time.perf_counter() - t0

def have_live():
    return bool(TAGS) and not USE_MOCK

# Client-composed WebID resolution (the library gives primitives; we compose):
def resolve_batch(base, server, tags, **auth):
    reqs = {str(i): {"Method": "GET",
                     "Resource": f"{base}/points?path=" + quote(rf"\\{server}\{t}", safe="")}
            for i, t in enumerate(tags)}
    resp = batch(base, reqs, **auth)
    return [resp[str(i)]["Content"]["WebId"] for i in range(len(tags))]

def resolve_serial(base, server, tags, **auth):
    return [get_point(base, server, t, **auth)["WebId"] for t in tags]

results = []

# Resolve once up-front; the value connector reads WebIDs.
WEB_IDS = []
if have_live():
    WEB_IDS = resolve_batch(ENDPOINT_URL.rstrip("/"), PI_SERVER, TAGS, **AUTH)

BASE_OPTS = {"endpoint_url": ENDPOINT_URL, "web_ids": ",".join(WEB_IDS),
             "read_mode": READ_MODE, "interval": INTERVAL,
             "initial_watermark": WINDOW_START, "lookback_seconds": "3600",
             **AUTH}

def read_count(opts):
    df = spark.read.format("aveva_pi_timeseries").options(**opts).load()
    return timed(lambda: df.count())

# COMMAND ----------

# MAGIC %md ## A. WebID resolution — batch vs serial

# COMMAND ----------

if have_live():
    _base = ENDPOINT_URL.rstrip("/")
    for mode, fn in [("serial", resolve_serial), ("batch", resolve_batch)]:
        _, secs = timed(lambda fn=fn: fn(_base, PI_SERVER, TAGS, **AUTH))
        results.append({"experiment": "A_webid_resolution", "variant": mode, "tags": len(TAGS),
                        "points": None, "seconds": round(secs, 3),
                        "per_sec": round(len(TAGS) / secs, 1) if secs else None})
    display(spark.createDataFrame([r for r in results if r["experiment"] == "A_webid_resolution"]))
else:
    print("Skipped (set TAGS; or run the mock cell).")

# COMMAND ----------

# MAGIC %md ## B. Read — StreamSet bulk vs per-stream

# COMMAND ----------

if have_live():
    for bulk in ["false", "true"]:
        (n, _), secs = read_count({**BASE_OPTS, "bulk_read": bulk,
                                   "webids_per_call": "50", "partition_concurrency": "8"})
        results.append({"experiment": "B_read", "variant": f"bulk_read={bulk}", "tags": len(WEB_IDS),
                        "points": n, "seconds": round(secs, 3),
                        "per_sec": round(n / secs, 0) if secs else None})
    display(spark.createDataFrame([r for r in results if r["experiment"] == "B_read"]))

# COMMAND ----------

# MAGIC %md ## C. Sweep `webids_per_call`

# COMMAND ----------

if have_live():
    for wpc in WEBIDS_PER_CALL_SWEEP:
        (n, _), secs = read_count({**BASE_OPTS, "bulk_read": "true",
                                   "webids_per_call": str(wpc), "partition_concurrency": "8"})
        results.append({"experiment": "C_webids_per_call", "variant": str(wpc), "tags": len(WEB_IDS),
                        "points": n, "seconds": round(secs, 3),
                        "per_sec": round(n / secs, 0) if secs else None})
    display(spark.createDataFrame([r for r in results if r["experiment"] == "C_webids_per_call"]))

# COMMAND ----------

# MAGIC %md ## D. Sweep `partition_concurrency`

# COMMAND ----------

if have_live():
    for conc in PARTITION_CONCURRENCY_SWEEP:
        (n, _), secs = read_count({**BASE_OPTS, "bulk_read": "true",
                                   "webids_per_call": "100", "partition_concurrency": str(conc)})
        results.append({"experiment": "D_partition_concurrency", "variant": str(conc), "tags": len(WEB_IDS),
                        "points": n, "seconds": round(secs, 3),
                        "per_sec": round(n / secs, 0) if secs else None})
    display(spark.createDataFrame([r for r in results if r["experiment"] == "D_partition_concurrency"]))

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

if results:
    summary = spark.createDataFrame(results)
    display(summary.orderBy("experiment", "variant"))
    if RESULT_TABLE:
        summary.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(RESULT_TABLE)
        print("Saved to", RESULT_TABLE)
else:
    print("No results — set TAGS and re-run (or use the mock cell).")

# COMMAND ----------

# MAGIC %md ## (Optional) Mock — HTTP round-trip counts, no live PI

# COMMAND ----------

if USE_MOCK:
    class _MockSession:
        def __init__(self): self.headers = {}; self.n = 0
        def request(self, method, url, params=None, json=None, timeout=None):
            self.n += 1; time.sleep(0.02); return _MockResp(method, url, params, json)
    class _MockResp:
        def __init__(self, m, u, p, b): self.status_code = 200; self.headers = {}; self._m, self._u, self._p, self._b = m, u, p, b
        def raise_for_status(self): pass
        def json(self):
            if self._m == "POST":                       # POST /batch
                return {k: {"Status": 200, "Content": {"WebId": f"W-{k}"}} for k in self._b}
            return {"WebId": "W", "Path": "p", "EngineeringUnits": "u"}   # GET /points

    n_tags = 200
    mock_tags = [f"Plant.Area.Unit{i}.Temp" for i in range(n_tags)]
    rows = []
    for mode, fn in [("serial", resolve_serial), ("batch", resolve_batch)]:
        sess = _MockSession()
        for _mod in (_af_http, _afc):           # patch session everywhere it's imported by-name
            if hasattr(_mod, "session"):
                _mod.session = lambda *a, **k: sess
        _, secs = timed(lambda fn=fn: fn("http://mock/piwebapi", "PISRV", mock_tags, None))
        rows.append(("A_webid_resolution", mode, sess.n, round(secs, 2)))
    display(spark.createDataFrame(rows, "experiment string, variant string, http_calls int, seconds double"))
    print("Fewer http_calls = fewer round-trips = less rate-limit pressure & lower latency.")
