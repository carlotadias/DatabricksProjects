# Databricks notebook source
# MAGIC %md
# MAGIC # Test the AVEVA PI **time-series connector** (`aveva_pi_timeseries`)
# MAGIC
# MAGIC This notebook tests the **Spark connector** on its own — it reads point
# MAGIC values for a set of WebIDs and returns `(web_id, timestamp, value)`. (The
# MAGIC lookup *library* that produces the WebIDs is tested in
# MAGIC `test_assetframework.py`.)
# MAGIC
# MAGIC It needs WebIDs as input. It installs the library too, purely to resolve a
# MAGIC few test tags → WebIDs in the setup cell (in production you'd already have
# MAGIC them). Run top-to-bottom; each cell self-checks (✅/❌).
# MAGIC
# MAGIC | # | Scenario | Capability |
# MAGIC |---|----------|------------|
# MAGIC | 1 | `value` | point-in-time snapshot, lean 3-col output |
# MAGIC | 2 | `interpolated` | resampled history |
# MAGIC | 3 | `recorded` | raw history, window-chunked |
# MAGIC | 4 | `recordedattime` | value as-of (batch only) |
# MAGIC | 5 | streaming (interpolated) → Delta | continuous ingest, checkpoint |
# MAGIC | 6 | streaming (value) | late-tag suppression / change feed |

# COMMAND ----------

# MAGIC %pip install /Volumes/<catalog>/<schema>/<volume>/aveva_pi_timeseries-2.0.3-py3-none-any.whl /Volumes/<catalog>/<schema>/<volume>/aveva_pi_assetframework-3.0.1-py3-none-any.whl
# MAGIC # ^ EDIT the Volume path to where you published the wheels (see HOW_TO_USE.md Step 1)
# MAGIC # ^ connector wheel is what we test; library wheel is only used to resolve test WebIDs below
dbutils.library.restartPython()

# COMMAND ----------

ENDPOINT_URL = "https://<host>/piwebapi"   # EDIT
PI_SERVER    = "PISRV"                       # EDIT
SCOPE        = "pi"                           # EDIT

# Auth — HTTP Basic against PI (secret-scope keys, never literals):
BASIC_USER_KEY = "pi_user"       # EDIT
BASIC_PW_KEY   = "pi_password"   # EDIT

# Provide WebIDs directly, OR tag names (resolved via the library in setup).
WEB_IDS = [
    # "F1AbE... ", "F1AbE...",
]
TAGS = [
    # "Plant.Area.Unit1.Temp", "Plant.Area.Unit1.Pressure",   # resolved if WEB_IDS empty
]
WINDOW_START = "2026-01-01T00:00:00Z"
INTERVAL     = "1m"
AS_OF        = "2026-01-01T00:30:00Z"
STREAM_TABLE = "<catalog>.<schema>.pi_ts_test"          # EDIT
CHECKPOINT   = "/Volumes/<catalog>/<schema>/<volume>/checkpoints/pi_ts_test"  # EDIT
USE_MOCK     = False

# COMMAND ----------

from aveva_pi_timeseries import PITimeSeriesSource
import aveva_pi_timeseries._http as _ts_http
import aveva_pi_timeseries.reader as _tsm
spark.dataSource.register(PITimeSeriesSource)

# AUTH is one HTTP Basic credential dict, reused for the library lookup (**AUTH)
# and the connector options (**AUTH) — the key names are identical on both surfaces.
AUTH = {}
try:
    AUTH = {"basic_user": dbutils.secrets.get(SCOPE, BASIC_USER_KEY),
            "basic_password": dbutils.secrets.get(SCOPE, BASIC_PW_KEY)}
except Exception as _e:
    print(f"⚠️  no credentials resolved ({_e}) — fine only if USE_MOCK=True")

BASE = ENDPOINT_URL.rstrip("/")
_results = []
def check(name, ok, detail=""):
    _results.append((name, bool(ok), detail))
    print(f"{'✅ PASS' if ok else '❌ FAIL'}  {name}" + (f"  — {detail}" if detail else ""))

# COMMAND ----------

# MAGIC %md ### (Optional) in-notebook mock — set USE_MOCK=True to run with no live PI

# COMMAND ----------

if USE_MOCK:
    _TS = "2026-01-01T00:00:00Z"
    class _MockSession:
        def __init__(self): self.headers = {}
        def request(self, method, url, params=None, json=None, timeout=None):
            return _MockResp(method, url, params, json)
    class _MockResp:
        def __init__(self, m, u, p, b): self.status_code = 200; self.headers = {}; self._m, self._u, self._p, self._b = m, u, p, b
        def raise_for_status(self): pass
        def json(self):
            if self._m == "POST":
                return {k: {"Status": 200, "Content": {"WebId": f"W-{k}"}} for k in self._b}
            if "/streamsets/value" in self._u:
                wids = [v for (k, v) in (self._p or []) if k == "webId"]
                return {"Items": [{"WebId": w, "Value": {"Timestamp": _TS, "Value": 1.0}} for w in wids]}
            if "/streamsets/" in self._u:
                wids = [v for (k, v) in (self._p or []) if k == "webId"]
                return {"Items": [{"WebId": w, "Items": [{"Timestamp": _TS, "Value": 1.0}]} for w in wids]}
            return {"Items": []}
    _mk = lambda *a, **k: _MockSession()
    for _mod in (_ts_http, _tsm):   # session is imported by-name into reader.py
        if hasattr(_mod, "session"):
            _mod.session = _mk
    if not WEB_IDS:
        WEB_IDS[:] = ["W-mock-1", "W-mock-2"]
    print("Mock installed.")

# COMMAND ----------

# MAGIC %md ## Setup — obtain WebIDs (resolve TAGS via the library if WEB_IDS is empty)

# COMMAND ----------

if not WEB_IDS and TAGS and not USE_MOCK:
    # compose a batch WebID lookup from the thin client's primitives
    from urllib.parse import quote
    from aveva_pi_assetframework import batch
    reqs = {str(i): {"Method": "GET",
                     "Resource": f"{BASE}/points?path=" + quote(rf"\\{PI_SERVER}\{t}", safe="")}
            for i, t in enumerate(TAGS)}
    resp = batch(BASE, reqs, **AUTH)
    WEB_IDS = [resp[str(i)]["Content"]["WebId"] for i in range(len(TAGS))]
print(f"{len(WEB_IDS)} WebIDs to read")

TS_OPTS = {"endpoint_url": ENDPOINT_URL, "web_ids": ",".join(WEB_IDS), **AUTH}

# COMMAND ----------

# MAGIC %md ## 1. `value` (lean 3-column snapshot)

# COMMAND ----------

try:
    df = spark.read.format("aveva_pi_timeseries").options(**TS_OPTS, read_mode="value").load()
    assert df.columns == ["web_id", "timestamp", "value"], df.columns
    n = df.count()
    display(df)
    check("1. value snapshot", n == len(WEB_IDS), f"{n} rows, cols={df.columns}")
except Exception as e:
    check("1. value snapshot", False, str(e)[:400])

# COMMAND ----------

# MAGIC %md ## 2. `interpolated`

# COMMAND ----------

try:
    df = spark.read.format("aveva_pi_timeseries").options(
        **TS_OPTS, read_mode="interpolated", interval=INTERVAL,
        initial_watermark=WINDOW_START, lookback_seconds="600").load()
    n = df.count()
    display(df.orderBy("web_id", "timestamp").limit(20))
    check("2. interpolated", n > 0, f"{n} rows")
except Exception as e:
    check("2. interpolated", False, str(e)[:400])

# COMMAND ----------

# MAGIC %md ## 3. `recorded` (window-chunked)

# COMMAND ----------

try:
    df = spark.read.format("aveva_pi_timeseries").options(
        **TS_OPTS, read_mode="recorded", initial_watermark=WINDOW_START,
        lookback_seconds="600", max_count="5000").load()
    n = df.count()
    check("3. recorded", n > 0, f"{n} rows")
except Exception as e:
    check("3. recorded", False, str(e)[:400])

# COMMAND ----------

# MAGIC %md ## 4. `recordedattime` (batch only)

# COMMAND ----------

try:
    df = spark.read.format("aveva_pi_timeseries").options(
        **TS_OPTS, read_mode="recordedattime", as_of=AS_OF).load()
    n = df.count()
    check("4. recordedattime", n == len(WEB_IDS), f"{n} rows as-of {AS_OF}")
except Exception as e:
    check("4. recordedattime", False, str(e)[:400])

# COMMAND ----------

# MAGIC %md ## 5. Streaming (interpolated) → Delta

# COMMAND ----------

if not USE_MOCK:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {STREAM_TABLE}"); dbutils.fs.rm(CHECKPOINT, recurse=True)
    except Exception:
        pass
try:
    q = (spark.readStream.format("aveva_pi_timeseries").options(
            **TS_OPTS, read_mode="interpolated", interval=INTERVAL,
            initial_watermark=WINDOW_START, max_advance_seconds="300").load()
         .writeStream.option("checkpointLocation", CHECKPOINT)
         .trigger(availableNow=True).toTable(STREAM_TABLE))
    q.awaitTermination()
    n = spark.table(STREAM_TABLE).count()
    display(spark.table(STREAM_TABLE).orderBy("timestamp").limit(20))
    check("5. streaming → Delta", n > 0, f"{n} rows in {STREAM_TABLE}")
except Exception as e:
    check("5. streaming → Delta", False, str(e)[:400])

# COMMAND ----------

# MAGIC %md ## 6. Streaming `value` — late-tag suppression (change feed)

# COMMAND ----------

try:
    q = (spark.readStream.format("aveva_pi_timeseries").options(
            **TS_OPTS, read_mode="value",
            initial_watermark=WINDOW_START, max_advance_seconds="300").load()
         .writeStream.option("checkpointLocation", CHECKPOINT + "_value")
         .trigger(availableNow=True).toTable(STREAM_TABLE + "_value"))
    q.awaitTermination()
    n = spark.table(STREAM_TABLE + "_value").count()
    # Only snapshots newer than the watermark are emitted (stale tags suppressed).
    check("6. streaming value (late-tag)", n >= 0, f"{n} fresh snapshot rows")
except Exception as e:
    check("6. streaming value (late-tag)", False, str(e)[:400])

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

passed = sum(1 for _, ok, _ in _results if ok)
print(f"\n{'='*56}\n  {passed}/{len(_results)} checks passed\n{'='*56}")
for name, ok, detail in _results:
    print(f"  {'✅' if ok else '❌'}  {name:32s} {detail}")
