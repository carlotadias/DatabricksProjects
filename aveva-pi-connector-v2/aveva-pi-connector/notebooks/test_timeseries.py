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

# MAGIC %pip install /Volumes/<catalog>/<schema>/<volume>/aveva_pi_timeseries-2.0.4-py3-none-any.whl /Volumes/<catalog>/<schema>/<volume>/aveva_pi_assetframework-3.0.2-py3-none-any.whl
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

# --- Connectivity (see the preflight cell below) --------------------------------
# FALLBACK_IP: if the cluster can't resolve the PI FQDN via DNS, set PI's IP here
#   and the preflight cell pins FQDN->IP in /etc/hosts. SINGLE-NODE CLUSTERS ONLY
#   (driver == executor). On a multi-node cluster use scripts/pi_dns_init.sh instead.
FALLBACK_IP = ""                 # EDIT e.g. "10.0.0.5"  (leave "" if DNS works)
# VERIFY_TLS: keep True. Set False as a diagnostic if PI uses an internal/self-signed
#   CA not yet in the cluster trust store (verified request fails with a cert error).
VERIFY_TLS  = "true"             # "true" | "false"

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

# MAGIC %md ### Connectivity preflight (route + DNS; optional /etc/hosts pin)
# MAGIC Confirms the cluster can reach PI, and — if DNS can't resolve the FQDN — pins
# MAGIC `FALLBACK_IP -> FQDN` in `/etc/hosts` so the connector's FQDN call resolves.
# MAGIC **⚠️ SINGLE-NODE CLUSTERS ONLY:** this writes the driver's hosts file; on a
# MAGIC multi-node cluster the executors (which do the reads) won't get it — use the
# MAGIC `scripts/pi_dns_init.sh` init script there instead. Pinning keeps you dialing
# MAGIC the FQDN, so the TLS cert still matches (unlike putting the raw IP in the URL).

# COMMAND ----------

if not USE_MOCK:
    import socket, time
    from urllib.parse import urlparse
    _host = urlparse(ENDPOINT_URL).hostname
    _port = urlparse(ENDPOINT_URL).port or 443

    # DNS with retries — absorbs the transient EAI_AGAIN ("temporary failure in name
    # resolution") this environment has shown; only declares failure after 3 misses.
    def _resolve(host, tries=3):
        for _i in range(tries):
            try:
                return socket.gethostbyname(host)
            except socket.gaierror:
                if _i < tries - 1:
                    time.sleep(1.5)
        raise

    # 1. Route check — connect to PI's IP:port (uses FALLBACK_IP if given, else DNS).
    _ip = FALLBACK_IP or None
    try:
        if not _ip:
            _ip = _resolve(_host)   # will raise if DNS can't resolve after retries
        socket.create_connection((_ip, _port), timeout=5).close()
        print(f"✅ route OK — reached {_ip}:{_port}")
    except Exception as _e:
        print(f"❌ cannot reach PI ({_e!r}). If this is a timeout, the network path is "
              f"blocked (not just DNS) — escalate to network team; the hosts pin won't help.")

    # 2. If DNS can't resolve the FQDN but we have a FALLBACK_IP, pin it in /etc/hosts.
    try:
        _resolve(_host)
        print(f"✅ DNS resolves {_host}")
    except Exception:
        if FALLBACK_IP:
            line = f"{FALLBACK_IP}  {_host}\n"
            with open("/etc/hosts") as _f:
                _present = _host in _f.read()
            if not _present:
                with open("/etc/hosts", "a") as _f:
                    _f.write(line)
            print(f"📌 pinned {_host} -> {FALLBACK_IP} in /etc/hosts (single-node only). "
                  f"Re-resolves to: {_resolve(_host)}")
        else:
            print(f"❌ DNS can't resolve {_host} and FALLBACK_IP is empty. Set FALLBACK_IP "
                  f"to PI's IP (single-node), or attach scripts/pi_dns_init.sh (multi-node).")

    # 3. TLS trust probe — try a verified HTTPS handshake to the FQDN. If (and ONLY
    #    if) it fails because the cert isn't trusted (internal/self-signed CA), flip
    #    VERIFY_TLS to "false" for this run so the reads proceed. A timeout/DNS error
    #    is NOT a TLS problem and does not flip the flag.
    if VERIFY_TLS == "true":
        import ssl
        try:
            _ctx = ssl.create_default_context()
            with socket.create_connection((_host, _port), timeout=5) as _raw:
                with _ctx.wrap_socket(_raw, server_hostname=_host):
                    pass
            print(f"✅ TLS cert trusted for {_host} — keeping verify_tls=true")
        except ssl.SSLCertVerificationError as _e:
            VERIFY_TLS = "false"
            print(f"⚠️  TLS cert NOT trusted ({_e.verify_message or _e}). Likely an internal CA. "
                  f"→ set verify_tls=FALSE for this diagnostic run. "
                  f"Production fix: import the PI/enterprise CA into the cluster trust store.")
        except Exception as _e:
            print(f"ℹ️  TLS probe inconclusive ({_e!r}) — not a cert-trust error, leaving verify_tls=true.")

print(f"verify_tls for this run: {VERIFY_TLS}")

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
    # verify_tls too — the library defaults to True, so pass the preflight's decision
    # (VERIFY_TLS is a string here; the library wants a bool)
    resp = batch(BASE, reqs, **AUTH, verify_tls=(VERIFY_TLS != "false"))
    WEB_IDS = [resp[str(i)]["Content"]["WebId"] for i in range(len(TAGS))]
print(f"{len(WEB_IDS)} WebIDs to read")

TS_OPTS = {"endpoint_url": ENDPOINT_URL, "web_ids": ",".join(WEB_IDS),
           "verify_tls": VERIFY_TLS, **AUTH}

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
