# Databricks notebook source
# MAGIC %md
# MAGIC # AVEVA PI connector — benchmark BY DATA-REFERENCE TYPE
# MAGIC
# MAGIC A sibling to `benchmark_fanout.py`. Same measurements, but it **splits the WebIDs into
# MAGIC type buckets first** and runs every sweep **per type**, so you can see whether latency
# MAGIC differs between:
# MAGIC
# MAGIC | Bucket | WebID prefix | What it is | Cost of a read |
# MAGIC |---|---|---|---|
# MAGIC | `raw_point`   | `F1DP` | a PI point straight off the Data Archive | disk read |
# MAGIC | `passthrough` | `F1Ab` + `DataReferencePlugIn="PI Point"` | an AF attribute that just maps to a point | ≈ a point + a small AF hop |
# MAGIC | `formula`     | `F1Ab` + `DataReferencePlugIn="Formula"` | an AF attribute computed on read | AF runs the formula |
# MAGIC | `analysis`    | `F1Ab` + `DataReferencePlugIn="Analysis"`/rollup | aggregated/analysed on read | AF runs the analysis |
# MAGIC
# MAGIC **Why this notebook exists:** every read is issued the same way (`GET /streamsets/...`),
# MAGIC so a flat benchmark cannot tell you that a *formula* attribute costs more than a *point*.
# MAGIC The WebID prefix reveals point-vs-attribute for free; passthrough-vs-formula needs one
# MAGIC `GET /attributes/{WebId}` per attribute to read `DataReferencePlugIn`. This notebook does
# MAGIC both, then benchmarks each bucket at the SAME tag count so the numbers are comparable.
# MAGIC
# MAGIC > ⚠️ **This runs against production PI.** Agree a stop signal with whoever watches PI's
# MAGIC > CPU before you start. Every result carries UTC start/end so it can be correlated
# MAGIC > against PI-side CPU graphs.

# COMMAND ----------

# MAGIC # PICK ONE — uncomment the line that matches how you got the connector.
# MAGIC
# MAGIC # (A) You pulled the repo as a Git folder — installs from source, nothing to publish:
# MAGIC %pip install ../timeseries ../assetframework
# MAGIC
# MAGIC # (B) Wheels published to a UC Volume (see HOW_TO_USE.md Step 1) — EDIT the paths:
# MAGIC # %pip install /Volumes/<catalog>/<schema>/<volume>/aveva_pi_assetframework-3.2.0-py3-none-any.whl /Volumes/<catalog>/<schema>/<volume>/aveva_pi_timeseries-2.1.0-py3-none-any.whl
dbutils.library.restartPython()

# COMMAND ----------

# --- Endpoint --------------------------------------------------------------------
ENDPOINT_URL = "https://<host>/piwebapi"   # EDIT — the FQDN, never the IP
PI_SERVER    = "PISRV"                     # EDIT — PI Data Archive name (for tag lookup)

# --- Auth — HTTP Basic against PI (secret-scope keys, never literals) ------------
SCOPE          = "pi"            # EDIT — Databricks secret scope
BASIC_USER_KEY = "pi_user"       # EDIT
BASIC_PW_KEY   = "pi_password"   # EDIT

# --- Optional: a mock PI Web API behind a Databricks App (leave False for real PI) ----
# Only relevant if you run against a mock App rather than PI. The App sits behind the
# OAuth proxy, so it needs a Bearer token — a PLATFORM gate, not PI auth. Setting True
# also overrides ENDPOINT_URL and expects TAGS to be the mock's tag names.
USE_MOCK_APP  = False
WORKSPACE     = "https://<workspace>.azuredatabricks.net"          # EDIT if USE_MOCK_APP
PI_APP        = "https://<mock-app>.azure.databricksapps.com"      # EDIT if USE_MOCK_APP
SP_SCOPE      = "<scope>"                 # secret scope holding the SP credentials
SP_ID_KEY     = "client_id"
SP_SECRET_KEY = "client_secret"
if USE_MOCK_APP:
    ENDPOINT_URL = f"{PI_APP}/piwebapi"

# --- Connectivity ----------------------------------------------------------------
# FALLBACK_IP: if the cluster cannot resolve the PI FQDN, set PI's IP and the setup cell
#   pins FQDN->IP in /etc/hosts. SINGLE-NODE ONLY (driver == executor); on a multi-node
#   cluster use scripts/pi_dns_init.sh instead.
FALLBACK_IP = ""       # EDIT e.g. "10.0.0.5" (leave "" if DNS works)
# VERIFY_TLS: keep True. Set False only as a diagnostic if PI uses an internal CA that is
#   not yet in the cluster trust store (a verified request fails with a certificate error).
VERIFY_TLS  = True

# --- What to read ----------------------------------------------------------------
# Provide WEB_IDS directly (recommended for this notebook — you likely have a MIX of
# points and AF attributes, and the classify cell sorts them out). If WEB_IDS is empty,
# setup resolves TAGS via GET /points on PI_SERVER, exactly as benchmark_fanout does —
# but tag resolution only produces raw points, so the type split is only interesting when
# you paste a WEB_IDS list that already contains AF attributes.
WEB_IDS = [
    # "F1DP...",   # a raw PI point
    # "F1Ab...",   # an AF attribute (passthrough, formula, or analysis)
]
TAGS = [
    # "Plant.Area.Unit1.Temp", "Plant.Area.Unit1.Pressure",
]

READ_MODE    = "recorded"    # what the sweeps stress: "recorded" | "value" | "interpolated"
INTERVAL     = "1m"          # interpolated only
HTTP_TIMEOUT = 30
# Resolution give-up: only applies while NOTHING has resolved yet, so a partly-working
# tag list is always tried in full. Set high (>= len(TAGS)) to never give up early.
GIVE_UP_AFTER = 3000

RESULT_TABLE = None          # e.g. "<catalog>.<schema>.pi_benchmark_by_type", or None to skip
# § 1 streams, so it needs somewhere to write checkpoints (one throwaway dir per run).
CHECKPOINT_ROOT = "/Volumes/<catalog>/<schema>/<volume>/benchmark_checkpoints"   # EDIT

# COMMAND ----------

# MAGIC %md ## Setup — auth, WebID resolution, cluster shape
# MAGIC
# MAGIC Identical to `benchmark_fanout.py`. The type split happens in the cell AFTER this one.

# COMMAND ----------

import re
import time, json
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, urlparse

import requests
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

import aveva_pi_timeseries as _ts
from aveva_pi_assetframework import batch, get_point
from aveva_pi_timeseries import PITimeSeriesSource
spark.dataSource.register(PITimeSeriesSource)

# Confirm which version actually loaded. `%pip install <file>` can be a no-op if the
# package is already satisfied on the cluster, and restartPython() is what makes a new
# wheel take effect — so check rather than trust the filename in the %pip cell.
print(f"aveva_pi_timeseries {_ts.__version__}")
if tuple(int(x) for x in _ts.__version__.split(".")[:2]) < (2, 1):
    raise RuntimeError(
        f"Loaded {_ts.__version__}, need >= 2.1.0. This notebook reports on behaviour that "
        f"only exists in 2.1.0 (maxCount truncation detection, working partition_concurrency). "
        f"Detach and re-attach the cluster, then re-run the %pip cell.")

BASE = ENDPOINT_URL.rstrip("/")


def _cause(e) -> str:
    """The useful line from a Spark exception, not the first 110 chars of boilerplate.

    A connector failure surfaces as "An exception was thrown from the Python worker" with
    the real cause — HTTP status, a URL-too-long rejection, our own truncation RuntimeError —
    buried further down. Pull the deepest informative line out.
    """
    _s = str(e)
    _pat = ("HTTPError", "HTTP", "SSL", "Timeout", "timed out", "ConnectionError",
            "RuntimeError", "ValueError", "MemoryError", "OutOfMemory", "404", "413",
            "414", "429", "500", "502", "503", "truncated", "maxCount", "Max retries",
            "Connection aborted", "Connection reset", "RemoteDisconnected")
    # Skip Spark's own wrapper lines: they say a task died, never why. The cause is
    # nested below them, often after "Caused by" or in the Python traceback.
    _noise = ("Python worker", "Job aborted due to stage failure", "Lost task",
              "at org.apache", "at py4j", "at java.", "at scala.")
    _hits = [ln.strip() for ln in _s.splitlines()
             if any(p in ln for p in _pat) and not any(n in ln for n in _noise)]
    if _hits:
        return _hits[-1][:300]
    # Nothing matched: fall back to the last non-stack line, which beats the first line
    # (that is Spark's wrapper) when the real error is buried in a traceback.
    _lines = [ln.strip() for ln in _s.splitlines()
              if ln.strip() and not ln.strip().startswith(("at ", "\tat "))]
    return (_lines[-1][:300] if _lines else _s[:300])


# Auth. Two shapes: AUTH for the assetframework library (kwargs), CONN_AUTH for the
# connector (Spark options). The mock App needs a Bearer token for the OAuth proxy;
# real PI needs HTTP Basic.
if USE_MOCK_APP:
    _cid = dbutils.secrets.get(SP_SCOPE, SP_ID_KEY)          # noqa: F821
    _sec = dbutils.secrets.get(SP_SCOPE, SP_SECRET_KEY)      # noqa: F821
    BEARER = requests.post(f"{WORKSPACE}/oidc/v1/token", auth=(_cid, _sec),
                           data={"grant_type": "client_credentials", "scope": "all-apis"},
                           timeout=30).json()["access_token"]
    AUTH = {"bearer": BEARER, "verify_tls": VERIFY_TLS}      # library kwarg is `bearer`
    CONN_AUTH = {"bearer_token": BEARER}                     # connector option
    _hdrs = {"Authorization": f"Bearer {BEARER}"}
    print("auth: Bearer (Databricks App OAuth proxy — NOT PI auth)")
else:
    _user = dbutils.secrets.get(SCOPE, BASIC_USER_KEY)       # noqa: F821
    _pw = dbutils.secrets.get(SCOPE, BASIC_PW_KEY)           # noqa: F821
    AUTH = {"basic_user": _user, "basic_password": _pw, "verify_tls": VERIFY_TLS}
    CONN_AUTH = {"basic_user": _user, "basic_password": _pw}
    _hdrs = {}
    print("auth: HTTP Basic (credentials from the secret scope)")

if not VERIFY_TLS:
    requests.packages.urllib3.disable_warnings()
    print("⚠️  verify_tls=False — the certificate is NOT validated (diagnostic only)")

# Connectivity. A timeout means a blocked path; a name error means DNS.
_host = urlparse(BASE).hostname
if FALLBACK_IP:
    with open("/etc/hosts") as _f:
        if _host not in _f.read():
            with open("/etc/hosts", "a") as _f2:
                _f2.write(f"{FALLBACK_IP}  {_host}\n")
    print(f"📌 pinned {_host} -> {FALLBACK_IP} in /etc/hosts (single-node only)")
try:
    _r = requests.get(f"{BASE}/system", headers=_hdrs, timeout=10, verify=VERIFY_TLS,
                      **({} if USE_MOCK_APP else
                         {"auth": requests.auth.HTTPBasicAuth(_user, _pw)}))
    # A Databricks App behind the OAuth proxy answers an UNAUTHENTICATED request with the
    # login PAGE — HTTP 200, but HTML, not JSON. That looks "reached" but every later API
    # call then fails to parse ("Expecting value: line 1 column 1"). Detect it here so the
    # cause is named up front rather than surfacing as `unknown` buckets in the classify cell.
    _ctype = _r.headers.get("Content-Type", "")
    _looks_login = "text/html" in _ctype or _r.text[:15].lstrip().startswith("<")
    if _r.status_code == 200 and not _looks_login:
        print(f"✅ reached {_host} (HTTP {_r.status_code}, {_ctype or 'no content-type'})")
    elif _looks_login:
        print(f"❌ reached {_host} but got the OAuth LOGIN PAGE, not JSON (HTTP {_r.status_code}).")
        print(f"   The token is missing/invalid, or this service principal lacks CAN_USE on the")
        print(f"   App. Grant it, or check SP_SCOPE/SP_ID_KEY/SP_SECRET_KEY — until then every")
        print(f"   API call will fail to parse.")
    else:
        print(f"⚠️ reached {_host} but HTTP {_r.status_code} ({_ctype}) — {_r.text[:120]}")
except Exception as e:
    print(f"❌ cannot reach {_host} ({e!r}) — a timeout is a blocked path, a name error is DNS")

# Cluster shape: in-flight calls are capped by task slots, so this bounds every result.
try:
    CLUSTER_CORES = int(spark.sparkContext.defaultParallelism)
    _cores_src = "sparkContext.defaultParallelism"
except Exception:
    CLUSTER_CORES = int(spark.conf.get("spark.sql.shuffle.partitions", "8"))
    _cores_src = "spark.sql.shuffle.partitions (estimate)"
print(f"cluster parallelism (≈ parallel tasks): {CLUSTER_CORES}   [{_cores_src}]")

# WebIDs. Given directly? Use them. Otherwise resolve TAGS through the API (see
# benchmark_fanout.py for the full resolution notes — the logic here is identical).
WEB_IDS = [w for w in (WEB_IDS or []) if str(w).strip()]
TAGS = [t for t in (TAGS or []) if str(t).strip()]
print(f"input: {len(WEB_IDS)} WEB_IDS, {len(TAGS)} TAGS")
if not WEB_IDS and not TAGS:
    raise ValueError(
        "Both WEB_IDS and TAGS are empty. Set one of them in the config cell above.\n"
        "For a TYPE comparison, paste a WEB_IDS list that MIXES points (F1DP...) and AF\n"
        "attributes (F1Ab...) — TAG resolution only yields raw points, so the split would\n"
        "then have just one bucket.")

if WEB_IDS:
    print(f"using {len(WEB_IDS)} WebIDs given directly (no lookup needed)")
else:
    # Resolve TAGS -> WebIDs. GET /points first, POST /batch only as an optimisation.
    # (Same logic and caveats as benchmark_fanout.py.)
    WEB_IDS, _bad, _errs = [], [], {}
    _batched = False
    if len(TAGS) > 1:
        try:
            _reqs = {str(i): {"Method": "GET",
                              "Resource": f"{BASE}/points?path="
                                          + quote(rf"\\{PI_SERVER}\{t}", safe="")}
                     for i, t in enumerate(TAGS)}
            _resp = batch(BASE, _reqs, **AUTH)
            for i, t in enumerate(TAGS):
                wid = ((_resp.get(str(i)) or {}).get("Content") or {}).get("WebId")
                (WEB_IDS if wid else _bad).append(wid or t)
            _batched = True
            print(f"resolved {len(WEB_IDS)}/{len(TAGS)} WebIDs via POST /batch on {PI_SERVER}")
        except Exception as _be:
            print(f"POST /batch unavailable ({_cause(_be)})")
            print(f"  -> using GET /points instead: {len(TAGS)} requests rather than 1.")
            WEB_IDS, _bad = [], []
    if not _batched:
        for t in TAGS:
            try:
                WEB_IDS.append(get_point(BASE, PI_SERVER, t, **AUTH)["WebId"])
            except Exception as _pe:
                _bad.append(t)
                _k = re.sub(r"\s*for url:.*$", "", _cause(_pe)).strip()[:120]
                _errs[_k] = _errs.get(_k, 0) + 1
                if len(_bad) >= GIVE_UP_AFTER and not WEB_IDS:
                    print(f"     stopping after {len(_bad)} failures with nothing resolved")
                    break
        print(f"     resolved {len(WEB_IDS)}/{len(TAGS)} WebIDs via GET /points")
        for _k, _n in sorted(_errs.items(), key=lambda x: -x[1]):
            print(f"       {_n:>5}x  {_k}")
    if _bad:
        print(f"   ⚠️ {len(_bad)} did NOT resolve. Check PI_SERVER name/casing, or paste WEB_IDS.")
if not WEB_IDS:
    raise AssertionError(
        f"No WebIDs resolved from {len(TAGS)} tag name(s). See benchmark_fanout.py for the "
        f"grouped-reason diagnostics; the most common cause is a wrong PI_SERVER name.")

# THE CONNECTOR OPTIONS (shared; per-test knobs are set at each call site).
TS_OPTS = {"endpoint_url": ENDPOINT_URL,          # PI Web API base URL
           "web_ids": ",".join(WEB_IDS),          # overridden per bucket below
           **CONN_AUTH,                           # bearer_token, or basic_user+basic_password
           "verify_tls": str(VERIFY_TLS).lower(),
           "http_timeout_seconds": str(HTTP_TIMEOUT)}

results = []


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


# Pre-declared so any section can run on its own without NameError.
RATE = 0.0                            # measured tag rate, values/sec/tag
CAP = min(10_000, 150_000 // 50)      # maxCount per tag at 50 tags/call
BREAK, HIST = [], []                  # § 1 and § 2 results

# COMMAND ----------

# MAGIC %md ## Classify — split the WebIDs into type buckets ⭐
# MAGIC
# MAGIC The one cell that makes this notebook different. Two levels:
# MAGIC
# MAGIC * **Level 1 — free, offline.** The WebID prefix says point vs attribute:
# MAGIC   `F1DP…` = a **raw PI point**, `F1Ab…` = an **AF attribute**. No API call.
# MAGIC * **Level 2 — one `GET /attributes/{WebId}` per attribute.** Two AF attributes have
# MAGIC   identical-looking WebIDs whether one is a passthrough and the other a formula — the
# MAGIC   only way to tell is the attribute's **`DataReference`** field, which this fetches.
# MAGIC   (NB: the PI Web API JSON calls it `DataReference` — a string; the AF SDK calls the
# MAGIC   same thing `DataReferencePlugIn`. We read the REST name, and also fetch `ConfigString`
# MAGIC   as a cross-check because the display name can be localized or customer-customised.)
# MAGIC
# MAGIC A WebID whose lookup fails is bucketed `unknown` and listed — it never crashes the run.
# MAGIC Only `raw_point / passthrough / formula / analysis` are benchmarked; `static / other /
# MAGIC unknown` are reported but skipped.

# COMMAND ----------

# Buckets that get benchmarked, in the order they are reported.
_BENCH_TYPES = ["raw_point", "passthrough", "formula", "analysis"]


def classify_web_id(prefix: str, data_reference) -> str:
    """Pure mapping (WebID prefix, DataReference) -> bucket. Unit-testable, no I/O.

    Level 1 is the prefix (chars 3-4 of a WebID 2.0 string — confirmed against the PI Web
    API Reference: `DP`=PIPoint, `Ab`=AFAttribute, `Em`=AFElement, `DS`=PIServer). Level 2
    refines an AF attribute by its `DataReference` string (the REST field name; the AF SDK
    calls the same thing `DataReferencePlugIn`). `data_reference` is None for raw points
    (never looked up) and for static attributes with no data reference.
    """
    p4 = (prefix or "")[:4]
    if p4 == "F1DP":
        return "raw_point"                       # a PI point — no data reference to read
    if p4 == "F1Ab":
        dr = (data_reference or "").strip()
        if dr == "":
            return "static"                      # AF attribute with no data reference
        if dr == "PI Point":
            return "passthrough"                 # AF attribute that maps straight to a point
        if dr == "Formula":
            return "formula"                     # computed on read
        # Any other calc-backed reference (Analysis, Rollup, Table Lookup, custom, …).
        # Names can be localized/customised, so this is the catch-all, not an allow-list.
        return "analysis"
    return "other"                               # element/data-server/unknown prefix


# Pooled session for the Level-2 lookups (Bearer for the mock App, Basic for real PI).
_sess = requests.Session()
_sess.headers.update(_hdrs)
if not USE_MOCK_APP:
    _sess.auth = requests.auth.HTTPBasicAuth(_user, _pw)
_sess.verify = VERIFY_TLS

# SAFETY CHECK: before trusting the field name, print the RAW attribute JSON for the
# first couple of F1Ab WebIDs. The classifier keys on `DataReference`; if THIS server
# names it differently (older build, localized, customised), you will SEE it here and can
# adjust the one field name below — rather than silently bucketing everything as `static`.
_attr_wids = [w for w in WEB_IDS if w[:4] == "F1Ab"]
if _attr_wids:
    print("── sanity: raw /attributes JSON for the first AF attribute(s) ──")
    for _w in _attr_wids[:2]:
        try:
            _raw = _sess.get(f"{BASE}/attributes/{_w}", timeout=HTTP_TIMEOUT).json()
            print(f"  {_w[:24]}…  DataReference={_raw.get('DataReference')!r}  "
                  f"ConfigString={str(_raw.get('ConfigString'))[:60]!r}")
        except Exception as _e:
            print(f"  {_w[:24]}…  lookup failed: {_cause(_e)}")
    print("  ^ if DataReference is None but these ARE attributes, the field is named")
    print("    differently on this server — change the .get('DataReference') below.\n")

_buckets = {}                          # bucket -> [web_id, ...]
_dref_of = {}                          # web_id -> DataReference (for the table)
_unknown = []                          # web_ids whose Level-2 lookup errored
_n_attr_calls = 0

for _wid in WEB_IDS:
    _p4 = _wid[:4]
    if _p4 == "F1Ab":
        # Level 2: only AF attributes need the extra call. `DataReference` is the REST
        # field name (AF SDK calls it `DataReferencePlugIn`); ConfigString is fetched too
        # as a cross-check (a \\SERVER\TAG path => point; a formula expression => formula).
        try:
            _r = _sess.get(f"{BASE}/attributes/{_wid}",
                           params={"selectedFields": "Name;DataReference;ConfigString"},
                           timeout=HTTP_TIMEOUT)
            _n_attr_calls += 1
            _dr = (_r.json() or {}).get("DataReference") if _r.status_code == 200 else None
        except Exception:
            _buckets.setdefault("unknown", []).append(_wid)
            _unknown.append(_wid)
            continue
    else:
        _dr = None                     # points (and non-attributes) are Level-1 only
    _dref_of[_wid] = _dr
    _buckets.setdefault(classify_web_id(_p4, _dr), []).append(_wid)

# The dict the sweeps consume — only the benchmarkable buckets, non-empty ones.
WEB_IDS_BY_TYPE = {t: _buckets[t] for t in _BENCH_TYPES if _buckets.get(t)}

# Report every bucket (benchmarked + skipped), largest first.
print(f"classified {len(WEB_IDS)} WebIDs  ({_n_attr_calls} attribute lookups):\n")
_all_types = _BENCH_TYPES + ["static", "other", "unknown"]
for _t in _all_types:
    _ids = _buckets.get(_t, [])
    if not _ids:
        continue
    _flag = "" if _t in _BENCH_TYPES else "   (skipped — not benchmarkable)"
    print(f"  {_t:<12} {len(_ids):>5}{_flag}")
    if _t in ("other", "unknown"):     # these are the surprising ones — show a sample
        for _s in _ids[:3]:
            print(f"                 e.g. {_s[:60]}")

if not WEB_IDS_BY_TYPE:
    raise AssertionError(
        "No benchmarkable WebIDs after classification. Every WebID fell into static/other/"
        "unknown. Confirm you pasted point (F1DP) or attribute (F1Ab) WebIDs, and that the "
        "attribute lookups reached PI.")

# Small table for the notebook UI.
display(spark.createDataFrame(
    [(_t, len(_buckets.get(_t, [])), _t in _BENCH_TYPES) for _t in _all_types
     if _buckets.get(_t)],
    schema="tag_type string, count int, benchmarked boolean"))

# COMMAND ----------

# MAGIC %md ### Fairness cap
# MAGIC
# MAGIC To compare types apples-to-apples, every bucket is benchmarked at the SAME tag count —
# MAGIC the size of the smallest non-empty benchmarked bucket (`FAIR_N`). Comparing 96
# MAGIC passthroughs against 18 formulas would otherwise confound tag-count with type.

# COMMAND ----------

FAIR_N = min(len(ids) for ids in WEB_IDS_BY_TYPE.values())
print(f"FAIR_N = {FAIR_N} tags per bucket (smallest benchmarked bucket)")
print("each bucket is capped to its first FAIR_N WebIDs so the per-type numbers compare.\n")
for _t, _ids in WEB_IDS_BY_TYPE.items():
    _note = "" if len(_ids) == FAIR_N else f"  (capped from {len(_ids)})"
    print(f"  {_t:<12} using {min(len(_ids), FAIR_N):>4} of {len(_ids)}{_note}")

# The capped lists the sweeps actually read.
WEB_IDS_BY_TYPE = {t: ids[:FAIR_N] for t, ids in WEB_IDS_BY_TYPE.items()}

# COMMAND ----------

# MAGIC %md ## Smoke test — is the connector working, per type?
# MAGIC
# MAGIC One tiny `recorded` read of a few tags from EACH bucket. Confirms the connector can read
# MAGIC that type at all before the sweeps — a formula bucket that 403s or returns nothing shows
# MAGIC up here, not 20 minutes into § 1.

# COMMAND ----------

_from = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(timespec="seconds")
_ok_all = True
for _t, _ids in WEB_IDS_BY_TYPE.items():
    _probe = ",".join(_ids[:3])
    try:
        _df = (spark.read.format("aveva_pi_timeseries").options(**TS_OPTS)
               .option("web_ids", _probe).option("read_mode", "recorded")
               .option("webids_per_call", "3").option("initial_watermark", _from).load())
        _n = _df.count()
        _bad = [c for c in ("web_id", "timestamp", "value") if c not in _df.columns]
        _ok = _n > 0 and not _bad
        _ok_all &= _ok
        print(f"  {_t:<12} {_n:>6} rows  {'✅' if _ok else '⚠️ no rows / missing cols'}")
    except Exception as e:
        _ok_all = False
        print(f"  {_t:<12} FAILED: {_cause(e)}")
print("\n✅ all buckets read — safe to continue." if _ok_all else
      "\n⚠️ a bucket failed or returned nothing. `recorded` over 10 min can be empty for a\n"
      "   quiet tag; widen the window or check that type's auth/permissions before the sweeps.")

# COMMAND ----------

# MAGIC %md ## Archive density
# MAGIC
# MAGIC Values archived per tag per second — sizes the `recorded` windows. Measured across a
# MAGIC sample of ALL types together (it is a per-tag rate, so the mix does not matter). See
# MAGIC `benchmark_fanout.py` for the full caveats on what this rate does and does not mean.

# COMMAND ----------

_PROBE_S = 3600
_PROBE_TAGS = 5
_end = datetime.now(timezone.utc)
_counts = []
for _wid in WEB_IDS[:_PROBE_TAGS]:
    _r = _sess.get(f"{BASE}/streams/{_wid}/recorded",
                   params={"startTime": (_end - timedelta(seconds=_PROBE_S)).isoformat(),
                           "endTime": _end.isoformat(), "maxCount": "150000"},
                   timeout=HTTP_TIMEOUT)
    _counts.append(len((_r.json() or {}).get("Items") or []) if _r.status_code == 200 else 0)
_per = [c / _PROBE_S for c in _counts]
RATE = round(max(_per), 4) if _per else 0.0
CAP = min(10_000, 150_000 // 50)
print(f"{_PROBE_TAGS} tags over {_PROBE_S/3600:.0f}h -> {_counts} values archived")
print(f"  per tag/sec: max {RATE}   (sizing uses the max)")
print(f"-> per 60s cycle: `recorded` ~ {int(FAIR_N * RATE * 60):,} rows for {FAIR_N} tags")

# COMMAND ----------

# MAGIC %md ## § 1 Breaking point BY TYPE — how fresh can the data be? ⭐
# MAGIC
# MAGIC The `benchmark_fanout.py` freshness sweep, run once per type bucket at `FAIR_N` tags.
# MAGIC Each result row carries `tag_type`, so the freshness floor is reported per type — and a
# MAGIC formula bucket that overruns where a point bucket sails through is the whole point.
# MAGIC
# MAGIC **Cycle-time percentiles.** Each combination now observes several cycles and reports the
# MAGIC **p50** (typical cycle) and **p90** (the slow cycle that actually breaks a freshness SLA)
# MAGIC alongside the worst. The freshness decision uses p90, not the mean, because it is the
# MAGIC occasional slow cycle — not the average — that makes a streaming job fall behind.

# COMMAND ----------

# ---- WHAT THIS SECTION SWEEPS (per type) -------------------------------------------
MODE      = "recorded"       # "recorded" = every archived value | "value" = one snapshot/tag
TRIGGERS  = [30, 15, 5]      # seconds, LONG -> SHORT
CHUNKS    = [100]            # webids_per_call. Add 25 to compare chunk sizes (multiplies time)
CYCLES    = 6                # micro-batches observed per combination (>= a few, so p50/p90
                             #   over the steady-state cycles are meaningful — batch 0 dropped)
CONCURRENCY = 8              # partition_concurrency (only bites if a window splits)
# ------------------------------------------------------------------------------------


def _pctl(sorted_vals, q):
    """Linear-interpolated percentile of an already-sorted list (q in 0..1). No numpy dep."""
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = q * (len(sorted_vals) - 1)
    lo = int(pos)
    frac = pos - lo
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac

_combos = len(WEB_IDS_BY_TYPE) * len(CHUNKS) * len(TRIGGERS)
_mins = _combos * sum(TRIGGERS) * CYCLES / 60 / max(1, len(TRIGGERS))
print(f"mode={MODE}, types={list(WEB_IDS_BY_TYPE)}, {FAIR_N} tags each, "
      f"triggers={TRIGGERS}s, chunks={CHUNKS}")
print(f"{_combos} combinations across {len(WEB_IDS_BY_TYPE)} types "
      f"(stops each type at its first overrun)\n")


def stream_once(web_ids, trigger_s, chunk, cycles=CYCLES, mode=MODE):
    """Stream `web_ids` through the connector for `cycles` micro-batches.

    Fresh checkpoint per run — reusing one resumes from a committed offset and reads an
    empty window, which would look artificially fast.
    """
    opts = dict(TS_OPTS)
    opts["web_ids"] = ",".join(web_ids)
    reader = (spark.readStream.format("aveva_pi_timeseries")
              .options(**opts)
              .option("read_mode", mode)
              .option("webids_per_call", str(chunk))
              .option("partition_concurrency", str(CONCURRENCY)))
    if mode != "value":
        reader = reader.option(
            "initial_watermark",
            (datetime.now(timezone.utc) - timedelta(seconds=trigger_s)).isoformat(timespec="seconds"))
        if mode == "interpolated":
            reader = reader.option("interval", INTERVAL)
    q = (reader.load()
         .writeStream.format("noop")
         .option("checkpointLocation",
                 f"{CHECKPOINT_ROOT.rstrip(chr(47))}/bt_{mode}_{int(time.time()*1000)}")
         .trigger(processingTime=f"{trigger_s} seconds")
         .start())
    seen, deadline = [], time.time() + (cycles + 2) * trigger_s + 120
    try:
        while len(seen) < cycles and time.time() < deadline:
            time.sleep(2)
            if q.exception():
                raise RuntimeError(str(q.exception())[:300])
            for p in q.recentProgress:
                if p["batchId"] not in [s["batch_id"] for s in seen]:
                    seen.append({"batch_id": p["batchId"], "rows": p["numInputRows"],
                                 "duration_s": round(p["batchDuration"] / 1000, 2)})
    finally:
        q.stop()
    return seen


for _type, _ids in WEB_IDS_BY_TYPE.items():
    print(f"── {_type} ({len(_ids)} tags) ──────────────────────────────")
    for _chunk in CHUNKS:
        for _t in TRIGGERS:                 # long -> short
            _lbl = f"  {_type:<12} chunk={_chunk:<4} trigger={_t:>3}s"
            try:
                _s = stream_once(_ids, _t, _chunk)
                if not _s:
                    print(f"{_lbl}  no batches observed")
                    continue
                _steady = _s[1:] or _s      # batch 0 can include cold start
                _durs = sorted(x["duration_s"] for x in _steady)
                _dur = _durs[-1]            # worst cycle (the runaway trigger)
                _p50 = round(_pctl(_durs, 0.50), 2)   # typical cycle
                _p90 = round(_pctl(_durs, 0.90), 2)   # the slow cycle a freshness SLA feels
                _rows = max(x["rows"] for x in _steady)
                # Freshness decision keys on p90, not worst: one cold outlier shouldn't
                # condemn a trigger, but a p90 over the interval means it routinely won't fit.
                _pct = round(100 * _p90 / _t, 1)
                _pct_worst = round(100 * _dur / _t, 1)
                _n = len(_ids)
                _calls = -(-_n // _chunk)
                BREAK.append({"tag_type": _type, "tags": _n, "webids_per_call": _chunk,
                              "trigger_s": _t, "cycles_seen": len(_steady),
                              "p50_cycle_s": _p50, "p90_cycle_s": _p90, "worst_cycle_s": _dur,
                              "pct_of_trigger": _pct, "pct_of_trigger_worst": _pct_worst,
                              "rows": _rows, "fits": _pct < 100, "read_mode": MODE,
                              "calls_per_cycle": _calls,
                              "s_per_call": round(_p90 / _calls, 3),
                              "ms_per_tag": round(1000 * _p90 / _n, 1),
                              "rows_per_sec": round(_rows / _p90) if _p90 else None,
                              "cluster_parallelism": CLUSTER_CORES})
                print(f"{_lbl}  p50 {_p50:>6.2f}s  p90 {_p90:>6.2f}s  worst {_dur:>6.2f}s  "
                      f"{_pct:>6.1f}%(p90)  {_rows:>8,} rows  "
                      f"{'✅' if _pct < 50 else '⚠️ tight' if _pct < 100 else '🚨 OVERRUNS'}")
                if _pct >= 100:
                    print("        ⛔ stopping this type — shorter triggers only fail harder")
                    break
            except Exception as e:
                print(f"{_lbl}  FAILED: {_cause(e)}")

results.extend(BREAK)
if BREAK:
    display(spark.createDataFrame(BREAK))
    # Freshness floor PER TYPE — the fastest safe trigger for each.
    print(f"\n{'type':>12}  {'fastest safe trigger':>21}  {'used':>6}")
    for _type in WEB_IDS_BY_TYPE:
        _safe = [b for b in BREAK if b["tag_type"] == _type and b["pct_of_trigger"] < 50]
        _fits = [b for b in BREAK if b["tag_type"] == _type and b["fits"]]
        if _safe:
            _r = min(_safe, key=lambda b: b["trigger_s"])
            print(f"{_type:>12}  {str(_r['trigger_s']) + 's':>21}  {_r['pct_of_trigger']:>5.1f}%")
        else:
            print(f"{_type:>12}  {('fits but >50% used' if _fits else 'no viable trigger'):>21}"
                  f"  {(min(_fits, key=lambda b: b['trigger_s'])['pct_of_trigger'] if _fits else 0):>5.1f}%")

# COMMAND ----------

# MAGIC %md ### § 1b Chart — cycle time vs trigger, by type

# COMMAND ----------

if BREAK:
    import matplotlib.pyplot as plt
    _df = spark.createDataFrame(BREAK).toPandas()
    fig, ax = plt.subplots(figsize=(9, 5))
    for _type, _g in _df.groupby("tag_type"):
        _g = _g.sort_values("trigger_s")
        # Solid line = p90 (the decision metric); shaded band up to worst shows the tail.
        ax.plot(_g["trigger_s"], _g["p90_cycle_s"], "o-", label=f"{_type} (p90)")
        ax.fill_between(_g["trigger_s"], _g["p50_cycle_s"], _g["worst_cycle_s"], alpha=0.12)
    _lim = sorted(_df["trigger_s"].unique())
    ax.plot(_lim, _lim, "k--", lw=1, label="the deadline (cycle = trigger)")
    ax.fill_between(_lim, _lim, max(_lim) * 1.1, color="red", alpha=0.07)
    ax.fill_between(_lim, 0, [x / 2 for x in _lim], color="green", alpha=0.07)
    ax.set_xlabel("trigger interval (s)")
    ax.set_ylabel("cycle time (s) — p90 line, band = p50→worst")
    ax.set_title(f"Cycle time vs trigger by type ({MODE}, {FAIR_N} tags each) — "
                 f"under the dashed line fits")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    display(fig)
    plt.close(fig)

# COMMAND ----------

# MAGIC %md ## § 2 History load BY TYPE — how long does a backfill take?
# MAGIC
# MAGIC A finite `spark.read` of N hours per type bucket. Formula/analysis attributes make PI
# MAGIC recompute over the whole window, so this is where a calc-backed type can diverge most
# MAGIC from a raw point.

# COMMAND ----------

HIST_HOURS  = [1, 6]      # hours of history per run
HIST_CHUNKS = [100]       # webids_per_call (add 25 to compare, multiplies runtime)
HIST_CONC   = [8]         # partition_concurrency (add 1 to reproduce pre-2.1.0 behaviour)

# URL-length warning (AF attribute WebIDs are long — 130-200 chars — so a big chunk can
# exceed IIS maxUrl 4096 and PI rejects the request; see benchmark_fanout.py § 2).
_idlen = max(len(w) for w in WEB_IDS)
for _c in sorted(set(HIST_CHUNKS + CHUNKS)):
    _qs = _c * (_idlen + 8)
    if _qs > 4096:
        print(f"⚠️  chunk={_c}: ~{_qs:,}-char query string (longest WebID {_idlen} chars) may")
        print(f"    exceed IIS maxUrl 4096 — lower the chunk or raise maxUrl in PI's web.config.")
print()

_runs = len(WEB_IDS_BY_TYPE) * len(HIST_HOURS) * len(HIST_CHUNKS) * len(HIST_CONC)
print(f"{_runs} runs: {len(WEB_IDS_BY_TYPE)} types x {HIST_HOURS}h x chunks {HIST_CHUNKS} "
      f"x conc {HIST_CONC}, {FAIR_N} tags each\n")

# Warm-up: the first Spark action pays Python-worker startup (~12s); do it once, unmeasured.
try:
    (spark.read.format("aveva_pi_timeseries").options(**TS_OPTS)
     .option("web_ids", ",".join(next(iter(WEB_IDS_BY_TYPE.values()))))
     .option("read_mode", "recorded").option("webids_per_call", "100")
     .option("initial_watermark",
             (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(timespec="seconds"))
     .load().count())
    print("warm-up done\n")
except Exception as e:
    print(f"warm-up failed ({str(e)[:80]})\n")

for _type, _ids in WEB_IDS_BY_TYPE.items():
    print(f"── {_type} ({len(_ids)} tags) ──────────────────────────────")
    for _hours in HIST_HOURS:
        for _chunk in HIST_CHUNKS:
            for _conc in HIST_CONC:
                _start = (datetime.now(timezone.utc) - timedelta(hours=_hours)).isoformat(timespec="seconds")
                _windows = max(1, -(-(_hours * 3600) // (int(CAP / RATE) if RATE else CAP)))
                _lbl = f"  {_type:<12} {_hours}h chunk={_chunk:<4} conc={_conc:<2}"
                try:
                    _t0 = time.perf_counter()
                    _n = (spark.read.format("aveva_pi_timeseries").options(**TS_OPTS)
                          .option("web_ids", ",".join(_ids))
                          .option("read_mode", "recorded")
                          .option("webids_per_call", str(_chunk))
                          .option("partition_concurrency", str(_conc))
                          .option("initial_watermark", _start)
                          .load().count())
                    _secs = time.perf_counter() - _t0
                    _tasks = -(-len(_ids) // _chunk)
                    _calls_total = _windows * _tasks
                    HIST.append({"tag_type": _type, "hours": _hours, "webids_per_call": _chunk,
                                 "partition_concurrency": _conc, "seconds": round(_secs, 1),
                                 "rows": _n, "rows_per_sec": round(_n / _secs) if _secs else None,
                                 "sub_windows_per_task": _windows, "calls_total": _calls_total,
                                 "s_per_call": round(_secs / _calls_total, 3),
                                 "min_per_day": round(_secs / _hours * 24 / 60, 1),
                                 "cluster_parallelism": CLUSTER_CORES, "error": ""})
                    print(f"{_lbl} {_secs:>7.1f}s  {_n:>9,} rows  {round(_n/_secs):>8,} rows/s  "
                          f"= {_secs/_hours*24/60:>5.1f} min/day")
                except Exception as e:
                    HIST.append({"tag_type": _type, "hours": _hours, "webids_per_call": _chunk,
                                 "partition_concurrency": _conc, "seconds": None, "rows": None,
                                 "rows_per_sec": None, "sub_windows_per_task": _windows,
                                 "calls_total": None, "s_per_call": None, "min_per_day": None,
                                 "cluster_parallelism": CLUSTER_CORES, "error": _cause(e)})
                    print(f"{_lbl} FAILED: {_cause(e)}")

results.extend(HIST)
_ok = [h for h in HIST if h["seconds"]]
if HIST:
    display(spark.createDataFrame(
        [(h["tag_type"], int(h["hours"]), int(h["webids_per_call"]),
          int(h["partition_concurrency"]), h["seconds"], h["rows"], h["rows_per_sec"],
          h["sub_windows_per_task"], h["calls_total"], h["s_per_call"], h["min_per_day"],
          int(h["cluster_parallelism"]), h["error"]) for h in HIST],
        schema=StructType([
            StructField("tag_type", StringType()),
            StructField("hours", IntegerType()),
            StructField("webids_per_call", IntegerType()),
            StructField("partition_concurrency", IntegerType()),
            StructField("seconds", DoubleType()),
            StructField("rows", LongType()),
            StructField("rows_per_sec", LongType()),
            StructField("sub_windows_per_task", IntegerType()),
            StructField("calls_total", IntegerType()),
            StructField("s_per_call", DoubleType()),
            StructField("min_per_day", DoubleType()),
            StructField("cluster_parallelism", IntegerType()),
            StructField("error", StringType()),
        ])))
if _ok:
    print(f"\n{'type':>12}  {'min/day of history':>18}  {'rows/s':>9}")
    for _type in WEB_IDS_BY_TYPE:
        _t_ok = [h for h in _ok if h["tag_type"] == _type]
        if _t_ok:
            _b = min(_t_ok, key=lambda h: h["seconds"] / h["hours"])
            _pd = _b["seconds"] / _b["hours"] * 24
            print(f"{_type:>12}  {_pd/60:>15.1f} m  {_b['rows_per_sec']:>9,}")

# COMMAND ----------

# MAGIC %md ### § 2b Chart — backfill time by type

# COMMAND ----------

if _ok:
    import matplotlib.pyplot as plt
    _hdf = spark.createDataFrame(_ok).toPandas().sort_values(["tag_type", "hours"])
    fig, ax = plt.subplots(figsize=(9, 5))
    for _type, _g in _hdf.groupby("tag_type"):
        _g = _g.sort_values("hours")
        ax.plot(_g["hours"], _g["seconds"], "o-", label=_type)
    ax.set_xlabel("hours of history")
    ax.set_ylabel("seconds")
    ax.set_title(f"Backfill time by type ({FAIR_N} tags each) — steeper line = costlier type")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    display(fig)
    plt.close(fig)

# COMMAND ----------

# MAGIC %md ## Summary table — findings by type
# MAGIC
# MAGIC One row per (type × metric), so the differentiating numbers sit side by side. This is
# MAGIC the table to paste into the customer note: it answers "does a formula cost more than a
# MAGIC point?" directly.

# COMMAND ----------

print("=" * 74)
print("  AVEVA PI CONNECTOR BENCHMARK BY TYPE — RUN RECORD")
print("=" * 74)
print(f"  When (UTC)    : {_now_iso()}")
print(f"  Endpoint      : {BASE}")
print(f"  Connector     : aveva_pi_timeseries {_ts.__version__}")
print(f"  Tags/bucket   : {FAIR_N} (fairness cap)")
print(f"  Types         : {', '.join(f'{t}={len(WEB_IDS_BY_TYPE[t])}' for t in WEB_IDS_BY_TYPE)}")
print(f"  Tag rate      : {RATE} values/sec/tag")
print(f"  Cluster slots : {CLUSTER_CORES} ({_cores_src})")
print("=" * 74)

# One tidy comparison row per type, pulling the § 1 (incl. p50/p90 cycle) and § 2 numbers.
_SUM = []
for _type in WEB_IDS_BY_TYPE:
    _b1 = [b for b in BREAK if b["tag_type"] == _type]
    _safe = [b for b in _b1 if b["pct_of_trigger"] < 50]
    _h_ok = [h for h in _ok if h["tag_type"] == _type]
    _floor = (min(_safe, key=lambda b: b["trigger_s"])["trigger_s"] if _safe else None)
    _mstag = (min(b["ms_per_tag"] for b in _b1) if _b1 else None)
    _worst = (min(b["worst_cycle_s"] for b in _b1) if _b1 else None)
    # p50/p90 cycle at this type's fastest safe trigger (else its fastest that fits, else
    # just the lowest-p90 combination) — the cycle distribution at the operating point.
    _pool = _safe or [b for b in _b1 if b["fits"]] or _b1
    _op = min(_pool, key=lambda b: b["trigger_s"]) if _pool else None
    _minday = None
    if _h_ok:
        _b = min(_h_ok, key=lambda h: h["seconds"] / h["hours"])
        _minday = round(_b["seconds"] / _b["hours"] * 24 / 60, 1)
    _rps = (max((b["rows_per_sec"] or 0) for b in _b1) if _b1 else None)
    _SUM.append({"tag_type": _type, "tags": FAIR_N,
                 "p50_cycle_s": _op["p50_cycle_s"] if _op else -1.0,
                 "p90_cycle_s": _op["p90_cycle_s"] if _op else -1.0,
                 "freshness_floor_s": _floor if _floor is not None else -1,
                 "best_ms_per_tag": _mstag if _mstag is not None else -1.0,
                 "best_cycle_s": _worst if _worst is not None else -1.0,
                 "backfill_min_per_day": _minday if _minday is not None else -1.0,
                 "rows_per_sec": int(_rps) if _rps else -1})

if _SUM:
    display(spark.createDataFrame(_SUM, schema=StructType([
        StructField("tag_type", StringType()),
        StructField("tags", IntegerType()),
        StructField("p50_cycle_s", DoubleType()),
        StructField("p90_cycle_s", DoubleType()),
        StructField("freshness_floor_s", IntegerType()),
        StructField("best_ms_per_tag", DoubleType()),
        StructField("best_cycle_s", DoubleType()),
        StructField("backfill_min_per_day", DoubleType()),
        StructField("rows_per_sec", IntegerType()),
    ])))
    print("  (-1 = not measured for that type. p50/p90_cycle_s = § 1 cycle time at the fastest "
          "safe trigger; freshness_floor_s = fastest trigger with >50% headroom.)")

    # The headline the customer asked for: formula vs passthrough, if both ran.
    _by = {s["tag_type"]: s for s in _SUM}
    if "formula" in _by and "passthrough" in _by \
            and _by["formula"]["best_ms_per_tag"] > 0 and _by["passthrough"]["best_ms_per_tag"] > 0:
        _f, _p = _by["formula"]["best_ms_per_tag"], _by["passthrough"]["best_ms_per_tag"]
        print(f"\n→ FORMULA vs PASSTHROUGH: {_f} vs {_p} ms/tag "
              f"({_f/_p:.2f}x) — formulas cost {'more' if _f > _p else 'the same/less'} per tag.")
    if "passthrough" in _by and "raw_point" in _by \
            and _by["passthrough"]["best_ms_per_tag"] > 0 and _by["raw_point"]["best_ms_per_tag"] > 0:
        _pa, _rp = _by["passthrough"]["best_ms_per_tag"], _by["raw_point"]["best_ms_per_tag"]
        print(f"→ PASSTHROUGH vs RAW POINT: {_pa} vs {_rp} ms/tag "
              f"({_pa/_rp:.2f}x) — the AF-layer hop for a passthrough attribute.")

if RESULT_TABLE and results:
    spark.createDataFrame(results).write.mode("append").option("mergeSchema", "true") \
         .saveAsTable(RESULT_TABLE)
    print(f"\nSaved → {RESULT_TABLE}")
if RESULT_TABLE and _SUM:
    spark.createDataFrame([{**s, "run_utc": _now_iso()} for s in _SUM]) \
         .write.mode("append").option("mergeSchema", "true") \
         .saveAsTable(RESULT_TABLE + "_summary")
    print(f"Saved → {RESULT_TABLE}_summary")
