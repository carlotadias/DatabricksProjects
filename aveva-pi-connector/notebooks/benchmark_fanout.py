# Databricks notebook source
# MAGIC %md
# MAGIC # AVEVA PI connector — benchmark
# MAGIC
# MAGIC Two questions: **how fresh can the data be** (§ 1) and **how long does a history load
# MAGIC take** (§ 2). Everything is measured through the real connector, against real PI.
# MAGIC
# MAGIC > ⚠️ **This runs against production PI.** Agree a stop signal with whoever watches PI's
# MAGIC > CPU before you start. § 1 sweeps small→large and long→short and stops at the first
# MAGIC > overrun; § 2 reads history, which is the heavier one (archive reads compete with the
# MAGIC > interfaces writing new data). Every result carries UTC start/end so it can be
# MAGIC > correlated against PI-side CPU graphs.
# MAGIC
# MAGIC | § | Question |
# MAGIC |---|---|
# MAGIC | 1 | **Breaking point** — how fresh can the data be? (trigger × chunk) |
# MAGIC | 2 | **History load** — how long does a backfill take? |
# MAGIC
# MAGIC ### The knobs
# MAGIC
# MAGIC | Option | What it does |
# MAGIC |---|---|
# MAGIC | `read_mode` | `value` = one snapshot per tag (**drops everything between polls**). `recorded` = every archived value in the window (lossless). |
# MAGIC | `webids_per_call` | tags per HTTP call **and** tags per Spark task. The main lever. Fewer, larger calls are often faster — per-call and per-task overhead is paid once each. |
# MAGIC | `partition_concurrency` | calls in flight per task. Only does anything when a task has **>1 call**, i.e. a window wide enough to split. Inert before connector 2.1.0. |
# MAGIC | `assumed_values_per_second` | sizes `recorded` windows (2.1.0+). The density cell below measures the real rate. |
# MAGIC | `trigger` (Spark, not the connector) | how often a micro-batch starts. A cycle must finish inside it. |
# MAGIC
# MAGIC **The failure mode to find:** if a cycle takes longer than the trigger, the next starts
# MAGIC late, its window is wider, so it takes longer still — the job falls permanently behind.
# MAGIC Nothing errors; the data just gets staler. That is what § 1 locates.

# COMMAND ----------

# MAGIC # PICK ONE — uncomment the line that matches how you got the connector.
# MAGIC
# MAGIC # (A) You pulled the repo as a Git folder — installs from source, nothing to publish:
# MAGIC %pip install ../timeseries ../assetframework
# MAGIC
# MAGIC # (B) Wheels published to a UC Volume (see HOW_TO_USE.md Step 1) — EDIT the paths:
# MAGIC # %pip install /Volumes/<catalog>/<schema>/<volume>/aveva_pi_assetframework-3.0.2-py3-none-any.whl /Volumes/<catalog>/<schema>/<volume>/aveva_pi_timeseries-2.1.0-py3-none-any.whl
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
# Provide WEB_IDS directly, OR tag names in TAGS. If WEB_IDS is empty, setup resolves TAGS
# via the API — one POST /batch of GET /points?path=\\<PI_SERVER>\<tag> — which needs
# PI_SERVER to be exactly right and TAGS to be full PI point names. Paste WEB_IDS instead
# when you already have them, or when resolution fails (a wrong server name gives 404s).
WEB_IDS = [
    # "F1AbEfg...", "F1AbEfg...",
]
TAGS = [
    # "Plant.Area.Unit1.Temp", "Plant.Area.Unit1.Pressure",
]

READ_MODE    = "recorded"    # what § 1 stresses: "recorded" | "value" | "interpolated"
INTERVAL     = "1m"          # interpolated only
HTTP_TIMEOUT = 30

RESULT_TABLE = None          # e.g. "<catalog>.<schema>.pi_benchmark", or None to skip
# § 1 streams, so it needs somewhere to write checkpoints (one throwaway dir per run).
CHECKPOINT_ROOT = "/Volumes/<catalog>/<schema>/<volume>/benchmark_checkpoints"   # EDIT

# COMMAND ----------

# MAGIC %md ## Setup — auth, WebID resolution, cluster shape

# COMMAND ----------

import time, json
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, urlparse

import requests
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

import aveva_pi_timeseries as _ts
from aveva_pi_assetframework import batch
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
    print(f"✅ reached {_host} (HTTP {_r.status_code})")
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

# WebIDs. Given directly? Use them. Otherwise resolve TAGS through the API: one POST
# /batch whose sub-requests are GET /points?path=\\<PI_SERVER>\<tag>. A wrong PI_SERVER
# or a name that is not a full PI point path comes back 404 and is listed below.
if WEB_IDS:
    print(f"using {len(WEB_IDS)} WebIDs given directly (no lookup needed)")
else:
    assert TAGS, "Set WEB_IDS or TAGS — there is nothing to read."
    _reqs = {str(i): {"Method": "GET",
                      "Resource": f"{BASE}/points?path=" + quote(rf"\\{PI_SERVER}\{t}", safe="")}
             for i, t in enumerate(TAGS)}
    _resp = batch(BASE, _reqs, **AUTH)
    WEB_IDS, _bad = [], []
    for i, t in enumerate(TAGS):
        wid = ((_resp.get(str(i)) or {}).get("Content") or {}).get("WebId")
        (WEB_IDS if wid else _bad).append(wid or t)
    print(f"resolved {len(WEB_IDS)}/{len(TAGS)} WebIDs via {PI_SERVER}"
          + (f" — {len(_bad)} FAILED: {_bad[:5]}" if _bad else ""))
    if _bad:
        print("   Check PI_SERVER and that these are full PI point names, or paste WEB_IDS.")
assert WEB_IDS, "No WebIDs — check auth, PI_SERVER and the tag names."

# THE CONNECTOR OPTIONS. Every read below is spark.read / spark.readStream with
# .format("aveva_pi_timeseries") and these options; the per-test knobs
# (read_mode / webids_per_call / partition_concurrency) are set at each call site.
TS_OPTS = {"endpoint_url": ENDPOINT_URL,          # PI Web API base URL
           "web_ids": ",".join(WEB_IDS),          # which tags to read
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

# MAGIC %md ## Smoke test — is the connector working?
# MAGIC
# MAGIC One tiny read per mode through the real connector, before anything expensive. If this
# MAGIC cell is green, auth, networking, WebIDs, the wheel and the DataSource registration are
# MAGIC all fine, and any later failure is about load rather than setup.

# COMMAND ----------

_probe_ids = ",".join(WEB_IDS[:3])                       # 3 tags is enough
_from = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(timespec="seconds")
_smoke, _ok_all = [], True

for _m in ("value", "recorded", "interpolated"):
    try:
        # ---- THE CONNECTOR, batch mode ---------------------------------------------
        _rd = (spark.read.format("aveva_pi_timeseries")
               .options(**TS_OPTS)                       # endpoint / auth / verify_tls
               .option("web_ids", _probe_ids)            # override: just 3 tags
               .option("read_mode", _m)                  # value | recorded | interpolated
               .option("webids_per_call", "3"))
        if _m != "value":
            _rd = _rd.option("initial_watermark", _from)  # 10-minute window
        if _m == "interpolated":
            _rd = _rd.option("interval", INTERVAL)
        _df = _rd.load()
        # ----------------------------------------------------------------------------
        _n = _df.count()
        _cols = _df.columns
        _bad = [c for c in ("web_id", "timestamp", "value") if c not in _cols]
        _smoke.append({"mode": _m, "rows": _n, "columns": ",".join(_cols),
                       "status": "ok" if _n > 0 and not _bad else
                                 ("no rows" if _n == 0 else f"missing {_bad}")})
        _ok_all &= (_n > 0 and not _bad)
        print(f"  {_m:<13} {_n:>6} rows  columns={_cols}")
    except Exception as e:
        _smoke.append({"mode": _m, "rows": None, "columns": None, "status": str(e)[:120]})
        _ok_all = False
        print(f"  {_m:<13} FAILED: {str(e)[:140]}")

if _ok_all:
    print(f"\n✅ connector works — 3 tags read in all three modes. Safe to continue.")
    # Show a few rows so you can eyeball actual values, not just a count.
    display(spark.read.format("aveva_pi_timeseries").options(**TS_OPTS)
            .option("web_ids", _probe_ids).option("read_mode", "recorded")
            .option("initial_watermark", _from).load()
            .orderBy("web_id", "timestamp").limit(10))
else:
    print("\n❌ fix this before running §§ 1-2 — the sweeps will only fail more slowly.")
    print("   `value` failing     -> auth / networking / WebIDs")
    print("   only history modes  -> the time window, or maxCount truncation (2.1.0 raises)")
    print("   0 rows in `recorded`-> nothing archived in the last 10 min (try a wider window)")

# COMMAND ----------

# MAGIC %md ## Archive density
# MAGIC
# MAGIC Measures how many values PI has **archived** per tag per second — which is what a
# MAGIC `recorded` read returns, so it is the right number for sizing.
# MAGIC
# MAGIC It is **not** the sensor scan rate. The interface (exception reporting) and the archive
# MAGIC (compression) both discard readings that have not moved, so a tag scanned every second
# MAGIC can archive far less. A read returns what is stored, not a fresh poll — so this is
# MAGIC genuinely what is on disk, but it is a sample of the **last hour**: during a plant event
# MAGIC tags move, less is discarded, and the rate rises toward the scan rate. Size with headroom,
# MAGIC and ask the customer for their scan rate and compression settings too.

# COMMAND ----------

_PROBE_S = 3600            # look back this far; longer smooths over a quiet spell
_PROBE_TAGS = 5

_sess = requests.Session()
_sess.headers.update(_hdrs)          # Bearer for the mock App; empty for real PI
if not USE_MOCK_APP:
    _sess.auth = requests.auth.HTTPBasicAuth(_user, _pw)
_sess.verify = VERIFY_TLS

_end = datetime.now(timezone.utc)
_counts = []
for _wid in WEB_IDS[:_PROBE_TAGS]:
    _r = _sess.get(f"{BASE}/streams/{_wid}/recorded",
                   params={"startTime": (_end - timedelta(seconds=_PROBE_S)).isoformat(),
                           "endTime": _end.isoformat(), "maxCount": "150000"},
                   timeout=HTTP_TIMEOUT)
    _counts.append(len((_r.json() or {}).get("Items") or []) if _r.status_code == 200 else 0)

_per = [c / _PROBE_S for c in _counts]
RATE = round(max(_per), 4)                       # size for the BUSIEST tag, not the average
print(f"{_PROBE_TAGS} tags over {_PROBE_S/3600:.0f}h -> {_counts} values archived")
print(f"  per tag/sec: min {min(_per):.4f}  max {RATE}   (sizing uses the max)")
if len(set(_counts)) > 1:
    print("  they differ — normal on real PI: each tag has its own scan and compression")

print(f"\n-> {RATE} values/sec/tag = one every {round(1/RATE, 1) if RATE else '-'}s")
print(f"-> per 60s cycle: `recorded` ~ {int(len(WEB_IDS) * RATE * 60):,} rows, "
      f"`value` = {len(WEB_IDS)} rows (one snapshot per tag, whatever the rate)")
_span = int(CAP / RATE) if RATE else None
print(f"-> `recorded` windows split every ~{_span}s to stay under maxCount={CAP}/tag")

# COMMAND ----------

# MAGIC %md ## § 1 Breaking point — how fresh can the data be? ⭐
# MAGIC
# MAGIC Streams through the connector at progressively **shorter triggers**, over a growing
# MAGIC **number of tags**, until a cycle no longer fits inside its interval. The shortest
# MAGIC trigger that still leaves headroom is the freshness floor — the number to quote.
# MAGIC
# MAGIC Starts small (few tags, long trigger) and works up, **stopping each combination at the
# MAGIC first overrun**: shorter triggers only fail harder, and there is no point loading PI to
# MAGIC prove it. Reads the **worst** cycle, not the mean, because one overrun starts the runaway.

# COMMAND ----------

# ---- WHAT THIS SECTION SWEEPS ------------------------------------------------------
MODE      = "recorded"       # THE READ MODE USED HERE. "recorded" = every archived value
                             #   (lossless, volume scales with tag rate); "value" = one
                             #   snapshot per tag (fixed rows, drops everything between polls)
TAG_COUNTS = [10, 50, 190]   # how many tags to read — SMALL FIRST, then scale up
TRIGGERS   = [30, 15, 5]     # seconds, LONG -> SHORT
CHUNKS     = [100]           # webids_per_call (add 25 to compare; multiplies the runtime)
CYCLES     = 2               # micro-batches observed per combination
CONCURRENCY = 8              # partition_concurrency (only bites if a window splits)
# ------------------------------------------------------------------------------------

TAG_COUNTS = [n for n in TAG_COUNTS if n <= len(WEB_IDS)] or [len(WEB_IDS)]
_combos = len(TAG_COUNTS) * len(CHUNKS) * len(TRIGGERS)
_mins = len(TAG_COUNTS) * len(CHUNKS) * sum(TRIGGERS) * CYCLES / 60
print(f"mode={MODE}, tags={TAG_COUNTS}, triggers={TRIGGERS}s, chunks={CHUNKS}")
print(f"{_combos} combinations, ~{_mins:.0f}-{_mins*1.6:.0f} min "
      f"(stops early at the first overrun, so often less)")
print(f"at {RATE}/sec/tag, `{MODE}` returns ~"
      + ", ".join(f"{int(n * RATE * 60):,} rows/60s cycle for {n} tags"
                  if MODE != "value" else f"{n} rows/cycle for {n} tags"
                  for n in TAG_COUNTS))
print("   Trim TAG_COUNTS/TRIGGERS/CYCLES if that is too long.\n")


def stream_once(trigger_s: int, chunk: int, n_tags: int, cycles: int = CYCLES,
                mode: str = MODE):
    """Stream `n_tags` tags through the connector for `cycles` micro-batches.

    Fresh checkpoint per run — reusing one resumes from a committed offset and reads an
    empty window, which would look artificially fast.
    """
    # ---- THE CONNECTOR, streaming mode ---------------------------------------------
    opts = dict(TS_OPTS)
    opts["web_ids"] = ",".join(WEB_IDS[:n_tags])          # <- the tag-count variable
    reader = (spark.readStream.format("aveva_pi_timeseries")
              .options(**opts)                                     # endpoint / web_ids / auth
              .option("read_mode", mode)                           # value | recorded | interpolated
              .option("webids_per_call", str(chunk))               # tags per call AND per task
              .option("partition_concurrency", str(CONCURRENCY)))  # calls in flight per task
    if mode != "value":
        # Start one interval back so batch 0 is a NORMAL cycle, not a backfill — we are
        # measuring steady state here. § 2 measures backfill deliberately.
        reader = reader.option(
            "initial_watermark",
            (datetime.now(timezone.utc) - timedelta(seconds=trigger_s)).isoformat(timespec="seconds"))
        if mode == "interpolated":
            reader = reader.option("interval", INTERVAL)
    # --------------------------------------------------------------------------------

    q = (reader.load()
         .writeStream.format("noop")        # measure the READ, not a Delta write
         .option("checkpointLocation",
                 f"{CHECKPOINT_ROOT.rstrip(chr(47))}/bm_{mode}_{int(time.time()*1000)}")
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


for _n in TAG_COUNTS:                       # small -> large
    for _chunk in CHUNKS:
        for _t in TRIGGERS:                 # long -> short
            _lbl = f"  {_n:>4} tags  chunk={_chunk:<4} trigger={_t:>3}s"
            try:
                _s = stream_once(_t, _chunk, _n)
                if not _s:
                    print(f"{_lbl}  no batches observed")
                    continue
                _steady = _s[1:] or _s      # batch 0 can include cold start
                _dur = max(x["duration_s"] for x in _steady)
                _rows = max(x["rows"] for x in _steady)
                _pct = round(100 * _dur / _t, 1)
                BREAK.append({"tags": _n, "webids_per_call": _chunk, "trigger_s": _t,
                              "worst_cycle_s": _dur, "pct_of_trigger": _pct, "rows": _rows,
                              "fits": _pct < 100, "read_mode": MODE,
                              "calls_per_cycle": -(-_n // _chunk),
                              "cluster_parallelism": CLUSTER_CORES})
                print(f"{_lbl}  worst {_dur:>6.2f}s  {_pct:>6.1f}%  {_rows:>8,} rows  "
                      f"{'✅' if _pct < 50 else '⚠️ tight' if _pct < 100 else '🚨 OVERRUNS'}")
                if _pct >= 100:
                    print("        ⛔ stopping this tag count — shorter triggers only fail harder")
                    break
            except Exception as e:
                print(f"{_lbl}  FAILED: {str(e)[:110]}")

results.extend(BREAK)
if BREAK:
    display(spark.createDataFrame(BREAK))
    _safe = [b for b in BREAK if b["pct_of_trigger"] < 50]
    _fits = [b for b in BREAK if b["fits"]]
    # Report the floor per tag count: the answer depends on how many tags you read.
    print(f"\n{'tags':>6}  {'fastest safe trigger':>21}  {'used':>6}")
    for _n in TAG_COUNTS:
        _s2 = [b for b in _safe if b["tags"] == _n]
        if _s2:
            _r = min(_s2, key=lambda b: b["trigger_s"])
            print(f"{_n:>6}  {str(_r['trigger_s']) + 's':>21}  {_r['pct_of_trigger']:>5.1f}%")
        else:
            _f2 = [b for b in _fits if b["tags"] == _n]
            print(f"{_n:>6}  {('fits but >50% used' if _f2 else 'no viable trigger'):>21}"
                  f"  {(min(_f2, key=lambda b: b['trigger_s'])['pct_of_trigger'] if _f2 else 0):>5.1f}%")
    if _safe:
        _r = min(_safe, key=lambda b: (b["trigger_s"], -b["tags"]))
        print(f"\n→ FRESHNESS FLOOR: {_r['trigger_s']}s trigger for {_r['tags']} tags at "
              f"webids_per_call={_r['webids_per_call']} ({_r['pct_of_trigger']}% used)")
        print("  Quote this. Going below it works until PI has a slow minute.")
    if all(b["fits"] for b in BREAK):
        print(f"→ no overrun down to {min(TRIGGERS)}s at {max(TAG_COUNTS)} tags — the floor is")
        print("  lower than this sweep reached. Extend TRIGGERS or TAG_COUNTS to find it.")

# COMMAND ----------

# MAGIC %md ### § 1b Chart — cycle time vs trigger

# COMMAND ----------

if BREAK:
    import matplotlib.pyplot as plt
    _df = spark.createDataFrame(BREAK).toPandas()
    fig, ax = plt.subplots(figsize=(9, 5))
    for (_n, _c), _g in _df.groupby(["tags", "webids_per_call"]):
        _g = _g.sort_values("trigger_s")
        ax.plot(_g["trigger_s"], _g["worst_cycle_s"], "o-", label=f"{_n} tags, chunk={_c}")
    _lim = sorted(_df["trigger_s"].unique())
    ax.plot(_lim, _lim, "k--", lw=1, label="the deadline (cycle = trigger)")
    ax.fill_between(_lim, _lim, max(_lim) * 1.1, color="red", alpha=0.07)
    ax.fill_between(_lim, 0, [x / 2 for x in _lim], color="green", alpha=0.07)
    ax.set_xlabel("trigger interval (s)")
    ax.set_ylabel("worst cycle (s)")
    ax.set_title(f"Cycle time vs trigger ({MODE}) — under the dashed line fits; "
                 f"green = >50% headroom")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    display(fig)
    plt.close(fig)   # Databricks also auto-renders the active figure; without
                     # this the chart appears twice.

# COMMAND ----------

# MAGIC %md ## § 2 History load — how long does a backfill take?
# MAGIC
# MAGIC A **batch** read of N days through the connector. This is where `partition_concurrency`
# MAGIC earns its keep: a wide window splits into many sub-windows per task, and 2.1.0 reads
# MAGIC them concurrently where 2.0.4 walked them one at a time. `conc=1` in the sweep
# MAGIC reproduces the old behaviour for comparison.
# MAGIC
# MAGIC Uses `spark.read` rather than a stream because a backfill is a finite pile — which is
# MAGIC also the only way to measure *capacity* rather than the rate data happens to arrive at.

# COMMAND ----------

# HOURS, not days: at the measured rate this is ~{rows}/hour, and days would be a
# multi-hour sweep. Two points is enough to confirm the cost is LINEAR, which is what
# makes extrapolation to 30/180/365 days legitimate.
HIST_HOURS  = [1, 6]      # hours of history per run
HIST_CHUNKS = [100]       # webids_per_call (add 25 to compare, doubles the runtime)
HIST_CONC   = [8, 1]      # partition_concurrency — 1 reproduces pre-2.1.0 behaviour

_est = int(len(WEB_IDS) * RATE * 3600)
_runs = len(HIST_HOURS) * len(HIST_CHUNKS) * len(HIST_CONC)
print(f"~{_est:,} rows/hour of history x {sum(HIST_HOURS)}h x "
      f"{len(HIST_CHUNKS)*len(HIST_CONC)} settings = "
      f"~{_est*sum(HIST_HOURS)*len(HIST_CHUNKS)*len(HIST_CONC):,} rows over {_runs} runs")
print("   (trim HIST_HOURS/HIST_CHUNKS if that is more than you want to wait for)\n")

# Warm-up: the first Spark action pays Python-worker startup on every executor (~12s),
# which would otherwise be blamed on the connector.
try:
    (spark.read.format("aveva_pi_timeseries").options(**TS_OPTS)
     .option("read_mode", "recorded").option("webids_per_call", "100")
     .option("initial_watermark",
             (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(timespec="seconds"))
     .load().count())
    print("warm-up done\n")
except Exception as e:
    print(f"warm-up failed ({str(e)[:80]})\n")

for _hours in HIST_HOURS:
    for _chunk in HIST_CHUNKS:
        for _conc in HIST_CONC:
            _start = (datetime.now(timezone.utc) - timedelta(hours=_hours)).isoformat(timespec="seconds")
            _windows = max(1, -(-(_hours * 3600) // (int(CAP / RATE) if RATE else CAP)))
            try:
                _t0 = time.perf_counter()
                # ---- THE CONNECTOR, batch mode -----------------------------------
                _n = (spark.read.format("aveva_pi_timeseries")
                      .options(**TS_OPTS)
                      .option("read_mode", "recorded")              # raw archive
                      .option("webids_per_call", str(_chunk))       # tags per call / per task
                      .option("partition_concurrency", str(_conc))  # sub-windows in flight
                      .option("initial_watermark", _start)          # how far back to read
                      .load().count())
                # ------------------------------------------------------------------
                _secs = time.perf_counter() - _t0
                HIST.append({"hours": _hours, "webids_per_call": _chunk,
                             "partition_concurrency": _conc, "seconds": round(_secs, 1),
                             "rows": _n, "rows_per_sec": round(_n / _secs) if _secs else None,
                             "sub_windows_per_task": _windows,
                             "cluster_parallelism": CLUSTER_CORES, "error": ""})
                print(f"  {_hours}h chunk={_chunk:<4} conc={_conc:<2} {_secs:>7.1f}s  "
                      f"{_n:>9} rows  {round(_n/_secs):>8} rows/s  (~{_windows} windows/task)")
            except Exception as e:
                HIST.append({"hours": _hours, "webids_per_call": _chunk,
                             "partition_concurrency": _conc, "seconds": None, "rows": None,
                             "rows_per_sec": None, "sub_windows_per_task": _windows,
                             "cluster_parallelism": CLUSTER_CORES, "error": str(e)[:200]})
                print(f"  {_hours}h chunk={_chunk:<4} conc={_conc:<2} FAILED: {str(e)[:110]}")

results.extend(HIST)
_ok = [h for h in HIST if h["seconds"]]
if HIST:
    # Explicit schema: on a failed run seconds/rows are None, and an all-None column
    # cannot be type-inferred (CANNOT_DETERMINE_TYPE).
    display(spark.createDataFrame(
        [(int(h["hours"]), int(h["webids_per_call"]), int(h["partition_concurrency"]),
          h["seconds"], h["rows"], h["rows_per_sec"], h["sub_windows_per_task"],
          int(h["cluster_parallelism"]), h["error"]) for h in HIST],
        schema=StructType([
            StructField("hours", IntegerType()),
            StructField("webids_per_call", IntegerType()),
            StructField("partition_concurrency", IntegerType()),
            StructField("seconds", DoubleType()),
            StructField("rows", LongType()),
            StructField("rows_per_sec", LongType()),
            StructField("sub_windows_per_task", IntegerType()),
            StructField("cluster_parallelism", IntegerType()),
            StructField("error", StringType()),
        ])))
if _ok:
    _b = min(_ok, key=lambda h: h["seconds"] / h["hours"])
    print(f"\n→ FASTEST: {_b['seconds']}s for {_b['hours']}h at chunk={_b['webids_per_call']}, "
          f"conc={_b['partition_concurrency']} ({_b['rows_per_sec']:,} rows/s)")
    _ph = _b["seconds"] / _b["hours"]
    _pd = _ph * 24
    print(f"→ EXTRAPOLATED at {_ph:.1f}s per hour of history ({_pd/60:.1f} min/day):")
    print(f"     7d ~ {_pd*7/60:>6.1f} min      30d ~ {_pd*30/3600:>5.1f} h")
    print(f"   180d ~ {_pd*180/3600:>6.1f} h      365d ~ {_pd*365/3600:>5.1f} h")
    print("  Linear because a wider window is more sub-windows of the SAME size — no cliff.")
    print("  Check § 2b: if the line bends, do not trust these figures.")
    # Did concurrency actually help? Compare conc=8 against conc=1 at matched settings.
    for _d in sorted({h["hours"] for h in _ok}):
        for _c in sorted({h["webids_per_call"] for h in _ok}):
            _p = {h["partition_concurrency"]: h for h in _ok
                  if h["hours"] == _d and h["webids_per_call"] == _c}
            if 8 in _p and 1 in _p and _p[8]["seconds"]:
                _note = ("" if (_p[8]["sub_windows_per_task"] or 1) > 1
                         else "   <- 1 window/task, so there is no fan-out to gain from")
                print(f"  {_d}h chunk={_c}: conc 1 -> {_p[1]['seconds']}s, "
                      f"conc 8 -> {_p[8]['seconds']}s "
                      f"({_p[1]['seconds']/_p[8]['seconds']:.2f}x){_note}")

# COMMAND ----------

# MAGIC %md ### § 2b Chart — backfill time and the effect of concurrency

# COMMAND ----------

if _ok:
    import matplotlib.pyplot as plt
    _hdf = spark.createDataFrame(_ok).toPandas().sort_values(
        ["webids_per_call", "partition_concurrency", "hours"])
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 5))
    for (_c, _cc), _g in _hdf.groupby(["webids_per_call", "partition_concurrency"]):
        a1.plot(_g["hours"], _g["seconds"], "o-", label=f"chunk={_c}, conc={_cc}")
    a1.set_xlabel("hours of history")
    a1.set_ylabel("seconds")
    a1.set_title("Backfill time (linear => safe to extrapolate)")
    a1.legend(fontsize=8)
    a1.grid(alpha=0.3)

    _lbls = [f"{r.hours}h c{r.webids_per_call} conc{r.partition_concurrency}"
             for r in _hdf.itertuples()]
    a2.barh(range(len(_hdf)), _hdf["rows_per_sec"],
            color=["#1f77b4" if c == 8 else "#aec7e8"
                   for c in _hdf["partition_concurrency"]])
    a2.set_yticks(range(len(_hdf)))
    a2.set_yticklabels(_lbls, fontsize=8)
    a2.set_xlabel("rows/sec")
    a2.set_title("Throughput (dark = conc 8, light = conc 1)")
    a2.grid(alpha=0.3, axis="x")
    plt.tight_layout()
    display(fig)
    plt.close(fig)   # Databricks also auto-renders the active figure; without
                     # this the chart appears twice.

# COMMAND ----------

# MAGIC %md ## Run record

# COMMAND ----------

print("=" * 74)
print("  AVEVA PI CONNECTOR BENCHMARK — RUN RECORD")
print("=" * 74)
print(f"  When (UTC)    : {_now_iso()}")
print(f"  Endpoint      : {BASE}")
print(f"  Connector     : aveva_pi_timeseries {_ts.__version__}")
print(f"  Tags          : {len(WEB_IDS)} of {len(TAGS)} resolved")
print(f"  Tag rate      : {RATE} values/sec/tag")
print(f"  Cluster slots : {CLUSTER_CORES} ({_cores_src})")
print(f"  Read mode     : {READ_MODE}")

if BREAK:
    _safe = [b for b in BREAK if b["pct_of_trigger"] < 50]
    if _safe:
        _r = min(_safe, key=lambda b: b["trigger_s"])
        print(f"\n  Freshness floor : trigger={_r['trigger_s']}s at "
              f"chunk={_r['webids_per_call']} ({_r['pct_of_trigger']}% of the interval used)")
if _ok:
    _b = min(_ok, key=lambda h: h["seconds"] / h["hours"])
    _pd = _b["seconds"] / _b["hours"] * 24
    print(f"  Backfill rate   : {_pd/60:.1f} min per day of history "
          f"(chunk={_b['webids_per_call']}, conc={_b['partition_concurrency']}) "
          f"=> 180d ~ {_pd*180/3600:.1f} h")

print("\n  Caveats")
print("  • Correlate started_utc/ended_utc against PI-side CPU before attributing any")
print("    slowdown to the connector.")
print(f"  • {CLUSTER_CORES} task slots. In-flight calls are capped by slots, so the same")
print("    settings on a different cluster size are NOT comparable.")
print("  • `value` mode returns one row per tag by design — it has no throughput ceiling to")
print("    find, and it discards everything between polls.")
print("=" * 74)

if RESULT_TABLE and results:
    spark.createDataFrame(results).write.mode("append").option("mergeSchema", "true") \
         .saveAsTable(RESULT_TABLE)
    print(f"\nSaved → {RESULT_TABLE}")
