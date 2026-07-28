# Databricks notebook source
# MAGIC %md
# MAGIC # Test the AVEVA PI **thin client library** (`aveva_pi_assetframework`)
# MAGIC
# MAGIC The library is a **thin PI Web API client** — one function per real API
# MAGIC call, returning the API's JSON verbatim. It has no orchestration and no
# MAGIC tag/asset model; you compose those. This notebook exercises each primitive
# MAGIC and shows the small compositions a caller writes (batch resolve, AF walk).
# MAGIC (The Spark *connector* that reads values is tested in `test_timeseries.py`.)
# MAGIC
# MAGIC Run top-to-bottom (DBR 15.x+ / Spark 4.0+). Each cell self-checks (✅/❌).
# MAGIC
# MAGIC | # | Primitive / composition | API call |
# MAGIC |---|-------------------------|----------|
# MAGIC | 1 | `get_point` | `GET /points?path=` |
# MAGIC | 2 | `batch` (compose many `/points`) | `POST /batch` |
# MAGIC | 3 | AF walk: `get_asset_database` → `get_database_elements` → `get_element_attributes` | AF endpoints |
# MAGIC | 4 | bad tag → 404 in batch (caller skips) | `POST /batch` |

# COMMAND ----------

# MAGIC %pip install /Volumes/<catalog>/<schema>/<volume>/aveva_pi_assetframework-3.0.1-py3-none-any.whl
# MAGIC # ^ EDIT the Volume path to where you published it (this notebook needs ONLY the library)
dbutils.library.restartPython()

# COMMAND ----------

ENDPOINT_URL = "https://<host>/piwebapi"   # EDIT — PI Web API base
PI_SERVER    = "PISRV"                       # EDIT — PI Data Archive server name
BASE         = ENDPOINT_URL.rstrip("/")

SCOPE          = "pi"            # EDIT — Databricks secret scope
BASIC_USER_KEY = "pi_user"      # EDIT — secret keys for HTTP Basic against PI
BASIC_PW_KEY   = "pi_password"

TAGS = [
    # EDIT — a few real tag names on PI_SERVER, e.g.:
    # "Plant.Area.Unit1.Temp", "Plant.Area.Unit1.Pressure",
]
AF_DATABASE = "MyAFDatabase"   # EDIT — an Asset Framework database name

# COMMAND ----------

import requests
from urllib.parse import quote
from aveva_pi_assetframework import (
    get_point, batch, get_asset_database, get_database_elements,
    get_child_elements, get_element_attributes, __version__,
)
print("aveva_pi_assetframework", __version__)

# Auth — HTTP Basic against PI (secret-scope keys, never literals).
AUTH = dict(basic_user=dbutils.secrets.get(SCOPE, BASIC_USER_KEY),
            basic_password=dbutils.secrets.get(SCOPE, BASIC_PW_KEY))

# One pooled Basic session reused across the primitive calls (same creds as AUTH).
SESS = requests.Session()
SESS.auth = requests.auth.HTTPBasicAuth(AUTH["basic_user"], AUTH["basic_password"])

_results = []
def check(name, ok, detail=""):
    _results.append((name, bool(ok), detail))
    print(f"{'✅ PASS' if ok else '❌ FAIL'}  {name}" + (f"  — {detail}" if detail else ""))

assert TAGS, "Set TAGS (a few real tag names on PI_SERVER) before running."

# COMMAND ----------

# MAGIC %md ## 1. `get_point` — one tag → its point object (`GET /points?path=`)

# COMMAND ----------

try:
    pt = get_point(BASE, PI_SERVER, TAGS[0], **AUTH)
    check("1. get_point", bool(pt.get("WebId")),
          f"WebId={pt.get('WebId')}, unit={pt.get('EngineeringUnits')}")
except Exception as e:
    check("1. get_point", False, str(e)[:400])

# COMMAND ----------

# MAGIC %md ## 2. `batch` — resolve many tags in one call (compose sub-requests yourself)

# COMMAND ----------

WEB_IDS = []
try:
    reqs = {str(i): {"Method": "GET",
                     "Resource": f"{BASE}/points?path=" + quote(rf"\\{PI_SERVER}\{t}", safe="")}
            for i, t in enumerate(TAGS)}
    resp = batch(BASE, reqs, sess=SESS)                       # {id: {Status, Content}}
    WEB_IDS = [resp[str(i)]["Content"]["WebId"] for i in range(len(TAGS))]
    check("2. batch resolve", len(WEB_IDS) == len(TAGS) and all(WEB_IDS),
          f"{len(WEB_IDS)} WebIDs in one POST /batch")
    print("WebIDs to feed the connector:", WEB_IDS)
except Exception as e:
    check("2. batch resolve", False, str(e)[:400])

# COMMAND ----------

# MAGIC %md ## 3. AF walk — compose the tree traversal + YOUR tag/asset model

# COMMAND ----------

try:
    db = get_asset_database(BASE, PI_SERVER, AF_DATABASE, sess=SESS)
    def _walk(el):
        for a in get_element_attributes(BASE, el["WebId"], sess=SESS)["Items"]:
            yield (el.get("Name"), a.get("Name"), a.get("WebId"))   # (asset_id, tag, web_id) — YOUR choice
        for child in get_child_elements(BASE, el["WebId"], sess=SESS)["Items"]:
            yield from _walk(child)
    recs = [r for el in get_database_elements(BASE, db["WebId"], sess=SESS)["Items"]
            for r in _walk(el)]
    check("3. AF walk (client-composed)", len(recs) > 0 and all(w for (_a, _t, w) in recs),
          f"{len(recs)} attributes; sample={recs[0] if recs else None}")
except Exception as e:
    check("3. AF walk (client-composed)", False, str(e)[:400])

# COMMAND ----------

# MAGIC %md ## 4. Bad tag → 404 in the batch sub-response (caller decides skip/raise)

# COMMAND ----------

BOGUS = "This.Tag.Does.Not.Exist.12345"
try:
    mixed = TAGS + [BOGUS]
    reqs = {str(i): {"Method": "GET",
                     "Resource": f"{BASE}/points?path=" + quote(rf"\\{PI_SERVER}\{t}", safe="")}
            for i, t in enumerate(mixed)}
    resp = batch(BASE, reqs, sess=SESS)
    statuses = [resp[str(i)].get("Status") for i in range(len(mixed))]
    good = [s for s in statuses if s and s < 400]
    check("4. bad tag flagged (caller skips)",
          len(good) == len(TAGS) and statuses[-1] == 404,
          f"{len(good)} good, bogus Status={statuses[-1]}")
except Exception as e:
    check("4. bad tag flagged (caller skips)", False, str(e)[:400])

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

passed = sum(1 for _, ok, _ in _results if ok)
print(f"\n{'='*56}\n  {passed}/{len(_results)} checks passed\n{'='*56}")
for name, ok, detail in _results:
    print(f"  {'✅' if ok else '❌'}  {name:34s} {detail}")
print("\nNext: feed WEB_IDS to the connector — see test_timeseries.py")
