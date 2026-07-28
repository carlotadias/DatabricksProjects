# AVEVA PI → Databricks

Two **separate, independently-installed** pieces — a Spark *connector* and a
lookup *library*. They are different kinds of thing and ship as different wheels:

| Piece | Kind | Wheel / import | Does |
|-------|------|----------------|------|
| **`aveva-pi-timeseries`** | a **Spark connector** (a DataSource you register + `spark.read`) | `aveva_pi_timeseries` | Reads point values for a set of **WebIDs** → **`(web_id, timestamp, value)`**. |
| **`aveva-pi-assetframework`** | a **thin PI Web API client** (plain functions) | `aveva_pi_assetframework` | One function per real API call — `get_point`, `batch`, `get_asset_database`, `get_database_elements`, `get_child_elements`, `get_element_attributes`. Returns the API's JSON verbatim; **you** compose lookups/walks and choose your tag/asset model. |

They are **decoupled on purpose**: the connector has *no* dependency on the
library. The library gives you thin PI Web API primitives to produce the WebIDs
the connector consumes. Install whichever you need — or both.

```bash
pip install aveva_pi_assetframework-3.0.1-py3-none-any.whl   # the thin API client
pip install aveva_pi_timeseries-2.0.3-py3-none-any.whl        # the Spark connector
```

The usual flow is **resolve IDs (client), then read (connector)**:

```python
base = "https://<host>/piwebapi"

# Auth — HTTP Basic against PI. Keep creds in a secret scope, never a literal.
# The SAME keys work for both the library calls and the connector options (**AUTH).
AUTH = dict(basic_user=dbutils.secrets.get("pi", "pi_user"),
            basic_password=dbutils.secrets.get("pi", "pi_password"))

# Step 1 — CLIENT: names -> WebIDs. The library gives primitives; you compose the
# lookup. One POST /batch resolves many tags (each sub-request is a GET /points).
from urllib.parse import quote
from aveva_pi_assetframework import batch
reqs = {str(i): {"Method": "GET",
                 "Resource": f"{base}/points?path=" + quote(rf"\\PISRV\{t}", safe="")}
        for i, t in enumerate(tag_names)}
resp = batch(base, reqs, **AUTH)                     # {id: {Status, Content}}
web_ids = [resp[str(i)]["Content"]["WebId"] for i in range(len(tag_names))]
# (or one at a time: get_point(base, "PISRV", tag, **AUTH)["WebId"])

# Step 2 — CONNECTOR: read values for those WebIDs (a Spark DataSource)
from aveva_pi_timeseries import PITimeSeriesSource
spark.dataSource.register(PITimeSeriesSource)
df = spark.read.format("aveva_pi_timeseries").options(
    endpoint_url=base, **AUTH,
    web_ids=",".join(web_ids),
    read_mode="interpolated", interval="1m",
    initial_watermark="2026-01-01T00:00:00Z",
).load()   # -> (web_id, timestamp, value)
```

> **Think of it like a phone book + a phone.** The client looks up numbers
> (WebIDs); the connector dials them and streams the call. The client only exposes
> the raw lookups the PI API supports — how you use them is up to you.

The connector is built to pull **thousands of points efficiently** — see
[Fan-out architecture](#fan-out-architecture) — and to stay inside AVEVA's
documented server limits.

---

## Table of contents
1. [What's in this folder](#whats-in-this-folder)
2. [Concepts in 60 seconds](#concepts-in-60-seconds)
3. [The two pieces & the workflow](#the-two-pieces--the-workflow)
4. [Fan-out architecture](#fan-out-architecture)
5. [AVEVA PI Web API limits](#aveva-pi-web-api-limits)
6. [Configuration parameters](#configuration-parameters)
7. [Library API reference](#library-api-reference-aveva_pi_assetframework) — thin PI Web API client
8. [Authentication](#authentication) — HTTP Basic (+ proxy / Kerberos / OIDC context)
9. [Setup & deploy](#setup--deploy)
10. [Usage examples](#usage-examples)
11. [Testing & benchmarking](#testing--benchmarking)
12. [Troubleshooting](#troubleshooting)

---

## What's in this folder

This repo holds **two independent packages** side by side (each builds its own
wheel), plus shared docs/notebooks:

```
aveva-pi-connector/
├── README.md · HOW_TO_USE.md · CHANGELOG.md   ← shared docs
│
├── auth/                             ← Basic-auth feasibility probe + runbook (self-contained)
│   ├── AUTH_RUNBOOK.md               ←   how to run the probe + read the result
│   └── basic_auth_probe.py           ←   does Basic work from Databricks over the PI FQDN?
│
├── timeseries/                       ← PACKAGE 1: the Spark connector
│   ├── pyproject.toml                ←   → wheel: aveva_pi_timeseries-2.0.3
│   ├── src/aveva_pi_timeseries/
│   │   ├── __init__.py               ←   public API: PITimeSeriesSource
│   │   ├── reader.py                 ←   the DataSource: web_ids → (web_id, timestamp, value)
│   │   └── _http.py                  ←   vendored HTTP (private copy)
│   └── tests/test_timeseries.py
│
├── assetframework/                   ← PACKAGE 2: the thin PI Web API client
│   ├── pyproject.toml                ←   → wheel: aveva_pi_assetframework-3.0.1
│   ├── src/aveva_pi_assetframework/
│   │   ├── __init__.py               ←   public API: get_point, batch, get_*_elements, …
│   │   ├── client.py                 ←   one thin function per real PI Web API call
│   │   └── _http.py                  ←   vendored HTTP (private copy — identical to the connector's)
│   └── tests/test_assetframework.py
│
├── scripts/deploy.sh                 ← build BOTH wheels → publish to the UC Volume
├── databricks.yml                    ← Asset Bundle: provisions the UC Volume
└── notebooks/                        ← test notebooks + a fan-out benchmark
    ├── test_assetframework.py        ←   client scenarios (get_point, batch, AF walk)
    ├── test_timeseries.py            ←   connector scenarios (all read modes, streaming)
    └── benchmark_fanout.py           ←   throughput & tuning
```

> **Why two packages, not one?** So the split is real at install time: the
> connector is a Spark DataSource, the library is plain functions. Each wheel is
> self-contained (they vendor an identical private `_http.py`) — **no
> cross-dependency**, so you can install just the connector, just the library, or
> both. The trade-off: the shared `_http.py` is duplicated and must be kept in
> sync between the two.

---

## Concepts in 60 seconds

- **Point** = one sensor / signal (e.g. `Plant.Area.Unit1.Temp`). The *name* is a
  label — an admin can **rename** it, so the name is not its true identity.
- **Reading / value** = one measurement from a point at one timestamp
  (`540.2 @ 09:00:01`). One point produces many readings over time.
- **WebId** = PI's stable internal ID for the point. With **WebID 2.0** (the
  default) it's derived from the point's persistent GUID, so it **survives a
  rename**. The time-series connector takes WebIDs directly.
- **Two axes of scale:** *how many points* (wide) and *how many readings per point
  over the window* (deep). The connector fans out over both — see
  [Fan-out architecture](#fan-out-architecture).

---

## The two pieces & the workflow

The two pieces divide the work into **name/asset → WebID lookups** (the thin
client) and **reading values** (the connector). The normal workflow is two steps:

**Step 1 — CLIENT: names/assets → WebIDs** (`aveva_pi_assetframework` primitives; you compose)

```python
from urllib.parse import quote
from aveva_pi_assetframework import (
    batch, get_point, get_asset_database, get_database_elements,
    get_child_elements, get_element_attributes,
)

# AUTH is the credential dict from the quickstart:
#   AUTH = dict(basic_user=..., basic_password=...)

# by tag name — resolve many in one POST /batch (sub-requests are GET /points):
reqs = {str(i): {"Method": "GET",
                 "Resource": f"{base}/points?path=" + quote(rf"\\PISRV\{t}", safe="")}
        for i, t in enumerate(["Plant.Area.Unit1.Temp", "Plant.Area.Unit1.Pressure"])}
resp = batch(base, reqs, **AUTH)
web_ids = [resp[str(i)]["Content"]["WebId"] for i in range(2)]

# or walk an AF database yourself, choosing your own asset/tag model:
db = get_asset_database(base, "PISRV", "MyAFDatabase", **AUTH)
for el in get_database_elements(base, db["WebId"], **AUTH)["Items"]:
    for attr in get_element_attributes(base, el["WebId"], **AUTH)["Items"]:
        web_id, asset_id, tag = attr["WebId"], el["Name"], attr["Name"]   # YOUR choice
        # ...recurse with get_child_elements(base, el["WebId"], ...) if nested
```

**Step 2 — CONNECTOR: read values for those WebIDs** (`aveva_pi_timeseries`, a Spark DataSource)

```python
from aveva_pi_timeseries import PITimeSeriesSource
spark.dataSource.register(PITimeSeriesSource)

df = spark.read.format("aveva_pi_timeseries").options(
    endpoint_url=base, **AUTH, web_ids=",".join(web_ids),
).load()   # -> (web_id, timestamp, value)
```

> `basic_user`/`basic_password` are the same keys on both surfaces — connector
> `.options()` and library kwargs — which is why one `**AUTH` dict drops into either.

Join `web_id` back to your tag/asset dimension downstream. Keeping the value
connector lean (three columns, no AF coupling) is what makes it simple to reason
about and fast to run — and is why the two pieces ship as separate wheels.

### `read_mode` (on `aveva_pi_timeseries`, default `value`)

| `read_mode` | What you get | When to use |
|-------------|--------------|-------------|
| **`value`** | current snapshot — one row per WebID | "What is it right now?" **Default** — cheapest read. |
| **`interpolated`** | one value per fixed `interval` over the window | Even, gap-free series for dashboards/ML. |
| **`recorded`** | every raw archived point in the window | Faithful history / audit; can be large. |
| **`recordedattime`** | value as-of a specific `as_of` timestamp | "What was it at 12:00 last Tuesday?" **Batch only.** |

### Batch vs Streaming

- **`spark.read`** — runs once over `[watermark, now]`, stops. Backfills / ad-hoc.
- **`spark.readStream`** — micro-batch polling: each trigger reads a new
  half-open window, advances a watermark, checkpoints so restarts resume cleanly.

> **"Streaming" = micro-batch polling, not push.** Fresh to your trigger interval
> (seconds–minutes), not millisecond push. PI's true push (Channels/WebSockets)
> doesn't fit Spark's pull model and is not implemented here.

---

## Fan-out architecture

**Goal:** read many points fast without tripping AVEVA's limits. A **hybrid** that
stacks three levels of parallelism over a resilient HTTP client.

```
   Spark      ┌────────────┐ ┌────────────┐ ┌────────────┐   ← LEVEL 1: partitions
   tasks      │ WebID chunk│ │ WebID chunk│ │ WebID chunk│     (one per chunk, across cores)
              └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
                    ▼              ▼              ▼
          GET /streamsets/{mode}?webId=..&webId=..            ← LEVEL 2: StreamSet bulk
          (ONE call returns values for the whole chunk)          (N GETs → 1)
                    │  + time-window chunking so no single
                    ▼    call approaches MaxReturnedItemsPerCall
          ┌───────────────────────┐                           ← LEVEL 3: bounded threads
          │ ThreadPool             │                              (concurrent sub-calls)
          │ (partition_concurrency)│
          └───────────────────────┘
```

- **Level 1 — partitions:** WebIDs are grouped into chunks (`webids_per_call`);
  each chunk is one Spark task. **More points? Add executor cores.**
- **Level 2 — StreamSet bulk:** a chunk is read in one `/streamsets/{mode}` call,
  and each read is split into **time sub-windows** sized so no single call nears
  the 150k item ceiling (batch backfills of any window are safe).
- **Level 3 — bounded threads:** large chunks fan their sub-calls out concurrently.
- **Underneath:** pooled keep-alive sessions; retry + exponential backoff on 429
  and 5xx **and** transient network errors; `WebException`-on-200 is caught and
  raised so a truncated response is retried, never silently committed.

WebID resolution (step 1) uses the **Batch controller** (`POST /batch`) to collapse
N `GET /points` into a few POSTs.

---

## AVEVA PI Web API limits

Verified against the official PI Web API 2023 SP2 reference; encoded in
`aveva_pi/http.py`.

| Config item | Default | On breach | How we respect it |
|-------------|---------|-----------|-------------------|
| `MaxReturnedItemsPerCall` | **150,000** | HTTP **400** (else auto-pages) | per-stream `maxCount = min(max_count, 150k // n_streams)`; time-window chunking. |
| `RateLimitMaxRequests`/`Duration` | **1,000 req/s per IP** | HTTP **429** | bulk + batch cut call counts; retry honours `Retry-After`. Per-IP ⇒ each executor is a client. |
| `RateLimitSearchMaxRequests` | **50/s** (20× tighter) | HTTP **429** | only relevant if you swap path resolution for the *search* controller — noted so you don't. |
| `MaxRequestContentLength` | **4 MB** | HTTP **413** | `batch_size` keeps each `POST /batch` small. |

---

## Configuration parameters

### `aveva_pi_timeseries` (the value connector)

**Required:** `endpoint_url`, `basic_user` + `basic_password`, `web_ids`.

| Option | Default | Description |
|--------|---------|-------------|
| `endpoint_url` | *(required)* | PI Web API base, `https://<host>/piwebapi` |
| `basic_user` / `basic_password` | *(required)* | HTTP Basic auth against PI. Sent pre-emptively, so it works even if PI advertises only `Negotiate`. **Use a secret store** + a dedicated read-only account. |
| `web_ids` | *(required)* | Comma-separated PI WebIDs (resolve names via the `aveva_pi_assetframework` client — `get_point` / `batch`). |
| `read_mode` | `value` | `value` \| `interpolated` \| `recorded` \| `recordedattime` |
| `interval` | `1m` | Sampling interval for `interpolated` |
| `as_of` | — | Timestamp — required for `recordedattime` (batch only) |
| `max_count` | `10000` | Per-stream row cap (clamped under the 150k ceiling) |
| `webids_per_call` | `50` | WebIDs per StreamSet call / Spark partition |
| `partition_concurrency` | `8` | In-partition thread-pool size |
| `bulk_read` | `true` | `true` = StreamSet bulk; `false` = one call per WebID |
| `lookback_seconds` | `3600` | Window when no `initial_watermark` |
| `initial_watermark` | — | ISO start; **naive = UTC**, e.g. `2026-01-01T00:00:00` |
| `max_advance_seconds` | `300` | Max seconds a streaming micro-batch advances |
| `http_timeout_seconds` | `60` | Per-request timeout |

---

## Library API reference (`aveva_pi_assetframework`)

A **thin PI Web API client**: each function is a single HTTP call that returns the
API's JSON **verbatim**. No orchestration, no paging loops, no tree-walking, no
tag/asset model — you compose those. All take `basic_user`+`basic_password`
(HTTP Basic) and an optional `sess` (reuse one pooled session); `base` is the PI
Web API root.

| Function | PI Web API call | Returns |
|----------|-----------------|---------|
| `get_point(base, server, tag)` | `GET /points?path=\\{server}\{tag}` | the point object (`WebId`, `Path`, `EngineeringUnits`, …) |
| `batch(base, requests)` | `POST /batch` | PI's id-keyed response (`{id: {Status, Headers, Content}}`); `requests` is the id-keyed batch body you build |
| `get_asset_database(base, server, database)` | `GET /assetdatabases?path=\\{server}\{database}` | the AF database object |
| `get_database_elements(base, database_web_id, *, start_index=0, max_count=1000)` | `GET /assetdatabases/{id}/elements` | one page of top-level elements (`{"Items": […]}`) |
| `get_child_elements(base, element_web_id, *, start_index=0, max_count=1000)` | `GET /elements/{id}/elements` | one page of child elements |
| `get_element_attributes(base, element_web_id, *, start_index=0, max_count=1000)` | `GET /elements/{id}/attributes` | one page of attributes |

**You compose the workflows.** Resolve many tags → one `batch()` of `/points`
sub-requests (see [the workflow](#the-two-pieces--the-workflow)). Walk AF → loop
`get_database_elements` / `get_child_elements` / `get_element_attributes`, page
with `start_index`/`max_count`, and decide your own `asset_id`/`tag` mapping. A bad
tag comes back as a `Status >= 400` in its batch sub-response (or an HTTP error
from `get_point`) — you decide whether to skip or fail.

> **Note:** we don't wrap the Search controller (`GET /search/query`); it exists
> but has a 20× tighter rate limit (50/s). Resolve by exact path via `get_point`.

---

## Authentication

The connector authenticates every request to PI with **HTTP Basic**
(`basic_user` + `basic_password`) — sent pre-emptively, so it works even where PI
advertises only `Negotiate`. Use a secret store; never a literal, and a dedicated
read-only AD account. **Confirm Basic actually works from Databricks over the FQDN**
(`auth/basic_auth_probe.py`) — it can be enabled on-box yet blocked for remote
callers.

That's the only credential the connector sends. What PI needs *behind* that
depends on the environment — the table below is context for the estate, not extra
connector options:

| PI environment | What it means for you |
|----------------|-----------------------|
| **Basic accepted** *(the supported path)* | Set `basic_user`/`basic_password`. Simplest by far — a pass-through to a Windows/AD identity. |
| **In-domain proxy** *(if PI is Kerberos-only)* | A domain-joined box does Kerberos → PI and exposes a Basic-speaking endpoint; point `endpoint_url` at the proxy. Isolates the hard auth off the cluster. **Not part of this repo** — it's customer infrastructure. |
| **Direct Kerberos** *(not supported here)* | Kerberos-from-Spark needs keytab + `krb5.conf` + `requests-gssapi` + KDC reachability + ticket renewal on every executor. Fragile, and still authenticates as one shared identity — the proxy above gets the same outcome with far fewer moving parts. Prefer the proxy. |
| **Bearer / OIDC** *(long-term, once PI is 2023+)* | The eventual best answer, but out of scope for this Basic-only connector. Revisit when PI Web API supports OIDC. |

> **Note:** the connector *code* also carries a `bearer_token` option, used solely
> to pass the demo's OAuth-gated mock App — it is **not** a PI auth path and is
> intentionally undocumented as such. Against a real PI server, use Basic.

**Before relying on Basic, prove it works from Databricks** — changing nothing on
the PI/AD side:
- [`auth/basic_auth_probe.py`](auth/basic_auth_probe.py) — does Basic reach PI over the FQDN, and do the creds work? (any cluster)
- [`auth/AUTH_RUNBOOK.md`](auth/AUTH_RUNBOOK.md) — how to run it and read the result (incl. the on-box ≠ remote trap)

---

## Setup & deploy

### Prerequisites
- Unity Catalog workspace; cluster on **DBR 15.x+ / Spark 4.0+**; network to PI.
- Databricks CLI ≥ 0.210; Python ≥ 3.9 (`pip install build pytest`).
- PI Basic credentials (a read-only AD account) in a Databricks **secret scope** — e.g. `pi_user` / `pi_password`.

### Build, test, publish
Each package builds + tests on its own:
```bash
(cd timeseries     && python -m build --wheel --outdir dist && pytest)  # connector
(cd assetframework && python -m build --wheel --outdir dist && pytest)  # library
./scripts/deploy.sh dev   # builds BOTH wheels + publishes them to the UC Volume
```

### Install on a cluster
Install whichever you need (usually both):
```bash
pip install /Volumes/<catalog>/<schema>/<volume>/aveva_pi_assetframework-3.0.1-py3-none-any.whl
pip install /Volumes/<catalog>/<schema>/<volume>/aveva_pi_timeseries-2.0.3-py3-none-any.whl
```

---

## Usage examples

### Resolve names, then stream values
```python
from urllib.parse import quote
from aveva_pi_assetframework import batch
from aveva_pi_timeseries import PITimeSeriesSource
spark.dataSource.register(PITimeSeriesSource)

base = "https://<host>/piwebapi"
# one credential dict, reused for library calls (**AUTH) and connector options (**AUTH)
AUTH = dict(basic_user=dbutils.secrets.get("pi", "pi_user"),
            basic_password=dbutils.secrets.get("pi", "pi_password"))

# resolve names -> WebIDs with one POST /batch (compose the sub-requests)
reqs = {str(i): {"Method": "GET",
                 "Resource": f"{base}/points?path=" + quote(rf"\\PISRV\{t}", safe="")}
        for i, t in enumerate(my_tag_names)}
resp = batch(base, reqs, **AUTH)
web_ids = [resp[str(i)]["Content"]["WebId"] for i in range(len(my_tag_names))]

ts = spark.readStream.format("aveva_pi_timeseries").options(
    endpoint_url=base, **AUTH, web_ids=",".join(web_ids),
    read_mode="interpolated", interval="1m",
    initial_watermark="2026-01-01T00:00:00Z",
).load()

(ts.writeStream
   .option("checkpointLocation", "/Volumes/main/ops/checkpoints/pi_ts")
   .toTable("main.raw.pi_timeseries"))   # (web_id, timestamp, value)
```

### Batch backfill of raw history
```python
df = spark.read.format("aveva_pi_timeseries").options(
    endpoint_url=base, **AUTH, web_ids=",".join(web_ids),
    read_mode="recorded", initial_watermark="2026-01-01T00:00:00Z",
).load()   # time-window chunked automatically under the 150k ceiling
```

### AF-driven: walk the tree, keep asset_id alongside
```python
from aveva_pi_assetframework import (
    get_asset_database, get_database_elements, get_child_elements, get_element_attributes,
)
db = get_asset_database(base, "PISRV", "MyAFDatabase", **AUTH)
asset_of = {}   # web_id -> asset_id  (YOUR mapping choice: asset_id = element name)
def walk(el):
    for a in get_element_attributes(base, el["WebId"], **AUTH)["Items"]:
        asset_of[a["WebId"]] = el["Name"]
    for child in get_child_elements(base, el["WebId"], **AUTH)["Items"]:
        walk(child)
for el in get_database_elements(base, db["WebId"], **AUTH)["Items"]:
    walk(el)

df = (spark.read.format("aveva_pi_timeseries")
      .options(endpoint_url=base, **AUTH,
               web_ids=",".join(asset_of), read_mode="value").load())
# join web_id -> asset_of downstream to attach asset context
```

---

## Testing & benchmarking

- **Unit tests** — per package, fast/offline: `(cd timeseries && pytest)` and
  `(cd assetframework && pytest)`.
- **Scenario notebooks** — one per package: `notebooks/test_assetframework.py`
  (library) and `notebooks/test_timeseries.py` (connector), each end-to-end
  against a PI mock or live.
- **Benchmark** (`notebooks/benchmark_fanout.py`): timings + knob tuning.

See [HOW_TO_USE.md](HOW_TO_USE.md) for the full install + test walkthrough and a
plain-English explanation of every option.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| **HTTP 429** | per-IP rate limit (1,000/s) | keep `bulk_read=true`; lower `partition_concurrency`; raise `webids_per_call` |
| **HTTP 503** | server overloaded | lower `partition_concurrency` / fewer concurrent partitions |
| **HTTP 400** "greater than maximum" | `max_count × chunk` over 150k | lower `max_count` or `webids_per_call` |
| **HTTP 413** | `POST /batch` body > 4 MB | lower `batch_size` |
| **`RuntimeError: WebException`** | truncated 200 response | transient — the connector raises so it's retried; investigate if persistent |
| **`ValueError: N tag(s) could not be resolved`** | bad tag / wrong `pi_server` | fix names, or `on_missing_tag=skip` |
| Backwards/empty after restart | (fixed in 2.0) offset handling | ensure you're on ≥ 2.0 |

---

**Versions** · `aveva_pi_timeseries` 2.0.3 · `aveva_pi_assetframework` 3.0.1 · each requires only `requests` at runtime.
