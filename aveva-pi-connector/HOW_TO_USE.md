# How to use the AVEVA PI connector

A practical, step-by-step guide: **install it in Databricks**, **test it**, and a
**plain-English explanation of every option**. For architecture and the "why",
see [README.md](README.md).

> **New here?** Read [README §Concepts in 60 seconds](README.md#concepts-in-60-seconds)
> first — it defines *point*, *reading/value*, and *WebId*.

> **v2.0 changed the interface.** The value connector is now `aveva_pi_timeseries`,
> takes **`web_ids`** (not tag names), and returns **`(web_id, timestamp, value)`**.
> Resolve names → WebIDs first with the aveva_pi_assetframework library (Part 1, Step 3).

---

## Part 1 — Install & test in Databricks

### Step 0: prerequisites
- Unity Catalog workspace; cluster on **DBR 15.x+ / Spark 4.0+** with network to PI.
- PI **HTTP Basic** credentials (a read-only AD account) in a Databricks secret scope:
  ```bash
  databricks secrets create-scope pi
  databricks secrets put-secret pi pi_user       # e.g. UK\svc-databricks-pi
  databricks secrets put-secret pi pi_password
  ```
  > Confirm Basic actually works **from Databricks over the FQDN** before relying on
  > it — run `auth/basic_auth_probe.py` (see [auth/AUTH_RUNBOOK.md](auth/AUTH_RUNBOOK.md)).
  > An on-box `curl` success does not prove the remote path works.

### Step 1: build & publish the two wheels to a UC Volume
There are **two packages** — a Spark connector and a lookup library — each with its
own `pyproject.toml`. One command builds **and** publishes both:

1. Edit the **CONFIG block at the top of `scripts/deploy.sh`** — the *single* place
   to set `CATALOG` / `LIBS_SCHEMA` / `LIBS_VOLUME` (where the wheels go). They flow
   into `databricks.yml` automatically via `BUNDLE_VAR_*`, so you never edit the yml
   — the two can't drift. The **workspace** is auth, not config: pass a CLI profile.
2. Run it:
   ```bash
   pip install build
   PROFILE=my-profile ./scripts/deploy.sh dev   # or export DATABRICKS_HOST/TOKEN
   ```

`deploy.sh` builds both wheels, runs `bundle deploy` (provisions the Volume), and
copies them to `/Volumes/<CATALOG>/<LIBS_SCHEMA>/<LIBS_VOLUME>/`. The wheels **must**
land on the Volume before Step 2 — that's where `%pip` reads them from.

> Just need the files by hand? `(cd timeseries && python -m build --wheel --outdir dist)`
> and the same in `assetframework/` produce `aveva_pi_timeseries-2.0.3` /
> `aveva_pi_assetframework-3.0.1` in each `dist/`; then copy them to the Volume yourself.

### Step 2: install on the cluster
Install both (or just the one you need) **from the Volume you published to in Step 1**:
```python
%pip install /Volumes/<catalog>/<schema>/<volume>/aveva_pi_assetframework-3.0.1-py3-none-any.whl \
             /Volumes/<catalog>/<schema>/<volume>/aveva_pi_timeseries-2.0.3-py3-none-any.whl
dbutils.library.restartPython()
```

### Step 3: the two-step read
The **library** turns name/asset → WebID; the **connector** reads values for those
WebIDs. Two packages, one workflow.

```python
from urllib.parse import quote
from aveva_pi_assetframework import batch, get_point          # thin API client
from aveva_pi_timeseries import PITimeSeriesSource             # the connector
spark.dataSource.register(PITimeSeriesSource)   # format: aveva_pi_timeseries

base = "https://<host>/piwebapi"
# One HTTP Basic credential dict — the SAME keys work for library calls (**AUTH)
# and connector options (**AUTH).
AUTH = dict(basic_user=dbutils.secrets.get("pi", "pi_user"),
            basic_password=dbutils.secrets.get("pi", "pi_password"))

# Step 3a — resolve tag names to WebIDs (compose a batch of GET /points)
tags = ["Plant.Area.Unit1.Temp"]
reqs = {str(i): {"Method": "GET",
                 "Resource": f"{base}/points?path=" + quote(rf"\\PISRV\{t}", safe="")}
        for i, t in enumerate(tags)}
resp = batch(base, reqs, **AUTH)
web_ids = [resp[str(i)]["Content"]["WebId"] for i in range(len(tags))]

# Step 3b — read values (lean connector) — cheapest smoke test is read_mode=value
df = spark.read.format("aveva_pi_timeseries").options(
    endpoint_url=base, **AUTH,
    web_ids=",".join(web_ids), read_mode="value",
).load()
display(df)   # -> (web_id, timestamp, value)
```
One row per WebID back → you're connected. 🎉

### Testing the connector

| Level | What | Where | Live PI? |
|-------|------|-------|----------|
| **Unit** | wiring & request shapes, offline | `timeseries/tests/`, `assetframework/tests/` — `pytest` | No |
| **Scenario** | each usage pattern end-to-end | `notebooks/test_assetframework.py` (library) + `notebooks/test_timeseries.py` (connector) | Yes (or mock) |
| **Benchmark** | throughput & tuning | `notebooks/benchmark_fanout.py` | Yes (or mock) |

```bash
(cd timeseries && pytest) && (cd assetframework && pytest)   # both, offline
```

---

## Part 2 — Every option explained

Options are passed as strings via `.option("name", "value")` / `.options(**dict)`.

### TL;DR — what you must set vs. what has a default

**`aveva_pi_timeseries` must set:** `endpoint_url`, auth, `web_ids`. Everything
else has a default:

| Option | Default | | Option | Default |
|--------|---------|-|--------|---------|
| `read_mode` | `value` | | `webids_per_call` | `50` |
| `interval` | `1m` | | `partition_concurrency` | `8` |
| `max_count` | `10000` | | `bulk_read` | `true` |
| `lookback_seconds` | `3600` | | `max_advance_seconds` | `300` |
| `http_timeout_seconds` | `60` | | | |

**Client library** (`aveva_pi_assetframework`) — plain functions, no options; each
takes `base`, `basic_user`+`basic_password`, and (for lookups) `server`/`tag`. See
[the client section](#the-client-library-aveva_pi_assetframework) below.

### Connection & auth

#### `endpoint_url`  *(required)*
PI Web API base, e.g. `https://piserver.corp.com/piwebapi`.

#### `basic_user` + `basic_password`  *(required)*
HTTP Basic credentials against PI (`Authorization: Basic …`). Sent pre-emptively,
so it works even if PI advertises only `Negotiate` — confirm it works *from
Databricks over the FQDN* with `auth/basic_auth_probe.py` first.

**Use `dbutils.secrets.get(...)` — never hardcode.** Spark options can surface in
the UI/logs, so treat credentials as secrets.

> The connector code also accepts a `bearer_token` option, but it exists only to
> pass the demo's OAuth-gated mock App (a Databricks-platform gate, not PI auth).
> Against a real PI server, use Basic.

### `aveva_pi_timeseries` options

#### `web_ids`  *(required)*
Comma-separated PI **WebIDs** — the stable IDs, not tag names. Get them from the
`aveva_pi_assetframework` client (`get_point` / `batch`). This is *what* you read;
the output is `(web_id, timestamp, value)`.

#### `read_mode`  *(default `value`)*
Which flavour of data. Full table in
[README](README.md#read_mode-on-aveva_pi_timeseries-default-value):
- `value` — current snapshot, one row per WebID. **Default** — cheapest read.
- `interpolated` — one value every `interval` (even, gap-free).
- `recorded` — every raw stored point (can be large).
- `recordedattime` — value as-of a past instant (needs `as_of`; **batch only**).

> **Can't find "read_mode" in the AVEVA docs?** It's *our* option name — each value
> maps to a Stream/StreamSet controller action
> ([stream](https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/stream.html) ·
> [streamset](https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/streamset.html)).

#### `interval`  *(default `1m`)* — for `interpolated`: `30s`, `1m`, `1h`, …
#### `as_of`  *(required for `recordedattime`)* — e.g. `2026-06-01T12:00:00Z`. Batch only.

#### ⭐ `max_count` — depth cap per stream  *(default `10000`)*
The max **readings per WebID** requested in one `recorded` call (PI's `maxCount`).

**Why it matters — the 150,000 wall.** PI won't return more than
`MaxReturnedItemsPerCall` (150k) **total** per response. `maxCount` is applied
**per stream**, so with N WebIDs in a chunk the connector sends
`min(max_count, 150000 // N)` per stream — keeping the response total under the
ceiling. (This is the corrected math: it *divides* by the chunk size, never
multiplies.) Combined with time-window chunking, no single call approaches 150k.

- **Lower it** on HTTP 400 "greater than the maximum allowed".
- Only `recorded` uses it; `interpolated` is bounded by `interval`, point-in-time
  modes return one row.

#### ⭐ `partition_concurrency` — threads *within* one task  *(default `8`)*
How many HTTP calls a **single Spark task** makes at once (sub-calls when a chunk
or time-range needs several). Different from `webids_per_call`:

```
webids_per_call       = how WebIDs split ACROSS Spark tasks (horizontal, cluster-wide)
partition_concurrency = how many calls run AT ONCE inside one task (local)
```

- **Lower it (→1–4)** on 429/503 — the 1,000 req/s limit is **per IP**, and each
  executor is one client, so total load ≈ executors × concurrency.
- Usually leave at 8; scale out with more executors, not this.

#### `webids_per_call` — chunk size / Spark parallelism  *(default `50`)*
WebIDs per StreamSet call = one Spark partition = one task. **Primary scale lever:**
more points → more executors, keep this ~50. Lower it if a `/streamsets` URL is
rejected for length. Keep `max_count × webids_per_call` sensible vs. the 150k wall
(the connector clamps regardless).

#### `bulk_read`  *(default `true`)*
`true` = read a whole chunk in one `/streamsets/{mode}` call. `false` = one
`/streams/{id}/{mode}` call per WebID (slower; debug/parity only).

#### The streaming window trio
- **`initial_watermark`** — ISO start on the first run (before any checkpoint).
  **Naive timestamps are treated as UTC**, so the window doesn't shift on a
  non-UTC driver. On restart, the checkpoint takes over.
- **`lookback_seconds`** *(3600)* — fallback start ("now − N") when no watermark;
  also the batch window length.
- **`max_advance_seconds`** *(300)* — biggest jump per micro-batch (paces
  backfills). Windows are **half-open**, so a boundary timestamp is never emitted
  in two consecutive batches.

> **Late tags in `value` mode (streaming).** A snapshot is emitted only if its own
> timestamp is newer than the last committed watermark. A tag that hasn't updated
> (a "late"/stale tag) is **suppressed** rather than re-emitted every trigger — so
> you get a change feed of genuinely new values, not duplicates. (For a periodic
> full snapshot regardless of staleness, use a batch `read_mode=value` on a schedule.)

### The client library (`aveva_pi_assetframework`)

Thin functions — one real PI Web API call each, JSON returned verbatim. You
compose lookups/walks and choose your own tag/asset model (full list in
[README §Library API reference](README.md#library-api-reference-aveva_pi_assetframework)):

- `get_point(base, server, tag, **auth)` → `GET /points?path=` (one point object)
- `batch(base, requests, **auth)` → `POST /batch` (many sub-requests in one call)

  ...where `**auth` is `basic_user=…, basic_password=…` (HTTP Basic).
- `get_asset_database`, `get_database_elements`, `get_child_elements`,
  `get_element_attributes` → the AF endpoints (list pages via `start_index`/`max_count`)

**Resolve many tags** in one call — build a `batch` of `/points` sub-requests:
```python
from urllib.parse import quote
from aveva_pi_assetframework import batch
reqs = {str(i): {"Method": "GET",
                 "Resource": f"{base}/points?path=" + quote(rf"\\PISRV\{t}", safe="")}
        for i, t in enumerate(tags)}
resp = batch(base, reqs, **AUTH)
web_ids = [resp[str(i)]["Content"]["WebId"] for i in range(len(tags))]
```
**Handling a bad tag** is your call: its sub-response has `Status >= 400` (e.g. 404)
— skip it and keep the good WebIDs, or raise. `get_point` raises an HTTP error for
a missing tag. There's no built-in error/skip mode — you own that policy.

---

## Quick reference: which knob for which problem

| You want to… / You see… | Turn this |
|-------------------------|-----------|
| Go faster with more points | more executors + raise `webids_per_call` |
| **HTTP 429** | lower `partition_concurrency`; keep `bulk_read=true` |
| **HTTP 503** | lower `partition_concurrency` / fewer partitions |
| **HTTP 400** too many items | lower `max_count` or `webids_per_call` |
| **HTTP 413** | lower `batch_size` |
| Bad tag in resolution | its `batch` sub-response has `Status 404` — skip it, or fix the name/`server` |
| Faster streaming catch-up | raise `max_advance_seconds` |
| Connectivity smoke test | `read_mode=value` on one WebID |
