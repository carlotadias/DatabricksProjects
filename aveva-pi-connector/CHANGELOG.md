# Changelog

All notable changes to the AVEVA PI → Databricks connector.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## Truncation detection + window concurrency — timeseries [2.1.0]

Fixes the three defects recorded in `KNOWN_ISSUES.md` #1-#4. **Behaviour changes** —
re-run your own tests before upgrading a live pipeline.

### Fixed: `recorded` silently lost data above `maxCount` (#1) — DATA LOSS

`maxCount` is a per-stream ceiling that PI applies **silently**: it returns the first
`maxCount` values with HTTP 200 and no error, so a truncated response was
indistinguishable from a complete one. The connector emitted those rows, committed the
watermark past the whole window, and the remainder was gone — 90% loss at 1 value/100ms,
99.9% at 1/ms.

`_read_window` now **counts the response** and, if any stream came back at the ceiling,
re-reads the window as two halves (recursively, bounded by `PI_MAX_WINDOW_SPLITS = 12`).
If a window still truncates when it cannot be split further, it **raises** rather than
returning partial data. Measuring the response beats predicting the tag rate.

### Fixed: `recorded` window sizing used an item count as seconds (#2)

`span_cap = per_stream_cap` treated 3,000 *items* as 3,000 *seconds*, implicitly assuming
1 value/sec/tag. Now converted via `assumed_values_per_second` (new option, default 1.0 —
conservative, since too-wide windows are recovered by truncation detection while too-narrow
ones only cost extra calls). Measure the real rate with § 1 of
`notebooks/benchmark_fanout.py`.

`interval` is also no longer used as a floor for `recorded`, where it has no meaning — it
was preventing dense tags from being split below 60 s.

### Fixed: `partition_concurrency` was inert (#3) — a regression since 2.0.0

Two stacked causes: partition size == call size (so a task re-chunked to exactly one call
and the `ThreadPoolExecutor` was never constructed), and the pool sat *inside* the serial
window loop where it received a 1-item list. `_fetch` now flattens work across **both**
axes — `windows × webid_batches` — so concurrency parallelises the axis that actually has
many items: the sub-windows of a wide backfill/catch-up read. Submitted in bounded waves so
a multi-thousand-window backfill does not hold every response in memory.

⚠️ **`partition_concurrency` already defaulted to 8**, merely inert. Activating it means an
unchanged `recorded` config now issues up to 8 concurrent calls per task instead of 1. Set
it to `1` to keep the old load profile. Partition count and `webids_per_call` are unchanged,
so Spark still parallelises tags across machines exactly as before — steady-state cycles
(one window) are unaffected either way.

### Removed: `max_advance_seconds` (#4)

Dead since 2.0.0 — assigned, never read. `latestOffset` returns `now_epoch()`
unconditionally and a wide first batch is bounded by `_time_windows` instead. Passing the
option is harmless but has no effect; remove it from job parameters.

### Tests

23 unit tests (was 15), offline. New coverage: truncation detected and re-read as
contiguous halves; unsplittable truncation raises; no extra calls under the cap; `value`
never treated as truncated; `recorded` span scales with the assumed rate; `interpolated`
span still exact; every sub-window read exactly once, both concurrent and serial.

## TLS verification toggle — timeseries [2.0.4], assetframework [3.0.2]

Add a **`verify_tls`** option (default `true`) so the connector can talk to a PI
server whose **internal / self-signed CA** isn't yet in the cluster trust store.

- **timeseries connector:** `.option("verify_tls", "false")`.
- **assetframework library:** `verify_tls=False` kwarg on every primitive.
- Threaded through the shared `session()` → sets `requests` `session.verify` and
  silences the `InsecureRequestWarning` when off.
- **Diagnostic only.** `false` disables cert + hostname validation, exposing the
  reusable Basic credential to MITM. The proper fix is to import the CA and keep
  verification on. It fixes *certificate* errors only — not DNS resolution of the
  PI FQDN from the cluster.
- Mirrors the `verify_tls` widget in the customer's own `basic_auth_probe`-style
  test, so a connector run matches what that test already proved.

## HTTP Basic auth — timeseries [2.0.3], assetframework [3.0.1]

Make **HTTP Basic** the connector's PI authentication scheme, across both packages
(shared vendored `_http.py`).

- **Added** `basic_user` / `basic_password` — on the connector as Spark options, and
  on every `aveva_pi_assetframework` primitive as kwargs. Threaded through to a
  `requests.auth.HTTPBasicAuth` session.
- Credentials are sent **pre-emptively** (on the first request, not only after a
  401), so Basic works even where PI advertises only `Negotiate` in its
  `WWW-Authenticate` header — the header advertises, it doesn't define what the
  server accepts (RFC 9110 §11.6.2).
- Use a secret scope + a dedicated read-only AD account. Confirm Basic actually
  works *from Databricks over the FQDN* (`auth/basic_auth_probe.py`) — an on-box
  `curl` success does not prove the remote path.
- **Docs are now Basic-only.** Every README / HOW_TO_USE example and both scenario
  notebooks lead with Basic; `bearer_token` / `api_key` remain in the *code* but
  are undocumented as a PI path — they exist solely so the demo can pass its
  OAuth-gated mock App (a Databricks-platform gate, not PI auth). The long-term
  OIDC path is noted as future, out of scope here.

## assetframework [3.0.0] — thin PI Web API client

Reduce `aveva_pi_assetframework` to **only what the PI Web API can do** — one thin
function per real API call, JSON returned verbatim, no orchestration or opinions.

- **Removed** `resolve_webids`, `discover_af`, and the `aveva_pi_points`
  DataSource. Those baked in multi-step logic (loops, paging, tree-walking) and a
  tag/asset model that isn't the API's — the caller now composes those.
- **Added** primitives: `get_point` (`GET /points`), `batch` (`POST /batch`),
  `get_asset_database`, `get_database_elements`, `get_child_elements`,
  `get_element_attributes` (paged via `start_index`/`max_count`).
- Callers compose WebID resolution (a `batch` of `/points` sub-requests) and AF
  walks themselves, and decide their own tag/asset mapping and bad-tag policy
  (a bad tag surfaces as `Status >= 400` in its batch sub-response).
- Demo (`sse_thermal_pi_maximo_sdp`) updated: `ingest_pi_to_raw` composes the
  batch lookup; `ingest_af_structure` walks the tree and maps element/attribute
  names to its `sdp.mapping.af_assets` model in one place.
- The `aveva-pi-timeseries` connector is unchanged (still 2.0.0).

## [2.0.0] — unreleased (in progress)

A structural refactor that **decouples Asset Framework from time-series ingest**,
makes the time-series connector accept **`web_ids`** directly and return a lean
**`(web_id, timestamp, value)`** dataset, and folds in the correctness fixes from
the 2026-07-24 code review (Dan Keeling).

### Work items (this change set)

- [x] **1. Change tracking** — this `CHANGELOG.md`, checked off as items land.
- [x] **2. Decouple AF** — split into **two independently-installed wheels** (no
  cross-dependency; each vendors its own private `_http.py`):
    - **`aveva-pi-timeseries`** (`timeseries/`) — the Spark connector.
      `import aveva_pi_timeseries`.
    - **`aveva-pi-assetframework`** (`assetframework/`) — the lookup library
      (`resolve_webids`, `discover_af`, point metadata). `import aveva_pi_assetframework`.
  Two wheels so the connector-vs-library split is real at install time.
- [x] **3. Lean connector interface** — the time-series source takes `web_ids`
  (comma-separated) and returns exactly `(web_id, timestamp, value)`. Tag→WebID
  resolution and asset context moved out to the `aveva_pi_assetframework` library.
- [x] **4. Terminology** — replaced "event" with "point" across docs and
  docstrings; the timestamp column is now `timestamp` (was `event_ts`) and the
  source is `aveva_pi_timeseries` (was `aveva_pi_events`).
- [x] **5. Late-tag edge case (value mode)** — in streaming `value` mode, a tag
  whose snapshot timestamp has not advanced past the committed watermark is
  suppressed (not re-emitted). Reasoning documented in `timeseries.py` and README.
- [x] **7. Review fixes** — see below.
- [x] **Tests + notebooks** — per-package unit tests (`timeseries/tests`,
  `assetframework/tests`; 17 pass total). One scenario notebook **per package** —
  `notebooks/test_assetframework.py` (library: resolve, points, discover_af,
  on_missing_tag) and `notebooks/test_timeseries.py` (connector: all read modes +
  streaming) — so each is tested standalone. Mock cells patch `session` in each
  package's `_http` + `resolve`/`reader` module (it's imported by-name).

### Review fixes folded in (2026-07-24 review)

- [x] **H1** — `recorded` maxCount math corrected: per-stream cap is now
  `min(max_count, 150k // n_streams)` so the response total stays under the ceiling.
- [x] **H2** — time-window chunking: batch/stream reads split `[start, end]` into
  sub-windows sized so no single call approaches `MaxReturnedItemsPerCall`.
- [x] **H3** — inspect `WebException` on HTTP 200 and raise, so a truncated
  response fails (and is retried) instead of being silently committed.
- [x] **H4** — AF mode uses the WebID from discovery directly (no re-resolution,
  no malformed `\\server\\AF-path`); `/streams/{webId}` accepts AF attribute WebIDs.
- [x] **H5** — half-open windows: micro-batch start is advanced so boundary
  timestamps are not emitted in two consecutive batches.
- [x] **H6** — streaming `latestOffset` derives from the committed/engine offset,
  not private reader state, so restarts don't invert the window.
- [x] **H7** — safe value coercion: digital/enumeration objects use their numeric
  `Value`; strings/errors → `null` (no crash). Value stays numeric (`double`).
- [x] **H8** — `.get("Timestamp")`; per-value `Errors` route to a null value
  instead of a `KeyError`.
- [x] **M1** — retry transient network errors (ConnectionError/ReadTimeout/…),
  not just HTTP status codes.
- [x] **M2** — `recordedattime` is rejected in streaming (batch-only).
- [x] **M3** — points reader reads Path/EngineeringUnits from the batch response
  (no extra per-tag GET).
- [x] **M4** — naive watermarks are interpreted as UTC explicitly.
- [x] **M5** — AF discovery pages results and can recurse the element tree.
- [x] **M6** — `Good` compared explicitly (`is True` / not the string "false").
- [x] **L1** — removed the unused `PI_DEFAULT_RATE_LIMIT_PER_SEC` constant.
- [x] **L2** — documented the tighter search rate limit (50/s).
- [x] **L3** — documented `_asset_of` as best-effort.
- [x] **L4** — README version footer matches the package version.
- [x] **L5** — `Retry-After` HTTP-date form documented (falls back to backoff).
- [x] **L6** — docs reinforce secrets-only for the bearer token.

### Migration notes

- One wheel → **two wheels**: install `aveva_pi_assetframework` (library) and/or
  `aveva_pi_timeseries` (connector). The old single `aveva_pi_datasource` wheel and
  the `from aveva_pi import ...` / `aveva_pi_datasource` shim are **removed** —
  update imports to the new package names.
- `aveva_pi_events` (option `tags`, column `event_ts`) → **`aveva_pi_timeseries`**
  (option `web_ids`, column `timestamp`). Resolve tags to WebIDs first via
  `aveva_pi_assetframework.resolve_webids(...)` or the `aveva_pi_points` source.

## [1.1.0]
- Added `on_missing_tag` (`error` | `skip`) for unresolved tags.
- Fixed batch WebID resolution to embed the query in the sub-request `Resource`.

## [1.0.0]
- First standalone release: `aveva_pi_points` + `aveva_pi_events`, hybrid fan-out.
