# Known issues — timeseries

> **#1-#4 are FIXED in 2.1.0** (see `CHANGELOG.md`). This file is kept because the
> reasoning explains *why* the fixes look the way they do, and because **#5 is still
> open**. Anyone still on **2.0.4 or earlier** is exposed to all of #1-#4 — most
> importantly the silent `recorded` data loss.
>
> Found by code review (2026-08-10). Ordered by whether they cost *data* or merely *time*.
> All line references are `timeseries/src/aveva_pi_timeseries/` **as of 2.0.4** and will
> not match 2.1.0.

---

## 1. 🚨 `recorded` silently truncates — DATA LOSS  ✅ FIXED in 2.1.0

**What happens.** `maxCount` is a *per-stream* ceiling:
`min(max_count=10_000, 150_000 // tags_per_call)` = **3,000** at 50 tags/call
(`reader.py:184-187`). If a window holds more values than that, PI returns the first
3,000 and stops — **HTTP 200, no error, no `Links.Next` followed**. The connector emits
those rows, commits the watermark past the *whole* window, and the remainder is
unrecoverable.

There is no truncation check anywhere: nothing compares `len(values)` to the requested
`maxCount`.

**Why it's the worst one.** It looks like success. Row counts are plausible, no
exception, dashboards populate. Only an independent count of expected values reveals it.

**When it bites.** Any tag producing >3,000 values in one `span_cap` window:

| Tag rate | Values per 3,000 s window | Kept | Lost |
|---|---|---|---|
| 1 per 10 s | 300 | all | none |
| 1 per second | 3,000 | all | none ← the implicit assumption |
| 1 per 100 ms | 30,000 | 3,000 | **90%** |
| 1 per ms | 3,000,000 | 3,000 | **99.9%** |

**Confirm it.** Request a window you know holds >3,000 values for a single tag and count
the rows. Exactly 3,000 = confirmed. (`§ 0` of `notebooks/benchmark_fanout.py` does this.)

**Fix.** Detect the truncation rather than predicting density:

```python
# maxCount is a per-stream ceiling; hitting it exactly means PI truncated and there is
# more data in this window than we asked for. Re-split (or follow Links.Next) instead of
# silently dropping the remainder.
if len(values) >= max_count_used:
    ...
```

Following `Links.Next` is the PI-native answer. ~15 lines. **Mitigation today:** lower
`webids_per_call` (raises the per-tag cap: 10 tags/call → 15,000) or narrow the window.

---

## 2. `recorded` window sizing treats an item count as seconds  ✅ FIXED in 2.1.0

**What happens.** `_time_windows` (`reader.py:165-182`):

```python
if self._read_mode == "interpolated":
    span_cap = per_stream_cap * self._interval_s   # items × s/item = seconds ✅
else:  # recorded
    span_cap = per_stream_cap                       # items used AS seconds ❌
```

`interpolated` is dimensionally correct — it knows the interval. `recorded` has no
interval, so the code uses the item count (3,000) as a duration (3,000 s), i.e. it
implicitly **assumes 1 value/second/tag**. The code comment concedes this is
"heuristic".

Wrong in both directions: under-splits at high density (causing #1), over-splits at low.

**Fix.** Subsumed by #1 — let truncation detection drive the splitting instead of
guessing density up front.

---

## 3. `partition_concurrency` is inert — a regression  ✅ FIXED in 2.1.0

**What happens.** Two stacked reasons the thread pool never runs:

1. **Partition size == call size.** `webids_per_call` is used twice — to split the tag
   list into Spark tasks (`reader.py:259-261`, `:314-316`) *and* to split a task's tags
   into HTTP calls (`:233`). A partition therefore re-chunks to exactly **one** batch, so
   the guard `len(webid_batches) <= 1` at `:238` is always true and the
   `ThreadPoolExecutor` at `:248` is **never constructed**.
2. **The pool is on the wrong axis.** `_fetch` loops time windows *serially* (`:236`) and
   nests the pool *inside* that loop, handing it a 1-item list each iteration. The
   windows — the axis that can hold thousands of items — never reach the pool.

**This is a regression.** v0.4 had a separate `tags_per_partition` option (default 50)
and the knob worked. Commit `6aa0e5f2` ("v2.0: decouple AF, lean web_ids interface",
2026-07-27) dropped it; it now appears nowhere in the repo.

**What it costs.** Speed only, and only when one task has >1 call to make —
`calls_per_task = ceil(span ÷ span_cap)`, with `span_cap` ≈ 3,000 s at 50 tags/call:

| Span read | Calls/task | Fix worth anything? |
|---|---|---|
| 1-min cycle | 1 | **no** — 4 calls on 4 machines is already optimal |
| 15-min cycle | 1 | **no** |
| 1 day | 29 | yes |
| 6 months (first backfill) | 5,184 | yes, ~8× (≈22 min → ≈3 min) |

**More time in the window does NOT mean more calls** until the span exceeds ~50 min. So
steady-state streaming never benefits; only first-run backfill and long outage catch-up.

**Fix (`recorded` only, ~10 lines).** Hoist the pool above the window loop so it receives
`windows × webid_batches` as one flat work list. **Partition count and `webids_per_call`
stay exactly as they are** — Spark keeps parallelising tags across machines, and threads
parallelise windows *within* each task. The two stack.

Each work item must carry its own window index: responses complete out of order, and the
half-open boundary dedup at `:223` depends on `is_first_window` and that window's
`start_ts`.

`tags_per_partition` is only needed for **`value`** (no windows, tags are the only axis)
— and there it buys nothing at small tag counts, because Spark already owns that axis.
Worth it only when `tags ÷ webids_per_call >> task slots`.

### ⚠️ Upgrade trap

`partition_concurrency` **already defaults to 8** (`_http.py:22`), merely inert. Hoisting
the pool *activates* it with no config change — an existing `recorded` config would jump
from 1 to 8 in-flight calls per task against the customer's production PI. Either default
it to 1, or ship as a loud behaviour change requiring re-test.

### Also: bounded waves needed

`pool.map(_one, webid_batches)` (`:249`) submits every item at once and accumulates all
response bodies. Harmless today (the pool never runs) but fixing the above unmasks it —
5,184 windows would mean 5,184 responses in memory. Submit in waves of `concurrency`.

---

## 4. `max_advance_seconds` is dead code  ✅ REMOVED in 2.1.0

Assigned at `reader.py:143`, **never read again**. v0.4 used it to pace `latestOffset`;
v2's `latestOffset` returns `now_epoch()` unconditionally (`:304-312`), so there is no
pacing guard on a wide first batch.

Still exposed as a job widget in
`sse_thermal_pi_maximo_sdp/sdp/ingestion/ingest_pi_to_raw.py:58`, so someone will set it
and expect pacing they will not get.

**Fix.** Delete the option and the widget, or reimplement the pacing.

---

## 5. Streaming `value` bypasses `_fetch` entirely  ⚠️ STILL OPEN

`reader.py:323-343` is a separate sequential generator with **no thread pool at all** and
a session built without `pool_maxsize`. Any concurrency fix to `_fetch` does not reach it.

Note it is not redundant: line 340 (`> partition.start_ts`) suppresses tags whose
timestamp has not advanced, giving a clean change-feed rather than re-emitting all tags
every trigger. **Preserve that** — routing it through `_fetch` naively would emit every
tag every batch.

---

## Not a bug: `value` mode drops data between polls

Worth stating because it surprises people. `value` returns **one snapshot per tag** — the
current reading, nothing else. Polling it repeatedly is *sampling*, not recording:

```
tag ticks:  ●●●●●●●●●●●●   (12 values in 60 s at a 5 s scan rate)
you keep:              ●   (1 — the other 11 stay in PI's archive, unread)
```

Fine for "current state" dashboards. **Not** fine for trending, calculations, or
compliance. If every reading is required, that is `read_mode=recorded` — a **one-option
change, no connector fix needed**.

`value` therefore has **no throughput ceiling to benchmark**: N tags returns ≤N rows by
design (fewer in streaming, because unchanged tags are suppressed). The only meaningful
number is cycle time vs trigger interval.

---

## Status

| # | Issue | Cost | 2.1.0 |
|---|---|---|---|
| 1 | `recorded` silent truncation | **data loss** | ✅ fixed — detected and re-read as halves |
| 2 | span sizing: items used as seconds | causes #1 | ✅ fixed — `assumed_values_per_second` |
| 3 | `partition_concurrency` inert | speed (backfill only) | ✅ fixed — pool now spans the window axis |
| 4 | `max_advance_seconds` dead | confusion | ✅ removed |
| 5 | streaming `value` bypasses `_fetch` | none today | ⚠️ **open** |

**#5 is left open deliberately.** Threading it would need the change-suppression at line
340 preserved, and it buys nothing at realistic tag counts: `value` has no window axis, so
tags are the only axis and Spark already parallelises those across machines. It becomes
worth doing only when `tags ÷ webids_per_call` greatly exceeds the task-slot count.

**Still get the customer's PI scan rate and compression settings.** 2.1.0 makes truncation
loud instead of silent, but a very dense archive will now *raise* where it previously
returned quiet, wrong data — and the right `webids_per_call` / `assumed_values_per_second`
depend on that number. `§ 1` of `notebooks/benchmark_fanout.py` measures it.
