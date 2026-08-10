# AVEVA PI → Databricks connector — v3

| Package | Version | Wheel |
|---|---|---|
| `aveva-pi-timeseries` | **2.1.0** | Spark DataSource: `web_ids` → `(web_id, timestamp, value)` |
| `aveva-pi-assetframework` | **3.0.2** | thin PI Web API client (tag/AF lookups) |

Two independent packages — the connector does **not** depend on the library. Install
either or both.

## Start here

1. `README.md` — what the two pieces are, options reference, PI limits, troubleshooting.
2. `HOW_TO_USE.md` — build/publish the wheels, then the two-step read.
3. **`KNOWN_ISSUES.md`** — read before a customer run.
4. `notebooks/test_timeseries.py` / `test_assetframework.py` — verify it works.
5. `notebooks/benchmark_fanout.py` — how fresh can the data be, and how long a history
   load takes.

## What changed in 2.1.0

Fixes three defects in 2.0.4 — see `CHANGELOG.md` for detail and the upgrade note.

- **`recorded` silently lost data** above PI's per-stream `maxCount`. PI applies the cap
  with HTTP 200 and no error, so a truncated response was indistinguishable from a
  complete one. Now detected by counting the response and re-read in halves; if a window
  still truncates when it cannot be split further, it raises rather than returning
  partial data.
- **Window sizing used an item count as seconds** for `recorded`, implicitly assuming
  1 value/sec/tag. Now derived from the new `assumed_values_per_second` option.
- **`partition_concurrency` did nothing** — a regression since 2.0.0. The thread pool sat
  inside a serial window loop and was never constructed. It now parallelises the
  sub-windows of a wide read.

⚠️ `partition_concurrency` already defaulted to **8** while inert, so activating it means
an unchanged `recorded` backfill now issues up to 8 concurrent calls per task instead of
1. Set it to `1` to keep the old load profile.

Tests: 23 (timeseries) + 9 (assetframework), offline — no Spark or PI needed:

```bash
(cd timeseries && python -m pytest tests/ -q)
(cd assetframework && python -m pytest tests/ -q)
```

## Not included

Pre-built wheels (`dist/`) — build them with `scripts/deploy.sh`, or
`(cd timeseries && python -m build --wheel)`. Behind a proxy, `python -m build` needs
`PIP_INDEX_URL` pointed at your mirror (it installs the build backend with pip, so
`UV_INDEX_URL` alone is not enough).
