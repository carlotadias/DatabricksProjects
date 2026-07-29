"""AVEVA PI time-series connector — lean PySpark DataSource.

`aveva_pi_timeseries` reads point values for a set of **WebIDs** and returns a
clean **`(web_id, timestamp, value)`** dataset. It is a self-contained Spark
connector: it does NO name resolution and NO Asset Framework walking. Resolve tag
names / discover AF assets first with the separate **`aveva-pi-assetframework`**
library, then pass the WebIDs here.

    from aveva_pi_timeseries import PITimeSeriesSource
    spark.dataSource.register(PITimeSeriesSource)

    df = (spark.readStream.format("aveva_pi_timeseries")
        .option("endpoint_url", "https://<host>/piwebapi")
        .option("basic_user", dbutils.secrets.get("pi", "pi_user"))
        .option("basic_password", dbutils.secrets.get("pi", "pi_password"))
        .option("web_ids", ",".join(web_ids))     # from aveva_pi_assetframework
        .option("read_mode", "interpolated").option("interval", "1m")
        .option("initial_watermark", "2026-01-01T00:00:00Z")
        .load())                                    # -> (web_id, timestamp, value)

Read modes (`read_mode`, default `value`; each maps to a PI Web API Stream/StreamSet action):
  * `value`          — point-in-time: current snapshot (one row per WebID). DEFAULT.
  * `interpolated`   — history resampled on a fixed `interval` over the window
  * `recorded`       — history, raw archived points over the window
  * `recordedattime` — point-in-time: value as-of `as_of` (batch only)

Value typing: the `value` column is numeric (`double`). Digital/enumeration
points contribute their numeric `Value`; string points and per-value error
objects yield `null`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    InputPartition,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from ._http import (
    PI_DEFAULT_MAX_COUNT,
    PI_DEFAULT_PARTITION_CONCURRENCY,
    PI_DEFAULT_WEBIDS_PER_CALL,
    PI_MAX_RETURNED_ITEMS,
    chunk,
    iso,
    now_epoch,
    parse_ts,
    parse_watermark_epoch,
    raise_on_web_exception,
    request_json,
    session,
)

_READ_MODES = {"interpolated", "recorded", "value", "recordedattime"}
_POINT_IN_TIME = {"value", "recordedattime"}

_INTERVAL_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

# Lean output contract: exactly (web_id, timestamp, value).
TIMESERIES_SCHEMA = StructType([
    StructField("web_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("value", DoubleType(), True),
])


def _web_ids(options: dict) -> list[str]:
    raw = options.get("web_ids") or ""
    return [w.strip() for w in raw.split(",") if w.strip()]


def _interval_seconds(interval: str) -> int:
    """'1m' -> 60, '30s' -> 30, '1h' -> 3600. Best-effort; defaults to 60s."""
    try:
        n, unit = int(interval[:-1]), interval[-1].lower()
        return max(1, n * _INTERVAL_SECONDS.get(unit, 60))
    except (ValueError, IndexError):
        return 60


def _coerce_value(raw):
    """Safely coerce a PI value to float. PI values may be a JSON number, a
    digital/enumeration OBJECT ({"Name","Value",...}), a string, or absent on an
    error item. Return a float where meaningful, else None — never raise."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, dict):
        inner = raw.get("Value")
        return float(inner) if isinstance(inner, (int, float)) else None
    return None


@dataclass
class _WebIdChunkPartition(InputPartition):
    """A partition covers a CHUNK of WebIDs plus the [start, end) window."""
    web_ids: tuple
    start_ts: int = 0
    end_ts: int = 0


class PITimeSeriesSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "aveva_pi_timeseries"

    def schema(self) -> StructType:
        return TIMESERIES_SCHEMA

    def reader(self, schema: StructType) -> "PITimeSeriesBatchReader":
        return PITimeSeriesBatchReader(self.options)

    def streamReader(self, schema: StructType) -> "PITimeSeriesStreamReader":
        return PITimeSeriesStreamReader(self.options)


class _PITimeSeriesMixin:
    """Shared config + StreamSet bulk fetch (with window chunking + safety)."""

    def _init(self, options: dict) -> None:
        self._base = options["endpoint_url"].rstrip("/")
        self._bearer = options.get("bearer_token")
        self._api_key = options.get("api_key")
        self._basic_user = options.get("basic_user")
        self._basic_password = options.get("basic_password")
        self._verify_tls = (options.get("verify_tls", "true").lower() != "false")
        self._web_ids = _web_ids(options)
        if not self._web_ids:
            raise ValueError("aveva_pi_timeseries requires the 'web_ids' option "
                             "(comma-separated PI WebIDs; resolve names via the "
                             "aveva-pi-assetframework library)")
        self._interval = options.get("interval", "1m")
        self._interval_s = _interval_seconds(self._interval)
        self._max_advance = int(options.get("max_advance_seconds", "300"))
        self._lookback = int(options.get("lookback_seconds", "3600"))
        self._timeout = int(options.get("http_timeout_seconds", "60"))
        self._initial_watermark = options.get("initial_watermark")
        self._webids_per_call = int(options.get("webids_per_call",
                                                str(PI_DEFAULT_WEBIDS_PER_CALL)))
        self._max_count = int(options.get("max_count", str(PI_DEFAULT_MAX_COUNT)))
        self._concurrency = int(options.get("partition_concurrency",
                                            str(PI_DEFAULT_PARTITION_CONCURRENCY)))
        self._bulk_read = (options.get("bulk_read", "true").lower() != "false")
        self._read_mode = (options.get("read_mode") or "value").lower()
        if self._read_mode not in _READ_MODES:
            raise ValueError(f"read_mode must be one of {sorted(_READ_MODES)}")
        self._as_of = options.get("as_of")
        if self._read_mode == "recordedattime" and not self._as_of:
            raise ValueError("read_mode=recordedattime requires the 'as_of' option (a timestamp)")

    def _watermark_start(self) -> int:
        if self._initial_watermark:
            return parse_watermark_epoch(self._initial_watermark)
        return now_epoch() - self._lookback

    def _time_windows(self, start_ts: int, end_ts: int, n_streams: int) -> list[tuple]:
        """Split [start, end] into half-open sub-windows sized so a single call
        stays under MaxReturnedItemsPerCall across all streams in the chunk."""
        if self._read_mode in _POINT_IN_TIME or end_ts <= start_ts:
            return [(start_ts, end_ts)]
        per_stream_cap = min(self._max_count, max(1, PI_MAX_RETURNED_ITEMS // max(1, n_streams)))
        if self._read_mode == "interpolated":
            span_cap = per_stream_cap * self._interval_s
        else:  # recorded — bound the window span heuristically by the cap
            span_cap = per_stream_cap
        span_cap = max(self._interval_s, span_cap)
        windows = []
        s = start_ts
        while s < end_ts:
            e = min(end_ts, s + span_cap)
            windows.append((s, e))
            s = e
        return windows or [(start_ts, end_ts)]

    def _per_stream_max_count(self, n_streams: int) -> int:
        """maxCount is PER STREAM; keep the response total under the ceiling:
        min(max_count, 150k // n_streams)."""
        return min(self._max_count, max(1, PI_MAX_RETURNED_ITEMS // max(1, n_streams)))

    def _streamset_url_params(self, web_ids: list[str], start_ts: int, end_ts: int):
        mode = self._read_mode
        params: list[tuple] = [("webId", w) for w in web_ids]
        if mode == "value":
            url = f"{self._base}/streamsets/value"
        elif mode == "recordedattime":
            url = f"{self._base}/streamsets/recordedattime"
            params.append(("time", self._as_of))
        elif mode == "recorded":
            url = f"{self._base}/streamsets/recorded"
            params += [("startTime", iso(start_ts)), ("endTime", iso(end_ts)),
                       ("maxCount", str(self._per_stream_max_count(len(web_ids))))]
        else:  # interpolated
            url = f"{self._base}/streamsets/interpolated"
            params += [("startTime", iso(start_ts)), ("endTime", iso(end_ts)),
                       ("interval", self._interval)]
        return url, params

    def _emit_streamset(self, body: dict, start_ts: int, is_first_window: bool) -> Iterator[tuple]:
        """Yield (web_id, timestamp, value) from a StreamSet response, guarding
        WebException-on-200, half-open window boundaries, and missing/error items."""
        raise_on_web_exception(body)
        history = self._read_mode not in _POINT_IN_TIME
        for stream in body.get("Items", []):
            raise_on_web_exception(stream)
            web_id = stream.get("WebId")
            values = stream.get("Items")
            if values is None and stream.get("Value") is not None:
                values = [stream["Value"]]
            for item in values or []:
                ts = item.get("Timestamp")
                if not ts:
                    continue
                epoch = int(parse_ts(ts).timestamp())
                if history and not is_first_window and epoch <= start_ts:
                    continue
                yield (web_id, parse_ts(ts), _coerce_value(item.get("Value")))

    def _fetch(self, partition: _WebIdChunkPartition) -> Iterator[tuple]:
        s = session(self._bearer, self._api_key, pool_maxsize=max(self._concurrency, 8),
                    basic_user=self._basic_user, basic_password=self._basic_password,
                    verify_tls=self._verify_tls)
        web_ids = list(partition.web_ids)
        windows = self._time_windows(partition.start_ts, partition.end_ts, len(web_ids))
        webid_batches = chunk(web_ids, self._webids_per_call) if self._bulk_read \
            else [[w] for w in web_ids]

        for wi, (ws, we) in enumerate(windows):
            first = (wi == 0)
            if self._concurrency <= 1 or len(webid_batches) <= 1:
                for wc in webid_batches:
                    url, params = self._streamset_url_params(wc, ws, we)
                    body = request_json(s, "GET", url, params=params, timeout=self._timeout)
                    yield from self._emit_streamset(body, ws, first)
            else:
                from concurrent.futures import ThreadPoolExecutor
                def _one(wc, _ws=ws, _we=we):
                    url, params = self._streamset_url_params(wc, _ws, _we)
                    return request_json(s, "GET", url, params=params, timeout=self._timeout)
                with ThreadPoolExecutor(max_workers=self._concurrency) as pool:
                    for body in pool.map(_one, webid_batches):
                        yield from self._emit_streamset(body, ws, first)


class PITimeSeriesBatchReader(_PITimeSeriesMixin, DataSourceReader):
    def __init__(self, options: dict) -> None:
        self._init(options)
        self._start = self._watermark_start()
        self._end = now_epoch()

    def partitions(self) -> list[_WebIdChunkPartition]:
        return [_WebIdChunkPartition(tuple(c), self._start, self._end)
                for c in chunk(self._web_ids, self._webids_per_call)]

    def read(self, partition: _WebIdChunkPartition) -> Iterator[tuple]:
        return self._fetch(partition)


class PITimeSeriesStreamReader(_PITimeSeriesMixin, DataSourceStreamReader):
    """Micro-batch streaming reader.

    Offsets are epoch seconds. Uses the basic DataSourceStreamReader contract:
    `initialOffset()` gives the first-run start, `latestOffset()` (no args) returns
    the newest available offset (now), and each micro-batch reads the half-open
    window `[start, end)` via `partitions(start, end)`.

    Restart-safe: on restart Spark restores the committed offset from the
    checkpoint and passes it as `start` to `partitions()`, so the window is always
    forward (`[committed, now]`) and never inverts. `latestOffset` deliberately
    keeps NO private cursor state and takes NO `limit` arg — a `limit` parameter
    makes Spark route through admission control (SupportsAdmissionControl), which
    would also require `getDefaultReadLimit()`; we use the simpler contract.

    Per-partition `_time_windows` still chunks a wide `[start, end)` into
    sub-windows under MaxReturnedItemsPerCall, so a large first (backfill) batch is
    read as many bounded calls rather than one oversized request. Control batch
    cadence with the query's `.trigger(...)` (e.g. `availableNow=True` for a
    one-shot backfill, or a processing-time trigger for steady polling).

    Late-tag handling in `value` mode: a snapshot is emitted only if its own
    Timestamp is strictly newer than the batch start (the committed watermark). A
    tag that hasn't updated ("late"/stale) is suppressed rather than re-emitted
    every trigger — giving a clean change-feed.
    """

    def __init__(self, options: dict) -> None:
        self._init(options)
        if self._read_mode == "recordedattime":
            raise ValueError("read_mode=recordedattime is batch-only; use spark.read, "
                             "not readStream (it would re-emit the same as-of rows forever)")
        self._start_watermark = self._watermark_start()

    def initialOffset(self) -> dict:
        return {"ts": self._start_watermark}

    def latestOffset(self) -> dict:
        # Newest available offset = now. ONE micro-batch covers [committed, now];
        # a wide (e.g. months-long backfill) window is not paced batch-by-batch here
        # — instead `_fetch` splits it internally into sub-windows under
        # MaxReturnedItemsPerCall (see `_time_windows`). Pacing per batch would
        # otherwise mean tens of thousands of 5-min micro-batches for a big backfill.
        # (Basic no-arg contract: a `limit` param would trigger admission control
        # and demand getDefaultReadLimit, which we don't implement.)
        return {"ts": now_epoch()}

    def partitions(self, start: dict, end: dict) -> list[_WebIdChunkPartition]:
        return [_WebIdChunkPartition(tuple(c), int(start["ts"]), int(end["ts"]))
                for c in chunk(self._web_ids, self._webids_per_call)]

    def commit(self, end: dict) -> None:
        # Nothing to persist locally — Spark checkpoints the committed offset and
        # replays it as `start` to partitions() on the next batch / after restart.
        pass

    def read(self, partition: _WebIdChunkPartition):
        if self._read_mode == "value":
            def _gen():
                s = session(self._bearer, self._api_key,
                            basic_user=self._basic_user, basic_password=self._basic_password,
                            verify_tls=self._verify_tls)
                for wc in (chunk(list(partition.web_ids), self._webids_per_call)
                           if self._bulk_read else [[w] for w in partition.web_ids]):
                    url, params = self._streamset_url_params(wc, partition.start_ts, partition.end_ts)
                    body = request_json(s, "GET", url, params=params, timeout=self._timeout)
                    raise_on_web_exception(body)
                    for stream in body.get("Items", []):
                        raise_on_web_exception(stream)
                        val = stream.get("Value")
                        ts = (val or {}).get("Timestamp")
                        if not ts:
                            continue
                        if int(parse_ts(ts).timestamp()) > partition.start_ts:
                            yield (stream.get("WebId"), parse_ts(ts),
                                   _coerce_value(val.get("Value")))
            return _gen()
        return self._fetch(partition)
