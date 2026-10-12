"""Thin, allow-listed Lakebase Data API (PostgREST) client for the HH portal demo.

GET only. Views must be in queries.ALLOWED_VIEWS (schema portal_gold); parameters are limited to `select`
(plain column lists), `order`, `limit`, and column filters with eq / in / gte / lte / is. Every call is
recorded as {path, rows, status, ms} so the UI can show exactly how the data was fetched; the bearer token
is never part of that record or of any error text.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import quote

import httpx

from .queries import ALLOWED_VIEWS

_SECRETS = re.compile(r"(dapi[0-9a-f]{16,}(-\d+)?|Bearer\s+[A-Za-z0-9._~+/=-]+|eyJ[A-Za-z0-9._-]{20,})")
_COLUMN = re.compile(r"^[a-z_][a-z0-9_]*$")
_SELECT = re.compile(r"^[a-z_][a-z0-9_]*(,[a-z_][a-z0-9_]*)*$")
_ORDER = re.compile(r"^[a-z_][a-z0-9_]*\.(asc|desc)(,[a-z_][a-z0-9_]*\.(asc|desc))*$")
_FILTER = re.compile(r"^(eq|gte|lte)\.[A-Za-z0-9_:+-]{1,40}$|^in\.\([A-Za-z0-9_-]{1,40}(,[A-Za-z0-9_-]{1,40}){0,49}\)$"
                     r"|^is\.(true|false|null)$")
REQUEST_TIMEOUT_S = 30.0


def sanitize(message: str | None, limit: int = 300) -> str:
    return _SECRETS.sub("[redacted]", message or "")[:limit]


class DataApiError(Exception):
    def __init__(self, category: str, message: str, status: int | None = None, calls: list[dict] | None = None):
        super().__init__(message)
        self.category, self.message, self.status, self.calls = category, message, status, calls or []


@dataclass
class Fetch:
    rows: list[dict]
    calls: list[dict] = field(default_factory=list)


class TokenSource:
    """Workspace OAuth headers for the app service principal. The SDK's blocking token fetch runs in a worker
    thread (never on the event loop); headers are cached for max_age_s and refetched after a 401."""

    def __init__(self, fetch: Callable[[], dict[str, str]], *, max_age_s: float = 600.0,
                 clock: Callable[[], float] = time.monotonic):
        self._fetch, self._max_age, self._clock = fetch, max_age_s, clock
        self._headers: dict[str, str] | None = None
        self._at = float("-inf")
        self._lock: asyncio.Lock | None = None
        self.fetches = 0

    async def headers(self) -> dict[str, str]:
        if self._headers is not None and self._clock() - self._at < self._max_age:
            return self._headers
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._headers is None or self._clock() - self._at >= self._max_age:
                self.fetches += 1
                self._headers = dict(await asyncio.to_thread(self._fetch))
                self._at = self._clock()
        return self._headers

    def invalidate(self) -> None:
        self._headers = None


def sdk_token_fetch() -> dict[str, str]:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient().config.authenticate()


def _check(view: str, params: list[tuple[str, str]]) -> None:
    if view not in ALLOWED_VIEWS:
        raise DataApiError("not_allowed", f"view {view[:40]!r} is not exposed to the portal")
    for k, v in params:
        ok = ((k == "select" and _SELECT.match(v)) or (k == "order" and _ORDER.match(v))
              or (k == "limit" and v.isdigit()) or (k not in ("select", "order", "limit", "offset", "or", "and")
                                                   and _COLUMN.match(k) and _FILTER.match(v)))
        if not ok:
            raise DataApiError("not_allowed", f"parameter {k[:20]}={v[:40]} is not allowed")


def _qs(params: list[tuple[str, str]]) -> str:
    return "&".join(f"{quote(k, safe='_')}={quote(v, safe='(),.:_-')}" for k, v in params)


def _category(status: int) -> str:
    if status in (401, 403):
        return "permission"
    if status == 404:
        return "not_found"
    if status == 429:
        return "rate_limited"
    if status >= 500:
        return "server"
    return "bad_request"


class DataApiClient:
    def __init__(self, base_url: str, schema: str, tokens: TokenSource, http: httpx.AsyncClient, *,
                 page_size: int = 2000, max_pages: int = 10):
        self._base = base_url.rstrip("/")
        self._schema = schema
        self._tokens = tokens
        self._http = http
        self._page = int(page_size)
        self._max_pages = int(max_pages)

    def path(self, view: str, params: list[tuple[str, str]]) -> str:
        return f"/{self._schema}/{view}?{_qs(params)}"

    async def _one(self, path: str, calls: list[dict]) -> list[dict]:
        t0 = time.perf_counter()
        try:
            r = await self._send(path)
            if r.status_code == 401:  # token rotated: refetch off-loop once and retry
                self._tokens.invalidate()
                r = await self._send(path)
        except httpx.TimeoutException:
            calls.append({"path": path, "rows": None, "status": None, "ms": round((time.perf_counter() - t0) * 1000)})
            raise DataApiError("timeout", "the Data API did not answer in time (the database may be waking up; "
                               "try again)", None, calls) from None
        except httpx.HTTPError as e:
            calls.append({"path": path, "rows": None, "status": None, "ms": round((time.perf_counter() - t0) * 1000)})
            raise DataApiError("network", sanitize(f"{type(e).__name__}: {e}"), None, calls) from None
        ms = round((time.perf_counter() - t0) * 1000)
        if r.status_code != 200:
            calls.append({"path": path, "rows": None, "status": r.status_code, "ms": ms})
            raise DataApiError(_category(r.status_code), sanitize(f"HTTP {r.status_code}: {r.text}"), r.status_code,
                               calls)
        try:
            body = r.json()
        except ValueError:
            body = None
        if not isinstance(body, list):
            calls.append({"path": path, "rows": None, "status": r.status_code, "ms": ms})
            raise DataApiError("server", "unexpected Data API response (not a JSON array)", r.status_code, calls)
        calls.append({"path": path, "rows": len(body), "status": 200, "ms": ms})
        return body

    async def _send(self, path: str) -> httpx.Response:
        headers = {**await self._tokens.headers(), "Accept": "application/json"}
        return await self._http.get(self._base + path, headers=headers, timeout=REQUEST_TIMEOUT_S)

    async def get(self, view: str, params: list[tuple[str, str]]) -> Fetch:
        _check(view, params)
        calls: list[dict] = []
        if any(k == "limit" for k, _ in params):
            return Fetch(await self._one(self.path(view, params), calls), calls)
        rows: list[dict] = []
        for page in range(self._max_pages):
            got = await self._one(self.path(view, [*params, ("limit", str(self._page)),
                                                   ("offset", str(page * self._page))]), calls)
            rows.extend(got)
            if len(got) < self._page:
                return Fetch(rows, calls)
        raise DataApiError("too_many_rows", f"more than {self._max_pages * self._page} rows: narrow the date range "
                           "or the selection", 200, calls)

    async def aclose(self) -> None:
        await self._http.aclose()
