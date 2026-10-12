"""FastAPI backend of the HH portal demo: an allow-listed proxy to the Lakebase Data API (docs/ADR.md 0002).

No SQL and no Postgres driver: every /api route maps to one or two PostgREST GETs on schema portal_gold
(hhportal.queries) and returns the rows plus the exact Data API paths it used (`data_api`).
"""

from __future__ import annotations

import logging
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

from . import queries as q
from .dataapi import DataApiClient, DataApiError, TokenSource, sdk_token_fetch

log = logging.getLogger("hhportal")
STATIC_DIR = Path(__file__).resolve().parents[1] / "static"


@dataclass(frozen=True)
class Settings:
    data_api_url: str
    schema: str = "portal_gold"
    environment: str = "dev"
    page_size: int = 2000

    @property
    def configured(self) -> bool:
        return self.data_api_url.startswith("https://")

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(data_api_url=os.environ.get("HH_PORTAL_DATA_API_URL", "").strip(),
                   schema=os.environ.get("HH_PORTAL_SCHEMA", "portal_gold").strip() or "portal_gold",
                   environment=os.environ.get("HH_PORTAL_ENVIRONMENT", "dev"),
                   page_size=int(os.environ.get("HH_PORTAL_MAX_ROWS", "2000") or 2000))


_HOST = re.compile(r"^https://[A-Za-z0-9.-]+(:\d+)?/?$")
_ID = re.compile(r"^[A-Za-z0-9_-]{1,80}$")
_TAB = re.compile(r"^[a-z0-9_-]+(/[a-z0-9_-]+)*$")
_REPO = re.compile(r"^https://[A-Za-z0-9.-]+/[A-Za-z0-9._/~-]+$")
_SCHEDULE = re.compile(r"^[A-Za-z0-9 :/_+*?,.()-]{1,80}$")


def _env(name: str, pattern: re.Pattern) -> str | None:
    v = os.environ.get(name, "").strip()
    return v if v and v != "auto" and pattern.match(v) else None


def architecture_config(gold_schema: str, data_api_url: str = "") -> dict:
    """Bundle-supplied identifiers for the Architecture page links (never secrets); unsafe values are dropped."""
    host = _env("HH_PORTAL_WORKSPACE_HOST", _HOST)
    if host is None:  # Databricks Apps set DATABRICKS_HOST, sometimes without the scheme
        raw = os.environ.get("DATABRICKS_HOST", "").strip()
        raw = raw if raw.startswith("https://") or not raw else f"https://{raw}"
        host = raw if _HOST.match(raw) else None
    return {
        "host": host.rstrip("/") if host else None,
        "catalog": _env("HH_PORTAL_CATALOG", _ID), "data_schema": _env("HH_PORTAL_DATA_SCHEMA", _ID),
        "synced_schema": _env("HH_PORTAL_SYNCED_SCHEMA", _ID) or "portal",
        "gold_schema": gold_schema, "sync_pipeline_id": _env("HH_PORTAL_SYNC_PIPELINE_ID", _ID),
        "daily_job_id": _env("HH_PORTAL_DAILY_JOB_ID", _ID), "setup_job_id": _env("HH_PORTAL_SETUP_JOB_ID", _ID),
        "lakebase_project": _env("HH_PORTAL_LAKEBASE_PROJECT", _ID),
        "lakebase_project_uid": _env("HH_PORTAL_LAKEBASE_PROJECT_UID", _ID),
        "lakebase_branch_uid": _env("HH_PORTAL_LAKEBASE_BRANCH_UID", _ID),
        "lakebase_data_api_tab": _env("HH_PORTAL_LAKEBASE_DATA_API_TAB", _TAB) or "data-api",
        "data_api_url": data_api_url if data_api_url.startswith("https://") else None,
        "lakebase_branch": _env("HH_PORTAL_LAKEBASE_BRANCH", _ID), "repo_url": _env("HH_PORTAL_REPO_URL", _REPO),
        "daily_schedule": _env("HH_PORTAL_DAILY_SCHEDULE", _SCHEDULE),
    }


def _identity(request: Request) -> str | None:
    h = request.headers
    return h.get("x-forwarded-email") or h.get("x-forwarded-preferred-username") or h.get("x-forwarded-user")


def create_app(settings: Settings | None = None, client: DataApiClient | None = None,
               static_dir: Path = STATIC_DIR) -> FastAPI:
    settings = settings or Settings.from_env()
    state: dict = {"client": client}

    @asynccontextmanager
    async def lifespan(_app):
        yield
        if state["client"] is not None:
            await state["client"].aclose()

    app = FastAPI(title="Energy portal (demo)", lifespan=lifespan)

    def api() -> DataApiClient:
        if not settings.configured:
            raise HTTPException(503, {"error": "not_configured",
                                      "message": "HH_PORTAL_DATA_API_URL is not set (set the lakebase_data_api_url bundle variable)"})
        if state["client"] is None:
            state["client"] = DataApiClient(settings.data_api_url, settings.schema, TokenSource(sdk_token_fetch),
                                            httpx.AsyncClient(), page_size=settings.page_size)
        return state["client"]

    @app.exception_handler(q.ValidationError)
    async def _invalid(_req, e: q.ValidationError):
        return JSONResponse({"errors": [str(e)]}, status_code=400)

    @app.exception_handler(DataApiError)
    async def _upstream(_req, e: DataApiError):
        log.warning("Data API %s: %s", e.category, e.message)
        code = 504 if e.category == "timeout" else 413 if e.category == "too_many_rows" else 400 \
            if e.category == "not_allowed" else 502
        return JSONResponse({"error": e.category, "message": e.message, "data_api": e.calls}, status_code=code)

    @app.get("/api/me")
    def me(request: Request) -> dict:
        return {"identity": _identity(request), "environment": settings.environment, "schema": settings.schema,
                "data_api_configured": settings.configured, "synthetic": True}

    @app.get("/api/health")
    async def health() -> dict:
        res = await api().get(*q.health_query())
        return {"ok": True, "data_api": res.calls}

    @app.get("/api/architecture")
    def architecture() -> dict:
        return architecture_config(settings.schema, settings.data_api_url)

    @app.get("/api/facts")
    async def facts() -> dict:
        (v1, p1), (v2, p2), (v3, p3) = q.facts_queries()
        last, orgs, sample = await api().get(v1, p1), await api().get(v2, p2), await api().get(v3, p3)
        calls = last.calls + orgs.calls + sample.calls
        return {"last_date": last.rows[0]["reading_date"] if last.rows else None, "orgs": len(orgs.rows),
                "accounts": sum(int(o.get("accounts") or 0) for o in orgs.rows),
                "mpans": sum(int(o.get("mpans") or 0) for o in orgs.rows),
                "sample_mpan": sample.rows[0]["mpan"] if sample.rows else None,
                "latency_ms": sum(c["ms"] for c in calls), "data_api": calls}

    @app.get("/api/freshness")
    async def freshness() -> dict:
        (v1, p1), (v2, p2) = q.freshness_queries()
        last, first = await api().get(v1, p1), await api().get(v2, p2)
        return {"last_date": (last.rows[0]["reading_date"] if last.rows else None),
                "first_date": (first.rows[0]["reading_date"] if first.rows else None),
                "data_api": last.calls + first.calls}

    @app.get("/api/customers")
    async def customers(tpi: bool = False) -> dict:
        res = await api().get(*q.customers_query(tpi_only=tpi))
        return {"customers": res.rows, "data_api": res.calls}

    @app.get("/api/customer")
    async def customer(org_id: str) -> dict:
        _, org = q.parse_lookup(org_id)
        if not org.startswith("88"):
            raise q.ValidationError("org_id must be an organisation number (88 + 6 digits)")
        o = await api().get(*q.org_query(org))
        if not o.rows:
            raise HTTPException(404, {"error": "not_found", "message": f"no organisation {org}", "data_api": o.calls})
        accounts = await api().get(*q.accounts_query("org", org))
        meters = await api().get(*q.directory_query("org", org))
        return {"org": o.rows[0], "accounts": accounts.rows, "mpans": meters.rows,
                "data_api": o.calls + accounts.calls + meters.calls}

    @app.get("/api/lookup")
    async def lookup(number: str) -> dict:
        kind, value = q.parse_lookup(number)
        res = await api().get(*(q.org_query(value) if kind == "org" else q.account_query(value)))
        if not res.rows:
            raise HTTPException(404, {"error": "not_found", "message": f"no {'organisation' if kind == 'org' else 'account'} "
                                      f"{value}", "data_api": res.calls})
        row = res.rows[0]
        return {"kind": kind, "org_id": row["org_id"], "org_name": row["org_name"],
                "account_id": row.get("account_id") if kind == "account" else None,
                "account_name": row.get("account_name") if kind == "account" else None, "data_api": res.calls}

    @app.get("/api/usage")
    async def usage(scope: str, ids: str, granularity: str, start: str, end: str) -> dict:
        r = q.parse_usage(scope, ids, granularity, start, end)
        sv, sp = q.series_query(r)
        mv, mp = q.summary_query(r)
        summary_rows = await api().get(mv, mp)
        series_rows = summary_rows if (sv, sp) == (mv, mp) else await api().get(sv, sp)
        calls = summary_rows.calls + ([] if series_rows is summary_rows else series_rows.calls)
        bars = q.combine_series(r, series_rows.rows, summary_rows.rows)
        return {"request": {"scope": r.scope.value, "ids": list(r.ids), "granularity": r.granularity.value,
                            "start": r.start.isoformat(), "end": r.end.isoformat()},
                "rows": bars, "summary": q.apply_peak(r, q.summarise(summary_rows.rows), bars), "data_api": calls}

    if static_dir.exists():
        @app.get("/{path:path}", include_in_schema=False)
        def spa(path: str) -> FileResponse:
            if path.startswith("api/"):
                raise HTTPException(404)
            root = static_dir.resolve()
            candidate = (static_dir / path).resolve()
            if path and candidate.is_file() and candidate.is_relative_to(root) and candidate.name != "index.html":
                return FileResponse(candidate)
            return FileResponse(root / "index.html", headers={"Cache-Control": "no-cache, no-store, must-revalidate"})

    return app
