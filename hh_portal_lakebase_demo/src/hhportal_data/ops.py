"""Steps of the daily job that talk to Lakebase, written against small interfaces so they
are unit-tested with fakes: the synced-table REST read, the SDK's pipelines API and a DB-API connection.
"""

from __future__ import annotations

import re
import time
from typing import Callable

from . import gold as portal_gold
from .silver import pipeline_id

# Redacts tokens from error text before it is logged (same pattern as the app's dataapi.sanitize).
_SECRETS = re.compile(r"(dapi[0-9a-f]{16,}(-\d+)?|Bearer\s+[A-Za-z0-9._~+/=-]+|eyJ[A-Za-z0-9._-]{20,})")

TERMINAL_OK = {"COMPLETED"}
TERMINAL_BAD = {"FAILED", "CANCELED"}


def sanitize(message: str | None, limit: int = 300) -> str:
    return _SECRETS.sub("[redacted]", message or "")[:limit]


class StepFailed(RuntimeError):
    def __init__(self, message: str, rows: list[dict]):
        super().__init__(message)
        self.rows = rows


def _state(update) -> str:
    st = getattr(getattr(update, "update", update), "state", None)
    return str(getattr(st, "value", st) or "UNKNOWN")


def sync_silver(api_get: Callable[[str], dict], pipelines, synced_table_id: str, *,
                sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic,
                poll_s: float = 30.0, timeout_s: float = 4 * 3600) -> dict:
    """Start an incremental update of the TRIGGERED synced table's pipeline and wait until it ends.

    The pipeline id comes from get-synced-table (status.pipeline_id). If an update is already running (for
    example the initial snapshot), wait for that one instead of starting another.
    """
    st = api_get(f"/api/2.0/postgres/synced_tables/{synced_table_id}")
    pid = pipeline_id(st)
    try:
        update_id = pipelines.start_update(pipeline_id=pid, full_refresh=False).update_id
    except Exception as e:  # noqa: BLE001 - the SDK raises ResourceConflict for an active update
        text = str(e).lower()
        if "active update" not in text and "already" not in text and "conflict" not in text:
            raise
        update_id = pipelines.get(pipeline_id=pid).latest_updates[0].update_id
    started = clock()
    while True:
        state = _state(pipelines.get_update(pipeline_id=pid, update_id=update_id))
        if state in TERMINAL_OK:
            break
        if state in TERMINAL_BAD:
            raise RuntimeError(f"synced table pipeline {pid} update {update_id} ended {state}")
        if clock() - started >= timeout_s:
            raise RuntimeError(f"synced table pipeline {pid} update {update_id} not finished after {timeout_s} s "
                               f"(state {state})")
        sleep(poll_s)
    after = api_get(f"/api/2.0/postgres/synced_tables/{synced_table_id}")
    synced_state = str(((after or {}).get("status") or {}).get("detailed_state") or "UNKNOWN")
    return {"pipeline_id": pid, "update_id": update_id, "state": state, "synced_state": synced_state,
            "seconds": round(clock() - started, 1)}


def refresh_gold(conn, statements: list[str], *, clock: Callable[[], float] = time.time) -> list[dict]:
    """REFRESH each materialized view in dependency order (CONCURRENTLY for portal_gold), count rows, ANALYZE.

    Returns one log row per view (step, started, ended, rows, status, detail). Stops at the first failure
    and raises StepFailed carrying the rows so far (the failed one included).
    """
    rows: list[dict] = []
    conn.execute("SET statement_timeout = 0")
    for mv, stmt in portal_gold.refresh_statements(statements):
        t0 = clock()
        try:
            conn.execute(stmt)
            n = conn.execute(portal_gold.count_sql(mv)).fetchone()[0]
        except Exception as e:  # noqa: BLE001 - reported in the log, then re-raised as StepFailed
            rows.append({"step": f"refresh_gold:{mv}", "started": t0, "ended": clock(), "rows": None,
                         "status": "failed", "detail": sanitize(f"{type(e).__name__}: {e}", 1000)})
            raise StepFailed(f"refresh of {portal_gold.qualified(mv)} failed", rows) from e
        rows.append({"step": f"refresh_gold:{mv}", "started": t0, "ended": clock(), "rows": int(n),
                     "status": "ok", "detail": stmt})
    t0 = clock()
    for stmt in portal_gold.analyze_statements(statements):
        conn.execute(stmt)
    rows.append({"step": "analyze", "started": t0, "ended": clock(), "rows": None, "status": "ok",
                 "detail": f"ANALYZE {len(portal_gold.mv_objects(statements))} materialized views"})
    return rows


def lakebase_connect(w, endpoint_path: str, database: str = "databricks_postgres"):
    """psycopg connection as the calling identity (the job's run_as user), OAuth token kept in memory."""
    import psycopg

    host = w.postgres.get_endpoint(name=endpoint_path).status.hosts.host
    token = w.postgres.generate_database_credential(endpoint=endpoint_path).token
    user = w.current_user.me().user_name
    return psycopg.connect(host=host, user=user, password=token, dbname=database, sslmode="require",
                           autocommit=True, connect_timeout=60)
