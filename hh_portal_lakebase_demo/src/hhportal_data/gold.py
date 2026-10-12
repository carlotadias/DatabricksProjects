"""Helpers for the Lakebase gold layer of the demo (sql/lakebase_gold.sql).

The SQL file is the reviewable source of truth; this module splits it, checks it (DDL only, every materialized
view has a UNIQUE index so REFRESH ... CONCURRENTLY works, no grants) and derives the refresh, ANALYZE and
rebuild statements in dependency order (the file's order).
"""

from __future__ import annotations

import re
from pathlib import Path

GOLD_SCHEMA = "portal_gold"  # exposed through the Data API; refreshed CONCURRENTLY (readers never blocked)
WORK_SCHEMA = "portal_work"  # internal intermediates, not granted or exposed; refreshed without CONCURRENTLY
SCHEMAS = (GOLD_SCHEMA, WORK_SCHEMA)
GOLD_SQL_PATH = Path(__file__).resolve().parents[2] / "sql" / "lakebase_gold.sql"

_MV = re.compile(r"^CREATE MATERIALIZED VIEW IF NOT EXISTS (\w+)\.(\w+) AS\s+(.*?)(?:\s+WITH DATA)?$", re.S)
_VIEW = re.compile(r"^CREATE OR REPLACE VIEW (\w+)\.(\w+) AS\s+(.*)$", re.S)
_UNIQUE = re.compile(r"^CREATE UNIQUE INDEX IF NOT EXISTS \w+ ON (\w+)\.(\w+) \(([^)]*)\)$")
_ALLOWED = ("CREATE SCHEMA IF NOT EXISTS ", "COMMENT ON ", "CREATE MATERIALIZED VIEW IF NOT EXISTS ",
            "CREATE OR REPLACE VIEW ", "CREATE UNIQUE INDEX IF NOT EXISTS ", "CREATE INDEX IF NOT EXISTS ")


def split_statements(text: str) -> list[str]:
    """Statements end with a semicolon at the end of a line; full-line `--` comments are dropped."""
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("--")]
    for ln in lines:
        if ";" in ln.rstrip()[:-1]:
            raise ValueError(f"semicolon inside a line (one statement per line end): {ln.strip()[:80]}")
    body = "\n".join(lines)
    return [s.strip() for s in re.split(r";[ \t]*$", body, flags=re.M) if s.strip()]


def load(path: Path | None = None) -> list[str]:
    return split_statements((path or GOLD_SQL_PATH).read_text())


def mv_objects(statements: list[str]) -> list[tuple[str, str]]:
    """(schema, name) of every materialized view, in creation (= dependency) order."""
    return [(m.group(1), m.group(2)) for s in statements if (m := _MV.match(s))]


def materialized_views(statements: list[str]) -> list[str]:
    """The exposed (portal_gold) materialized views."""
    return [n for sch, n in mv_objects(statements) if sch == GOLD_SCHEMA]


def _label(schema: str, name: str) -> str:
    return name if schema == GOLD_SCHEMA else f"{schema}.{name}"


def plain_views(statements: list[str]) -> list[str]:
    return [m.group(2) for s in statements if (m := _VIEW.match(s))]


def view_bodies(statements: list[str]) -> dict[str, str]:
    out = {}
    for s in statements:
        m = _MV.match(s) or _VIEW.match(s)
        if m:
            out[m.group(2)] = m.group(3)
    return out


def unique_indexes(statements: list[str]) -> dict[str, tuple[str, ...]]:
    out = {}
    for s in statements:
        m = _UNIQUE.match(s)
        if m and m.group(1) in SCHEMAS:
            out[_label(m.group(1), m.group(2))] = tuple(c.strip() for c in m.group(3).split(","))
    return out


def sanity_issues(text: str) -> list[str]:
    try:
        stmts = split_statements(text)
    except ValueError as e:
        return [str(e)]
    issues = []
    for s in stmts:
        if not s.startswith(_ALLOWED):
            issues.append(f"not an allowed DDL statement (no GRANT/DML/DROP in the gold file): {s[:60]}")
        m = _MV.match(s) or _VIEW.match(s)
        if m and m.group(1) not in SCHEMAS:
            issues.append(f"{m.group(1)}.{m.group(2)} is outside schemas {', '.join(SCHEMAS)}")
        if (m := _VIEW.match(s)) and m.group(1) != GOLD_SCHEMA:
            issues.append(f"plain view {m.group(1)}.{m.group(2)} must be in {GOLD_SCHEMA}")
    uniq = unique_indexes(stmts)
    for sch, mv in mv_objects(stmts):
        if sch in SCHEMAS and _label(sch, mv) not in uniq:
            issues.append(f"materialized view {sch}.{mv} has no UNIQUE index (REFRESH CONCURRENTLY needs one)")
    return issues


def refresh_statements(statements: list[str]) -> list[tuple[str, str]]:
    """(label, REFRESH ...) in dependency order: CONCURRENTLY for exposed views, plain for portal_work."""
    out = []
    for sch, mv in mv_objects(statements):
        mode = "CONCURRENTLY " if sch == GOLD_SCHEMA else ""
        out.append((_label(sch, mv), f"REFRESH MATERIALIZED VIEW {mode}{sch}.{mv}"))
    return out


def analyze_statements(statements: list[str]) -> list[str]:
    return [f"ANALYZE {sch}.{mv}" for sch, mv in mv_objects(statements)]


def qualified(label: str) -> str:
    return label if "." in label else f"{GOLD_SCHEMA}.{label}"


def count_sql(mv: str) -> str:
    return f"SELECT count(*) FROM {qualified(mv)}"


def drop_statements(statements: list[str]) -> list[str]:
    """Drop every view and materialized view in reverse creation order (no CASCADE, the schema stays)."""
    out = []
    for s in reversed(statements):
        if m := _VIEW.match(s):
            out.append(f"DROP VIEW IF EXISTS {m.group(1)}.{m.group(2)}")
        elif m := _MV.match(s):
            out.append(f"DROP MATERIALIZED VIEW IF EXISTS {m.group(1)}.{m.group(2)}")
    return out
