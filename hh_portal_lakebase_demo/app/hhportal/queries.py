"""UI request -> PostgREST GET mapping for the HH portal demo. Pure functions, no I/O.

Every request is validated (id formats, ISO dates, enums, range caps) and mapped to ONE allow-listed view in
schema portal_gold with only `select`, `eq`/`in`/`gte`/`lte`/`is` filters and `order`. No SQL anywhere.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from enum import Enum


class ValidationError(ValueError):
    pass


class Scope(str, Enum):
    mpan = "mpan"
    account = "account"
    org = "org"


class Granularity(str, Enum):
    halfhour = "halfhour"
    hour = "hour"
    day = "day"
    month = "month"


KEY_COLUMN = {Scope.mpan: "mpan", Scope.account: "account_id", Scope.org: "org_id"}
ID_PATTERN = {Scope.mpan: re.compile(r"^\d{13}$"), Scope.account: re.compile(r"^87\d{6}$"),
              Scope.org: re.compile(r"^88\d{6}$")}
ID_LABEL = {Scope.mpan: "a 13-digit MPAN", Scope.account: "an account number (87 + 6 digits)",
            Scope.org: "an organisation number (88 + 6 digits)"}
MAX_MPANS = 20
# Longest range (days, inclusive) per granularity and scope: keeps every response within a few Data API pages.
MAX_DAYS = {
    Granularity.halfhour: {Scope.mpan: 31, Scope.account: 31, Scope.org: 7},
    Granularity.hour: {Scope.mpan: 92, Scope.account: 92, Scope.org: 31},
    Granularity.day: {Scope.mpan: 400, Scope.account: 400, Scope.org: 400},
    Granularity.month: {Scope.mpan: 400, Scope.account: 400, Scope.org: 400},
}
SLOTS_PER_DAY = {Granularity.halfhour: 48, Granularity.hour: 24}

DAILY_COLUMNS = ("reading_date,kwh,consumption_gbp,standing_charge_gbp,total_gbp,peak_kwh,peak_ts,peak_mpan,"
                 "avg_kwh_per_reading,readings")
MONTHLY_COLUMNS = ("month,kwh,consumption_gbp,standing_charge_gbp,total_gbp,peak_kwh,peak_ts,peak_mpan,"
                   "avg_kwh_per_reading,readings")

ALLOWED_VIEWS = frozenset({
    "directory", "account_directory", "org_directory",
    "hh_mpan", "hh_account", "hh_org", "hourly_mpan", "hourly_account", "hourly_org",
    "daily_mpan", "daily_account", "daily_org", "monthly_mpan", "monthly_account", "monthly_org",
})

Params = list[tuple[str, str]]


@dataclass(frozen=True)
class UsageRequest:
    scope: Scope
    ids: tuple[str, ...]
    granularity: Granularity
    start: date
    end: date


def _enum(cls, value: str, what: str):
    try:
        return cls(value)
    except ValueError:
        raise ValidationError(f"{what} must be one of {', '.join(x.value for x in cls)}") from None


def _date(value: str, what: str) -> date:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""):
        raise ValidationError(f"{what} must be a date YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValidationError(f"{what} is not a valid date") from None


def parse_usage(scope: str, ids: str, granularity: str, start: str, end: str) -> UsageRequest:
    sc = _enum(Scope, scope, "scope")
    gr = _enum(Granularity, granularity, "granularity")
    d0, d1 = _date(start, "from"), _date(end, "to")
    if d1 < d0:
        raise ValidationError("to must not be before from")
    span = (d1 - d0).days + 1
    cap = MAX_DAYS[gr][sc]
    if span > cap:
        raise ValidationError(f"{gr.value} data for one {sc.value} covers at most {cap} days (asked {span})")
    items = list(dict.fromkeys(x.strip() for x in (ids or "").split(",") if x.strip()))
    if not items:
        raise ValidationError("ids is required")
    for x in items:
        if not ID_PATTERN[sc].match(x):
            raise ValidationError(f"{x[:20]!r} is not {ID_LABEL[sc]}")
    limit = MAX_MPANS if sc is Scope.mpan else 1
    if len(items) > limit:
        raise ValidationError(f"at most {limit} {sc.value} id(s) per request")
    return UsageRequest(sc, tuple(items), gr, d0, d1)


def _key_filter(scope: Scope, ids: tuple[str, ...]) -> tuple[str, str]:
    col = KEY_COLUMN[scope]
    return (col, f"eq.{ids[0]}") if len(ids) == 1 else (col, f"in.({','.join(ids)})")


def _month_start(d: date) -> date:
    return d.replace(day=1)


def series_query(r: UsageRequest) -> tuple[str, Params]:
    """The bars: one view per (granularity, scope)."""
    key = _key_filter(r.scope, r.ids)
    s, e = r.start.isoformat(), r.end.isoformat()
    if r.granularity is Granularity.halfhour:
        return f"hh_{r.scope.value}", [("select", "reading_ts,reading_date,kwh,cost_gbp"), key,
                                       ("reading_date", f"gte.{s}"), ("reading_date", f"lte.{e}"),
                                       ("order", "reading_ts.asc")]
    if r.granularity is Granularity.hour:
        return f"hourly_{r.scope.value}", [("select", "hour_ts,reading_date,kwh,cost_gbp"), key,
                                           ("reading_date", f"gte.{s}"), ("reading_date", f"lte.{e}"),
                                           ("order", "hour_ts.asc")]
    if r.granularity is Granularity.day:
        return f"daily_{r.scope.value}", [("select", DAILY_COLUMNS), key, ("reading_date", f"gte.{s}"),
                                          ("reading_date", f"lte.{e}"), ("order", "reading_date.asc")]
    return f"monthly_{r.scope.value}", [("select", MONTHLY_COLUMNS), key,
                                        ("month", f"gte.{_month_start(r.start).isoformat()}"), ("month", f"lte.{e}"),
                                        ("order", "month.asc")]


def summary_query(r: UsageRequest) -> tuple[str, Params]:
    """Totals, standing charges and the peak for the period come from the daily gold (monthly for month views)."""
    if r.granularity is Granularity.month:
        return series_query(r)
    return series_query(UsageRequest(r.scope, r.ids, Granularity.day, r.start, r.end))


def _num(v) -> float:
    return float(v) if v is not None else 0.0


def combine_series(r: UsageRequest, series: list[dict], daily: list[dict]) -> list[dict]:
    """One row per bar: rows of several MPANs are summed; half-hourly and hourly bars get the day's standing
    charge spread evenly over the day's slots (48 or 24)."""
    if r.granularity in SLOTS_PER_DAY:
        tcol = "reading_ts" if r.granularity is Granularity.halfhour else "hour_ts"
        standing: dict[str, float] = {}
        for d in daily:
            standing[str(d["reading_date"])] = standing.get(str(d["reading_date"]), 0.0) + _num(
                d.get("standing_charge_gbp"))
        slots = SLOTS_PER_DAY[r.granularity]
        out: dict[str, dict] = {}
        for row in series:
            t = str(row[tcol])
            cur = out.setdefault(t, {"t": t, "kwh": 0.0, "consumption_gbp": 0.0,
                                     "standing_gbp": standing.get(str(row["reading_date"]), 0.0) / slots})
            cur["kwh"] += _num(row.get("kwh"))
            cur["consumption_gbp"] += _num(row.get("cost_gbp"))
        rows = sorted(out.values(), key=lambda x: x["t"])
        for x in rows:
            x["total_gbp"] = x["consumption_gbp"] + x["standing_gbp"]
        return rows
    tcol = "reading_date" if r.granularity is Granularity.day else "month"
    agg: dict[str, dict] = {}
    for row in series:
        t = str(row[tcol])
        cur = agg.setdefault(t, {"t": t, "kwh": 0.0, "consumption_gbp": 0.0, "standing_gbp": 0.0, "total_gbp": 0.0})
        cur["kwh"] += _num(row.get("kwh"))
        cur["consumption_gbp"] += _num(row.get("consumption_gbp"))
        cur["standing_gbp"] += _num(row.get("standing_charge_gbp"))
        cur["total_gbp"] += _num(row.get("total_gbp"))
    return sorted(agg.values(), key=lambda x: x["t"])


def summed_peak(bars: list[dict]) -> dict | None:
    """Peak of combined half-hourly bars (several MPANs summed per half-hour)."""
    best = max(bars, key=lambda b: (b["kwh"], [-ord(c) for c in b["t"]]), default=None)
    return {"kwh": best["kwh"], "ts": best["t"], "mpan": None, "kind": "summed"} if best else None


def apply_peak(r: UsageRequest, summary: dict, bars: list[dict]) -> dict:
    """Account / organisation peaks come from gold (peak of summed half-hours); a single MPAN's peak is its own
    reading. For several MPANs the gold rows only hold per-meter peaks: with half-hourly bars the peak of the
    summed half-hours is taken from them, otherwise the highest single meter reading is labelled as such."""
    if summary["peak"] is None:
        return summary
    if r.scope is Scope.mpan and len(r.ids) > 1:
        if r.granularity is Granularity.halfhour:
            summary["peak"] = summed_peak(bars)
        else:
            summary["peak"]["kind"] = "meter"
    else:
        summary["peak"]["kind"] = "summed"
    return summary


def summarise(rows: list[dict]) -> dict:
    """Period summary from daily (or monthly) gold rows, possibly of several MPANs (then see apply_peak)."""
    out = {"kwh": 0.0, "consumption_gbp": 0.0, "standing_gbp": 0.0, "total_gbp": 0.0, "readings": 0,
           "avg_kwh_per_reading": None, "peak": None, "days": 0, "first": None, "last": None}
    if not rows:
        return out
    dates = set()
    peak = None
    for row in rows:
        out["kwh"] += _num(row.get("kwh"))
        out["consumption_gbp"] += _num(row.get("consumption_gbp"))
        out["standing_gbp"] += _num(row.get("standing_charge_gbp"))
        out["total_gbp"] += _num(row.get("total_gbp"))
        out["readings"] += int(row.get("readings") or 0)
        dates.add(str(row.get("reading_date") or row.get("month")))
        if row.get("peak_kwh") is not None and (peak is None or float(row["peak_kwh"]) > peak["kwh"]):
            peak = {"kwh": float(row["peak_kwh"]), "ts": row.get("peak_ts"), "mpan": row.get("peak_mpan")}
    out["peak"] = peak
    out["avg_kwh_per_reading"] = out["kwh"] / out["readings"] if out["readings"] else None
    out["days"] = len(dates)
    out["first"], out["last"] = min(dates), max(dates)
    return out


def parse_lookup(number: str) -> tuple[str, str]:
    n = (number or "").strip()
    if ID_PATTERN[Scope.org].match(n):
        return "org", n
    if ID_PATTERN[Scope.account].match(n):
        return "account", n
    raise ValidationError("enter an organisation number (88 + 6 digits) or an account number (87 + 6 digits)")


ORG_COLUMNS = "org_id,org_name,is_tpi,accounts,sites,mpans"
ACCOUNT_COLUMNS = "account_id,account_name,org_id,org_name,is_tpi,sites,mpans"
DIRECTORY_COLUMNS = "mpan,site_id,address_line,postcode,region,account_id,account_name,org_id,org_name,meter_type"


def org_query(org_id: str) -> tuple[str, Params]:
    return "org_directory", [("select", ORG_COLUMNS), ("org_id", f"eq.{org_id}")]


def account_query(account_id: str) -> tuple[str, Params]:
    return "account_directory", [("select", ACCOUNT_COLUMNS), ("account_id", f"eq.{account_id}")]


def accounts_query(kind: str, value: str) -> tuple[str, Params]:
    col = "org_id" if kind == "org" else "account_id"
    return "account_directory", [("select", ACCOUNT_COLUMNS), (col, f"eq.{value}"), ("order", "account_name.asc")]


def directory_query(kind: str, value: str) -> tuple[str, Params]:
    col = "org_id" if kind == "org" else "account_id"
    return "directory", [("select", DIRECTORY_COLUMNS), (col, f"eq.{value}"), ("order", "account_id.asc,mpan.asc")]


def customers_query(tpi_only: bool = False) -> tuple[str, Params]:
    params = [("select", ORG_COLUMNS), ("order", "org_name.asc")]
    if tpi_only:
        params.append(("is_tpi", "is.true"))
    return "org_directory", params


def freshness_queries() -> list[tuple[str, Params]]:
    return [("daily_org", [("select", "reading_date"), ("order", "reading_date.desc"), ("limit", "1")]),
            ("daily_org", [("select", "reading_date"), ("order", "reading_date.asc"), ("limit", "1")])]


def facts_queries() -> list[tuple[str, Params]]:
    """Architecture page facts: latest day in gold, and the in-scope organisations with their counts."""
    return [("daily_mpan", [("select", "reading_date"), ("order", "reading_date.desc"), ("limit", "1")]),
            ("org_directory", [("select", "org_id,accounts,mpans"), ("order", "org_id.asc")]),
            ("directory", [("select", "mpan"), ("order", "mpan.asc"), ("limit", "1")])]


def health_query() -> tuple[str, Params]:
    return "org_directory", [("select", "org_id"), ("limit", "1")]
