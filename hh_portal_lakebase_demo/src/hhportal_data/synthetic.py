"""SQL for the SYNTHETIC half-hourly energy data of the demo. Everything is fabricated.

Kept apart from the jobs so the identifier arithmetic, sizes and SQL are unit-tested. Ids are
bijections of a row index (unique by construction); parents are skewed so a few
organisations, accounts and sites are much larger than the rest, and every parent has
at least one child (child i < n_parents maps to parent i).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

REGIONS = [
    "North Scotland", "South Scotland", "North West", "North East", "Yorkshire",
    "East Midlands", "West Midlands", "East England", "London", "South East",
    "Southern", "South West", "South Wales", "North Wales and Merseyside",
]
POSTCODE_AREAS = ["AB", "B", "BS", "CF", "EH", "G", "L", "LS", "M", "NE", "NG", "S", "SO", "SW", "WV", "YO"]
STREETS = ["High", "Station", "Mill", "Church", "Park", "Victoria", "Market", "Bridge", "Canal", "Quarry"]
STREET_TYPES = ["Street", "Road", "Lane", "Way", "Industrial Estate", "Business Park"]
ORG_WORDS = ["Alder", "Birch", "Cedar", "Elm", "Hazel", "Juniper", "Larch", "Maple", "Rowan", "Willow"]
ORG_TRADES = ["Foods", "Logistics", "Retail", "Estates", "Engineering", "Hospitality", "Care", "Leisure"]
ORG_SUFFIXES = ["Ltd", "Group", "Holdings", "Services", "plc"]
ACCOUNT_LABELS = ["Head Office", "Retail Estate", "Distribution", "Offices", "Depots", "Northern Sites",
                  "Southern Sites"]

_MPAN_BASE = 1_000_000_000_000  # 13 digits
_MPAN_SPAN = 9_000_000_000_000
_MPAN_MULT = 7919 * 1_000_003  # coprime with the span (no factor 2, 3 or 5)


@dataclass(frozen=True)
class Sizes:
    """Hierarchy sizes. Every parent gets at least one child, so all MPANs belong to whole organisations."""

    orgs: int = 20
    accounts: int = 200
    sites: int = 600
    mpans: int = 1_000
    days: int = 396
    tariffs: int = 40

    @classmethod
    def for_target(cls, mpan_target: int, days: int) -> "Sizes":
        """About 50 MPANs per organisation, 5 per account and 1.67 per site (the default 1,000 MPANs give 20
        organisations, 200 accounts and 600 sites)."""
        n = int(mpan_target)
        if n < 1:
            raise ValueError("mpan_target must be >= 1")
        return cls(orgs=max(1, n // 50), accounts=max(1, n // 5), sites=max(1, n * 3 // 5), mpans=n,
                   days=int(days), tariffs=max(1, min(40, n // 25)))

    @property
    def readings(self) -> int:
        return self.mpans * self.days * 48

    def issues(self) -> list[str]:
        out = []
        if min(self.orgs, self.accounts, self.sites, self.mpans, self.days, self.tariffs) < 1:
            out.append("all sizes must be >= 1")
        if not self.orgs <= self.accounts <= self.sites <= self.mpans:
            out.append("sizes must satisfy orgs <= accounts <= sites <= mpans (every parent gets a child)")
        if max(self.orgs, self.accounts) > 1_000_000:
            out.append("organisations and accounts must be <= 1,000,000 (6-digit numbers)")
        return out


def arr(values: list[str]) -> str:
    return "array(" + ", ".join("'" + v.replace("'", "") + "'" for v in values) + ")"


def pick(values: list[str], seed: int, col: str = "id") -> str:
    return f"element_at({arr(values)}, cast(pmod(hash({col}, {seed}), {len(values)}) AS INT) + 1)"


def unit(seed: int, col: str = "id") -> str:
    """Deterministic pseudo-uniform value in [0, 1)."""
    return f"(pmod(hash({col}, {seed}), 1000003) / 1000003.0)"


def parent_idx(n_parents: int, seed: int, col: str = "id") -> str:
    return (f"CASE WHEN {col} < {n_parents} THEN {col} "
            f"ELSE CAST(floor({n_parents} * pow({unit(seed, col)}, 2.0)) AS BIGINT) END")


def numbered_id(prefix: str, mult: int, offset: int, col: str = "id") -> str:
    """prefix + 6 digits, a bijection of col for col < 1,000,000 (mult coprime with 10)."""
    return f"concat('{prefix}', lpad(cast(pmod({col} * {mult} + {offset}, 1000000) AS STRING), 6, '0'))"


def org_id(col: str = "id") -> str:
    return numbered_id("88", 7919, 104729, col)


def account_id(col: str = "id") -> str:
    return numbered_id("87", 104729, 7919, col)


def mpan(col: str = "id") -> str:
    return f"cast({_MPAN_BASE} + pmod({col} * {_MPAN_MULT} + 271828, {_MPAN_SPAN}) AS STRING)"


def numbered_id_py(mult: int, offset: int, i: int) -> int:
    return (i * mult + offset) % 1_000_000


def mpan_py(i: int) -> int:
    return _MPAN_BASE + (i * _MPAN_MULT + 271828) % _MPAN_SPAN


def consumption_expr() -> str:
    """kWh per half hour: per-MPAN scale and base load, business-hours peak, weekend dip, winter uplift."""
    hour = "((period - 1) / 2.0)"
    scale = f"pow(10, {unit(17, 'mpan_idx')} * 1.4 - 0.6)"  # about 0.25 to 6.3 kWh per half hour at peak
    base = f"(0.10 + 0.45 * {unit(19, 'mpan_idx')})"  # overnight base load share
    weekday = f"exp(-pow(({hour} - 12.5) / 3.5, 2))"
    weekend = f"0.35 * exp(-pow(({hour} - 13.0) / 4.0, 2))"
    peak = f"(CASE WHEN dayofweek(reading_date) IN (1, 7) THEN {weekend} ELSE {weekday} END)"
    season = "(1 + 0.15 * cos(2 * pi() * (dayofyear(reading_date) - 15) / 365.0))"
    return f"round({scale} * ({base} + (1 - {base}) * {peak}) * {season} * noise, 3)"


def is_tpi(col: str = "id") -> str:
    """Every fifth organisation (1, 6, 11, ...) is a TPI (third-party intermediary), so the demo always has some."""
    return f"(pmod({col}, 5) = 1)"


def _orgs_cte(s: Sizes) -> str:
    name = f"concat({pick(ORG_WORDS, 1)}, ' ', {pick(ORG_TRADES, 2)}, ' ', {pick(ORG_SUFFIXES, 3)})"
    return (f"SELECT id AS org_idx, {org_id()} AS org_id, {name} AS org_name, {is_tpi()} AS is_tpi "
            f"FROM range({int(s.orgs)})")


def _accounts_cte(s: Sizes) -> str:
    return (f"SELECT x.*, {org_id('org_idx')} AS org_id FROM (SELECT id AS account_idx, {account_id()} AS account_id, "
            f"{parent_idx(s.orgs, 5)} AS org_idx, {pick(ACCOUNT_LABELS, 6)} AS label "
            f"FROM range({int(s.accounts)})) x")


def _sites_cte(s: Sizes) -> str:
    address = f"concat(cast(1 + pmod(hash(id, 8), 250) AS STRING), ' ', {pick(STREETS, 9)}, ' ', {pick(STREET_TYPES, 10)})"
    postcode = (f"concat({pick(POSTCODE_AREAS, 11)}, cast(1 + pmod(hash(id, 12), 30) AS STRING), ' ', "
                "cast(pmod(hash(id, 13), 10) AS STRING), "
                "element_at(array('AB','BD','EF','GH','JL','NP','QR','SU','WX','YZ'), "
                "cast(pmod(hash(id, 14), 10) AS INT) + 1))")
    return (f"SELECT id AS site_idx, concat('ST', lpad(cast(id AS STRING), 7, '0')) AS site_id, "
            f"{parent_idx(s.accounts, 7)} AS account_idx, {address} AS address_line, {postcode} AS postcode, "
            f"{pick(REGIONS, 15)} AS region FROM range({int(s.sites)})")


def _mpans_cte(s: Sizes) -> str:
    return (f"SELECT id AS mpan_idx, {mpan()} AS mpan, {parent_idx(s.sites, 16)} AS site_idx, "
            f"pmod(hash(id, 18), {int(s.tariffs)}) AS tariff_idx FROM range({int(s.mpans)})")


def tariff_id(col: str = "tariff_idx") -> str:
    return f"concat('TAR', lpad(cast({col} AS STRING), 3, '0'))"


DIMENSIONS = ("organisations", "accounts", "sites", "tariffs", "mpans")


def dimension_selects(s: Sizes, start: date, switch: date) -> dict[str, str]:
    """SELECT for each dimension table. Tariffs have two versions: v0 until `switch`, v1 from it."""
    if s.issues():
        raise ValueError("; ".join(s.issues()))
    o, a, si, m = _orgs_cte(s), _accounts_cte(s), _sites_cte(s), _mpans_cte(s)
    return {
        "organisations": f"WITH o AS ({o}) SELECT org_id, org_name, is_tpi FROM o",
        "accounts": (f"WITH o AS ({o}), a AS ({a}) SELECT a.account_id, a.org_id, concat(o.org_name, ' - ', a.label, ' ', "
                     "lpad(cast(a.account_idx AS STRING), 4, '0')) AS account_name FROM a JOIN o ON o.org_idx = a.org_idx"),
        "sites": (f"WITH a AS ({a}), s AS ({si}) SELECT s.site_id, a.account_id, s.address_line, s.postcode, s.region "
                  "FROM s JOIN a ON a.account_idx = s.account_idx"),
        "tariffs": (f"SELECT {tariff_id()} AS tariff_id, "
                    "round(0.18 + pmod(hash(tariff_idx, v, 21), 1401) / 10000.0, 4) AS unit_rate_gbp_per_kwh, "
                    "round(0.40 + pmod(hash(tariff_idx, v, 22), 2101) / 1000.0, 4) AS standing_charge_gbp_per_day, "
                    f"CASE WHEN v = 0 THEN DATE'{start - timedelta(days=365)}' ELSE DATE'{switch}' END AS valid_from, "
                    f"CASE WHEN v = 0 THEN DATE'{switch}' ELSE CAST(NULL AS DATE) END AS valid_to "
                    f"FROM (SELECT pmod(id, {int(s.tariffs)}) AS tariff_idx, floor(id / {int(s.tariffs)}) AS v "
                    f"FROM range({int(s.tariffs) * 2}))"),
        "mpans": (f"WITH a AS ({a}), s AS ({si}), m AS ({m}) SELECT m.mpan, s.site_id, a.account_id, a.org_id, "
                  f"'HH' AS meter_type, {tariff_id('m.tariff_idx')} AS tariff_id "
                  "FROM m JOIN s ON s.site_idx = m.site_idx JOIN a ON a.account_idx = s.account_idx"),
    }


def date_range(end: date, months: int) -> tuple[date, date, int]:
    """(start, tariff switch date, days) for history ending at `end` and covering `months` months."""
    from .silver import window_start

    start = window_start(end, months)
    days = (end - start).days + 1
    return start, start + timedelta(days=days // 2), days
