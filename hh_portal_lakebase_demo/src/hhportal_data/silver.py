"""UC silver of the demo and its daily simulated arrivals. Everything here is SYNTHETIC.

`<catalog>.<data_schema>.silver_hh_readings` holds the last HISTORY_MONTHS months of half-hourly readings with
Change Data Feed on, so a TRIGGERED synced table can replicate it incrementally into Lakebase schema `portal`.
The daily job appends the next simulated day (same load shape as the generator, `synthetic.consumption_expr`)
and deletes the rows that fall out of the window; both reach Lakebase through the Change Data Feed.

Kept apart from the jobs so the date logic and the SQL are unit-tested.
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta

from . import synthetic as syn
from .sqlnames import fqn

SILVER_TABLE = "silver_hh_readings"
HISTORY_MONTHS = 13
PRIMARY_KEY = ("mpan", "reading_ts")
REFRESH_LOG_TABLE = "portal_refresh_log"
MPAN_TARGET = 1000
SYNCED_SCHEMA = "portal"  # UC schema of the synced tables AND their Postgres schema (same name)
GOLD_SCHEMA = "portal_gold"  # Postgres schema of the gold views and materialized views (Data API)

# Dimension tables synced once (SNAPSHOT) next to the silver replica, with their primary keys.
# tariff_id is not unique: every tariff has two versions (valid_from tells them apart).
DIMENSION_KEYS: dict[str, tuple[str, ...]] = {
    "organisations": ("org_id",),
    "accounts": ("account_id",),
    "sites": ("site_id",),
    "tariffs": ("tariff_id", "valid_from"),
    "mpans": ("mpan",),
}
# Lookup indexes on the replica (primary keys are created by the sync).
REPLICA_INDEXES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (SILVER_TABLE, ("mpan", "reading_date")),
    (SILVER_TABLE, ("reading_date",)),
    ("mpans", ("account_id",)),
    ("mpans", ("org_id",)),
    ("accounts", ("org_id",)),
)


def add_months(d: date, months: int) -> date:
    """Spark add_months: same day of the target month, clamped to its last day."""
    y, m = divmod(d.month - 1 + months, 12)
    year, month = d.year + y, m + 1
    return date(year, month, min(d.day, calendar.monthrange(year, month)[1]))


def window_start(max_date: date, months: int = HISTORY_MONTHS) -> date:
    """First reading_date kept: rows with reading_date > add_months(max_date, -months)."""
    if months < 1:
        raise ValueError("months must be >= 1")
    return add_months(max_date, -months) + timedelta(days=1)


def dates_to_append(max_date: date, today: date, max_days: int = 1, present: set[date] | None = None) -> list[date]:
    """Days (max_date, min(yesterday, max_date + max_days)] not already present: the next simulated arrivals.

    Arrivals represent yesterday's meter readings, so a re-run on the same day after a success appends nothing.
    """
    if max_days < 1:
        raise ValueError("max_days must be >= 1")
    present = present or set()
    yesterday = today - timedelta(days=1)
    last = min(yesterday, max_date + timedelta(days=max_days))
    days = [max_date + timedelta(days=i) for i in range(1, (last - max_date).days + 1)]
    return [d for d in days if d not in present]


def create_silver_sql(catalog: str, schema: str, months: int = HISTORY_MONTHS) -> str:
    """Replace the silver table: primary key, liquid clustering and Change Data Feed (a TRIGGERED sync needs it)."""
    if int(months) < 1:
        raise ValueError("months must be >= 1")
    return f"""CREATE OR REPLACE TABLE {fqn(catalog, schema, SILVER_TABLE)} (
  mpan STRING NOT NULL,
  reading_date DATE NOT NULL,
  reading_ts TIMESTAMP NOT NULL,
  period INT,
  consumption_kwh DOUBLE,
  cost_gbp DOUBLE,
  estimated BOOLEAN,
  CONSTRAINT silver_hh_readings_pk PRIMARY KEY (mpan, reading_ts)
)
CLUSTER BY (mpan, reading_date)
COMMENT 'SYNTHETIC half-hourly readings, last {int(months)} months (HH portal Lakebase demo). Not customer data.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""


def dimension_ddl(catalog: str, schema: str, table: str, select: str) -> str:
    if table not in DIMENSION_KEYS:
        raise ValueError(f"unknown dimension {table!r}")
    return (f"CREATE OR REPLACE TABLE {fqn(catalog, schema, table)} "
            "COMMENT 'SYNTHETIC dimension (HH portal Lakebase demo). Not customer data.' AS " + select)


def append_day_sql(catalog: str, schema: str, day: date, n_mpans: int) -> str:
    """INSERT one simulated day (48 half-hours) for every MPAN, priced with the tariff valid that day.

    mpan_idx is recovered from the generator's bijection (`synthetic.mpan`) so each MPAN keeps its scale and
    base load; noise and the estimated flag are deterministic hashes (the generator's dbldatagen randoms are
    not reproducible per row). Inserts nothing if the day is already present.
    """
    if int(n_mpans) < 1:
        raise ValueError("n_mpans must be >= 1")
    silver = fqn(catalog, schema, SILVER_TABLE)
    mpans, tariffs = fqn(catalog, schema, "mpans"), fqn(catalog, schema, "tariffs")
    d = f"DATE'{day.isoformat()}'"
    return f"""INSERT INTO {silver}
WITH ids AS (
  SELECT id AS mpan_idx, {syn.mpan("id")} AS mpan
  FROM range({int(n_mpans)})
),
base AS (
  SELECT i.mpan_idx, m.mpan, m.tariff_id, {d} AS reading_date, CAST(s.p AS INT) AS period
  FROM ids i
  JOIN {mpans} m ON m.mpan = i.mpan
  CROSS JOIN (SELECT explode(sequence(1, 48)) AS p) s
),
noisy AS (
  SELECT base.*,
         0.85 + pmod(hash(mpan_idx, reading_date, period, 31), 301) / 1000.0 AS noise,
         pmod(hash(mpan_idx, reading_date, period, 37), 1000) < 25 AS estimated
  FROM base
),
g AS (
  SELECT noisy.*, {syn.consumption_expr()} AS consumption_kwh
  FROM noisy
)
SELECT g.mpan,
       g.reading_date,
       timestampadd(MINUTE, (g.period - 1) * 30, CAST(g.reading_date AS TIMESTAMP)) AS reading_ts,
       g.period,
       g.consumption_kwh,
       round(g.consumption_kwh * t.unit_rate_gbp_per_kwh, 4) AS cost_gbp,
       g.estimated
FROM g
JOIN {tariffs} t
  ON t.tariff_id = g.tariff_id
 AND g.reading_date >= t.valid_from
 AND (t.valid_to IS NULL OR g.reading_date < t.valid_to)
WHERE NOT EXISTS (SELECT 1 FROM {silver} x WHERE x.reading_date = {d})"""


def delete_old_sql(catalog: str, schema: str, first_kept: date) -> str:
    return f"DELETE FROM {fqn(catalog, schema, SILVER_TABLE)} WHERE reading_date < DATE'{first_kept.isoformat()}'"


def day_count_sql(catalog: str, schema: str, day: date) -> str:
    return f"SELECT count(*) AS n FROM {fqn(catalog, schema, SILVER_TABLE)} WHERE reading_date = DATE'{day.isoformat()}'"


def max_date_sql(catalog: str, schema: str) -> str:
    return f"SELECT max(reading_date) AS max_date, min(reading_date) AS min_date FROM {fqn(catalog, schema, SILVER_TABLE)}"


def refresh_log_ddl(catalog: str, schema: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {fqn(catalog, schema, REFRESH_LOG_TABLE)} (
  run_id STRING,
  step STRING,
  started TIMESTAMP,
  ended TIMESTAMP,
  rows BIGINT,
  status STRING,
  detail STRING
) COMMENT 'HH portal demo job step log: row counts and timings only, never data rows.'"""


def pipeline_id(synced_table: dict) -> str:
    """The synced table's Lakeflow pipeline (status.pipeline_id of get-synced-table)."""
    pid = ((synced_table or {}).get("status") or {}).get("pipeline_id")
    if not pid:
        raise RuntimeError("synced table has no pipeline id yet (is it created and provisioned?)")
    return str(pid)
