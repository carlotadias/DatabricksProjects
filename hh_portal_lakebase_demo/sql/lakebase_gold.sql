-- Gold layer of the HH portal demo, built INSIDE Lakebase Postgres. SYNTHETIC data only.
-- This file is the single source of truth for gold: `scripts/provision.py gold` applies it as the deployer, who
-- owns every object here, and the daily job refreshes the materialized views (REFRESH ... CONCURRENTLY, so each
-- one has a UNIQUE index on plain columns). Grants are NOT in this file: `scripts/provision.py grants` gives the
-- app's Postgres role USAGE on schema portal_gold and SELECT on its objects only. Schema portal_work holds an
-- internal materialized view (half-hour totals per account) that is neither granted nor exposed by the Data API;
-- it is refreshed without CONCURRENTLY because only the gold refresh reads it.
--
-- Sources (read-only synced tables, all in Postgres schema `portal`):
--   portal.silver_hh_readings  13 months of half-hourly readings (TRIGGERED synced table, refreshed daily)
--   portal.mpans, portal.sites, portal.accounts, portal.organisations, portal.tariffs  dimensions (SNAPSHOT)
--
-- Plain views read the silver replica through its (mpan, reading_date) index and are always fresh; filters
-- on their grouping columns (mpan / account_id / org_id, reading_date) are pushed below the GROUP BY.
-- Peak = the highest half-hourly consumption of the scope with its time: for an MPAN its own reading, for an
-- account or organisation the highest SUM over its MPANs at one half-hour (peak_mpan is NULL there).
-- Statements are separated by a semicolon at the end of a line; the file is idempotent (IF NOT EXISTS /
-- OR REPLACE). Changing a materialized view's definition needs `scripts/provision.py gold --rebuild`.

CREATE SCHEMA IF NOT EXISTS portal_gold;

CREATE SCHEMA IF NOT EXISTS portal_work;

COMMENT ON SCHEMA portal_gold IS 'SYNTHETIC HH portal demo gold. Exposed through the Data API.';

-- Directory: only MPANs that have readings in the replica, so no customer without data is listed;
-- account_directory and org_directory are built from it.

CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold.directory AS
SELECT m.mpan,
       m.site_id,
       s.address_line,
       s.postcode,
       s.region,
       a.account_id,
       a.account_name,
       o.org_id,
       o.org_name,
       o.is_tpi,
       m.tariff_id,
       m.meter_type
FROM portal.mpans m
JOIN portal.sites s ON s.site_id = m.site_id
JOIN portal.accounts a ON a.account_id = m.account_id
JOIN portal.organisations o ON o.org_id = a.org_id
WHERE EXISTS (SELECT 1 FROM portal.silver_hh_readings r WHERE r.mpan = m.mpan)
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS directory_pk ON portal_gold.directory (mpan);

CREATE INDEX IF NOT EXISTS directory_account_idx ON portal_gold.directory (account_id);

CREATE INDEX IF NOT EXISTS directory_org_idx ON portal_gold.directory (org_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold.account_directory AS
SELECT account_id,
       min(account_name) AS account_name,
       min(org_id) AS org_id,
       min(org_name) AS org_name,
       bool_or(is_tpi) AS is_tpi,
       count(DISTINCT site_id) AS sites,
       count(*) AS mpans
FROM portal_gold.directory
GROUP BY account_id
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS account_directory_pk ON portal_gold.account_directory (account_id);

CREATE INDEX IF NOT EXISTS account_directory_org_idx ON portal_gold.account_directory (org_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold.org_directory AS
SELECT org_id,
       min(org_name) AS org_name,
       bool_or(is_tpi) AS is_tpi,
       count(DISTINCT account_id) AS accounts,
       count(DISTINCT site_id) AS sites,
       count(*) AS mpans
FROM portal_gold.directory
GROUP BY org_id
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS org_directory_pk ON portal_gold.org_directory (org_id);

-- Daily per MPAN and half-hour totals per account (the two materialized views that scan the silver replica)

CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold.daily_mpan AS
WITH d AS (
  SELECT r.mpan,
         r.reading_date,
         sum(r.consumption_kwh) AS kwh,
         sum(r.cost_gbp) AS consumption_gbp,
         max(r.consumption_kwh) AS peak_kwh,
         (array_agg(r.reading_ts ORDER BY r.consumption_kwh DESC, r.reading_ts))[1] AS peak_ts,
         count(*) AS readings,
         count(*) FILTER (WHERE r.estimated) AS estimated_readings
  FROM portal.silver_hh_readings r
  GROUP BY r.mpan, r.reading_date
)
SELECT d.mpan,
       m.account_id,
       m.org_id,
       d.reading_date,
       d.kwh,
       d.consumption_gbp,
       coalesce(t.standing_charge_gbp_per_day, 0)::double precision AS standing_charge_gbp,
       d.consumption_gbp + coalesce(t.standing_charge_gbp_per_day, 0)::double precision AS total_gbp,
       d.peak_kwh,
       d.peak_ts,
       d.mpan AS peak_mpan,
       d.kwh / nullif(d.readings, 0) AS avg_kwh_per_reading,
       d.readings,
       d.estimated_readings
FROM d
JOIN portal.mpans m ON m.mpan = d.mpan
LEFT JOIN portal.tariffs t
  ON t.tariff_id = m.tariff_id
 AND d.reading_date >= t.valid_from
 AND (t.valid_to IS NULL OR d.reading_date < t.valid_to)
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS daily_mpan_pk ON portal_gold.daily_mpan (mpan, reading_date);

CREATE INDEX IF NOT EXISTS daily_mpan_account_idx ON portal_gold.daily_mpan (account_id, reading_date);

CREATE INDEX IF NOT EXISTS daily_mpan_org_idx ON portal_gold.daily_mpan (org_id, reading_date);

CREATE INDEX IF NOT EXISTS daily_mpan_date_idx ON portal_gold.daily_mpan (reading_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS portal_work.hh_account_totals AS
SELECT m.account_id,
       min(m.org_id) AS org_id,
       r.reading_date,
       r.reading_ts,
       sum(r.consumption_kwh) AS kwh
FROM portal.silver_hh_readings r
JOIN portal.mpans m ON m.mpan = r.mpan
GROUP BY m.account_id, r.reading_date, r.reading_ts
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS hh_account_totals_pk ON portal_work.hh_account_totals (account_id, reading_ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold.daily_account AS
WITH d AS (
  SELECT account_id,
         min(org_id) AS org_id,
         reading_date,
         count(*) AS mpans,
         sum(kwh) AS kwh,
         sum(consumption_gbp) AS consumption_gbp,
         sum(standing_charge_gbp) AS standing_charge_gbp,
         sum(total_gbp) AS total_gbp,
         sum(readings) AS readings,
         sum(estimated_readings) AS estimated_readings
  FROM portal_gold.daily_mpan
  GROUP BY account_id, reading_date
),
p AS (
  SELECT DISTINCT ON (account_id, reading_date)
         account_id,
         reading_date,
         kwh AS peak_kwh,
         reading_ts AS peak_ts
  FROM portal_work.hh_account_totals
  ORDER BY account_id, reading_date, kwh DESC, reading_ts
)
SELECT d.account_id,
       d.org_id,
       d.reading_date,
       d.mpans,
       d.kwh,
       d.consumption_gbp,
       d.standing_charge_gbp,
       d.total_gbp,
       p.peak_kwh,
       p.peak_ts,
       NULL::text AS peak_mpan,
       d.kwh / nullif(d.readings, 0) AS avg_kwh_per_reading,
       d.readings,
       d.estimated_readings
FROM d
LEFT JOIN p ON p.account_id = d.account_id AND p.reading_date = d.reading_date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS daily_account_pk ON portal_gold.daily_account (account_id, reading_date);

CREATE INDEX IF NOT EXISTS daily_account_org_idx ON portal_gold.daily_account (org_id, reading_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold.daily_org AS
WITH d AS (
  SELECT org_id,
         reading_date,
         count(*) AS mpans,
         sum(kwh) AS kwh,
         sum(consumption_gbp) AS consumption_gbp,
         sum(standing_charge_gbp) AS standing_charge_gbp,
         sum(total_gbp) AS total_gbp,
         sum(readings) AS readings,
         sum(estimated_readings) AS estimated_readings
  FROM portal_gold.daily_mpan
  GROUP BY org_id, reading_date
),
h AS (
  SELECT org_id,
         reading_date,
         reading_ts,
         sum(kwh) AS kwh
  FROM portal_work.hh_account_totals
  GROUP BY org_id, reading_date, reading_ts
),
p AS (
  SELECT DISTINCT ON (org_id, reading_date)
         org_id,
         reading_date,
         kwh AS peak_kwh,
         reading_ts AS peak_ts
  FROM h
  ORDER BY org_id, reading_date, kwh DESC, reading_ts
)
SELECT d.org_id,
       d.reading_date,
       d.mpans,
       d.kwh,
       d.consumption_gbp,
       d.standing_charge_gbp,
       d.total_gbp,
       p.peak_kwh,
       p.peak_ts,
       NULL::text AS peak_mpan,
       d.kwh / nullif(d.readings, 0) AS avg_kwh_per_reading,
       d.readings,
       d.estimated_readings
FROM d
LEFT JOIN p ON p.org_id = d.org_id AND p.reading_date = d.reading_date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS daily_org_pk ON portal_gold.daily_org (org_id, reading_date);

CREATE INDEX IF NOT EXISTS daily_org_date_idx ON portal_gold.daily_org (reading_date);

-- Monthly (from the daily materialized views; a month's peak is the highest of its daily peaks) ---------

CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold.monthly_mpan AS
SELECT mpan,
       min(account_id) AS account_id,
       min(org_id) AS org_id,
       date_trunc('month', reading_date)::date AS month,
       count(*) AS days,
       sum(kwh) AS kwh,
       sum(consumption_gbp) AS consumption_gbp,
       sum(standing_charge_gbp) AS standing_charge_gbp,
       sum(total_gbp) AS total_gbp,
       max(peak_kwh) AS peak_kwh,
       (array_agg(peak_ts ORDER BY peak_kwh DESC, peak_ts))[1] AS peak_ts,
       mpan AS peak_mpan,
       sum(kwh) / nullif(sum(readings), 0) AS avg_kwh_per_reading,
       sum(readings) AS readings,
       sum(estimated_readings) AS estimated_readings
FROM portal_gold.daily_mpan
GROUP BY mpan, date_trunc('month', reading_date)::date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS monthly_mpan_pk ON portal_gold.monthly_mpan (mpan, month);

CREATE INDEX IF NOT EXISTS monthly_mpan_account_idx ON portal_gold.monthly_mpan (account_id, month);

CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold.monthly_account AS
SELECT account_id,
       min(org_id) AS org_id,
       date_trunc('month', reading_date)::date AS month,
       count(*) AS days,
       sum(kwh) AS kwh,
       sum(consumption_gbp) AS consumption_gbp,
       sum(standing_charge_gbp) AS standing_charge_gbp,
       sum(total_gbp) AS total_gbp,
       max(peak_kwh) AS peak_kwh,
       (array_agg(peak_ts ORDER BY peak_kwh DESC NULLS LAST, peak_ts))[1] AS peak_ts,
       NULL::text AS peak_mpan,
       sum(kwh) / nullif(sum(readings), 0) AS avg_kwh_per_reading,
       sum(readings) AS readings,
       sum(estimated_readings) AS estimated_readings
FROM portal_gold.daily_account
GROUP BY account_id, date_trunc('month', reading_date)::date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS monthly_account_pk ON portal_gold.monthly_account (account_id, month);

CREATE INDEX IF NOT EXISTS monthly_account_org_idx ON portal_gold.monthly_account (org_id, month);

CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold.monthly_org AS
SELECT org_id,
       date_trunc('month', reading_date)::date AS month,
       count(*) AS days,
       sum(kwh) AS kwh,
       sum(consumption_gbp) AS consumption_gbp,
       sum(standing_charge_gbp) AS standing_charge_gbp,
       sum(total_gbp) AS total_gbp,
       max(peak_kwh) AS peak_kwh,
       (array_agg(peak_ts ORDER BY peak_kwh DESC NULLS LAST, peak_ts))[1] AS peak_ts,
       NULL::text AS peak_mpan,
       sum(kwh) / nullif(sum(readings), 0) AS avg_kwh_per_reading,
       sum(readings) AS readings,
       sum(estimated_readings) AS estimated_readings
FROM portal_gold.daily_org
GROUP BY org_id, date_trunc('month', reading_date)::date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS monthly_org_pk ON portal_gold.monthly_org (org_id, month);

-- Half-hourly and hourly (plain views over the silver replica) ------------------------------------------

CREATE OR REPLACE VIEW portal_gold.hh_mpan AS
SELECT r.mpan,
       r.reading_ts,
       r.reading_date,
       r.period,
       r.consumption_kwh AS kwh,
       r.cost_gbp,
       r.estimated
FROM portal.silver_hh_readings r;

CREATE OR REPLACE VIEW portal_gold.hh_account AS
SELECT m.account_id,
       r.reading_ts,
       r.reading_date,
       r.period,
       sum(r.consumption_kwh) AS kwh,
       sum(r.cost_gbp) AS cost_gbp,
       count(*) AS mpans
FROM portal.silver_hh_readings r
JOIN portal.mpans m ON m.mpan = r.mpan
GROUP BY m.account_id, r.reading_ts, r.reading_date, r.period;

CREATE OR REPLACE VIEW portal_gold.hh_org AS
SELECT m.org_id,
       r.reading_ts,
       r.reading_date,
       r.period,
       sum(r.consumption_kwh) AS kwh,
       sum(r.cost_gbp) AS cost_gbp,
       count(*) AS mpans
FROM portal.silver_hh_readings r
JOIN portal.mpans m ON m.mpan = r.mpan
GROUP BY m.org_id, r.reading_ts, r.reading_date, r.period;

CREATE OR REPLACE VIEW portal_gold.hourly_mpan AS
SELECT r.mpan,
       r.reading_date,
       (r.period - 1) / 2 AS hour,
       min(r.reading_ts) AS hour_ts,
       sum(r.consumption_kwh) AS kwh,
       sum(r.cost_gbp) AS cost_gbp,
       count(*) AS readings
FROM portal.silver_hh_readings r
GROUP BY r.mpan, r.reading_date, (r.period - 1) / 2;

CREATE OR REPLACE VIEW portal_gold.hourly_account AS
SELECT m.account_id,
       r.reading_date,
       (r.period - 1) / 2 AS hour,
       min(r.reading_ts) AS hour_ts,
       sum(r.consumption_kwh) AS kwh,
       sum(r.cost_gbp) AS cost_gbp,
       count(*) AS readings
FROM portal.silver_hh_readings r
JOIN portal.mpans m ON m.mpan = r.mpan
GROUP BY m.account_id, r.reading_date, (r.period - 1) / 2;

CREATE OR REPLACE VIEW portal_gold.hourly_org AS
SELECT m.org_id,
       r.reading_date,
       (r.period - 1) / 2 AS hour,
       min(r.reading_ts) AS hour_ts,
       sum(r.consumption_kwh) AS kwh,
       sum(r.cost_gbp) AS cost_gbp,
       count(*) AS readings
FROM portal.silver_hh_readings r
JOIN portal.mpans m ON m.mpan = r.mpan
GROUP BY m.org_id, r.reading_date, (r.period - 1) / 2;
