"""sql/lakebase_gold.sql sanity and hhportal_data.gold helpers."""

import re

import pytest

from hhportal_data import gold as pg

TEXT = pg.GOLD_SQL_PATH.read_text()
STMTS = pg.split_statements(TEXT)


def test_file_parses_into_ddl_statements_only():
    assert STMTS[0] == "CREATE SCHEMA IF NOT EXISTS portal_gold"
    assert STMTS[1] == "CREATE SCHEMA IF NOT EXISTS portal_work"
    allowed = ("CREATE SCHEMA IF NOT EXISTS ", "COMMENT ON ", "CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold.",
               "CREATE MATERIALIZED VIEW IF NOT EXISTS portal_work.",
               "CREATE OR REPLACE VIEW portal_gold.", "CREATE UNIQUE INDEX IF NOT EXISTS ", "CREATE INDEX IF NOT EXISTS ")
    for s in STMTS:
        assert s.startswith(allowed), s[:80]
    assert pg.sanity_issues(TEXT) == []


def test_expected_objects_exist():
    assert pg.materialized_views(STMTS) == [
        "directory", "account_directory", "org_directory", "daily_mpan", "daily_account", "daily_org",
        "monthly_mpan", "monthly_account", "monthly_org"]
    assert pg.plain_views(STMTS) == ["hh_mpan", "hh_account", "hh_org", "hourly_mpan", "hourly_account", "hourly_org"]


def test_every_materialized_view_has_a_unique_index_for_concurrent_refresh():
    uniq = pg.unique_indexes(STMTS)
    assert uniq["daily_mpan"] == ("mpan", "reading_date")
    assert uniq["daily_account"] == ("account_id", "reading_date")
    assert uniq["daily_org"] == ("org_id", "reading_date")
    assert uniq["monthly_org"] == ("org_id", "month")
    assert uniq["directory"] == ("mpan",)
    assert uniq["portal_work.hh_account_totals"] == ("account_id", "reading_ts")
    assert set(uniq) == set(pg.materialized_views(STMTS)) | {"portal_work.hh_account_totals"}


def test_account_and_org_peaks_are_peaks_of_summed_half_hours():
    bodies = pg.view_bodies(STMTS)
    assert pg.mv_objects(STMTS)[4] == ("portal_work", "hh_account_totals")
    totals = bodies["hh_account_totals"]
    assert "sum(r.consumption_kwh) AS kwh" in totals and "GROUP BY m.account_id, r.reading_date, r.reading_ts" in totals
    for name, key in (("daily_account", "account_id"), ("daily_org", "org_id")):
        b = bodies[name]
        assert "FROM portal_work.hh_account_totals" in b
        assert f"DISTINCT ON ({key}, reading_date)" in b
        assert f"ORDER BY {key}, reading_date, kwh DESC, reading_ts" in b
        assert "NULL::text AS peak_mpan" in b and "max(peak_kwh)" not in b
    assert "GROUP BY org_id, reading_date, reading_ts" in bodies["daily_org"]  # org half-hour = sum of accounts
    for name in ("monthly_account", "monthly_org"):  # a month's peak = highest of its daily (summed) peaks
        assert "max(peak_kwh) AS peak_kwh" in bodies[name] and "NULL::text AS peak_mpan" in bodies[name]
    assert "(array_agg(r.reading_ts ORDER BY r.consumption_kwh DESC, r.reading_ts))[1] AS peak_ts" in bodies["daily_mpan"]


def test_materialized_views_are_created_after_their_dependencies():
    mvs = pg.materialized_views(STMTS)
    bodies = {name: body for name, body in pg.view_bodies(STMTS).items()}
    for i, name in enumerate(mvs):
        for dep in re.findall(r"portal_gold\.(\w+)", bodies[name]):
            assert dep in mvs[:i], f"{name} reads {dep} before it is created"


def test_no_grants_no_writes_and_only_known_sources():
    for s in STMTS:
        assert not re.match(r"(GRANT|REVOKE|INSERT|UPDATE|DELETE|TRUNCATE|DROP|ALTER)\b", s.upper())
    sources = set(re.findall(r"\b(portal|bench|public)\.(\w+)", " ".join(STMTS)))
    assert sources == {("portal", "silver_hh_readings"), ("portal", "mpans"), ("portal", "sites"),
                       ("portal", "accounts"), ("portal", "organisations"), ("portal", "tariffs")}


def test_sanity_catches_a_missing_unique_index_and_a_grant():
    bad = TEXT.replace("CREATE UNIQUE INDEX IF NOT EXISTS daily_org_pk", "CREATE INDEX IF NOT EXISTS daily_org_pk")
    assert any("daily_org" in x for x in pg.sanity_issues(bad))
    bad = TEXT + "\nGRANT ALL ON ALL TABLES IN SCHEMA portal_gold TO someone;\n"
    assert any("GRANT" in x for x in pg.sanity_issues(bad))
    bad = TEXT + "\nCREATE MATERIALIZED VIEW IF NOT EXISTS public.x AS SELECT 1 WITH DATA;\n"
    assert any("outside schemas" in x for x in pg.sanity_issues(bad))
    bad = TEXT + "\nCREATE OR REPLACE VIEW portal_work.v AS SELECT 1;\n"
    assert any("must be in portal_gold" in x for x in pg.sanity_issues(bad))


def test_refresh_statements_follow_dependency_order_and_analyze():
    stmts = pg.refresh_statements(STMTS)
    assert stmts[0] == ("directory", "REFRESH MATERIALIZED VIEW CONCURRENTLY portal_gold.directory")
    names = [n for n, _ in stmts]
    assert names.index("daily_mpan") < names.index("portal_work.hh_account_totals") < names.index("daily_account") \
        < names.index("daily_org") < names.index("monthly_account")
    work = dict(stmts)["portal_work.hh_account_totals"]
    assert work == "REFRESH MATERIALIZED VIEW portal_work.hh_account_totals"  # internal: no CONCURRENTLY needed
    assert all(s.startswith("REFRESH MATERIALIZED VIEW CONCURRENTLY portal_gold.") for n, s in stmts if "." not in n)
    assert pg.analyze_statements(STMTS)[0] == "ANALYZE portal_gold.directory"
    assert "ANALYZE portal_work.hh_account_totals" in pg.analyze_statements(STMTS)
    assert pg.count_sql("daily_mpan") == "SELECT count(*) FROM portal_gold.daily_mpan"
    assert pg.count_sql("portal_work.hh_account_totals") == "SELECT count(*) FROM portal_work.hh_account_totals"


def test_rebuild_drops_in_reverse_dependency_order():
    drops = pg.drop_statements(STMTS)
    assert drops[0] == "DROP VIEW IF EXISTS portal_gold.hourly_org"
    assert drops.index("DROP MATERIALIZED VIEW IF EXISTS portal_gold.monthly_org") < drops.index(
        "DROP MATERIALIZED VIEW IF EXISTS portal_gold.daily_org")
    assert drops[-1] == "DROP MATERIALIZED VIEW IF EXISTS portal_gold.directory"
    assert not any("SCHEMA" in d or "CASCADE" in d for d in drops)


def test_split_ignores_comments_and_keeps_inline_semicolons_out():
    assert pg.split_statements("-- a; b\nSELECT 1;\n\n-- c\nSELECT 2;\n") == ["SELECT 1", "SELECT 2"]
    with pytest.raises(ValueError):
        pg.split_statements("SELECT 1; SELECT 2;\n")  # two statements on one line are not allowed


def test_directory_is_limited_to_mpans_in_the_replica():
    bodies = pg.view_bodies(STMTS)
    assert "WHERE EXISTS (SELECT 1 FROM portal.silver_hh_readings r WHERE r.mpan = m.mpan)" in bodies["directory"]
    for name in ("account_directory", "org_directory"):
        assert "FROM portal_gold.directory" in bodies[name]
