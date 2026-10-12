"""hhportal_data.silver: UC silver of the demo - date logic and SQL builders."""

from datetime import date

import pytest

from hhportal_data import silver as pd
from hhportal_data import synthetic as syn


def test_add_months_clamps_to_the_month_end_like_spark():
    assert pd.add_months(date(2026, 10, 5), -13) == date(2025, 9, 5)
    assert pd.add_months(date(2026, 3, 31), -1) == date(2026, 2, 28)
    assert pd.add_months(date(2024, 3, 31), -1) == date(2024, 2, 29)
    assert pd.add_months(date(2026, 1, 15), 1) == date(2026, 2, 15)
    assert pd.add_months(date(2026, 12, 31), 2) == date(2027, 2, 28)


def test_window_keeps_13_months_after_add_months():
    # rows with reading_date > add_months(max, -13) are kept: the first kept date is the day after
    assert pd.window_start(date(2026, 10, 5)) == date(2025, 9, 6)
    assert pd.window_start(date(2026, 10, 5), months=1) == date(2026, 9, 6)
    with pytest.raises(ValueError):
        pd.window_start(date(2026, 10, 5), months=0)


def test_dates_to_append_never_runs_ahead_of_yesterday():
    today = date(2026, 10, 9)
    assert pd.dates_to_append(date(2026, 10, 5), today) == [date(2026, 10, 6)]
    assert pd.dates_to_append(date(2026, 10, 5), today, max_days=7) == [
        date(2026, 10, 6), date(2026, 10, 7), date(2026, 10, 8)]
    assert pd.dates_to_append(date(2026, 10, 8), today) == []  # caught up: a re-run appends nothing
    assert pd.dates_to_append(date(2026, 10, 9), today) == []
    with pytest.raises(ValueError):
        pd.dates_to_append(date(2026, 10, 5), today, max_days=0)


def test_dates_to_append_skips_dates_already_present():
    # idempotency: a date that exists (e.g. a re-run after a partial failure) is never appended again
    got = pd.dates_to_append(date(2026, 10, 5), date(2026, 10, 9), max_days=3, present={date(2026, 10, 6)})
    assert got == [date(2026, 10, 7), date(2026, 10, 8)]  # max_days bounds the window, not the count


def test_silver_ddl_has_pk_cluster_and_change_data_feed():
    ddl = pd.create_silver_sql("cat", "demo")
    assert ddl.startswith("CREATE OR REPLACE TABLE `cat`.`demo`.`silver_hh_readings`")
    assert "CONSTRAINT silver_hh_readings_pk PRIMARY KEY (mpan, reading_ts)" in ddl
    assert "mpan STRING NOT NULL" in ddl and "reading_ts TIMESTAMP NOT NULL" in ddl
    assert "CLUSTER BY (mpan, reading_date)" in ddl
    assert "'delta.enableChangeDataFeed' = 'true'" in ddl and "SYNTHETIC" in ddl
    with pytest.raises(ValueError):
        pd.create_silver_sql("cat", "demo", months=0)
    with pytest.raises(ValueError):
        pd.create_silver_sql("cat", "bad name", months=13)


def test_dimension_ddl_and_keys():
    sql = pd.dimension_ddl("cat", "demo", "mpans", "SELECT 1")
    assert sql.startswith("CREATE OR REPLACE TABLE `cat`.`demo`.`mpans` COMMENT 'SYNTHETIC")
    assert sql.endswith(" AS SELECT 1")
    with pytest.raises(ValueError):
        pd.dimension_ddl("cat", "demo", "readings", "SELECT 1")
    assert set(pd.DIMENSION_KEYS) == set(syn.DIMENSIONS)
    assert pd.DIMENSION_KEYS["tariffs"] == ("tariff_id", "valid_from")
    assert pd.SYNCED_SCHEMA == "portal" and pd.GOLD_SCHEMA == "portal_gold"


def test_append_day_reuses_the_synthetic_load_shape():
    sql = pd.append_day_sql("cat", "samp", date(2026, 10, 6), n_mpans=1_000)
    assert sql.startswith("INSERT INTO `cat`.`samp`.`silver_hh_readings`")
    assert "FROM range(1000)" in sql
    assert syn.mpan("id") in sql  # mpan_idx recovered from the same bijection as the generator
    assert syn.consumption_expr() in sql  # same per-MPAN scale, base load, peak and season
    assert "DATE'2026-10-06'" in sql
    assert "sequence(1, 48)" in sql
    assert "t.valid_to IS NULL OR g.reading_date < t.valid_to" in sql  # priced with the tariff valid that day
    assert "WHERE NOT EXISTS" in sql  # never inserts a key that is already there
    with pytest.raises(ValueError):
        pd.append_day_sql("cat", "samp", date(2026, 10, 6), n_mpans=0)


def test_delete_and_count_sql():
    assert pd.delete_old_sql("cat", "samp", date(2025, 9, 6)) == (
        "DELETE FROM `cat`.`samp`.`silver_hh_readings` WHERE reading_date < DATE'2025-09-06'")
    assert "count(*) AS n" in pd.day_count_sql("cat", "samp", date(2026, 10, 6))
    assert "max(reading_date) AS max_date" in pd.max_date_sql("cat", "samp")


def test_refresh_log_ddl_and_rows():
    ddl = pd.refresh_log_ddl("cat", "app")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS `cat`.`app`.`portal_refresh_log`")
    for col in ("run_id STRING", "step STRING", "started TIMESTAMP", "ended TIMESTAMP", "rows BIGINT",
                "status STRING", "detail STRING"):
        assert col in ddl


def test_pipeline_id_from_synced_table_status():
    assert pd.pipeline_id({"status": {"pipeline_id": "p-1"}}) == "p-1"
    with pytest.raises(RuntimeError, match="no pipeline"):
        pd.pipeline_id({"status": {}})
