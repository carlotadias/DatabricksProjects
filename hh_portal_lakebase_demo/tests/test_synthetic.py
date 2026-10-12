"""hhportal_data.synthetic: sizes, identifiers and the dimension SQL of the SYNTHETIC data."""

from datetime import date

import pytest

from hhportal_data import synthetic as syn


def test_default_target_gives_whole_organisations_and_about_19m_readings():
    s = syn.Sizes.for_target(1000, days=396)
    assert (s.orgs, s.accounts, s.sites, s.mpans, s.days, s.tariffs) == (20, 200, 600, 1000, 396, 40)
    assert s.readings == 19_008_000 and s.issues() == []
    assert syn.Sizes() == s
    small = syn.Sizes.for_target(10, days=30)
    assert small.issues() == [] and small.orgs == 1 and small.mpans == 10
    with pytest.raises(ValueError):
        syn.Sizes.for_target(0, days=30)


def test_sizes_must_nest():
    assert syn.Sizes(orgs=10, accounts=5).issues()
    assert syn.Sizes(mpans=0).issues()


def test_numbered_ids_are_unique_with_prefix_and_six_digits():
    orgs = {syn.numbered_id_py(7919, 104729, i) for i in range(200_000)}
    accounts = {syn.numbered_id_py(104729, 7919, i) for i in range(200_000)}
    assert len(orgs) == len(accounts) == 200_000
    assert "concat('88', lpad(" in syn.org_id() and "concat('87', lpad(" in syn.account_id()


def test_mpans_are_unique_13_digit_numbers():
    values = [syn.mpan_py(i) for i in range(100_000)]
    assert len(set(values)) == len(values)
    assert all(len(str(v)) == 13 for v in values)


def test_parent_mapping_covers_every_parent():
    expr = syn.parent_idx(200, 5)
    assert expr.startswith("CASE WHEN id < 200 THEN id ELSE")


def test_consumption_expression_uses_shape_inputs_and_three_decimals():
    e = syn.consumption_expr()
    assert e.startswith("round(") and e.endswith(", 3)")
    for token in ("period", "reading_date", "mpan_idx", "noise", "dayofweek"):
        assert token in e



def test_dimension_selects_cover_every_table_and_nest_by_index():
    sel = syn.dimension_selects(syn.Sizes(), date(2025, 9, 13), date(2026, 3, 27))
    assert list(sel) == list(syn.DIMENSIONS)
    assert "FROM range(20)" in sel["organisations"] and syn.is_tpi() in sel["organisations"]
    assert "JOIN o ON o.org_idx = a.org_idx" in sel["accounts"] and "FROM range(200)" in sel["accounts"]
    assert syn.org_id("org_idx") in sel["accounts"]  # account -> organisation by the same bijection
    assert "JOIN a ON a.account_idx = s.account_idx" in sel["sites"] and "FROM range(600)" in sel["sites"]
    assert "FROM range(1000)" in sel["mpans"] and "'HH' AS meter_type" in sel["mpans"]
    assert syn.mpan() in sel["mpans"]  # the daily append recovers mpan_idx from this bijection
    assert "DATE'2024-09-13'" in sel["tariffs"] and "DATE'2026-03-27'" in sel["tariffs"]
    assert "FROM range(80)" in sel["tariffs"]  # two versions per tariff
    with pytest.raises(ValueError):
        syn.dimension_selects(syn.Sizes(orgs=10, accounts=5), date(2025, 1, 1), date(2025, 6, 1))


def test_tpi_flag_is_deterministic_so_the_demo_always_has_tpis():
    assert syn.is_tpi() == "(pmod(id, 5) = 1)"
    assert sum(1 for i in range(20) if i % 5 == 1) == 4


def test_date_range_covers_the_13_month_window():
    start, switch, days = syn.date_range(date(2026, 10, 11), 13)
    assert start == date(2025, 9, 12) and days == 395
    assert start < switch < date(2026, 10, 11)
