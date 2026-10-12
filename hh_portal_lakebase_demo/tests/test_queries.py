"""hhportal.queries: UI request -> allow-listed PostgREST GET mapping and validation."""

from datetime import date

import pytest

from hhportal import queries as q

MPAN_A, MPAN_B = "1000000271828", "1007919271828"


def test_parse_usage_validates_ids_dates_and_enums():
    r = q.parse_usage("account", "87007919", "day", "2026-09-01", "2026-09-30")
    assert r == q.UsageRequest(q.Scope.account, ("87007919",), q.Granularity.day, date(2026, 9, 1), date(2026, 9, 30))
    bad = [
        ("account", "88007919", "day", "2026-09-01", "2026-09-30"),  # an org number is not an account
        ("org", "8800791", "day", "2026-09-01", "2026-09-30"),  # 7 digits
        ("mpan", "123", "day", "2026-09-01", "2026-09-30"),
        ("mpan", f"{MPAN_A},x;drop", "day", "2026-09-01", "2026-09-30"),
        ("site", "1", "day", "2026-09-01", "2026-09-30"),
        ("account", "87007919", "minute", "2026-09-01", "2026-09-30"),
        ("account", "87007919", "day", "2026-09-31", "2026-10-01"),
        ("account", "87007919", "day", "2026-10-02", "2026-10-01"),  # end before start
        ("account", "87007919,87112648", "day", "2026-09-01", "2026-09-30"),  # one account at a time
        ("account", "87007919", "halfhour", "2026-08-01", "2026-09-30"),  # too long for half-hourly
        ("org", "88104729", "halfhour", "2026-09-01", "2026-09-30"),  # org half-hourly is limited to 7 days
    ]
    for args in bad:
        with pytest.raises(q.ValidationError):
            q.parse_usage(*args)


def test_mpan_multi_select_is_bounded_and_deduplicated():
    r = q.parse_usage("mpan", f"{MPAN_A},{MPAN_B},{MPAN_A}", "day", "2026-09-01", "2026-09-07")
    assert r.ids == (MPAN_A, MPAN_B)
    many = ",".join(str(1_000_000_000_000 + i) for i in range(q.MAX_MPANS + 1))
    with pytest.raises(q.ValidationError, match="at most"):
        q.parse_usage("mpan", many, "day", "2026-09-01", "2026-09-07")


def test_series_query_maps_to_one_view_with_postgrest_filters():
    r = q.parse_usage("account", "87007919", "halfhour", "2026-10-01", "2026-10-01")
    view, params = q.series_query(r)
    assert view == "hh_account"
    assert params == [("select", "reading_ts,reading_date,kwh,cost_gbp"), ("account_id", "eq.87007919"),
                      ("reading_date", "gte.2026-10-01"), ("reading_date", "lte.2026-10-01"),
                      ("order", "reading_ts.asc")]
    r = q.parse_usage("mpan", f"{MPAN_A},{MPAN_B}", "day", "2026-09-01", "2026-09-30")
    view, params = q.series_query(r)
    assert view == "daily_mpan" and ("mpan", f"in.({MPAN_A},{MPAN_B})") in params
    r = q.parse_usage("org", "88104729", "month", "2025-10-15", "2026-09-30")
    view, params = q.series_query(r)
    assert view == "monthly_org" and ("month", "gte.2025-10-01") in params and ("month", "lte.2026-09-30") in params
    r = q.parse_usage("org", "88104729", "hour", "2026-09-01", "2026-09-02")
    assert q.series_query(r)[0] == "hourly_org"


def test_summary_query_uses_daily_or_monthly_gold():
    r = q.parse_usage("account", "87007919", "halfhour", "2026-10-01", "2026-10-01")
    view, params = q.summary_query(r)
    assert view == "daily_account" and ("reading_date", "gte.2026-10-01") in params
    assert params[0][1].startswith("reading_date,kwh,consumption_gbp,standing_charge_gbp,total_gbp,peak_kwh")
    r = q.parse_usage("org", "88104729", "month", "2025-10-01", "2026-09-30")
    assert q.summary_query(r)[0] == "monthly_org"


def test_every_view_used_is_in_the_allow_list():
    for scope in q.Scope:
        for gran in q.Granularity:
            r = q.UsageRequest(scope, ("x",), gran, date(2026, 9, 1), date(2026, 9, 1))
            assert q.series_query(r)[0] in q.ALLOWED_VIEWS
            assert q.summary_query(r)[0] in q.ALLOWED_VIEWS


def test_combine_series_sums_mpans_and_allocates_the_standing_charge():
    r = q.parse_usage("mpan", f"{MPAN_A},{MPAN_B}", "halfhour", "2026-10-01", "2026-10-01")
    series = [
        {"reading_ts": "2026-10-01T00:00:00+00:00", "reading_date": "2026-10-01", "kwh": 1.0, "cost_gbp": 0.25},
        {"reading_ts": "2026-10-01T00:00:00+00:00", "reading_date": "2026-10-01", "kwh": 2.0, "cost_gbp": 0.5},
        {"reading_ts": "2026-10-01T00:30:00+00:00", "reading_date": "2026-10-01", "kwh": 3.0, "cost_gbp": 0.75},
    ]
    daily = [{"reading_date": "2026-10-01", "kwh": 3.0, "standing_charge_gbp": 0.96},
             {"reading_date": "2026-10-01", "kwh": 3.0, "standing_charge_gbp": 1.44}]
    rows = q.combine_series(r, series, daily)
    assert [x["t"] for x in rows] == ["2026-10-01T00:00:00+00:00", "2026-10-01T00:30:00+00:00"]
    assert rows[0]["kwh"] == pytest.approx(3.0) and rows[0]["consumption_gbp"] == pytest.approx(0.75)
    assert rows[0]["standing_gbp"] == pytest.approx(2.4 / 48)
    assert rows[0]["total_gbp"] == pytest.approx(0.75 + 2.4 / 48)


def test_combine_series_daily_rows_carry_their_own_charges():
    r = q.parse_usage("account", "87007919", "day", "2026-09-01", "2026-09-02")
    daily = [{"reading_date": "2026-09-01", "kwh": 10.0, "consumption_gbp": 2.5, "standing_charge_gbp": 1.0,
              "total_gbp": 3.5}]
    rows = q.combine_series(r, daily, daily)
    assert rows == [{"t": "2026-09-01", "kwh": 10.0, "consumption_gbp": 2.5, "standing_gbp": 1.0, "total_gbp": 3.5}]


def test_summarise_totals_peak_and_average_per_reading():
    daily = [
        {"reading_date": "2026-09-01", "kwh": 10.0, "consumption_gbp": 2.0, "standing_charge_gbp": 1.0,
         "total_gbp": 3.0, "peak_kwh": 0.9, "peak_ts": "2026-09-01T12:30:00+00:00", "peak_mpan": MPAN_A,
         "readings": 48},
        {"reading_date": "2026-09-02", "kwh": 20.0, "consumption_gbp": 4.0, "standing_charge_gbp": 1.0,
         "total_gbp": 5.0, "peak_kwh": 1.4, "peak_ts": "2026-09-02T13:00:00+00:00", "peak_mpan": MPAN_B,
         "readings": 48},
    ]
    s = q.summarise(daily)
    assert s["kwh"] == 30.0 and s["total_gbp"] == 8.0 and s["standing_gbp"] == 2.0 and s["readings"] == 96
    assert s["peak"] == {"kwh": 1.4, "ts": "2026-09-02T13:00:00+00:00", "mpan": MPAN_B}
    assert s["avg_kwh_per_reading"] == pytest.approx(30 / 96)
    assert s["days"] == 2 and s["first"] == "2026-09-01" and s["last"] == "2026-09-02"
    assert q.summarise([])["kwh"] == 0 and q.summarise([])["peak"] is None


def test_lookup_numbers_and_directory_queries():
    assert q.parse_lookup(" 88104729 ") == ("org", "88104729")
    assert q.parse_lookup("87007919") == ("account", "87007919")
    for bad in ("", "8810472", "99104729", "88104729;", "1000000271828"):
        with pytest.raises(q.ValidationError):
            q.parse_lookup(bad)
    assert q.org_query("88104729") == ("org_directory", [
        ("select", "org_id,org_name,is_tpi,accounts,sites,mpans"), ("org_id", "eq.88104729")])
    view, params = q.directory_query("org", "88104729")
    assert view == "directory" and ("org_id", "eq.88104729") in params and ("order", "account_id.asc,mpan.asc") in params
    assert q.accounts_query("org", "88104729")[0] == "account_directory"
    assert q.customers_query(tpi_only=True)[1][-1] == ("is_tpi", "is.true")


def test_peak_kind_summed_for_accounts_and_from_bars_for_several_mpans():
    daily = [{"reading_date": "2026-10-01", "kwh": 10.0, "peak_kwh": 0.9, "peak_ts": "2026-10-01T12:00:00+00:00",
              "peak_mpan": MPAN_A, "readings": 48},
             {"reading_date": "2026-10-01", "kwh": 12.0, "peak_kwh": 1.1, "peak_ts": "2026-10-01T09:00:00+00:00",
              "peak_mpan": MPAN_B, "readings": 48}]
    acc = q.parse_usage("account", "87007919", "day", "2026-10-01", "2026-10-01")
    s = q.apply_peak(acc, q.summarise(daily[:1]), [])
    assert s["peak"]["kind"] == "summed"
    two = q.parse_usage("mpan", f"{MPAN_A},{MPAN_B}", "halfhour", "2026-10-01", "2026-10-01")
    bars = [{"t": "2026-10-01T09:00:00+00:00", "kwh": 1.5}, {"t": "2026-10-01T12:00:00+00:00", "kwh": 1.7},
            {"t": "2026-10-01T12:30:00+00:00", "kwh": 1.7}]
    s = q.apply_peak(two, q.summarise(daily), bars)
    assert s["peak"] == {"kwh": 1.7, "ts": "2026-10-01T12:00:00+00:00", "mpan": None, "kind": "summed"}
    two_daily = q.parse_usage("mpan", f"{MPAN_A},{MPAN_B}", "day", "2026-10-01", "2026-10-01")
    s = q.apply_peak(two_daily, q.summarise(daily), [])
    assert s["peak"]["kind"] == "meter" and s["peak"]["mpan"] == MPAN_B
    assert q.apply_peak(acc, q.summarise([]), [])["peak"] is None
