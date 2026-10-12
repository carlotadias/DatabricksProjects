"""Setup job task: generate the SYNTHETIC data and build UC silver (Change Data Feed on). All values are fabricated.

Hierarchy organisations -> accounts -> sites -> MPANs (whole organisations, about --mpan-target MPANs), plus
tariffs with two versions. Dimensions are Spark SQL over range(); the half-hourly readings (the last --months
months up to yesterday, about 19 M rows for 1,000 MPANs) are generated with dbldatagen straight into
<catalog>.<data_schema>.silver_hh_readings.

Skipped when silver already holds rows, unless the job parameter rebuild=true: replacing silver resets its Change Data Feed,
so the TRIGGERED synced table must then be re-created (docs/RUNBOOK.md).
"""

import argparse
import sys
import time
from datetime import date, datetime, timedelta, timezone


def _flag(v: str) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--data-schema", required=True)
    ap.add_argument("--mpan-target", type=int, default=1000)
    ap.add_argument("--months", type=int, default=13)
    ap.add_argument("--rebuild", default="false")
    ap.add_argument("--run-id", default="manual")
    ap.add_argument("--bundle-root", required=True)
    args = ap.parse_args()
    sys.path.insert(0, f"{args.bundle_root}/src")

    import dbldatagen as dg
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from hhportal_data import silver as sv
    from hhportal_data import synthetic as syn
    from hhportal_data.sqlnames import fqn

    spark = SparkSession.builder.getOrCreate()
    cat, sch = args.catalog, args.data_schema
    silver = fqn(cat, sch, sv.SILVER_TABLE)
    spark.sql(sv.refresh_log_ddl(cat, sch))
    t0 = time.time()

    def log(status: str, rows, detail: str) -> None:
        data = [(args.run_id, "generate", datetime.fromtimestamp(t0, tz=timezone.utc), datetime.now(timezone.utc),
                 rows, status, detail[:1000])]
        schema = "run_id STRING, step STRING, started TIMESTAMP, ended TIMESTAMP, rows BIGINT, status STRING, detail STRING"
        spark.createDataFrame(data, schema).write.mode("append").saveAsTable(fqn(cat, sch, sv.REFRESH_LOG_TABLE))
        print(f"generate: {status} rows={rows} {round(time.time() - t0, 1)} s {detail[:300]}")

    if not _flag(args.rebuild) and spark.catalog.tableExists(f"{cat}.{sch}.{sv.SILVER_TABLE}"):
        n = spark.sql(f"SELECT count(*) FROM {silver}").collect()[0][0]
        if n > 0:
            log("skipped", n, "silver already holds data (run with job parameter rebuild=true to regenerate everything)")
            return

    end = date.today() - timedelta(days=1)
    start, switch, days = syn.date_range(end, args.months)
    sizes = syn.Sizes.for_target(args.mpan_target, days)
    if sizes.issues():
        raise SystemExit("; ".join(sizes.issues()))
    for table, select in syn.dimension_selects(sizes, start, switch).items():
        spark.sql(sv.dimension_ddl(cat, sch, table, select))
        print(f"wrote {table}")

    # Unit rate per MPAN for each tariff version (one row per MPAN, broadcast).
    mpans = spark.sql(f"SELECT {syn.mpan('id')} AS mpan, id AS mpan_idx FROM range({sizes.mpans})")
    m = spark.table(fqn(cat, sch, "mpans")).select("mpan", "tariff_id")
    t = spark.table(fqn(cat, sch, "tariffs"))
    rates = (mpans.join(m, "mpan")
             .join(t.where("valid_to IS NOT NULL").selectExpr("tariff_id", "unit_rate_gbp_per_kwh AS rate_v0"), "tariff_id")
             .join(t.where("valid_to IS NULL").selectExpr("tariff_id", "unit_rate_gbp_per_kwh AS rate_v1"), "tariff_id")
             .select("mpan_idx", "mpan", "rate_v0", "rate_v1"))

    slots = sizes.days * 48
    rows = sizes.readings
    spec = (
        dg.DataGenerator(spark, name="hh_readings", rows=rows, partitions=max(8, rows // 2_000_000), randomSeed=42)
        .withColumn("mpan_idx", "long", expr=f"floor(id / {slots})")
        .withColumn("slot", "long", expr=f"id % {slots}", omit=True)
        .withColumn("noise", "double", minValue=0.85, maxValue=1.15, step=0.001, random=True, omit=True)
        .withColumn("est_u", "int", minValue=0, maxValue=999, random=True, omit=True)
        .withColumn("reading_date", "date", baseColumn="slot",
                    expr=f"date_add(DATE'{start}', cast(floor(slot / 48) AS INT))")
        .withColumn("period", "int", baseColumn="slot", expr="cast(slot % 48 AS INT) + 1")
        .withColumn("reading_ts", "timestamp", baseColumn=["reading_date", "period"],
                    expr="timestampadd(MINUTE, (period - 1) * 30, cast(reading_date AS TIMESTAMP))")
        .withColumn("consumption_kwh", "double", baseColumn=["mpan_idx", "reading_date", "period", "noise"],
                    expr=syn.consumption_expr())
        .withColumn("estimated", "boolean", baseColumn="est_u", expr="est_u < 25")
    )
    readings = (spec.build().join(F.broadcast(rates), "mpan_idx").selectExpr(
        "mpan", "reading_date", "reading_ts", "period", "consumption_kwh",
        f"round(consumption_kwh * CASE WHEN reading_date < DATE'{switch}' THEN rate_v0 ELSE rate_v1 END, 4) AS cost_gbp",
        "estimated"))
    readings.createOrReplaceTempView("hh_portal_demo_readings")
    spark.sql(sv.create_silver_sql(cat, sch, args.months))
    spark.sql(f"INSERT INTO {silver} SELECT mpan, reading_date, reading_ts, period, consumption_kwh, cost_gbp, "
              "estimated FROM hh_portal_demo_readings")

    n = spark.sql(f"SELECT count(*) FROM {silver}").collect()[0][0]
    got = spark.sql(f"SELECT count(DISTINCT mpan) FROM {silver}").collect()[0][0]
    dups = spark.sql(f"SELECT count(*) FROM (SELECT mpan, reading_ts FROM {silver} GROUP BY 1, 2 "
                     "HAVING count(*) > 1)").collect()[0][0]
    ok = n == rows and got == sizes.mpans and dups == 0
    log("ok" if ok else "failed", n,
        f"{start} to {end}: {sizes.orgs} organisations ({sum(1 for i in range(sizes.orgs) if i % 5 == 1)} TPI), "
        f"{sizes.accounts} accounts, {sizes.sites} sites, {got} MPANs, duplicate keys {dups}, Change Data Feed on")
    if not ok:
        raise SystemExit(f"silver_hh_readings: {n}/{rows} rows, {got}/{sizes.mpans} MPANs, {dups} duplicate keys")


if __name__ == "__main__":
    main()
