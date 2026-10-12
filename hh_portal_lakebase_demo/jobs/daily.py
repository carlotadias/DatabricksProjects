"""Daily job tasks (SYNTHETIC data only), in this order:

--step append_day    append the next simulated day for every MPAN to UC silver and delete the days older than
                     --months months (both reach Lakebase through the Change Data Feed)
--step sync_silver   start the TRIGGERED synced table's pipeline update and wait for it
--step refresh_gold  REFRESH MATERIALIZED VIEW [CONCURRENTLY] each gold view in Lakebase, then ANALYZE

Every step appends row counts and timings (never data rows) to <catalog>.<data_schema>.portal_refresh_log.
"""

import argparse
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--step", required=True, choices=["append_day", "sync_silver", "refresh_gold"])
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--data-schema", required=True)
    ap.add_argument("--synced-table", default="", help="UC name of the silver synced table, cat.portal.silver_hh_readings")
    ap.add_argument("--endpoint", default="", help="Lakebase endpoint path projects/<id>/branches/<b>/endpoints/<e>")
    ap.add_argument("--database", default="databricks_postgres")
    ap.add_argument("--months", type=int, default=13)
    ap.add_argument("--max-days", type=int, default=1)
    ap.add_argument("--run-id", default="manual")
    ap.add_argument("--bundle-root", required=True)
    args = ap.parse_args()
    sys.path.insert(0, f"{args.bundle_root}/src")

    from pyspark.sql import SparkSession

    from hhportal_data import silver as sv
    from hhportal_data.sqlnames import fqn

    spark = SparkSession.builder.getOrCreate()
    cat, sch = args.catalog, args.data_schema
    spark.sql(sv.refresh_log_ddl(cat, sch))
    silver = fqn(cat, sch, sv.SILVER_TABLE)

    def log(rows: list[dict]) -> None:
        def ts(x):
            return datetime.fromtimestamp(x, tz=timezone.utc) if x is not None else None
        data = [(args.run_id, r["step"], ts(r["started"]), ts(r["ended"]), r.get("rows"), r["status"],
                 str(r.get("detail") or "")[:1000]) for r in rows]
        schema = "run_id STRING, step STRING, started TIMESTAMP, ended TIMESTAMP, rows BIGINT, status STRING, detail STRING"
        spark.createDataFrame(data, schema).write.mode("append").saveAsTable(fqn(cat, sch, sv.REFRESH_LOG_TABLE))
        for r in rows:
            print(f"{r['step']}: {r['status']} rows={r.get('rows')} {round(r['ended'] - r['started'], 1)} s "
                  f"{str(r.get('detail') or '')[:200]}")

    def scalar(sql: str):
        return spark.sql(sql).collect()[0][0]

    if args.step == "append_day":
        rows = []
        max_date = scalar(sv.max_date_sql(cat, sch))
        if max_date is None:
            raise SystemExit("silver_hh_readings is empty: run the setup job first")
        n_mpans = scalar(f"SELECT count(*) FROM {fqn(cat, sch, 'mpans')}")
        days = sv.dates_to_append(max_date, date.today(), max_days=args.max_days)
        if not days:
            rows.append({"step": "append_day", "started": time.time(), "ended": time.time(), "rows": 0,
                         "status": "skipped", "detail": f"max reading_date {max_date} is already yesterday or later"})
        for d in days:
            t0 = time.time()
            if scalar(sv.day_count_sql(cat, sch, d)) > 0:
                rows.append({"step": "append_day", "started": t0, "ended": time.time(), "rows": 0, "status": "skipped",
                             "detail": f"{d} already present"})
                continue
            spark.sql(sv.append_day_sql(cat, sch, d, n_mpans))
            n = scalar(sv.day_count_sql(cat, sch, d))
            rows.append({"step": "append_day", "started": t0, "ended": time.time(), "rows": n, "status": "ok",
                         "detail": str(d)})
            max_date = d
        t0 = time.time()
        first_kept = sv.window_start(max_date, args.months)
        old = scalar(f"SELECT count(*) FROM {silver} WHERE reading_date < DATE'{first_kept.isoformat()}'")
        if old:
            spark.sql(sv.delete_old_sql(cat, sch, first_kept))
        rows.append({"step": "delete_old", "started": t0, "ended": time.time(), "rows": old, "status": "ok",
                     "detail": f"kept reading_date >= {first_kept}"})
        log(rows)
        return

    from databricks.sdk import WorkspaceClient

    from hhportal_data import gold, ops

    w = WorkspaceClient()
    if args.step == "sync_silver":
        if not args.synced_table:
            raise SystemExit("--synced-table is required")
        t0 = time.time()
        try:
            out = ops.sync_silver(lambda p: w.api_client.do("GET", p), w.pipelines, args.synced_table)
        except Exception as e:
            log([{"step": "sync_silver", "started": t0, "ended": time.time(), "rows": None, "status": "failed",
                  "detail": ops.sanitize(str(e), 1000)}])
            raise
        log([{"step": "sync_silver", "started": t0, "ended": time.time(), "rows": None, "status": "ok",
              "detail": f"pipeline {out['pipeline_id']} update {out['update_id']} {out['state']}, "
                        f"synced table {out['synced_state']}"}])
        return

    if args.step == "refresh_gold":
        if not args.endpoint:
            raise SystemExit("--endpoint is required")
        statements = gold.load(Path(args.bundle_root) / "sql" / "lakebase_gold.sql")
        conn = ops.lakebase_connect(w, args.endpoint, args.database)
        try:
            rows = ops.refresh_gold(conn, statements)
        except ops.StepFailed as e:
            log(e.rows)
            raise
        finally:
            conn.close()
        log(rows)


if __name__ == "__main__":
    main()
