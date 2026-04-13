# Databricks notebook source

# MAGIC %md
# MAGIC # Inspect Pipeline Health
# MAGIC
# MAGIC **Run this notebook after the pipeline to see what happened.**
# MAGIC
# MAGIC It shows:
# MAGIC 1. **Pipeline event log** — which tables succeeded/failed, how many rows, expectations
# MAGIC 2. **Rescued data** — rows that didn't match the expected schema (captured, not lost)
# MAGIC 3. **Auto Loader checkpoint status** — which files were processed per source
# MAGIC
# MAGIC This is the native Databricks replacement for the Azure SQL MetaDB audit tables.

# COMMAND ----------

dbutils.widgets.text("catalog",       "carlota_tko_day3_catalog")
dbutils.widgets.text("bronze_schema", "sse_bronze")
dbutils.widgets.text("config_schema", "sse_demo")

catalog       = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
config_schema = dbutils.widgets.get("config_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline Event Log — Data Quality
# MAGIC
# MAGIC Every pipeline run logs expectations (pass/fail counts) automatically.
# MAGIC This replaces custom audit logging in the MetaDB.

# COMMAND ----------

bronze_table_ref = f"{catalog}.{bronze_schema}.bronze_contact_records"

try:
    display(
        spark.sql(f"""
            SELECT
                timestamp,
                origin.flow_name AS table_name,
                details:flow_progress.data_quality.expectations AS expectations
            FROM event_log(TABLE({bronze_table_ref}))
            WHERE event_type = 'flow_progress'
              AND details:flow_progress.data_quality IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 20
        """)
    )
except Exception as e:
    print(f"Event log not available: {e}")
    print("Make sure the pipeline has run at least once.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Row Counts Per Table
# MAGIC
# MAGIC Quick sanity check: how many rows ended up in each bronze table,
# MAGIC and which source files contributed.

# COMMAND ----------

tables_to_check = [
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {catalog}.{bronze_schema}").collect()
    if row.tableName.startswith("bronze_")
]

for table_name in tables_to_check:
    full_name = f"{catalog}.{bronze_schema}.{table_name}"
    try:
        count = spark.table(full_name).count()
        print(f"  {table_name}: {count} rows")
    except Exception as e:
        print(f"  {table_name}: could not check — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality — Dropped Rows (Event Log)
# MAGIC
# MAGIC The `expect_or_drop` expectations in bronze tables filter out rows where
# MAGIC the primary key is NULL (this happens when JSON parsing fails — invalid JSON,
# MAGIC wrong schema). These rows are **dropped before reaching the table**.
# MAGIC
# MAGIC The event log is the **only** place to see them. This replaces the MetaDB audit.

# COMMAND ----------

try:
    dq_df = spark.sql(f"""
        SELECT
            timestamp,
            origin.flow_name AS table_name,
            details:flow_progress.metrics.num_output_rows AS output_rows,
            details:flow_progress.data_quality.dropped_records AS dropped_rows,
            details:flow_progress.data_quality.expectations AS expectations
        FROM event_log(TABLE({bronze_table_ref}))
        WHERE event_type = 'flow_progress'
          AND details:flow_progress.data_quality IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 20
    """)
    display(dq_df)
    dropped = dq_df.filter("CAST(dropped_rows AS INT) > 0")
    if dropped.count() > 0:
        print("⚠ Some rows were DROPPED by data quality expectations (invalid JSON or wrong schema).")
        print("  These records never reached the bronze table — inspect the source files to fix them.")
    else:
        print("✓ No rows were dropped — all records passed data quality checks.")
except Exception as e:
    print(f"Event log not available: {e}")
    print("Make sure the pipeline has run at least once.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. File-Level Audit — Which Files Were Processed
# MAGIC
# MAGIC Every bronze row carries `source_file` and `source_modified` from Auto Loader's
# MAGIC `_metadata`. Query this to see exactly which files contributed to each table.

# COMMAND ----------

for table_name in tables_to_check:
    full_name = f"{catalog}.{bronze_schema}.{table_name}"
    try:
        cols = [c.name for c in spark.table(full_name).schema]
        if "source_file" not in cols:
            print(f"\n--- {table_name} --- (derived table, no source_file column)")
            continue
        print(f"\n--- {table_name} ---")
        display(
            spark.sql(f"""
                SELECT
                    source_file,
                    source_modified,
                    COUNT(*) AS row_count
                FROM {full_name}
                GROUP BY source_file, source_modified
                ORDER BY source_modified DESC
            """)
        )
    except Exception as e:
        print(f"  Could not query: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Restart After Failure
# MAGIC
# MAGIC Auto Loader checkpoints track exactly which files have been processed.
# MAGIC If a file caused an error:
# MAGIC
# MAGIC 1. **Fix the file** in staging (or remove it)
# MAGIC 2. **Re-run the pipeline** — Auto Loader picks up only unprocessed files
# MAGIC 3. Successfully processed files from the same batch are **not re-ingested**
# MAGIC
# MAGIC No manual checkpoint manipulation needed. No MetaDB updates.

# COMMAND ----------

config_table = f"{catalog}.{config_schema}.pipeline_config"

try:
    from pyspark.sql.functions import col
    sources = (
        spark.table(config_table)
        .filter(col("source_name") != "_settings")
        .filter(col("is_active") == True)
        .collect()
    )
    for src in sources:
        props = dict(src.properties)
        raw_path = props["raw_path"]
        ckpt = f"{raw_path}/_checkpoint"
        source_name = src.source_name
        try:
            ckpt_files = dbutils.fs.ls(ckpt)
            print(f"✓ {source_name}: checkpoint exists at {ckpt} ({len(ckpt_files)} entries)")
        except Exception:
            print(f"✗ {source_name}: no checkpoint yet at {ckpt}")
except Exception as e:
    print(f"Could not read config: {e}")
