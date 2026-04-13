# Databricks notebook source

# MAGIC %md
# MAGIC # Pipeline: Bronze (Non-PII tables with data quality expectations)
# MAGIC
# MAGIC **Responsibility**: Read from `raw_*` temp tables (produced by `ingest_and_decrypt.py`), select
# MAGIC only the non-PII columns defined in `pipeline_config.properties["bronze_columns"]`, apply data quality
# MAGIC expectations, and persist as Bronze streaming tables.
# MAGIC
# MAGIC **Reuse**: Include in every pipeline alongside `ingest_and_decrypt.py`.
# MAGIC This notebook has no awareness of PII — it only handles what `bronze_columns` specifies.
# MAGIC
# MAGIC **Outputs**: `bronze_{source_name}` per active source — persisted, no PII.

# COMMAND ----------

import dlt
import json
from pyspark.sql.functions import col

config_table = spark.conf.get("config_table")

config_rows = (
    spark.table(config_table)
    .filter(col("source_name") != "_settings")
    .filter(col("is_active") == True)
    .collect()
)

print(f"[bronze] Bronze tables to create: {[r.source_name for r in config_rows]}")

# COMMAND ----------

for cfg in config_rows:
    source_name  = cfg.source_name
    props        = dict(cfg.properties)
    bronze_table = props["bronze_table"]
    bronze_cols  = json.loads(props["bronze_columns"])

    @dlt.table(
        name=bronze_table,
        comment=f"Bronze {source_name} — non-PII columns only, DQ enforced, file-level audit via _metadata.",
    )
    @dlt.expect_or_drop("valid_record", f"{bronze_cols[0]} IS NOT NULL")
    def create_bronze(src=source_name, cols=bronze_cols):
        select_cols = cols + ["source_file", "source_modified", "_ingested_at"]
        return dlt.read_stream(f"raw_{src}").select(*select_cols)
